//  Copyright 2018 Google Inc. All Rights Reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package boltdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"sort"

	"strings"

	"github.com/boltdb/bolt"
	"github.com/gogo/protobuf/proto"
	"github.com/google/trillian"
	"github.com/google/trillian/crypto/sigpb"
	spb "github.com/google/trillian/crypto/sigpb"
	"github.com/google/trillian/storage"
	storageto "github.com/google/trillian/storage/testonly"
	"github.com/kylelemons/godebug/pretty"
)

const leavesToInsert = 5

// Time we will queue all leaves at
var fakeQueueTime = time.Date(2016, 11, 10, 15, 16, 27, 0, time.UTC)

// Time we'll request for guard cutoff in tests that don't test this (should include all above)
var fakeDequeueCutoffTime = time.Date(2016, 11, 10, 15, 16, 30, 0, time.UTC)

// Must be 32 bytes to match sha256 length if it was a real hash
var dummyHash = []byte("hashxxxxhashxxxxhashxxxxhashxxxx")

func TestSnapshot(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()

	frozenLogID := createLogForTests(db)
	if _, err := updateTree(db, frozenLogID, func(tree *trillian.Tree) {
		tree.TreeState = trillian.TreeState_FROZEN
	}); err != nil {
		t.Fatalf("Error updating frozen tree: %v", err)
	}

	activeLogID := createLogForTests(db)
	mapID := createMapForTests(db)

	tests := []struct {
		desc    string
		logID   int64
		wantErr bool
	}{
		{
			desc:    "unknownSnapshot",
			logID:   -1,
			wantErr: true,
		},
		{
			desc:  "activeLogSnapshot",
			logID: activeLogID,
		},
		{
			desc:  "frozenSnapshot",
			logID: frozenLogID,
		},
		{
			desc:    "mapSnapshot",
			logID:   mapID,
			wantErr: true,
		},
	}

	ctx := context.Background()
	s := NewLogStorage(db, nil)
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			tree := logTree(test.logID)
			tx, err := s.SnapshotForTree(ctx, tree)

			if err == storage.ErrTreeNeedsInit {
				defer tx.Close()
			}

			if hasErr := err != nil; hasErr != test.wantErr {
				t.Fatalf("err = %q, wantErr = %v", err, test.wantErr)
			} else if hasErr {
				return
			}
			defer tx.Close()

			_, err = tx.LatestSignedLogRoot(ctx)
			if err != nil {
				t.Errorf("LatestSignedLogRoot() returned err = %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Errorf("Commit() returned err = %v", err)
			}
		})
	}
}

func TestReadWriteTransaction(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	activeLogID := createLogForTests(db)

	tests := []struct {
		desc      string
		logID     int64
		wantErr   bool
		wantRev   int64
		wantTXRev int64
	}{
		{
			// Unknown logs IDs are now handled outside storage.
			desc:      "unknownBegin",
			logID:     -1,
			wantRev:   0,
			wantTXRev: -1,
		},
		{
			desc:      "activeLogBegin",
			logID:     activeLogID,
			wantRev:   0,
			wantTXRev: 1,
		},
	}

	ctx := context.Background()
	s := NewLogStorage(db, nil)
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			tree := logTree(test.logID)
			err := s.ReadWriteTransaction(ctx, tree, func(ctx context.Context, tx storage.LogTreeTX) error {
				root, err := tx.LatestSignedLogRoot(ctx)
				if err != nil {
					t.Errorf("%v: LatestSignedLogRoot() returned err = %v", test.desc, err)
				}
				if got, want := tx.WriteRevision(), test.wantTXRev; got != want {
					t.Errorf("%v: WriteRevision() = %v, want = %v", test.desc, got, want)
				}
				if got, want := root.TreeRevision, test.wantRev; got != want {
					t.Errorf("%v: TreeRevision() = %v, want = %v", test.desc, got, want)
				}
				return nil
			})
			if hasErr := err != nil; hasErr != test.wantErr {
				t.Fatalf("%v: err = %q, wantErr = %v", test.desc, err, test.wantErr)
			} else if hasErr {
				return
			}
		})
	}
}

func TestGetActiveLogIDs(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t)
	defer db.Close()
	admin := NewAdminStorage(db)

	// Create a few test trees
	log1 := proto.Clone(storageto.LogTree).(*trillian.Tree)
	log2 := proto.Clone(storageto.LogTree).(*trillian.Tree)
	log3 := proto.Clone(storageto.PreorderedLogTree).(*trillian.Tree)
	drainingLog := proto.Clone(storageto.LogTree).(*trillian.Tree)
	frozenLog := proto.Clone(storageto.LogTree).(*trillian.Tree)
	deletedLog := proto.Clone(storageto.LogTree).(*trillian.Tree)
	map1 := proto.Clone(storageto.MapTree).(*trillian.Tree)
	map2 := proto.Clone(storageto.MapTree).(*trillian.Tree)
	deletedMap := proto.Clone(storageto.MapTree).(*trillian.Tree)
	for _, tree := range []*trillian.Tree{log1, log2, log3, drainingLog, frozenLog, deletedLog, map1, map2, deletedMap} {
		newTree, err := storage.CreateTree(ctx, admin, tree)
		if err != nil {
			t.Fatalf("CreateTree(%+v) returned err = %v", tree, err)
		}
		*tree = *newTree
	}

	// FROZEN is not a valid initial state, so we have to update it separately.
	if _, err := storage.UpdateTree(ctx, admin, frozenLog.TreeId, func(t *trillian.Tree) {
		t.TreeState = trillian.TreeState_FROZEN
	}); err != nil {
		t.Fatalf("UpdateTree() returned err = %v", err)
	}
	// DRAINING is not a valid initial state, so we have to update it separately.
	if _, err := storage.UpdateTree(ctx, admin, drainingLog.TreeId, func(t *trillian.Tree) {
		t.TreeState = trillian.TreeState_DRAINING
	}); err != nil {
		t.Fatalf("UpdateTree() returned err = %v", err)
	}

	// Update deleted trees accordingly
	if _, err := storage.SoftDeleteTree(ctx, admin, deletedLog.TreeId); err != nil {
		t.Fatalf("UpdateTree() returned err = %v", err)
	}
	// Update deleted trees accordingly
	if _, err := storage.SoftDeleteTree(ctx, admin, deletedMap.TreeId); err != nil {
		t.Fatalf("UpdateTree() returned err = %v", err)
	}

	s := NewLogStorage(db, nil)
	tx, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot() returns err = %v", err)
	}
	defer tx.Close()
	got, err := tx.GetActiveLogIDs(ctx)
	if err != nil {
		t.Fatalf("GetActiveLogIDs() returns err = %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Errorf("Commit() returned err = %v", err)
	}

	want := []int64{log1.TreeId, log2.TreeId, log3.TreeId, drainingLog.TreeId}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	if diff := pretty.Compare(got, want); diff != "" {
		t.Errorf("post-GetActiveLogIDs diff (-got +want):\n%v", diff)
	}
}

func TestGetActiveLogIDsEmpty(t *testing.T) {
	ctx := context.Background()

	db := createTestDB(t)
	defer db.Close()
	s := NewLogStorage(db, nil)

	tx, err := s.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() = (_, %v), want = (_, nil)", err)
	}
	defer tx.Close()
	ids, err := tx.GetActiveLogIDs(ctx)
	if err != nil {
		t.Fatalf("GetActiveLogIDs() = (_, %v), want = (_, nil)", err)
	}
	if err := tx.Commit(); err != nil {
		t.Errorf("Commit() = %v, want = nil", err)
	}

	if got, want := len(ids), 0; got != want {
		t.Errorf("GetActiveLogIDs(): got %v IDs, want = %v", got, want)
	}
}

func TestQueueDuplicateLeaf(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	logID := createLogForTests(db)
	s := NewLogStorage(db, nil)
	tree := logTree(logID)
	count := 15
	leaves := createTestLeaves(int64(count), 10)
	leaves2 := createTestLeaves(int64(count), 12)
	leaves3 := createTestLeaves(3, 100)

	// Note that tests accumulate queued leaves on top of each other.
	var tests = []struct {
		desc   string
		leaves []*trillian.LogLeaf
		want   []*trillian.LogLeaf
	}{
		{
			desc:   "[10, 11, 12, ...]",
			leaves: leaves,
			want:   make([]*trillian.LogLeaf, count),
		},
		{
			desc:   "[12, 13, 14, ...] so first (count-2) are duplicates",
			leaves: leaves2,
			want:   append(leaves[2:], nil, nil),
		},
		{
			desc:   "[10, 100, 11, 101, 102] so [dup, new, dup, new, dup]",
			leaves: []*trillian.LogLeaf{leaves[0], leaves3[0], leaves[1], leaves3[1], leaves[2]},
			want:   []*trillian.LogLeaf{leaves[0], nil, leaves[1], nil, leaves[2]},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
				existing, err := tx.QueueLeaves(ctx, test.leaves, fakeQueueTime)
				if err != nil {
					t.Errorf("Failed to queue leaves: %v", err)
					return err
				}

				if len(existing) != len(test.want) {
					t.Fatalf("|QueueLeaves()|=%d; want %d", len(existing), len(test.want))
				}
				for i, want := range test.want {
					got := existing[i]
					if want == nil {
						if got != nil {
							t.Fatalf("QueueLeaves()[%d]=%v; want nil", i, got)
						}
						return nil
					}
					if got == nil {
						t.Fatalf("QueueLeaves()[%d]=nil; want non-nil", i)
					} else if !bytes.Equal(got.LeafIdentityHash, want.LeafIdentityHash) {
						t.Fatalf("QueueLeaves()[%d].LeafIdentityHash=%x; want %x", i, got.LeafIdentityHash, want.LeafIdentityHash)
					}
				}
				return nil
			})
		})
	}
}

func TestQueueLeaves(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	logID := createLogForTests(db)
	s := NewLogStorage(db, nil)

	leaves := createTestLeaves(leavesToInsert, 20)
	tree := logTree(logID)
	runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
		if _, err := tx.QueueLeaves(ctx, leaves, fakeQueueTime); err != nil {
			t.Fatalf("Failed to queue leaves: %v", err)
		}
		return nil
	})

	// The tx above committed so we should be able to go in and look at the Unsequenced
	// bucket for the tree directly.
	err := db.View(func(tx *bolt.Tx) error {
		qb := tx.Bucket(logKeyOf(logID)).Bucket([]byte(QueueBucket))
		lb := tx.Bucket(logKeyOf(logID)).Bucket([]byte(LeafBucket))

		if qb.Stats().KeyN != leavesToInsert {
			t.Errorf("B.Stats(qb) got: %d leaves, want: %d", qb.Stats().KeyN, leavesToInsert)
		}
		if lb.Stats().KeyN != leavesToInsert {
			t.Errorf("B.Stats(lb) got: %d leaves, want: %d", qb.Stats().KeyN, leavesToInsert)
		}

		i, c := 0, qb.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			ik, err := int64FromKey(k)
			if err != nil {
				t.Fatalf("Invalid key format found reading back queue: %v", err)
			}
			if ik != int64(i)+1 {
				t.Errorf("Got key: %d, want: %d", ik, i+1)
			}
			if !bytes.Equal(v, leaves[i].LeafIdentityHash) {
				t.Errorf("Mismatched leaf hash value got: %v, want: %v", hex.EncodeToString(v), hex.EncodeToString(leaves[i].LeafIdentityHash))
				continue
			}
			// The v we have is the leaf identity hash that we can use to find the leaf
			blob := lb.Get(v)
			if blob == nil {
				t.Errorf("No matching leaf or error looking up LeafIdentityHash: %v", err)
				continue
			}

			var rl trillian.LogLeaf
			if err := proto.Unmarshal(blob, &rl); err != nil {
				t.Errorf("Failed to unmarshal queued leaf: %v", err)
				continue
			}
			if !proto.Equal(leaves[i], &rl) {
				t.Errorf("Mismatched leaf got: %v, want: %v", rl, leaves[i])
			}
			i++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Error accessing db to check queue: %v", err)
	}
}

func TestDequeueLeavesNoneQueued(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	logID := createLogForTests(db)
	s := NewLogStorage(db, nil)
	tree := logTree(logID)

	runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
		leaves, err := tx.DequeueLeaves(ctx, 999, fakeDequeueCutoffTime)
		if err != nil {
			t.Fatalf("Didn't expect an error on dequeue with no work to be done: %v", err)
		}
		if len(leaves) > 0 {
			t.Fatalf("Expected nothing to be dequeued but we got %d leaves", len(leaves))
		}
		return nil
	})
}

func TestDequeueLeaves(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	logID := createLogForTests(db)
	s := NewLogStorage(db, nil)
	tree := logTree(logID)

	{
		runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
			leaves := createTestLeaves(leavesToInsert, 20)
			if _, err := tx.QueueLeaves(ctx, leaves, fakeDequeueCutoffTime); err != nil {
				t.Fatalf("Failed to queue leaves: %v", err)
			}
			return nil
		})
	}

	{
		// Now try to dequeue them
		runLogTX(s, tree, t, func(ctx context.Context, tx2 storage.LogTreeTX) error {
			leaves2, err := tx2.DequeueLeaves(ctx, 99, fakeDequeueCutoffTime)
			if err != nil {
				t.Fatalf("Failed to dequeue leaves: %v", err)
			}
			if len(leaves2) != leavesToInsert {
				t.Fatalf("Dequeued %d leaves but expected to get %d", len(leaves2), leavesToInsert)
			}
			ensureAllLeavesDistinct(leaves2, t)
			return nil
		})
	}

	{
		// If we dequeue again then we should now get nothing
		runLogTX(s, tree, t, func(ctx context.Context, tx3 storage.LogTreeTX) error {
			leaves3, err := tx3.DequeueLeaves(ctx, 99, fakeDequeueCutoffTime)
			if err != nil {
				t.Fatalf("Failed to dequeue leaves (second time): %v", err)
			}
			if len(leaves3) != 0 {
				t.Fatalf("Dequeued %d leaves but expected to get none", len(leaves3))
			}
			return nil
		})
	}
}

func TestDequeueLeavesHaveQueueTimestamp(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	logID := createLogForTests(db)
	s := NewLogStorage(db, nil)
	tree := logTree(logID)

	{
		runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
			leaves := createTestLeaves(leavesToInsert, 20)
			if _, err := tx.QueueLeaves(ctx, leaves, fakeDequeueCutoffTime); err != nil {
				t.Fatalf("Failed to queue leaves: %v", err)
			}
			return nil
		})
	}

	{
		// Now try to dequeue them
		runLogTX(s, tree, t, func(ctx context.Context, tx2 storage.LogTreeTX) error {
			leaves2, err := tx2.DequeueLeaves(ctx, 99, fakeDequeueCutoffTime)
			if err != nil {
				t.Fatalf("Failed to dequeue leaves: %v", err)
			}
			if len(leaves2) != leavesToInsert {
				t.Fatalf("Dequeued %d leaves but expected to get %d", len(leaves2), leavesToInsert)
			}
			return nil
		})
	}
}

func TestDequeueLeavesTwoBatches(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	logID := createLogForTests(db)
	s := NewLogStorage(db, nil)
	tree := logTree(logID)

	leavesToDequeue1 := 3
	leavesToDequeue2 := 2

	{
		runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
			leaves := createTestLeaves(leavesToInsert, 20)
			if _, err := tx.QueueLeaves(ctx, leaves, fakeDequeueCutoffTime); err != nil {
				t.Fatalf("Failed to queue leaves: %v", err)
			}
			return nil
		})
	}

	var err error
	var leaves2, leaves3, leaves4 []*trillian.LogLeaf
	{
		// Now try to dequeue some of them
		runLogTX(s, tree, t, func(ctx context.Context, tx2 storage.LogTreeTX) error {
			leaves2, err = tx2.DequeueLeaves(ctx, leavesToDequeue1, fakeDequeueCutoffTime)
			if err != nil {
				t.Fatalf("Failed to dequeue leaves: %v", err)
			}
			if len(leaves2) != leavesToDequeue1 {
				t.Fatalf("Dequeued %d leaves but expected to get %d", len(leaves2), leavesToInsert)
			}
			ensureAllLeavesDistinct(leaves2, t)
			return nil
		})

		// Now try to dequeue the rest of them
		runLogTX(s, tree, t, func(ctx context.Context, tx3 storage.LogTreeTX) error {
			leaves3, err = tx3.DequeueLeaves(ctx, leavesToDequeue2, fakeDequeueCutoffTime)
			if err != nil {
				t.Fatalf("Failed to dequeue leaves: %v", err)
			}
			if len(leaves3) != leavesToDequeue2 {
				t.Fatalf("Dequeued %d leaves but expected to get %d", len(leaves3), leavesToDequeue2)
			}
			ensureAllLeavesDistinct(leaves3, t)

			// Plus the union of the leaf batches should all have distinct hashes
			leaves4 = append(leaves2, leaves3...)
			ensureAllLeavesDistinct(leaves4, t)
			return nil
		})
	}

	{
		// If we dequeue again then we should now get nothing
		runLogTX(s, tree, t, func(ctx context.Context, tx4 storage.LogTreeTX) error {
			leaves5, err := tx4.DequeueLeaves(ctx, 99, fakeDequeueCutoffTime)
			if err != nil {
				t.Fatalf("Failed to dequeue leaves (second time): %v", err)
			}
			if len(leaves5) != 0 {
				t.Fatalf("Dequeued %d leaves but expected to get none", len(leaves5))
			}
			return nil
		})
	}
}

func TestDequeueLeavesOrdering(t *testing.T) {
	// Queue two small batches of leaves at different timestamps. Do two separate dequeue
	// transactions and make sure the returned leaves are respecting the time ordering of the
	// queue. Note this implementation maintains a queue by insertion order not by time and
	// the guard interval is not supported. This is a legacy concept from the C++ implementation.
	db := createTestDB(t)
	defer db.Close()
	logID := createLogForTests(db)
	s := NewLogStorage(db, nil)
	tree := logTree(logID)

	batchSize := 2
	leaves := createTestLeaves(int64(batchSize), 0)
	leaves2 := createTestLeaves(int64(batchSize), int64(batchSize))

	{
		runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
			if _, err := tx.QueueLeaves(ctx, leaves, fakeQueueTime); err != nil {
				t.Fatalf("QueueLeaves(1st batch) = %v", err)
			}
			// Queue a second batch, which should be behind the ones above in the queue.
			if _, err := tx.QueueLeaves(ctx, leaves2, fakeQueueTime); err != nil {
				t.Fatalf("QueueLeaves(2nd batch) = %v", err)
			}
			return nil
		})
	}

	{
		// Now try to dequeue two leaves and we should get the first batch
		runLogTX(s, tree, t, func(ctx context.Context, tx2 storage.LogTreeTX) error {
			dequeue1, err := tx2.DequeueLeaves(ctx, batchSize, fakeQueueTime)
			if err != nil {
				t.Fatalf("DequeueLeaves(1st) = %v", err)
			}
			if got, want := len(dequeue1), batchSize; got != want {
				t.Fatalf("Dequeue count mismatch (1st) got: %d, want: %d", got, want)
			}
			ensureAllLeavesDistinct(dequeue1, t)

			// Ensure this is the second batch queued by comparing leaf hashes (must be distinct as
			// the leaf data was).
			if !leafInBatch(dequeue1[0], leaves) || !leafInBatch(dequeue1[1], leaves) {
				t.Fatalf("Got leaf from wrong batch (1st dequeue): %v", dequeue1)
			}
			return nil
		})

		// Try to dequeue again and we should get the batch that was queued second
		runLogTX(s, tree, t, func(ctx context.Context, tx3 storage.LogTreeTX) error {
			dequeue2, err := tx3.DequeueLeaves(ctx, batchSize, fakeQueueTime)
			if err != nil {
				t.Fatalf("DequeueLeaves(2nd) = %v", err)
			}
			if got, want := len(dequeue2), batchSize; got != want {
				t.Fatalf("Dequeue count mismatch (2nd) got: %d, want: %d", got, want)
			}
			ensureAllLeavesDistinct(dequeue2, t)

			// Ensure this is the first batch by comparing leaf hashes.
			if !leafInBatch(dequeue2[0], leaves2) || !leafInBatch(dequeue2[1], leaves2) {
				t.Fatalf("Got leaf from wrong batch (2nd dequeue): %v", dequeue2)
			}
			return nil
		})
	}
}

func TestLatestSignedRootNoneWritten(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	ctx := context.Background()

	tree, err := createTree(db, storageto.LogTree)
	if err != nil {
		t.Fatalf("createTree: %v", err)
	}
	s := NewLogStorage(db, nil)

	tx, err := s.SnapshotForTree(ctx, tree)
	if err != storage.ErrTreeNeedsInit {
		t.Fatalf("SnapshotForTree gave %v, want %v", err, storage.ErrTreeNeedsInit)
	}
	commit(tx, t)
}

func TestLatestSignedLogRoot(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	logID := createLogForTests(db)
	s := NewLogStorage(db, nil)
	tree := logTree(logID)

	root := trillian.SignedLogRoot{
		LogId:          logID,
		TimestampNanos: 98765,
		TreeSize:       16,
		TreeRevision:   5,
		RootHash:       []byte(dummyHash),
		Signature:      &spb.DigitallySigned{Signature: []byte("notempty")},
	}

	runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
		if err := tx.StoreSignedLogRoot(ctx, root); err != nil {
			t.Fatalf("Failed to store signed root: %v", err)
		}
		return nil
	})

	{
		runLogTX(s, tree, t, func(ctx context.Context, tx2 storage.LogTreeTX) error {
			root2, err := tx2.LatestSignedLogRoot(ctx)
			if err != nil {
				t.Fatalf("Failed to read back new log root: %v", err)
			}
			if !proto.Equal(&root, &root2) {
				t.Fatalf("Root round trip failed: <%v> and: <%v>", root, root2)
			}
			return nil
		})
	}
}

func TestDuplicateSignedLogRoot(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	logID := createLogForTests(db)
	s := NewLogStorage(db, nil)
	tree := logTree(logID)

	runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
		root := trillian.SignedLogRoot{
			LogId:          logID,
			TimestampNanos: 98765,
			TreeSize:       16,
			TreeRevision:   5,
			RootHash:       []byte(dummyHash),
			Signature:      &spb.DigitallySigned{Signature: []byte("notempty")},
		}
		if err := tx.StoreSignedLogRoot(ctx, root); err != nil {
			t.Fatalf("Failed to store signed root: %v", err)
		}
		// Shouldn't be able to do it again
		if err := tx.StoreSignedLogRoot(ctx, root); err == nil {
			t.Fatal("Allowed duplicate signed root")
		}
		return nil
	})
}

func TestLogRootUpdate(t *testing.T) {
	// Write two roots for a log and make sure the one with the newest timestamp supersedes
	db := createTestDB(t)
	defer db.Close()
	logID := createLogForTests(db)
	s := NewLogStorage(db, nil)
	tree := logTree(logID)

	root := trillian.SignedLogRoot{
		LogId:          logID,
		TimestampNanos: 98765,
		TreeSize:       16,
		TreeRevision:   5,
		RootHash:       []byte(dummyHash),
		Signature:      &spb.DigitallySigned{Signature: []byte("notempty")},
	}
	root2 := trillian.SignedLogRoot{
		LogId:          logID,
		TimestampNanos: 98766,
		TreeSize:       16,
		TreeRevision:   6,
		RootHash:       []byte(dummyHash),
		Signature:      &spb.DigitallySigned{Signature: []byte("notempty")},
	}
	runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
		if err := tx.StoreSignedLogRoot(ctx, root); err != nil {
			t.Fatalf("Failed to store signed root: %v", err)
		}
		if err := tx.StoreSignedLogRoot(ctx, root2); err != nil {
			t.Fatalf("Failed to store signed root: %v", err)
		}
		return nil
	})

	runLogTX(s, tree, t, func(ctx context.Context, tx2 storage.LogTreeTX) error {
		root3, err := tx2.LatestSignedLogRoot(ctx)
		if err != nil {
			t.Fatalf("Failed to read back new log root: %v", err)
		}
		if !proto.Equal(&root2, &root3) {
			t.Fatalf("Root round trip failed: <%v> and: <%v>", root, root2)
		}
		return nil
	})
}

func TestUpdateSequencedLeaves(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	logID := createLogForTests(db)
	s := NewLogStorage(db, nil)
	tree := logTree(logID)
	leaves := createTestLeaves(leavesToInsert, 20)

	{
		runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
			if _, err := tx.QueueLeaves(ctx, leaves, fakeQueueTime); err != nil {
				t.Fatalf("Failed to queue leaves: %v", err)
			}
			return nil
		})
	}

	// Assign sequence numbers.
	for i, leaf := range leaves {
		leaf.LeafIndex = int64(i) + 20
	}

	{
		runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
			if err := tx.UpdateSequencedLeaves(ctx, leaves); err != nil {
				t.Fatalf("Failed to update leaves: %v", err)
			}
			return nil
		})
	}

	// Find the data structures by going in directly.
	{
		runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
			btx := (tx).(*logTreeTX)

			sb := btx.lb.Bucket([]byte(SequencedBucket))
			mb := btx.lb.Bucket([]byte(MerkleHashBucket))

			for _, leaf := range leaves {
				if got, want := sb.Get(keyOfInt64(leaf.LeafIndex)), leaf.MerkleLeafHash; !bytes.Equal(got, want) {
					return fmt.Errorf("sequence lookup got MLH: %v, want: %v", got, want)
				}
				if got, want := mb.Get(leaf.MerkleLeafHash), leaf.LeafIdentityHash; !bytes.Equal(got, want) {
					return fmt.Errorf("MLH lookup got LIH: %v, want: %v", got, want)
				}
			}
			return nil
		})
	}
}

func TestUpdateSequencedLeavesDuplicateSequence(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	logID := createLogForTests(db)
	s := NewLogStorage(db, nil)
	tree := logTree(logID)
	leaves := createTestLeaves(leavesToInsert, 20)

	{
		runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
			if _, err := tx.QueueLeaves(ctx, leaves, fakeQueueTime); err != nil {
				t.Fatalf("Failed to queue leaves: %v", err)
			}
			return nil
		})
	}

	// Assign sequence numbers - they're all the same so this should be rejected.
	for _, leaf := range leaves {
		leaf.LeafIndex = 23
	}

	{
		runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
			if err := tx.UpdateSequencedLeaves(ctx, leaves); err == nil || !strings.Contains(err.Error(), "sequence number") {
				t.Fatalf("tx.UpdateSequencedLeaves(duplicate): got: err=%v, want: err containing sequence number", err)
			}
			return nil
		})
	}
}

func TestUpdateSequencedLeavesDuplicateMLH(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	logID := createLogForTests(db)
	s := NewLogStorage(db, nil)
	tree := logTree(logID)
	leaves := createTestLeaves(leavesToInsert, 20)

	{
		runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
			if _, err := tx.QueueLeaves(ctx, leaves, fakeQueueTime); err != nil {
				t.Fatalf("Failed to queue leaves: %v", err)
			}
			return nil
		})
	}

	// Assign sequence numbers.
	for i, leaf := range leaves {
		leaf.LeafIndex = int64(i) + 20
	}
	// Cause a duplicate hash to be presented.
	leaves[1].MerkleLeafHash = leaves[0].MerkleLeafHash

	{
		runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
			if err := tx.UpdateSequencedLeaves(ctx, leaves); err == nil || !strings.Contains(err.Error(), "MLH") {
				t.Fatalf("tx.UpdateSequencedLeaves(duplicate): got: err=%v, want: err containing MLH", err)
			}
			return nil
		})
	}
}

func TestGetLeavesByHashNotPresent(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	logID := createLogForTests(db)
	s := NewLogStorage(db, nil)
	tree := logTree(logID)

	runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
		hashes := [][]byte{[]byte("thisdoesn'texist")}
		leaves, err := tx.GetLeavesByHash(ctx, hashes, false)
		if err != nil {
			t.Fatalf("Error getting leaves by hash: %v", err)
		}
		if len(leaves) != 0 {
			t.Fatalf("Expected no leaves returned but got %d", len(leaves))
		}
		return nil
	})
}

func TestGetLeavesByHash(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	logID := createLogForTests(db)
	s := NewLogStorage(db, nil)
	tree := logTree(logID)
	leaves := createTestLeaves(leavesToInsert, 20)

	{
		runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
			if _, err := tx.QueueLeaves(ctx, leaves, fakeQueueTime); err != nil {
				t.Fatalf("Failed to queue leaves: %v", err)
			}
			return nil
		})
	}

	{
		// Now try to dequeue them
		runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
			leaves2, err := tx.DequeueLeaves(ctx, 99, fakeDequeueCutoffTime)
			if err != nil {
				t.Fatalf("Failed to dequeue leaves: %v", err)
			}
			if len(leaves2) != leavesToInsert {
				t.Fatalf("Dequeued %d leaves but expected to get %d", len(leaves2), leavesToInsert)
			}
			ensureAllLeavesDistinct(leaves2, t)
			return nil
		})
	}

	// Assign sequence numbers - they're all the same so this should be rejected.
	for i, leaf := range leaves {
		leaf.LeafIndex = int64(i) + 20
	}

	{
		runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
			if err := tx.UpdateSequencedLeaves(ctx, leaves); err != nil {
				t.Fatalf("Failed to update leaves: %v", err)
			}
			return nil
		})
	}

	runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
		hashes := [][]byte{leaves[0].MerkleLeafHash, leaves[1].MerkleLeafHash}
		leaves2, err := tx.GetLeavesByHash(ctx, hashes, false)
		if err != nil {
			t.Fatalf("Unexpected error getting leaves by hash: %v", err)
		}
		if len(leaves2) != 2 {
			t.Fatalf("Got %d leaves but expected two", len(leaves))
		}

		// Ordering by hash value is guaranteed to be preserved.
		for i := 0; i < 2; i++ {
			if !proto.Equal(leaves[i], leaves2[i]) {
				t.Errorf("GetLeavesByHash(%d): got: %v, want: %v", leaves[i], leaves2[i])
			}
		}

		return nil
	})
}

func TestGetLeavesByHashUnsequencedInvisible(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	logID := createLogForTests(db)
	s := NewLogStorage(db, nil)
	tree := logTree(logID)
	leaves := createTestLeaves(leavesToInsert, 20)

	{
		runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
			if _, err := tx.QueueLeaves(ctx, leaves, fakeQueueTime); err != nil {
				t.Fatalf("Failed to queue leaves: %v", err)
			}
			return nil
		})
	}

	{
		runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
			leaves2, err := tx.DequeueLeaves(ctx, 99, fakeDequeueCutoffTime)
			if err != nil {
				t.Fatalf("Failed to dequeue leaves: %v", err)
			}
			if len(leaves2) != leavesToInsert {
				t.Fatalf("Dequeued %d leaves but expected to get %d", len(leaves2), leavesToInsert)
			}
			ensureAllLeavesDistinct(leaves2, t)
			return nil
		})
	}

	runLogTX(s, tree, t, func(ctx context.Context, tx storage.LogTreeTX) error {
		hashes := [][]byte{leaves[0].MerkleLeafHash, leaves[1].MerkleLeafHash}
		leaves3, err := tx.GetLeavesByHash(ctx, hashes, false)
		if err != nil {
			t.Fatalf("Unexpected error getting leaves by hash: %v", err)
		}
		if len(leaves3) != 0 {
			t.Fatalf("Got %d leaves but expected none", len(leaves3))
		}

		return nil
	})
}

// createLogForTests creates a log-type tree for tests. Returns the treeID of the new tree.
func createLogForTests(db *bolt.DB) int64 {
	tree, err := createTree(db, storageto.LogTree)
	if err != nil {
		panic(fmt.Sprintf("Error creating log: %v", err))
	}

	ctx := context.Background()
	l := NewLogStorage(db, nil)
	err = l.ReadWriteTransaction(ctx, tree, func(ctx context.Context, tx storage.LogTreeTX) error {
		if err := tx.StoreSignedLogRoot(ctx, trillian.SignedLogRoot{
			LogId:     tree.TreeId,
			RootHash:  []byte{0},
			Signature: &sigpb.DigitallySigned{Signature: []byte("asignature")}}); err != nil {
			return fmt.Errorf("Error storing new SignedLogRoot: %v", err)
		}
		return nil
	})
	if err != nil {
		panic(fmt.Sprintf("ReadWriteTransaction() = %v", err))
	}
	return tree.TreeId
}

// createMapForTests creates a map-type tree for tests. Returns the treeID of the new tree.
func createMapForTests(db *bolt.DB) int64 {
	tree, err := createTree(db, storageto.MapTree)
	if err != nil {
		panic(fmt.Sprintf("Error creating map: %v", err))
	}
	return tree.TreeId
}

// createTree creates the specified tree using AdminStorage.
func createTree(db *bolt.DB, tree *trillian.Tree) (*trillian.Tree, error) {
	ctx := context.Background()
	s := NewAdminStorage(db)
	tree, err := storage.CreateTree(ctx, s, tree)
	if err != nil {
		return nil, err
	}
	return tree, nil
}

// updateTree updates the specified tree using AdminStorage.
func updateTree(db *bolt.DB, treeID int64, updateFn func(*trillian.Tree)) (*trillian.Tree, error) {
	ctx := context.Background()
	s := NewAdminStorage(db)
	return storage.UpdateTree(ctx, s, treeID, updateFn)
}

func logTree(logID int64) *trillian.Tree {
	return &trillian.Tree{
		TreeId:       logID,
		TreeType:     trillian.TreeType_LOG,
		HashStrategy: trillian.HashStrategy_RFC6962_SHA256,
	}
}

// Convenience methods to avoid copying out "if err != nil { blah }" all over the place
func runLogTX(s storage.LogStorage, tree *trillian.Tree, t *testing.T, f storage.LogTXFunc) {
	t.Helper()
	if err := s.ReadWriteTransaction(context.Background(), tree, f); err != nil {
		t.Fatalf("Failed to run log tx: %v", err)
	}
}

// Creates some test leaves with predictable data
func createTestLeaves(n, startSeq int64) []*trillian.LogLeaf {
	var leaves []*trillian.LogLeaf
	for l := int64(0); l < n; l++ {
		lv := fmt.Sprintf("Leaf %d", l+startSeq)
		h := sha256.New()
		h.Write([]byte(lv))
		leafHash := h.Sum(nil)
		h.Reset()
		h.Write([]byte(strings.ToLower(lv)))
		merkleHash := h.Sum(nil)
		leaf := &trillian.LogLeaf{
			LeafIdentityHash: leafHash,
			MerkleLeafHash:   merkleHash,
			LeafValue:        []byte(lv),
			ExtraData:        []byte(fmt.Sprintf("Extra %d", l)),
			LeafIndex:        int64(startSeq + l),
		}
		leaves = append(leaves, leaf)
	}

	return leaves
}

func ensureAllLeavesDistinct(leaves []*trillian.LogLeaf, t *testing.T) {
	t.Helper()
	// All the leaf value hashes should be distinct because the leaves were created with distinct
	// leaf data. If only we had maps with slices as keys or sets or pretty much any kind of usable
	// data structures we could do this properly.
	for i := range leaves {
		for j := range leaves {
			if i != j && bytes.Equal(leaves[i].LeafIdentityHash, leaves[j].LeafIdentityHash) {
				t.Fatalf("Unexpectedly got a duplicate leaf hash: %v %v",
					leaves[i].LeafIdentityHash, leaves[j].LeafIdentityHash)
			}
		}
	}
}

func leafInBatch(leaf *trillian.LogLeaf, batch []*trillian.LogLeaf) bool {
	for _, bl := range batch {
		if bytes.Equal(bl.LeafIdentityHash, leaf.LeafIdentityHash) {
			return true
		}
	}

	return false
}

type committableTX interface {
	Commit() error
}

func commit(tx committableTX, t *testing.T) {
	t.Helper()
	if err := tx.Commit(); err != nil {
		t.Errorf("Failed to commit tx: %v", err)
	}
}
