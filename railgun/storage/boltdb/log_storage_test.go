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

	"github.com/boltdb/bolt"
	"github.com/gogo/protobuf/proto"
	"github.com/google/trillian"
	"github.com/google/trillian/crypto/sigpb"
	spb "github.com/google/trillian/crypto/sigpb"
	"github.com/google/trillian/storage"
	storageto "github.com/google/trillian/storage/testonly"
)

const leavesToInsert = 5

// Time we will queue all leaves at
var fakeQueueTime = time.Date(2016, 11, 10, 15, 16, 27, 0, time.UTC)

// Must be 32 bytes to match sha256 length if it was a real hash
var dummyHash = []byte("hashxxxxhashxxxxhashxxxxhashxxxx")

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
		leaf := &trillian.LogLeaf{
			LeafIdentityHash: leafHash,
			MerkleLeafHash:   leafHash,
			LeafValue:        []byte(lv),
			ExtraData:        []byte(fmt.Sprintf("Extra %d", l)),
			LeafIndex:        int64(startSeq + l),
		}
		leaves = append(leaves, leaf)
	}

	return leaves
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
