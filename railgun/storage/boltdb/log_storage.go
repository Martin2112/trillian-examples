// Copyright 2018 Google LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package boltdb

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/trillian"
	"github.com/google/trillian/merkle/hashers"
	"github.com/google/trillian/monitoring"
	"github.com/google/trillian/storage"
	"github.com/google/trillian/storage/cache"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	LeafBucket        = "Leaf"
	MerkleHashBucket  = "MerkleHashToLeafIdentityHash"
	UnsequencedBucket = "Unsequenced"
	SequencedBucket   = "Sequenced"
	TreeHeadBucket    = "TreeHead"
)

var (
	defaultLogStrata = []int{8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8}
	once             sync.Once
	treeBuckets      = []string{LeafBucket, MerkleHashBucket, UnsequencedBucket, SequencedBucket, TreeHeadBucket}
)

type boltLogStorage struct {
	*boltTreeStorage
	admin         storage.AdminStorage
	metricFactory monitoring.MetricFactory
}

// readOnlyLogTX implements storage.ReadOnlyLogTX
type readOnlyLogTX struct {
	ls *boltLogStorage
	tx *bolt.Tx
}

type logTreeTX struct {
	treeTX
	ls   *boltLogStorage
	lb   *bolt.Bucket
	root trillian.SignedLogRoot
}

// NewLogStorage creates a storage.LogStorage instance for the specified MySQL URL.
// It assumes storage.AdminStorage is backed by the same MySQL database as well.
func NewLogStorage(db *bolt.DB, mf monitoring.MetricFactory) storage.LogStorage {
	if mf == nil {
		mf = monitoring.InertMetricFactory{}
	}
	return &boltLogStorage{
		admin:           nil, /* NewAdminStorage(db) TODO(Martin2112): Implement */
		boltTreeStorage: newTreeStorage(db),
		metricFactory:   mf,
	}
}

func (m *boltLogStorage) CheckDatabaseAccessible(ctx context.Context) error {
	return nil
}

func (m *boltLogStorage) Snapshot(ctx context.Context) (storage.ReadOnlyLogTX, error) {
	tx, err := m.db.Begin(false)
	if err != nil {
		glog.Warningf("Could not start ReadOnlyLogTX: %s", err)
		return nil, err
	}
	return &readOnlyLogTX{m, tx}, nil
}

func (t *readOnlyLogTX) Commit() error {
	// Can't use Commit() here as it errors.
	return t.tx.Rollback()
}

func (t *readOnlyLogTX) Rollback() error {
	return t.tx.Rollback()
}

func (t *readOnlyLogTX) Close() error {
	if err := t.Rollback(); err != nil && err != sql.ErrTxDone {
		glog.Warningf("Rollback error on Close(): %v", err)
		return err
	}
	return nil
}

// TODO(Martin2112): Find a way of doing this that performs better on a large
// number of logs.
func (t *readOnlyLogTX) GetActiveLogIDs(ctx context.Context) ([]int64, error) {
	// Include logs that are DRAINING in the active list as we're still
	// integrating leaves into them.
	b := t.tx.Bucket([]byte(TreeBucket))
	if b == nil {
		return nil, errors.New("internal error - no TreeBucket exists")
	}
	c := b.Cursor()

	active := []int64{}
	for k, v := c.First(); k != nil; k, v = c.Next() {
		// Ensure to skip over any nested buckets
		if k != nil && v != nil {
			tree := trillian.Tree{}
			if err := proto.Unmarshal(v, &tree); err != nil {
				glog.Warningf("Failed to unmarshal tree proto id: %v, err=%v", k, err)
				continue
			}

			if (tree.TreeType == trillian.TreeType_LOG || tree.TreeType == trillian.TreeType_PREORDERED_LOG) &&
				(tree.TreeState == trillian.TreeState_ACTIVE || tree.TreeState == trillian.TreeState_DRAINING) &&
				!tree.Deleted {
				active = append(active, tree.TreeId)
			}
		}
	}

	return active, nil
}

func (m *boltLogStorage) beginInternal(ctx context.Context, tree *trillian.Tree) (storage.LogTreeTX, error) {
	once.Do(func() {
		// TODO(Martin2112): Implement metrics
		//createMetrics(m.metricFactory)
	})
	hasher, err := hashers.NewLogHasher(tree.HashStrategy)
	if err != nil {
		return nil, err
	}

	stCache := cache.NewLogSubtreeCache(defaultLogStrata, hasher)
	ttx, err := m.beginTreeTX(ctx, tree.TreeId, hasher.Size(), stCache, false)
	if err != nil && err != storage.ErrTreeNeedsInit {
		return nil, err
	}

	lb, err := ttx.tx.CreateBucketIfNotExists(logKeyOf(tree.TreeId))
	if err != nil {
		return nil, err
	}

	// Create all the top level tree buckets if necessary.
	for _, bucketName := range treeBuckets {
		_, err = lb.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return nil, err
		}
	}

	ltx := &logTreeTX{
		treeTX: ttx,
		ls:     m,
		lb:     lb,
	}

	ltx.root, err = ltx.fetchLatestRoot(ctx)
	if err != nil && err != storage.ErrTreeNeedsInit {
		ttx.Rollback()
		return nil, err
	}
	if err == storage.ErrTreeNeedsInit {
		return ltx, err
	}

	ltx.treeTX.writeRevision = ltx.root.TreeRevision + 1

	return ltx, nil
}

func (m *boltLogStorage) ReadWriteTransaction(ctx context.Context, tree *trillian.Tree, f storage.LogTXFunc) error {
	tx, err := m.beginInternal(ctx, tree)
	if err != nil && err != storage.ErrTreeNeedsInit {
		return err
	}
	defer tx.Close()
	if err := f(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (m *boltLogStorage) SnapshotForTree(ctx context.Context, tree *trillian.Tree) (storage.ReadOnlyLogTreeTX, error) {
	tx, err := m.beginInternal(ctx, tree)
	if err != nil && err != storage.ErrTreeNeedsInit {
		return nil, err
	}
	return tx.(storage.ReadOnlyLogTreeTX), err
}

func (m *boltLogStorage) AddSequencedLeaves(context.Context, *trillian.Tree, []*trillian.LogLeaf, time.Time) ([]*trillian.QueuedLogLeaf, error) {
	return nil, status.Errorf(codes.Unimplemented, "AddSequencedLeaves is not implemented")
}

func (m *boltLogStorage) QueueLeaves(ctx context.Context, tree *trillian.Tree, leaves []*trillian.LogLeaf, queueTimestamp time.Time) ([]*trillian.QueuedLogLeaf, error) {
	tx, err := m.beginInternal(ctx, tree)
	if err != nil {
		return nil, err
	}
	existing, err := tx.QueueLeaves(ctx, leaves, queueTimestamp)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	ret := make([]*trillian.QueuedLogLeaf, len(leaves))
	for i, e := range existing {
		if e != nil {
			ret[i] = &trillian.QueuedLogLeaf{
				Leaf:   e,
				Status: status.Newf(codes.AlreadyExists, "leaf already exists: %v", e.LeafIdentityHash).Proto(),
			}
			continue
		}
		ret[i] = &trillian.QueuedLogLeaf{Leaf: leaves[i]}
	}
	return ret, nil
}

func (t *logTreeTX) AddSequencedLeaves(ctx context.Context, leaves []*trillian.LogLeaf, timestamp time.Time) ([]*trillian.QueuedLogLeaf, error) {
	return nil, status.Errorf(codes.Unimplemented, "not yet implemented by boltdb_storage")
}

func (t *logTreeTX) ReadRevision() int64 {
	return t.root.TreeRevision
}

func (t *logTreeTX) WriteRevision() int64 {
	return t.treeTX.writeRevision
}

func (t *logTreeTX) DequeueLeaves(ctx context.Context, limit int, cutoffTime time.Time) ([]*trillian.LogLeaf, error) {
	lb := t.lb.Bucket([]byte(LeafBucket))
	qb := t.lb.Bucket([]byte(UnsequencedBucket))
	leaves := make([]*trillian.LogLeaf, 0, limit)

	c := qb.Cursor()
	for k, v := c.First(); k != nil && len(leaves) < limit; k, v = c.Next() {
		// The v we retrieved is the LeafIdentityHash of the leaf we want to fetch
		blob := lb.Get(v)
		var leaf trillian.LogLeaf
		err := proto.Unmarshal(blob, &leaf)
		if err != nil {
			return nil, err
		}
		if len(leaf.LeafIdentityHash) != t.hashSizeBytes {
			return nil, errors.New("dequeued a leaf with incorrect hash size")
		}

		leaves = append(leaves, &leaf)
		// Done with this work queue entry (if we successfully commit the tx)
		c.Delete()
	}
	return leaves, nil
}

func (t *logTreeTX) QueueLeaves(ctx context.Context, leaves []*trillian.LogLeaf, queueTimestamp time.Time) ([]*trillian.LogLeaf, error) {
	// Don't accept batches if any of the leaves are invalid.
	for _, leaf := range leaves {
		if len(leaf.LeafIdentityHash) != t.hashSizeBytes {
			return nil, fmt.Errorf("queued leaf must have a leaf ID hash of length %d", t.hashSizeBytes)
		}
		var err error
		leaf.QueueTimestamp, err = ptypes.TimestampProto(queueTimestamp)
		if err != nil {
			return nil, fmt.Errorf("got invalid queue timestamp: %v", err)
		}
	}

	existingCount := 0
	existingLeaves := make([]*trillian.LogLeaf, len(leaves))

	for i, leaf := range leaves {
		lb := t.lb.Bucket([]byte(LeafBucket))
		qb := t.lb.Bucket([]byte(UnsequencedBucket))

		// Check for duplicates
		if lb.Get(leaf.LeafIdentityHash) != nil {
			existingLeaves[i] = leaf
			existingCount++
			continue
		}

		blob, err := proto.Marshal(leaf)
		if err != nil {
			return nil, err
		}
		if err := lb.Put(leaf.LeafIdentityHash, blob); err != nil {
			glog.Warningf("Error inserting %d into Leaf bucket: %s", i, err)
			return nil, err
		}

		// Create the work queue entry
		uIndex, err := qb.NextSequence()
		if err != nil {
			return nil, err
		}

		if err := qb.Put(keyOfInt64(int64(uIndex)), leaf.LeafIdentityHash); err != nil {
			glog.Warningf("Error inserting into Unsequenced bucket: %s", err)
			return nil, err
		}

		// TODO(Martin2112): Create / populate other index buckets as needed
	}

	if existingCount == 0 {
		return existingLeaves, nil
	}

	return existingLeaves, nil
}

func (t *logTreeTX) GetSequencedLeafCount(ctx context.Context) (int64, error) {
	return 0, status.Error(codes.Unimplemented, "not implemented")
}

func (t *logTreeTX) GetLeavesByIndex(ctx context.Context, leaves []int64) ([]*trillian.LogLeaf, error) {
	var ret []*trillian.LogLeaf
	for _, index := range leaves {
		leaf := &trillian.LogLeaf{}

		// We have a direct mapping from index to MLH via the SequencedBucket. Then we go from that
		// to the leaf identity hash and then to the leaf. Hmmm. An extra map might better.
		sb := t.lb.Bucket([]byte(SequencedBucket))
		mb := t.lb.Bucket([]byte(MerkleHashBucket))
		lb := t.lb.Bucket([]byte(LeafBucket))

		mlh := sb.Get(keyOfInt64(index))
		if mlh == nil {
			// Leaf hash doesn't exist - ignore.
			continue
		}
		lih := mb.Get(mlh)
		if lih == nil {
			// We don't expect this situation as the sequence mapping existed implying MLH should map to something.
			return nil, fmt.Errorf("GetLeavesByHash() seq %d exists but MLH %s doesn't", index, hex.EncodeToString(lih))
		}
		blob := lb.Get(lih)
		if blob == nil {
			// We don't expect this situation as the sequence mapping existed implying MLH should map to something.
			return nil, fmt.Errorf("GetLeavesByHash() seq %d exists but LIH %s doesn't", index, hex.EncodeToString(lih))
		}

		var leafProto trillian.LogLeaf
		if err := proto.Unmarshal(blob, &leafProto); err != nil {
			return nil, err
		}

		if got, want := len(leafProto.MerkleLeafHash), t.hashSizeBytes; got != want {
			return nil, fmt.Errorf("LogID: %d Scanned leaf %s does not have hash length %d, got %d", t.treeID, hex.EncodeToString(leaf.LeafIdentityHash), want, got)
		}

		ret = append(ret, &leafProto)
	}

	if got, want := len(ret), len(leaves); got != want {
		return nil, fmt.Errorf("len(ret): %d, want %d", got, want)
	}

	return ret, nil
}

func (t *logTreeTX) GetLeavesByRange(ctx context.Context, start, count int64) ([]*trillian.LogLeaf, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (t *logTreeTX) GetLeavesByHash(ctx context.Context, leafHashes [][]byte, _ /* orderBySequence */ bool) ([]*trillian.LogLeaf, error) {
	// Order by sequence is always true for this implementation as the hash bucket preserves
	// key order.
	var ret []*trillian.LogLeaf
	for _, mlh := range leafHashes {
		leaf := &trillian.LogLeaf{}

		// We have a direct mapping from MLH -> LIH, then we use that to pull the leaf data.
		lb := t.lb.Bucket([]byte(LeafBucket))
		mb := t.lb.Bucket([]byte(MerkleHashBucket))

		lih := mb.Get(mlh)
		if lih == nil {
			// Leaf hash doesn't exist - ignore.
			continue
		}
		blob := lb.Get(lih)
		if blob == nil {
			// We don't expect this situation as the MLH existed implying it should map to something.
			return nil, fmt.Errorf("GetLeavesByHash() MLH %s exists but LIH %s doesn't", hex.EncodeToString(mlh), hex.EncodeToString(lih))
		}

		var leafProto trillian.LogLeaf
		if err := proto.Unmarshal(blob, &leafProto); err != nil {
			return nil, err
		}

		if got, want := len(leafProto.MerkleLeafHash), t.hashSizeBytes; got != want {
			return nil, fmt.Errorf("LogID: %d Scanned leaf %s does not have hash length %d, got %d", t.treeID, hex.EncodeToString(leaf.LeafIdentityHash), want, got)
		}

		ret = append(ret, &leafProto)
	}

	return ret, nil
}

func (t *logTreeTX) LatestSignedLogRoot(ctx context.Context) (trillian.SignedLogRoot, error) {
	return t.root, nil
}

// fetchLatestRoot reads the latest SignedLogRoot from the DB and returns it.
func (t *logTreeTX) fetchLatestRoot(ctx context.Context) (trillian.SignedLogRoot, error) {
	b := t.lb.Bucket([]byte(TreeHeadBucket))
	k, v := b.Cursor().Last()
	if k == nil && v == nil {
		// No tree heads exist yet
		return trillian.SignedLogRoot{}, storage.ErrTreeNeedsInit
	}

	var slr trillian.SignedLogRoot
	if err := proto.Unmarshal(v, &slr); err != nil {
		return trillian.SignedLogRoot{}, err
	}

	return slr, nil
}

func (t *logTreeTX) StoreSignedLogRoot(ctx context.Context, root trillian.SignedLogRoot) error {
	// First check that there isn't a version at this revision already.
	k := keyOfInt64(root.TreeRevision)
	b := t.lb.Bucket([]byte(TreeHeadBucket))
	if v := b.Get(k); v != nil {
		return fmt.Errorf("STH version: %d already exists for tree: %d", t.writeRevision, t.treeID)
	}

	blob, err := proto.Marshal(&root)
	if err != nil {
		return err
	}

	return b.Put(k, blob)
}

func (t *readOnlyLogTX) GetUnsequencedCounts(ctx context.Context) (storage.CountByLogID, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (t *logTreeTX) UpdateSequencedLeaves(ctx context.Context, leaves []*trillian.LogLeaf) error {
	// TODO(Martin2112): Currently not handling the integrate timestamps. Maybe doesn't matter
	// as this is a specialized storage version.
	for _, leaf := range leaves {
		if len(leaf.LeafIdentityHash) != t.hashSizeBytes {
			return errors.New("sequenced leaf has incorrect hash size")
		}

		// We need to create the links between sequence number and MerkleLeafHash and from the
		// MerkleLeafHash to the LeafIdentityHash.
		sb := t.lb.Bucket([]byte(SequencedBucket))
		mb := t.lb.Bucket([]byte(MerkleHashBucket))
		// The leaf index must not have already been assigned to a leaf.
		if sb.Get(keyOfInt64(leaf.LeafIndex)) != nil {
			return fmt.Errorf("UpdateSequencedLeaves(): duplicate sequence number: %d", leaf.LeafIndex)
		}
		// The merkle hash must not exist or we'd be overwriting some previous entry.
		if mb.Get(leaf.MerkleLeafHash) != nil {
			return fmt.Errorf("UpdateSequencedLeaves(): duplicate MLH: %v", leaf.MerkleLeafHash)
		}

		if err := sb.Put(keyOfInt64(leaf.LeafIndex), leaf.MerkleLeafHash); err != nil {
			return err
		}
		if err := mb.Put(leaf.MerkleLeafHash, leaf.LeafIdentityHash); err != nil {
			return err
		}
	}

	return nil
}

func logKeyOf(treeID int64) []byte {
	return []byte(fmt.Sprintf("Tree_%s", strconv.FormatInt(treeID, 16)))
}
