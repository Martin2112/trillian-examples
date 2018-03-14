package boltdb

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/google/trillian"
	"github.com/google/trillian/merkle/hashers"
	"github.com/google/trillian/monitoring"
	"github.com/google/trillian/storage"
	"github.com/google/trillian/storage/cache"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	defaultLogStrata = []int{8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8}
	once             sync.Once
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
	return t.tx.Commit()
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
	c := t.tx.Bucket([]byte(TreeBucket)).Cursor()

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
				(tree.TreeState == trillian.TreeState_ACTIVE || tree.TreeState == trillian.TreeState_DRAINING) {
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

	ltx := &logTreeTX{
		treeTX: ttx,
		ls:     m,
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

func (m *boltLogStorage) AddSequencedLeaves(ctx context.Context, tree *trillian.Tree, leaves []*trillian.LogLeaf) ([]*trillian.QueuedLogLeaf, error) {
	return nil, status.Errorf(codes.Unimplemented, "AddSequencedLeaves is not implemented")
}

func (m *boltLogStorage) QueueLeaves(ctx context.Context, tree *trillian.Tree, leaves []*trillian.LogLeaf, queueTimestamp time.Time) ([]*trillian.QueuedLogLeaf, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (t *logTreeTX) ReadRevision() int64 {
	return t.root.TreeRevision
}

func (t *logTreeTX) WriteRevision() int64 {
	return t.treeTX.writeRevision
}

func (t *logTreeTX) DequeueLeaves(ctx context.Context, limit int, cutoffTime time.Time) ([]*trillian.LogLeaf, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (t *logTreeTX) QueueLeaves(ctx context.Context, leaves []*trillian.LogLeaf, queueTimestamp time.Time) ([]*trillian.LogLeaf, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (t *logTreeTX) GetSequencedLeafCount(ctx context.Context) (int64, error) {
	return 0, status.Error(codes.Unimplemented, "not implemented")
}

func (t *logTreeTX) GetLeavesByIndex(ctx context.Context, leaves []int64) ([]*trillian.LogLeaf, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (t *logTreeTX) GetLeavesByRange(ctx context.Context, start, count int64) ([]*trillian.LogLeaf, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (t *logTreeTX) GetLeavesByHash(ctx context.Context, leafHashes [][]byte, orderBySequence bool) ([]*trillian.LogLeaf, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (t *logTreeTX) LatestSignedLogRoot(ctx context.Context) (trillian.SignedLogRoot, error) {
	return t.root, nil
}

// fetchLatestRoot reads the latest SignedLogRoot from the DB and returns it.
func (t *logTreeTX) fetchLatestRoot(ctx context.Context) (trillian.SignedLogRoot, error) {
	return trillian.SignedLogRoot{}, status.Error(codes.Unimplemented, "not implemented")
}

func (t *logTreeTX) StoreSignedLogRoot(ctx context.Context, root trillian.SignedLogRoot) error {
	return status.Error(codes.Unimplemented, "not implemented")
}

func (t *readOnlyLogTX) GetUnsequencedCounts(ctx context.Context) (storage.CountByLogID, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (t *logTreeTX) UpdateSequencedLeaves(ctx context.Context, leaves []*trillian.LogLeaf) error {
	return status.Error(codes.Unimplemented, "not implemented")
}
