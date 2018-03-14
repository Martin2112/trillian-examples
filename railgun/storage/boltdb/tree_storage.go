package boltdb

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/boltdb/bolt"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/google/trillian/storage"
	"github.com/google/trillian/storage/cache"
	"github.com/google/trillian/storage/storagepb"
)

const (
	SubtreeBucket = "Subtree"
	TreeBucket    = "Tree"
)

var allBuckets = []string{SubtreeBucket, TreeBucket}

type boltTreeStorage struct {
	db *bolt.DB
}

type treeTX struct {
	closed        bool
	tx            *bolt.Tx
	bucket        *bolt.Bucket
	ts            *boltTreeStorage
	treeID        int64
	hashSizeBytes int
	subtreeCache  cache.SubtreeCache
	writeRevision int64
}

func newTreeStorage(db *bolt.DB) *boltTreeStorage {
	return &boltTreeStorage{db: db}
}

func OpenDB(dbFile string, opts *bolt.Options) (*bolt.DB, error) {
	db, err := bolt.Open(dbFile, 0600, opts)
	if err != nil {
		glog.Warningf("Could not open Bolt database, check config: %s", err)
		return nil, err
	}

	return maybeCreateBuckets(db)
}

func (t *treeTX) GetTreeRevisionIncludingSize(ctx context.Context, treeSize int64) (int64, int64, error) {
	// Negative size is not sensible and a zero sized tree has no nodes so no revisions
	if treeSize <= 0 {
		return 0, 0, fmt.Errorf("invalid tree size: %d", treeSize)
	}

	return t.writeRevision - 1, treeSize, nil
}

// getSubtreesAtRev returns a GetSubtreesFunc which reads at the passed in rev.
func (t *treeTX) getSubtreesAtRev(ctx context.Context, rev int64) cache.GetSubtreesFunc {
	return func(ids []storage.NodeID) ([]*storagepb.SubtreeProto, error) {
		return t.getSubtrees(ctx, rev, ids)
	}
}

// GetMerkleNodes returns the requests nodes at (or below) the passed in treeRevision.
func (t *treeTX) GetMerkleNodes(ctx context.Context, treeRevision int64, nodeIDs []storage.NodeID) ([]storage.Node, error) {
	return t.subtreeCache.GetNodes(nodeIDs, t.getSubtreesAtRev(ctx, treeRevision))
}

func (t *treeTX) SetMerkleNodes(ctx context.Context, nodes []storage.Node) error {
	for _, n := range nodes {
		err := t.subtreeCache.SetNodeHash(n.NodeID, n.Hash,
			func(nID storage.NodeID) (*storagepb.SubtreeProto, error) {
				return t.getSubtree(ctx, t.writeRevision, nID)
			})
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *treeTX) Commit() error {
	if t.writeRevision > -1 {
		if err := t.subtreeCache.Flush(func(st []*storagepb.SubtreeProto) error {
			return t.storeSubtrees(context.TODO(), st)
		}); err != nil {
			glog.Warningf("TX commit flush error: %v", err)
			return err
		}
	}
	t.closed = true
	if err := t.tx.Commit(); err != nil {
		glog.Warningf("TX commit error: %s, stack:\n%s", err, string(debug.Stack()))
		return err
	}
	return nil
}

func (t *treeTX) Rollback() error {
	t.closed = true
	if err := t.tx.Rollback(); err != nil {
		glog.Warningf("TX rollback error: %s, stack:\n%s", err, string(debug.Stack()))
		return err
	}
	return nil
}

func (t *treeTX) Close() error {
	if !t.closed {
		err := t.Rollback()
		if err != nil {
			glog.Warningf("Rollback error on Close(): %v", err)
		}
		return err
	}
	return nil
}

func (t *treeTX) IsOpen() bool {
	return !t.closed
}

func (t *treeTX) getSubtree(ctx context.Context, treeRevision int64, nodeID storage.NodeID) (*storagepb.SubtreeProto, error) {
	s, err := t.getSubtrees(ctx, treeRevision, []storage.NodeID{nodeID})
	if err != nil {
		return nil, err
	}
	switch len(s) {
	case 0:
		return nil, nil
	case 1:
		return s[0], nil
	default:
		return nil, fmt.Errorf("got %d subtrees, but expected 1", len(s))
	}
}

func (t *treeTX) getSubtrees(ctx context.Context, treeRevision int64, nodeIDs []storage.NodeID) ([]*storagepb.SubtreeProto, error) {
	if treeRevision >= t.writeRevision {
		return nil, fmt.Errorf("tree revision does not exist: %d, currently writing at: %d", treeRevision, t.writeRevision)
	}

	ret := make([]*storagepb.SubtreeProto, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		if nodeID.PrefixLenBits%8 != 0 {
			return nil, fmt.Errorf("invalid subtree ID - not multiple of 8: %d", nodeID.PrefixLenBits)
		}

		nodeIDBytes := nodeID.Path[:nodeID.PrefixLenBits/8]
		nodesRaw := t.bucket.Get(nodeIDBytes)
		var subtree storagepb.SubtreeProto
		if err := proto.Unmarshal(nodesRaw, &subtree); err != nil {
			glog.Warningf("Failed to unmarshal SubtreeProto: %s", err)
			return nil, err
		}
		if subtree.Prefix == nil {
			subtree.Prefix = []byte{}
		}
		ret = append(ret, &subtree)
	}

	return ret, nil
}

func (t *treeTX) storeSubtrees(ctx context.Context, subtrees []*storagepb.SubtreeProto) error {
	if len(subtrees) == 0 {
		glog.Warning("attempted to store 0 subtrees...")
		return nil
	}

	// TODO(Martin2112): probably need to be able to batch this in the case where we have
	// a really large number of subtrees to store.
	for _, s := range subtrees {
		s := s
		if s.Prefix == nil {
			panic(fmt.Errorf("nil prefix on %v", s))
		}
		subtreeBytes, err := proto.Marshal(s)
		if err != nil {
			return err
		}
		if err := t.bucket.Put(s.Prefix, subtreeBytes); err != nil {
			return err
		}
	}
	return nil
}

func (m *boltTreeStorage) beginTreeTX(ctx context.Context, treeID int64, hashSizeBytes int, cache cache.SubtreeCache, readonly bool) (treeTX, error) {
	tx, err := m.db.Begin(!readonly)
	if err != nil {
		glog.Warningf("Boltdb failed to begin tx: %v", err)
		return treeTX{}, err
	}

	name := fmt.Sprintf("%s_%x", SubtreeBucket, treeID)
	tb, err := tx.CreateBucketIfNotExists([]byte(name))
	if err != nil {
		return treeTX{}, err
	}

	return treeTX{tx: tx, bucket: tb, treeID: treeID, hashSizeBytes: hashSizeBytes, subtreeCache: cache, writeRevision: -1}, nil
}

func maybeCreateBuckets(db *bolt.DB) (*bolt.DB, error) {
	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()
	for _, name := range allBuckets {
		_, err := tx.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}
