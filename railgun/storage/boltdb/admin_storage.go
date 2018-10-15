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
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/trillian"
	"github.com/google/trillian/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewAdminStorage returns a MySQL storage.AdminStorage implementation backed by DB.
func NewAdminStorage(db *bolt.DB) storage.AdminStorage {
	// Ensure all top level buckets have been created
	if _, err := maybeCreateBuckets(db); err != nil {
		glog.Fatalf("Can't create admin storage buckets: %v", err)
	}
	return &boltAdminStorage{db}
}

// mysqlAdminStorage implements storage.AdminStorage
type boltAdminStorage struct {
	db *bolt.DB
}

func (s *boltAdminStorage) Snapshot(ctx context.Context) (storage.ReadOnlyAdminTX, error) {
	return s.beginInternal(ctx)
}

func (s *boltAdminStorage) beginInternal(ctx context.Context) (storage.AdminTX, error) {
	tx, err := s.db.Begin(true)
	if err != nil {
		return nil, err
	}
	return &adminTX{tx: tx, tb: tx.Bucket([]byte(TreeBucket))}, nil
}

func (s *boltAdminStorage) ReadWriteTransaction(ctx context.Context, f storage.AdminTXFunc) error {
	tx, err := s.beginInternal(ctx)
	if err != nil {
		return err
	}
	defer tx.Close()
	if err := f(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *boltAdminStorage) CheckDatabaseAccessible(ctx context.Context) error {
	return nil
}

type adminTX struct {
	tx *bolt.Tx
	tb *bolt.Bucket

	// mu guards *direct* reads/writes on closed, which happen only on
	// Commit/Rollback/IsClosed/Close methods.
	// We don't check closed on *all* methods (apart from the ones above),
	// as we trust tx to keep tabs on its state (and consequently fail to do
	// queries after closed).
	mu     sync.RWMutex
	closed bool
}

func (t *adminTX) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return t.tx.Commit()
}

func (t *adminTX) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return t.tx.Rollback()
}

func (t *adminTX) IsClosed() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.closed
}

func (t *adminTX) Close() error {
	// Acquire and release read lock manually, without defer, as if the txn
	// is not closed Rollback() will attempt to acquire the rw lock.
	t.mu.RLock()
	closed := t.closed
	t.mu.RUnlock()
	if !closed {
		err := t.Rollback()
		if err != nil {
			glog.Warningf("Rollback error on Close(): %v", err)
		}
		return err
	}
	return nil
}

func keyOf(treeID int64) []byte {
	return []byte(strconv.FormatInt(treeID, 16))
}

func (t *adminTX) GetTree(ctx context.Context, treeID int64) (*trillian.Tree, error) {
	v := t.tb.Get(keyOf(treeID))
	if v == nil {
		return nil, status.Errorf(codes.NotFound, "tree id: %d not found", treeID)
	}

	var tree trillian.Tree
	if err := proto.Unmarshal(v, &tree); err != nil {
		glog.Warningf("Failed to unmarshal Tree proto: %v", err)
		return nil, err
	}

	return &tree, nil
}

func (t *adminTX) ListTreeIDs(ctx context.Context, includeDeleted bool) ([]int64, error) {
	c := t.tx.Bucket([]byte(TreeBucket)).Cursor()

	var ids []int64
	for k, v := c.First(); k != nil; k, v = c.Next() {
		// Ensure to skip over any nested buckets
		if k != nil && v != nil {
			tree := trillian.Tree{}
			if err := proto.Unmarshal(v, &tree); err != nil {
				glog.Warningf("Failed to unmarshal tree proto id: %v, err=%v", k, err)
				continue
			}

			if !tree.Deleted || includeDeleted {
				ids = append(ids, tree.TreeId)
			}
		}
	}

	return ids, nil
}

func (t *adminTX) ListTrees(ctx context.Context, includeDeleted bool) ([]*trillian.Tree, error) {
	c := t.tx.Bucket([]byte(TreeBucket)).Cursor()

	var trees []*trillian.Tree
	for k, v := c.First(); k != nil; k, v = c.Next() {
		// Ensure to skip over any nested buckets
		if k != nil && v != nil {
			tree := trillian.Tree{}
			if err := proto.Unmarshal(v, &tree); err != nil {
				glog.Warningf("Failed to unmarshal tree proto id: %v, err=%v", k, err)
				continue
			}

			if !tree.Deleted || includeDeleted {
				trees = append(trees, &tree)
			}
		}
	}

	return trees, nil
}

func (t *adminTX) CreateTree(ctx context.Context, tree *trillian.Tree) (*trillian.Tree, error) {
	if err := storage.ValidateTreeForCreation(ctx, tree); err != nil {
		return nil, err
	}

	id, err := storage.NewTreeID()
	if err != nil {
		return nil, err
	}

	// Use the time truncated-to-millis throughout, as that's what's stored.
	nowMillis := toMillisSinceEpoch(time.Now())
	now := fromMillisSinceEpoch(nowMillis)

	newTree := *tree
	newTree.TreeId = id
	if newTree.CreateTime, err = ptypes.TimestampProto(now); err != nil {
		return nil, fmt.Errorf("failed to build create time: %v", err)
	}
	if newTree.UpdateTime, err = ptypes.TimestampProto(now); err != nil {
		return nil, fmt.Errorf("failed to build update time: %v", err)
	}

	v, err := proto.Marshal(&newTree)
	if err != nil {
		return nil, err
	}
	if err := t.tb.Put(keyOf(newTree.TreeId), v); err != nil {
		return nil, err
	}

	return &newTree, nil
}

func (t *adminTX) UpdateTree(ctx context.Context, treeID int64, updateFunc func(*trillian.Tree)) (*trillian.Tree, error) {
	tree, err := t.GetTree(ctx, treeID)
	if err != nil {
		return nil, err
	}

	beforeUpdate := *tree
	updateFunc(tree)
	if err := storage.ValidateTreeForUpdate(ctx, &beforeUpdate, tree); err != nil {
		return nil, err
	}

	// Use the time truncated-to-millis throughout, as that's what's stored.
	nowMillis := toMillisSinceEpoch(time.Now())
	now := fromMillisSinceEpoch(nowMillis)
	tree.UpdateTime, err = ptypes.TimestampProto(now)
	if err != nil {
		return nil, fmt.Errorf("failed to build update time: %v", err)
	}

	v, err := proto.Marshal(tree)
	if err != nil {
		return nil, err
	}
	if err := t.tb.Put(keyOf(tree.TreeId), v); err != nil {
		return nil, err
	}

	return tree, nil
}

func (t *adminTX) SoftDeleteTree(ctx context.Context, treeID int64) (*trillian.Tree, error) {
	return t.updateDeleted(ctx, treeID, true /* deleted */)
}

func (t *adminTX) UndeleteTree(ctx context.Context, treeID int64) (*trillian.Tree, error) {
	return t.updateDeleted(ctx, treeID, false /* deleted */)
}

// updateDeleted updates the Deleted and DeleteTimeMillis fields of the specified tree.
func (t *adminTX) updateDeleted(ctx context.Context, treeID int64, deleted bool) (*trillian.Tree, error) {
	if err := validateDeleted(ctx, t, treeID, !deleted); err != nil {
		return nil, err
	}
	// This can't be done via t.UpdateTree() as the validation checks in the admin layer above
	// this code prevent the deleted field from being changed.
	tree, err := t.GetTree(ctx, treeID)
	if err != nil {
		return nil, err
	}
	tree.Deleted = deleted
	if deleted {
		tree.DeleteTime = ptypes.TimestampNow()
	} else {
		// The tree has been undeleted.
		tree.DeleteTime = nil
	}

	v, err := proto.Marshal(tree)
	if err != nil {
		return nil, err
	}
	if err := t.tb.Put(keyOf(tree.TreeId), v); err != nil {
		return nil, err
	}

	return tree, nil
}

func (t *adminTX) HardDeleteTree(ctx context.Context, treeID int64) error {
	if err := validateDeleted(ctx, t, treeID, true /* wantDeleted */); err != nil {
		return err
	}

	return t.tb.Delete(keyOf(treeID))
}

func validateDeleted(_ context.Context, tx *adminTX, treeID int64, wantDeleted bool) error {
	v := tx.tb.Get(keyOf(treeID))
	if v == nil {
		return status.Errorf(codes.NotFound, "validateDeleted: tree %v not found", treeID)
	}

	tree := trillian.Tree{}
	if err := proto.Unmarshal(v, &tree); err != nil {
		return err
	}
	switch deleted := tree.Deleted; {
	case wantDeleted && !deleted:
		return status.Errorf(codes.FailedPrecondition, "tree %v is not soft deleted", treeID)
	case !wantDeleted && deleted:
		return status.Errorf(codes.FailedPrecondition, "tree %v already soft deleted", treeID)
	}
	return nil
}

func toMillisSinceEpoch(t time.Time) int64 {
	return t.UnixNano() / 1000000
}

func fromMillisSinceEpoch(ts int64) time.Time {
	secs := int64(ts / 1000)
	msecs := int64(ts % 1000)
	return time.Unix(secs, msecs*1000000)
}
