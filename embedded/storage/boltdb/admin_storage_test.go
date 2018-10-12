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
	"bytes"
	"context"
	"io/ioutil"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/trillian"
	"github.com/google/trillian/crypto/keyspb"
	"github.com/google/trillian/storage"
	"github.com/google/trillian/storage/testonly"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	dir string
)

func TestBoltAdminStorage(t *testing.T) {
	tester := &testonly.AdminStorageTester{NewAdminStorage: func() storage.AdminStorage {
		db := createTestDB(t)
		return NewAdminStorage(db)
	}}
	tester.RunAllTests(t)
}

func TestCreateTreeInvalidStates(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	s := NewAdminStorage(db)
	ctx := context.Background()

	states := []trillian.TreeState{trillian.TreeState_DRAINING, trillian.TreeState_FROZEN}

	for _, state := range states {
		inTree := proto.Clone(testonly.LogTree).(*trillian.Tree)
		inTree.TreeState = state
		if _, err := storage.CreateTree(ctx, s, inTree); err == nil {
			t.Errorf("CreateTree() state: %v got: nil want: err", state)
		}
	}
}

func TestAdminTX_StorageSettingsSupported(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	s := NewAdminStorage(db)
	ctx := context.Background()

	settings, err := ptypes.MarshalAny(&keyspb.PEMKeyFile{})
	if err != nil {
		t.Fatalf("Error marshaling proto: %v", err)
	}

	tests := []struct {
		desc string
		// fn attempts to either create or update a tree with a non-nil, valid Any proto
		// on Tree.StorageSettings. It's expected to return an error.
		fn func(storage.AdminStorage) error
	}{
		{
			desc: "CreateTree",
			fn: func(s storage.AdminStorage) error {
				tree := *testonly.LogTree
				tree.StorageSettings = settings
				_, err := storage.CreateTree(ctx, s, &tree)
				return err
			},
		},
		{
			desc: "UpdateTree",
			fn: func(s storage.AdminStorage) error {
				tree, err := storage.CreateTree(ctx, s, testonly.LogTree)
				if err != nil {
					t.Fatalf("CreateTree() failed with err = %v", err)
				}
				_, err = storage.UpdateTree(ctx, s, tree.TreeId, func(tree *trillian.Tree) { tree.StorageSettings = settings })
				return err
			},
		},
	}
	for _, test := range tests {
		if err := test.fn(s); err != nil {
			t.Errorf("%v: err = %v, want nil", test.desc, err)
		}
	}
}

func TestAdminTX_HardDeleteTree(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	s := NewAdminStorage(db)
	ctx := context.Background()

	tree, err := storage.CreateTree(ctx, s, testonly.LogTree)
	if err != nil {
		t.Fatalf("CreateTree() returned err = %v", err)
	}

	if err := s.ReadWriteTransaction(ctx, func(ctx context.Context, tx storage.AdminTX) error {
		if _, err := tx.SoftDeleteTree(ctx, tree.TreeId); err != nil {
			return err
		}
		return tx.HardDeleteTree(ctx, tree.TreeId)
	}); err != nil {
		t.Fatalf("ReadWriteTransaction() returned err = %v", err)
	}

	// Try to read it back and we expect that it's not found
	s.ReadWriteTransaction(ctx, func(_ context.Context, tx storage.AdminTX) error {
		_, err := tx.GetTree(ctx, tree.TreeId)
		s, ok := status.FromError(err)
		if err == nil || !ok || s.Code() != codes.NotFound {
			t.Fatalf("tx.GetTree()=%v, want err=NotFound", err)
		}
		return nil
	})
}

func TestKeyOf(t *testing.T) {
	if got, want := keyOf(0x12345678), []byte{0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38}; !bytes.Equal(got, want) {
		t.Errorf("keyOf(0x12345678), got: %v, want: %v", got, want)
	}
}

func createTestDB(t *testing.T) *bolt.DB {
	once.Do(func() {
		var err error
		dir, err = ioutil.TempDir("", "test")
		if err != nil {
			glog.Fatalf("Failed to create tmp dir for tests: %v", err)
		}
	})

	dbf, err := ioutil.TempFile(dir, t.Name())
	if err != nil {
		glog.Fatalf("Failed to create tmp file for tests: %v", err)
	}

	db, err := OpenDB(dbf.Name(), &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		glog.Fatalf("Failed to open writable bolt db for tests: %v", err)
	}

	return db
}
