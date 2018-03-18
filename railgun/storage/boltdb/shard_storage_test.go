//   Copyright 2018 Google Inc. All Rights Reserved.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package boltdb

import (
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/trillian-examples/railgun/shard"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type configTest struct {
	desc     string
	cfg      *shard.ShardProto
	wantErr  bool
	errCode  codes.Code
	updateFn func(shardProto *shard.ShardProto)
}

var (
	fakeTime  = time.Date(2018, 3, 18, 10, 12, 0, 0, time.UTC)
	createCfg = &shard.ShardProto{
		Description: "a valid config for create",
		Uuid:        []byte("uuid"),
		State:       shard.ShardState_SHARD_STATE_NEEDS_INIT,
		KeyHash:     []byte("hash"),
	}
)

func TestGetConfigNotCreated(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	s := NewShardStorage(db)

	if _, err := s.GetShardConfig(); err == nil || status.Code(err) != codes.NotFound {
		t.Errorf("GetShardConfig() = %v, want: err (NotFound)", err)
	}
}

func TestGetConfig(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	s := NewShardStorage(db)

	if err := s.CreateShardConfig(createCfg); err != nil {
		t.Errorf("CreateShardConfig() = %v, want: nil", err)
	}

	readCfg, err := s.GetShardConfig()
	if err != nil {
		t.Errorf("GetShardConfig() = %v, want: nil", err)
	}

	if !proto.Equal(readCfg, createCfg) {
		t.Errorf("GetShardConfig() mismatched got: %v, want: %v", readCfg, createCfg)
	}
}

func TestCreateConfig(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	s := NewShardStorage(db)

	if err := s.CreateShardConfig(createCfg); err != nil {
		t.Errorf("CreateShardConfig() = %v, want: nil", err)
	}

	if cfg, err := s.GetShardConfig(); err != nil || !proto.Equal(createCfg, cfg) {
		t.Errorf("GetShardConfig() read mismatch got: %v, want: %v", cfg, createCfg)
	}
}

func TestCreateConfigTwice(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	s := NewShardStorage(db)

	if err := s.CreateShardConfig(createCfg); err != nil {
		t.Errorf("CreateShardConfig() = %v, want: nil", err)
	}
	// Second attempt should fail.
	if err := s.CreateShardConfig(createCfg); err == nil {
		t.Error("CreateShardConfig() = nil, want: err")
	}
}

func TestCreateFailures(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	s := NewShardStorage(db)

	createTests := []configTest{
		{
			desc:    "missing uuid",
			cfg:     &shard.ShardProto{KeyHash: []byte("hash"), State: shard.ShardState_SHARD_STATE_NEEDS_INIT},
			wantErr: true,
			errCode: codes.FailedPrecondition,
		},
		{
			desc:    "missing hash",
			cfg:     &shard.ShardProto{KeyHash: []byte("hash"), State: shard.ShardState_SHARD_STATE_NEEDS_INIT},
			wantErr: true,
			errCode: codes.FailedPrecondition,
		},
		{
			desc:    "unknown state",
			cfg:     &shard.ShardProto{KeyHash: []byte("hash"), Uuid: []byte("uuid"), State: shard.ShardState_SHARD_STATE_UNKNOWN},
			wantErr: true,
			errCode: codes.FailedPrecondition,
		},
		{
			desc:    "active state",
			cfg:     &shard.ShardProto{KeyHash: []byte("hash"), Uuid: []byte("uuid"), State: shard.ShardState_SHARD_STATE_ACTIVE},
			wantErr: true,
			errCode: codes.FailedPrecondition,
		},
		{
			desc:    "failed state",
			cfg:     &shard.ShardProto{KeyHash: []byte("hash"), Uuid: []byte("uuid"), State: shard.ShardState_SHARD_STATE_FAILED},
			wantErr: true,
			errCode: codes.FailedPrecondition,
		},
	}

	for _, test := range createTests {
		t.Run(test.desc, func(t *testing.T) {
			if err := s.DeleteShardConfig(); err != nil && status.Code(err) != codes.NotFound {
				t.Fatalf("Failed to cleanup previous config: %v", err)
			}
			err := s.CreateShardConfig(test.cfg)
			if err == nil || status.Code(err) != test.errCode {
				t.Errorf("CreateShardConfig(%s) err=%v, want: err (code %v)", test.desc, err, test.errCode)
			}
		})
	}
}

func TestUpdateShardConfig(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	s := NewShardStorage(db)

	updateTests := []configTest{
		{
			desc:     "init state",
			updateFn: func(s *shard.ShardProto) { s.State = shard.ShardState_SHARD_STATE_NEEDS_INIT },
			wantErr:  true,
			errCode:  codes.FailedPrecondition,
		},
		{
			desc:     "unknown state",
			updateFn: func(s *shard.ShardProto) { s.State = shard.ShardState_SHARD_STATE_UNKNOWN },
			wantErr:  true,
			errCode:  codes.FailedPrecondition,
		},
		{
			desc:     "change uuid",
			updateFn: func(s *shard.ShardProto) { s.Uuid = []byte("different uuid") },
			wantErr:  true,
			errCode:  codes.FailedPrecondition,
		},
		{
			desc: "change create time",
			updateFn: func(s *shard.ShardProto) {
				pb, err := ptypes.TimestampProto(fakeTime)
				if err != nil {
					t.Fatalf("Failed to create timestamp proto: %v", err)
				}
				s.CreateTime = pb
			},
			wantErr: true,
			errCode: codes.FailedPrecondition,
		},
		{
			desc: "change update time",
			updateFn: func(s *shard.ShardProto) {
				pb, err := ptypes.TimestampProto(fakeTime)
				if err != nil {
					t.Fatalf("Failed to create timestamp proto: %v", err)
				}
				s.State = shard.ShardState_SHARD_STATE_ACTIVE
				s.UpdateTime = pb
			},
		},
		{
			desc: "change desc",
			updateFn: func(s *shard.ShardProto) {
				s.State = shard.ShardState_SHARD_STATE_ACTIVE
				s.Description = "a different description"
			},
		},
		{
			desc: "change hash",
			updateFn: func(s *shard.ShardProto) {
				s.State = shard.ShardState_SHARD_STATE_ACTIVE
				s.KeyHash = []byte("a different hash")
			},
		},
		{
			desc: "change state failed",
			updateFn: func(s *shard.ShardProto) {
				s.State = shard.ShardState_SHARD_STATE_FAILED
			},
		},
	}

	for _, test := range updateTests {
		t.Run(test.desc, func(t *testing.T) {
			// Start with the basic config and update it appropriately.
			cfg := proto.Clone(createCfg).(*shard.ShardProto)
			test.updateFn(cfg)

			if err := s.DeleteShardConfig(); err != nil && status.Code(err) != codes.NotFound {
				t.Fatalf("Failed to cleanup previous config: %v", err)
			}
			if err := s.CreateShardConfig(createCfg); err != nil {
				t.Fatalf("Failed to create test config: %v", err)
			}

			err := s.UpdateShardConfig(cfg)
			if test.wantErr {
				if err == nil || status.Code(err) != test.errCode {
					t.Errorf("UpdateShardConfig(%s) err=%v, want: err (code %v)", test.desc, err, test.errCode)
				}
			} else {
				if err != nil {
					t.Errorf("UpdateShardConfig(%s) err=%v, want: nil", test.desc, err)
				}
				if readCfg, err := s.GetShardConfig(); err != nil || !proto.Equal(readCfg, cfg) {
					t.Errorf("GetShardConfig() read mismatch got: %v, want: %v", cfg, createCfg)
				}
			}
		})
	}
}

func TestDeleteNonExistentConfig(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	s := NewShardStorage(db)

	if err := s.DeleteShardConfig(); err == nil || status.Code(err) != codes.NotFound {
		t.Errorf("DeleteShardConfig() = %v, want: nil", err)
	}
}

func TestDeleteConfig(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	s := NewShardStorage(db)

	if err := s.CreateShardConfig(createCfg); err != nil {
		t.Errorf("CreateShardConfig() = %v, want: nil", err)
	}

	// Ensure it exists before the deletion.
	if _, err := s.GetShardConfig(); err != nil {
		t.Errorf("GetShardConfig() = %v, want: nil", err)
	}

	if err := s.DeleteShardConfig(); err != nil {
		t.Errorf("DeleteShardConfig() = %v, want: nil", err)
	}

	if _, err := s.GetShardConfig(); err == nil || status.Code(err) != codes.NotFound {
		t.Errorf("GetShardConfig() = %v, want: err (NotFound)", err)
	}
}
