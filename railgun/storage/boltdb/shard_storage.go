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
	"bytes"
	"errors"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/trillian-examples/railgun/shard"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ShardConfigBucket = "ShardConfig"
	ShardConfigKey = "Config"
)


func NewShardStorage(db *bolt.DB) *boltShardStorage {
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(ShardConfigBucket))
		return err
	})

	if err != nil {
		glog.Fatalf("Could not create ShardConfig bucket: %v", err)
	}
	return &boltShardStorage{db: db}
}

type boltShardStorage struct {
	db *bolt.DB
}

func (b *boltShardStorage) CreateShardConfig(config *shard.ShardProto) error {
	if err := canCreate(config); err != nil {
		return err
	}
	tx, err := b.db.Begin(true)
	if err != nil {
		return nil
	}
	defer tx.Commit()
	_, err = b.getConfig(tx)
	if err != nil && status.Code(err) == codes.NotFound {
		return err
	}
	if err == nil {
		return errors.New("Config has already been created")
	}

	return b.putConfig(tx, config)
}

func (b *boltShardStorage) UpdateShardConfig(config *shard.ShardProto) error {
	tx, err := b.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Commit()
	prevConfig, err := b.getConfig(tx)
	if err != nil {
		return err
	}
	if err := canUpdate(prevConfig, config); err != nil {
		return err
	}
	return b.putConfig(tx, config)
}

func (b *boltShardStorage) DeleteShardConfig() error {
	tx, err := b.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Commit()
	if _, err := b.getConfig(tx); err != nil {
		// This includes the case where the config doesn't exist.
		return err
	}

	cb := tx.Bucket([]byte(ShardConfigBucket))
	return cb.Delete([]byte(ShardConfigKey))
}

func (b *boltShardStorage) GetShardConfig() (*shard.ShardProto, error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return b.getConfig(tx)
}

func (b *boltShardStorage) getConfig(tx *bolt.Tx) (*shard.ShardProto, error) {
	cb := tx.Bucket([]byte(ShardConfigBucket))
	blob := cb.Get([]byte(ShardConfigKey))
	if blob == nil {
		// We don't have a config
		return nil, status.Error(codes.NotFound, "Config has not been created")
	}
	var config shard.ShardProto
	if err := proto.Unmarshal(blob, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

func (b *boltShardStorage) putConfig(tx *bolt.Tx, config *shard.ShardProto) error {
	blob, err := proto.Marshal(config)
	if err != nil {
		return err
	}
	cb := tx.Bucket([]byte(ShardConfigBucket))
	return cb.Put([]byte(ShardConfigKey), blob)
}

func canCreate(config *shard.ShardProto) error {
	if len(config.Uuid) == 0 {
		return errors.New("a UUID must be set")
	}
	if len(config.KeyHash) == 0 {
		return errors.New("a KeyHash must be set")
	}
	if config.State != shard.ShardState_SHARD_STATE_NEEDS_INIT {
		return fmt.Errorf("can't create config in state: %v", config.State)
	}

	return nil
}

func canUpdate(c1, c2 *shard.ShardProto) error {
	// The UUID and create time cannot be modified
	// TODO(Martin2112): Possibly include the key hash as well
	if !bytes.Equal(c1.Uuid, c2.Uuid) || len(c2.Uuid) == 0 {
		return fmt.Errorf("can't update Uuid to: %v", c2.Uuid)
	}

	if ptypes.TimestampString(c1.CreateTime) != ptypes.TimestampString(c2.CreateTime) {
		return fmt.Errorf("can't update CreateTime to: %v", ptypes.TimestampString(c2.CreateTime))
	}

	// State can only be changed in limited ways
	if c2.State == shard.ShardState_SHARD_STATE_UNKNOWN || c2.State == shard.ShardState_SHARD_STATE_NEEDS_INIT {
		return fmt.Errorf("can't update ShardState to: %v", c2.State)
	}

	return nil
}
