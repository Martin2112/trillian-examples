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

package storage

import (
	"crypto"

	"github.com/golang/glog"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/trillian-examples/railgun/shard/shardproto"
	"github.com/google/trillian/crypto/keys/der"
	"github.com/google/trillian/crypto/keyspb"
	"github.com/google/uuid"
)

func NewCoordinatorConfig() (*shardproto.ShardProto, error) {
	newUuid, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}
	uuidBytes, err := newUuid.MarshalBinary()

	return &shardproto.ShardProto{
		State:      shardproto.ShardState_SHARD_STATE_ACTIVE,
		Uuid:       uuidBytes,
		CreateTime: ptypes.TimestampNow(),
	}, nil
}

func NewShardConfig(authorizedKey crypto.PublicKey) (*shardproto.ShardProto, error) {
	newUuid, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}
	uuidBytes, err := newUuid.MarshalBinary()
	if err != nil {
		return nil, err
	}
	keyBytes, err := der.MarshalPublicKey(authorizedKey)
	if err != nil {
		return nil, err
	}

	spec := &keyspb.Specification{}
	spec.Params = &keyspb.Specification_EcdsaParams{
		EcdsaParams: &keyspb.Specification_ECDSA{},
	}
	pKey, err := der.NewProtoFromSpec(spec)
	if err != nil {
		glog.Fatalf("Failed to generate keys: %v", err)
	}
	sig, err := der.FromProto(pKey)
	if err != nil {
		glog.Fatalf("Failed to create signer from key: %v", err)
	}
	pubKey, err := der.ToPublicProto(sig.Public())
	if err != nil {
		glog.Fatalf("Failed to get public key: %v", err)
	}

	return &shardproto.ShardProto{
		State:      shardproto.ShardState_SHARD_STATE_NEEDS_INIT,
		Uuid:       uuidBytes,
		KeyHash:    keyBytes,
		CreateTime: ptypes.TimestampNow(),
		PrivateKey: pKey,
		PublicKey:  pubKey,
	}, nil
}
