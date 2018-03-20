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

package shard

import (
	"crypto"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/trillian-examples/railgun/storage"
	tcrypto "github.com/google/trillian/crypto"
	"github.com/google/trillian/crypto/keys/der"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Opts struct {
	skipSignatureChecks bool // for use in tests etc. only.
}

type shardProvisioningServer struct {
	shardStorage  storage.ShardStorage
	opts          Opts
	authorizedKey crypto.PublicKey
}

func NewShardProvisioningServer(s storage.ShardStorage, key crypto.PublicKey, o Opts) *shardProvisioningServer {
	return &shardProvisioningServer{shardStorage: s, authorizedKey: key, opts: o}
}

func redactConfig(s *ShardProto) {
	s.Uuid = nil
	s.PrivateKey = nil
}

func (s *shardProvisioningServer) Provision(request *ShardProvisionRequest) (*ShardProvisionResponse, error) {
	// Check the signature before processing the request. This can be skipped - with the
	// obvious risks if this option is set in production.
	if !s.opts.skipSignatureChecks {
		if err := tcrypto.Verify(s.authorizedKey, request.ShardConfig, request.ConfigSig); err != nil {
			return nil, status.Errorf(codes.PermissionDenied, "failed to verify signature: %v", err)
		}
	}

	var config ShardProto
	if err := proto.Unmarshal(request.GetShardConfig(), &config); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "config did not unmarshal: %v", err)
	}

	// Merge the supplied config with the one we created. Don't overwrite uuids or keys.
	redactConfig(&config)
	config.PublicKey = nil
	config.CreateTime = ptypes.TimestampNow()
	// This is a transition to the active state.
	config.State = ShardState_SHARD_STATE_ACTIVE

	cfg, err := s.shardStorage.GetShardConfig()
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "shard config not initialized: %v", err)
	}
	proto.Merge(cfg, &config)
	if err := s.shardStorage.UpdateShardConfig(cfg); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to write new shard config: %v", err)
	}

	cs, err := der.FromProto(cfg.PrivateKey)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create signer: %v", err)
	}
	signer := tcrypto.NewSHA256Signer(cs)

	// Don't leak private key in the response. Leave public key alone.
	redactConfig(cfg)
	blob, err := proto.Marshal(cfg)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal response: %v", err)
	}
	sig, err := signer.Sign(blob)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to sign response: %v", err)
	}

	return &ShardProvisionResponse{ProvisionedConfig: blob, ConfigSig: sig}, nil
}
