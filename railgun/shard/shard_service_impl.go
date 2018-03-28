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

package shard

import (
	"context"
	"crypto"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/trillian-examples/railgun/shard/shardproto"
	"github.com/google/trillian-examples/railgun/storage"
	tcrypto "github.com/google/trillian/crypto"
	"github.com/google/trillian/crypto/keys/der"
	"github.com/google/trillian/util"
	cache "github.com/patrickmn/go-cache"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Opts struct {
	SkipSignatureChecks bool // for use in tests etc. only.
	TokenExpiry         time.Duration
}

type shardServiceServer struct {
	shardStorage  storage.ShardStorage
	opts          Opts
	authorizedKey crypto.PublicKey
	timeSource    util.TimeSource
	tokenCache    *cache.Cache
}

func NewShardServiceServer(s storage.ShardStorage, authorizedKey crypto.PublicKey, ts util.TimeSource, o Opts) *shardServiceServer {
	return &shardServiceServer{
		shardStorage:  s,
		authorizedKey: authorizedKey,
		timeSource:    ts,
		tokenCache:    cache.New(o.TokenExpiry, 2*o.TokenExpiry),
		opts:          o,
	}
}

func redactConfig(s *shardproto.ShardProto) {
	s.PrivateKey = nil
}

func (s *shardServiceServer) ProvisionHandshake(_ context.Context, _ *ProvisionHandshakeRequest) (*ProvisionHandshakeResponse, error) {
	b := make([]byte, 16)
	if n, err := rand.Read(b); err != nil || n != len(b) {
		return nil, status.Errorf(codes.Internal, "failed to create token")
	}

	token := hex.EncodeToString(b)
	s.tokenCache.Set(token, token, cache.DefaultExpiration)

	return &ProvisionHandshakeResponse{Token: token}, nil
}

func (s *shardServiceServer) GetConfig(_ context.Context, _ *GetShardConfigRequest) (*GetShardConfigResponse, error) {
	cfg, err := s.shardStorage.GetShardConfig()
	if status.Code(err) == codes.NotFound {
		return nil, err
	}
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "shard config not initialized: %v", err)
	}

	cs, err := der.FromProto(cfg.PrivateKey)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create signer: %v", err)
	}
	signer := tcrypto.NewSHA256Signer(cs)

	// Don't leak private key in the response. Leave public key alone.
	redactConfig(cfg)
	blob, sig, err := marshallAndSignConfig(signer, cfg)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal response: %v", err)
	}

	return &GetShardConfigResponse{ProvisionedConfig: blob, ConfigSig: sig}, nil
}

func (s *shardServiceServer) Provision(_ context.Context, request *ShardProvisionRequest) (*ShardProvisionResponse, error) {
	// Check the signature before processing the request. This can be skipped - with the
	// obvious risks if this option is set in production.
	if !s.opts.SkipSignatureChecks {
		if err := tcrypto.Verify(s.authorizedKey, crypto.SHA256, request.GetShardConfig(), request.GetConfigSig()); err != nil {
			return nil, status.Errorf(codes.PermissionDenied, "failed to verify signature: %v", err)
		}
	}

	// Unwrap the proto and check that we issued the token recently enough.
	var wp WrappedConfig
	if err := proto.Unmarshal(request.GetShardConfig(), &wp); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "wrapped config did not unmarshal: %v", err)
	}
	if _, ok := s.tokenCache.Get(wp.Token); !ok {
		// We don't seem to have issued the token.
		return nil, status.Error(codes.PermissionDenied, "missing or invalid token in request")
	}
	// The token has been used.
	s.tokenCache.Delete(wp.Token)

	var config shardproto.ShardProto
	if err := proto.Unmarshal(wp.GetShardConfig(), &config); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "config did not unmarshal: %v", err)
	}

	// Merge the supplied config with the one we created. Don't overwrite uuids or keys.
	redactConfig(&config)
	config.PublicKey = nil
	config.CreateTime = ptypes.TimestampNow()
	// This is a transition to the active state.
	config.State = shardproto.ShardState_SHARD_STATE_ACTIVE

	cfg, err := s.shardStorage.GetShardConfig()
	if cfg == nil || err != nil {
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

	blob, sig, err := marshallAndSignConfig(signer, cfg)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal response: %v", err)
	}

	return &ShardProvisionResponse{ProvisionedConfig: blob, ConfigSig: sig}, nil
}

func marshallAndSignConfig(signer *tcrypto.Signer, cfg *shardproto.ShardProto) ([]byte, []byte, error) {
	// Don't leak private key in the response. Leave public key alone.
	redactConfig(cfg)
	blob, err := proto.Marshal(cfg)
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "failed to marshal response: %v", err)
	}
	sig, err := signer.Sign(blob)
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "failed to sign response: %v", err)
	}

	return blob, sig, nil
}
