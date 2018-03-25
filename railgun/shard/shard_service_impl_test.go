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
	"errors"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/google/trillian-examples/railgun/shard/shardproto"
	"github.com/google/trillian-examples/railgun/storage"
	"github.com/google/trillian-examples/railgun/storage/mock_storage"
	tcrypto "github.com/google/trillian/crypto"
	"github.com/google/trillian/crypto/keys/der"
	"github.com/google/trillian/crypto/keys/pem"
	"github.com/google/trillian/crypto/keyspb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var validCfg = &shardproto.ShardProto{
	Uuid:  []byte("somebytes"),
	State: shardproto.ShardState_SHARD_STATE_ACTIVE,
}

func TestProvisionHandshake(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ss := mock_storage.NewMockShardStorage(ctrl)
	ss.EXPECT().GetShardConfig().Times(0).Return(nil, errors.New("not expected"))
	s, err := createServer(ss)
	if err != nil {
		t.Fatalf("Failed to setup server: %v", err)
	}

	// Try multiple times and ensure we get unique tokens.
	tokenMap := make(map[string]bool)
	for i := 1; i < 10; i++ {
		resp, err := s.ProvisionHandshake(context.Background(), &ProvisionHandshakeRequest{})
		if err != nil {
			t.Fatalf("ProvisionHandshake()=%v,%v, want token,nil", resp, err)
		}
		token := resp.GetToken()
		if len(token) < 32 {
			t.Errorf("ProvisionHandshake() len(token)=%v, want: >=32", len(token))
		}
		if _, found := tokenMap[token]; found {
			t.Fatalf("ProvisionHandshake() returned duplicate token: %v", token)
		}
	}
}

func TestGetConfig(t *testing.T) {
	type getTest struct {
		desc       string
		storageCfg *shardproto.ShardProto
		storageErr error
		wantErr    bool
		wantCode   codes.Code
	}

	tests := []getTest{
		{
			desc:       "StorageErr",
			storageCfg: nil,
			storageErr: errors.New("GetConfig() failed"),
			wantErr:    true,
			wantCode:   codes.FailedPrecondition,
		},
		{
			desc:       "NotFound",
			storageCfg: nil,
			storageErr: status.Errorf(codes.NotFound, "simulate NotFound from storage"),
			wantErr:    true,
			wantCode:   codes.NotFound,
		},
		{
			desc:       "OK",
			storageCfg: validCfg,
			storageErr: nil,
			wantErr:    false,
		},
	}

	key, err := genKey()
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ss := mock_storage.NewMockShardStorage(ctrl)
			var cfgWithKey *shardproto.ShardProto
			if test.storageCfg != nil {
				cfgWithKey = proto.Clone(test.storageCfg).(*shardproto.ShardProto)
				cfgWithKey.PrivateKey = key
			}
			ss.EXPECT().GetShardConfig().Times(1).Return(cfgWithKey, test.storageErr)
			s, err := createServer(ss)
			if err != nil {
				t.Fatalf("Failed to setup server: %v", err)
			}
			resp, err := s.GetConfig(context.Background(), &GetShardConfigRequest{})
			if test.wantErr {
				if err == nil || status.Code(err) != test.wantCode {
					t.Errorf("GetConfig()=%v, %v, want:err with code %v", resp, err, test.wantCode)
				}
			} else {
				if err != nil {
					t.Errorf("GetConfig()=%v, %v, want: resp, nil", resp, err)
				}
				signer, err := der.FromProto(key)
				if err != nil {
					t.Fatalf("Failed to create signer: %v", err)
				}
				if err := tcrypto.Verify(signer.Public(), crypto.SHA256, resp.ProvisionedConfig, resp.ConfigSig); err != nil {
					t.Errorf("GetConfig() response not signed: %v, sig: %v", err, resp.ConfigSig)
				}
				var gotCfg shardproto.ShardProto
				if err := proto.Unmarshal(resp.ProvisionedConfig, &gotCfg); err != nil {
					t.Errorf("GetConfig() response did not unmarshal: %v", err)
				}

				if got, want := &gotCfg, test.storageCfg; !proto.Equal(got, want) {
					t.Errorf("GetConfig()=%v, want: %v", got, want)
				}
			}

			ctrl.Finish()
		})
	}
}

func createServer(ss storage.ShardStorage) (*shardServiceServer, error) {
	key, err := pem.ReadPublicKeyFile("../testdata/keys/railgun-server.pubkey.pem")
	if err != nil {
		return nil, err
	}
	return NewShardServiceServer(ss, key, Opts{}), nil
}

func genKey() (*keyspb.PrivateKey, error) {
	spec := &keyspb.Specification{}
	spec.Params = &keyspb.Specification_EcdsaParams{
		EcdsaParams: &keyspb.Specification_ECDSA{},
	}
	return der.NewProtoFromSpec(spec)
}
