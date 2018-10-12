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
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/google/trillian-examples/railgun/shard/shardproto"
	"github.com/google/trillian-examples/railgun/storage"
	"github.com/google/trillian-examples/railgun/storage/mock_storage"
	tcrypto "github.com/google/trillian/crypto"
	"github.com/google/trillian/crypto/keys/der"
	"github.com/google/trillian/crypto/keys/pem"
	"github.com/google/trillian/crypto/keyspb"
	"github.com/google/trillian/util"
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
	s, err := createServer(ss, new(util.SystemTimeSource))
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
			s, err := createServer(ss, new(util.SystemTimeSource))
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

func TestProvision(t *testing.T) {
	type provTest struct {
		desc               string
		storageCfg         *shardproto.ShardProto
		storageErr         error
		storageTimes       int
		storageUpdateErr   error
		storageUpdateTimes int
		provCfg            *shardproto.ShardProto
		signCfg            bool
		getToken           bool
		sendBlob           []byte
		sendInnerBlob      []byte
		wantErr            bool
		errStr             string
		wantCode           codes.Code
		wantCfg            *shardproto.ShardProto
		checkSig           bool
	}

	tests := []provTest{
		{
			desc:     "not signed",
			provCfg:  &shardproto.ShardProto{State: shardproto.ShardState_SHARD_STATE_ACTIVE},
			wantErr:  true,
			errStr:   "verify sig",
			wantCode: codes.PermissionDenied,
		},
		{
			desc:     "no token",
			provCfg:  &shardproto.ShardProto{State: shardproto.ShardState_SHARD_STATE_ACTIVE},
			signCfg:  true,
			wantErr:  true,
			errStr:   "invalid token",
			wantCode: codes.PermissionDenied,
		},
		{
			desc:     "bad cfg in request",
			provCfg:  &shardproto.ShardProto{State: shardproto.ShardState_SHARD_STATE_ACTIVE},
			sendBlob: []byte("not a valid wire proto"),
			signCfg:  true,
			wantErr:  true,
			errStr:   "did not unmarshal",
			wantCode: codes.FailedPrecondition,
		},
		{
			desc:          "bad inner cfg in request",
			provCfg:       &shardproto.ShardProto{State: shardproto.ShardState_SHARD_STATE_ACTIVE},
			sendInnerBlob: []byte("not a valid wire proto"),
			signCfg:       true,
			getToken:      true,
			wantErr:       true,
			errStr:        "did not unmarshal",
			wantCode:      codes.FailedPrecondition,
		},
		{
			desc:         "nil stored config",
			provCfg:      &shardproto.ShardProto{State: shardproto.ShardState_SHARD_STATE_ACTIVE},
			storageTimes: 1,
			signCfg:      true,
			getToken:     true,
			wantErr:      true,
			errStr:       "not init",
			wantCode:     codes.FailedPrecondition,
		},
		{
			desc:         "error on stored config",
			provCfg:      &shardproto.ShardProto{State: shardproto.ShardState_SHARD_STATE_ACTIVE},
			storageTimes: 1,
			storageErr:   errors.New("failed to read config"),
			signCfg:      true,
			getToken:     true,
			wantErr:      true,
			errStr:       "not init",
			wantCode:     codes.FailedPrecondition,
		},
		{
			desc:               "error writing config",
			provCfg:            &shardproto.ShardProto{State: shardproto.ShardState_SHARD_STATE_ACTIVE},
			storageTimes:       1,
			storageCfg:         &shardproto.ShardProto{State: shardproto.ShardState_SHARD_STATE_NEEDS_INIT},
			storageUpdateTimes: 1,
			storageUpdateErr:   errors.New("update failed"),
			signCfg:            true,
			getToken:           true,
			wantErr:            true,
			errStr:             "failed to write",
			wantCode:           codes.Internal,
		},
		{
			desc: "ok",
			provCfg: &shardproto.ShardProto{
				State:       shardproto.ShardState_SHARD_STATE_ACTIVE,
				Description: "a provisioned shard",
			},
			storageTimes: 1,
			storageCfg: &shardproto.ShardProto{
				State: shardproto.ShardState_SHARD_STATE_NEEDS_INIT,
				Uuid:  []byte("uuid"),
			},
			storageUpdateTimes: 1,
			signCfg:            true,
			getToken:           true,
			wantCfg: &shardproto.ShardProto{
				State:       shardproto.ShardState_SHARD_STATE_ACTIVE,
				Uuid:        []byte("uuid"),
				Description: "a provisioned shard",
			},
			checkSig: true,
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
			ss.EXPECT().GetShardConfig().Times(test.storageTimes).Return(cfgWithKey, test.storageErr)
			ss.EXPECT().UpdateShardConfig(gomock.Any()).Times(test.storageUpdateTimes).Return(test.storageUpdateErr)
			s, err := createServer(ss, new(util.SystemTimeSource))
			if err != nil {
				t.Fatalf("Failed to setup server: %v", err)
			}
			blob, err := proto.Marshal(test.provCfg)
			if err != nil {
				t.Errorf("Failed to marshal config: %v", err)
			}
			if len(test.sendInnerBlob) > 0 {
				blob = test.sendInnerBlob
			}

			var token string
			if test.getToken {
				resp, err := s.ProvisionHandshake(context.Background(), &ProvisionHandshakeRequest{})
				if err != nil {
					t.Fatalf("ProvisionHandshake()=%v,%v, want token,nil", resp, err)
				}
				token = resp.GetToken()
			}
			wc := &WrappedConfig{Token: token, ShardConfig: blob}
			outerBlob, err := proto.Marshal(wc)
			if err != nil {
				t.Fatalf("Failed to marshal outer blob: %v", err)
			}
			if len(test.sendBlob) > 0 {
				outerBlob = test.sendBlob
			}
			provSig, err := maybeSignConfig(outerBlob, test.signCfg)
			if err != nil {
				t.Fatalf("Failed to sign the blob: %v", err)
			}
			resp, err := s.Provision(context.Background(), &ShardProvisionRequest{ConfigSig: provSig, ShardConfig: outerBlob})
			if test.wantErr {
				if err == nil || status.Code(err) != test.wantCode {
					t.Errorf("Provision()=%v, %v, want:err with code %v", resp, err, test.wantCode)
				}
				if len(test.errStr) > 0 && !strings.Contains(err.Error(), test.errStr) {
					t.Errorf("Provision()=%v, %v, want:err with string %v", resp, err, test.errStr)
				}
			} else {
				if err != nil {
					t.Errorf("Provision()=%v, %v, want: resp, nil", resp, err)
				}
				signer, err := der.FromProto(key)
				if err != nil {
					t.Fatalf("Failed to create signer: %v", err)
				}
				if err := tcrypto.Verify(signer.Public(), crypto.SHA256, resp.ProvisionedConfig, resp.ConfigSig); err != nil {
					t.Errorf("Provision() response not signed: %v, sig: %v", err, resp.ConfigSig)
				}
				var gotCfg shardproto.ShardProto
				if err := proto.Unmarshal(resp.ProvisionedConfig, &gotCfg); err != nil {
					t.Errorf("Provision() response did not unmarshal: %v", err)
				}

				// Remove timestamps before comparing.
				gotCfg.CreateTime = nil
				gotCfg.UpdateTime = nil
				if got, want := &gotCfg, test.wantCfg; !proto.Equal(got, want) {
					t.Errorf("Provision()=%v, want: %v", got, want)
				}
				if test.checkSig {
					vSigner, err := der.FromProto(key)
					if err != nil {
						t.Fatalf("Failed to create signer: %v", err)
					}
					if err := tcrypto.Verify(vSigner.Public(), crypto.SHA256, resp.GetProvisionedConfig(), resp.GetConfigSig()); err != nil {
						t.Errorf("Provision()=%v, failed to verify sig: %v", resp, err)
					}
				}
			}

			ctrl.Finish()
		})
	}
}

func createServer(ss storage.ShardStorage, ts util.TimeSource) (*shardServiceServer, error) {
	key, err := pem.ReadPublicKeyFile("../testdata/keys/railgun-server.pubkey.pem")
	if err != nil {
		return nil, err
	}
	return NewShardServiceServer(ss, key, ts, Opts{}), nil
}

func genKey() (*keyspb.PrivateKey, error) {
	spec := &keyspb.Specification{}
	spec.Params = &keyspb.Specification_EcdsaParams{
		EcdsaParams: &keyspb.Specification_ECDSA{},
	}
	return der.NewProtoFromSpec(spec)
}

func maybeSignConfig(blob []byte, sign bool) ([]byte, error) {
	if !sign {
		return nil, nil
	}
	cs, err := pem.ReadPrivateKeyFile("../testdata/keys/railgun-server.privkey.pem", "tesla")
	if err != nil {
		return nil, err
	}
	signer := tcrypto.NewSHA256Signer(cs)
	provSig, err := signer.Sign(blob)
	if err != nil {
		return nil, err
	}

	return provSig, nil
}
