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
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/trillian-examples/railgun/storage/mock_storage"
	"github.com/google/trillian/crypto/keys/pem"
)

func TestProvisionHandshake(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ss := mock_storage.NewMockShardStorage(ctrl)
	ss.EXPECT().GetShardConfig().Times(0).Return(nil, errors.New("not expected"))
	key, err := pem.ReadPublicKeyFile("../testdata/keys/railgun-server.pubkey.pem")
	if err != nil {
		t.Fatalf("Could not load key: %v", err)
	}
	s := NewShardServiceServer(ss, key, Opts{})

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
			t.Fatalf("ProvisionHandshake() return duplicate token: %v", token)
		}
	}
}
