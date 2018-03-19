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
	"github.com/gogo/protobuf/proto"
	"github.com/google/trillian-examples/railgun/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Opts struct {
	skipSignatureChecks bool // for use in tests etc only
}

type shardProvisioningServer struct {
	shardStorage storage.ShardStorage
	opts         Opts
}

func NewShardProvisioningServer(s storage.ShardStorage, o Opts) *shardProvisioningServer {
	return &shardProvisioningServer{shardStorage: s, opts: o}
}

func (s *shardProvisioningServer) Provision(request *ShardProvisionRequest) (*ShardProvisionResponse, error) {
	// Check the signature before processing the request. This can be skipped - with the
	// obvious risks if this option is set in production.
	if !s.opts.skipSignatureChecks {
		return nil, status.Error(codes.Unimplemented, "not implemented yet")
	}

	var config ShardProto
	if err := proto.Unmarshal(request.GetShardConfig(), &config); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "config did not unmarshal: %v", err)
	}

	return nil, status.Error(codes.Unimplemented, "not implemented yet")
}
