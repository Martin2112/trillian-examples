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

package conductor

import (
	"context"
	"crypto"
	"time"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/google/trillian-examples/railgun/discovery"
	"github.com/google/trillian-examples/railgun/shard"
	"github.com/google/trillian-examples/railgun/shard/shardproto"
	tcrypto "github.com/google/trillian/crypto"
	"google.golang.org/grpc"
)

type shardStatus struct {
	config       *shardproto.ShardProto
	service      discovery.ServiceResult
	provAttempts int
	provOK       bool
}

type Opts struct {
	LookupInterval time.Duration
	LookupTimeout  time.Duration
}

type Dialler interface {
	Dial(result discovery.ServiceResult) (*grpc.ClientConn, error)
}

type Provisioner struct {
	dialler Dialler
	disco   discovery.Discoverer
	cs      crypto.Signer
	opts    Opts

	// Map from discovered shard UUIDs to what we know about them.
	uuidMap map[string]*shardStatus
}

func NewProvisioner(dl Dialler, d discovery.Discoverer, cs crypto.Signer, o Opts) *Provisioner {
	m := make(map[string]*shardStatus)
	return &Provisioner{dialler: dl, disco: d, cs: cs, opts: o, uuidMap: m}
}

func (p *Provisioner) Start(ctx context.Context) chan bool {
	ch := make(chan bool)

	go func() {
		for _, ok := <-ch; ok; {
			// It's not vital that this runs exactly every interval.
			time.Sleep(p.opts.LookupInterval)
			r, err := p.disco.Lookup(p.opts.LookupTimeout)
			if err != nil {
				// TODO(Martin2112): Export metrics so we can detect errors via monitoring.
				glog.Warningf("Failed to lookup services in discovery: %v", err)
			}
			for _, s := range r {
				ss, ok := p.uuidMap[s.GetHost()]
				if ok {
					glog.Infof("Rediscovered: %v", ss)
				} else {
					glog.Infof("Discovered: %v", s)
					ss = &shardStatus{service: s}
				}
				p.uuidMap[s.GetHost()] = ss
			}

			// Now that we've updated our view of what shards exist we can query and provision them.
			p.updateStatus(ctx)
			p.provisionNew(ctx)
		}
	}()

	return ch
}

func (p *Provisioner) updateStatus(ctx context.Context) {
	for _, s := range p.uuidMap {
		if s.config == nil {
			conn, err := p.dialler.Dial(s.service)
			if err != nil {
				glog.Warningf("Couldn't dial: %v", err)
				continue
			}
			client := shard.NewShardServiceClient(conn)
			resp, err := client.GetConfig(ctx, &shard.GetShardConfigRequest{})
			if err != nil {
				glog.Warningf("Failed to get shard config: %v", err)
				continue
			}

			blob := resp.GetProvisionedConfig()
			var cfg shardproto.ShardProto
			// TODO(Martin2112): Check signature.
			if err := proto.Unmarshal(blob, &cfg); err != nil {
				glog.Warningf("Config did not unmarshal: %v", err)
				continue
			}
			s.config = &cfg
		}
	}
}

func (p *Provisioner) provisionNew(ctx context.Context) {
	for _, s := range p.uuidMap {
		if s.config != nil && !s.provOK && s.config.GetState() == shardproto.ShardState_SHARD_STATE_NEEDS_INIT {
			s.provAttempts++
			conn, err := p.dialler.Dial(s.service)
			if err != nil {
				glog.Warningf("Couldn't dial: %v", err)
				continue
			}
			client := shard.NewShardServiceClient(conn)
			pResp, err := client.ProvisionHandshake(ctx, &shard.ProvisionHandshakeRequest{})
			if err != nil {
				glog.Warningf("Failed to get token from shard: %v", err)
				continue
			}
			cfg := &shardproto.ShardProto{State: shardproto.ShardState_SHARD_STATE_ACTIVE}
			blob, err := proto.Marshal(cfg)
			if err != nil {
				glog.Warningf("Failed to marshall new config: %v", err)
				continue
			}
			outerCfg := &shard.WrappedConfig{ShardConfig: blob, Token: pResp.GetToken()}
			outerBlob, err := proto.Marshal(outerCfg)
			if err != nil {
				glog.Warningf("Failed to marshall outer config: %v", err)
			}
			signer := tcrypto.NewSHA256Signer(p.cs)
			sig, err := signer.Sign(outerBlob)
			if err != nil {
				glog.Warningf("Failed to sign response: %v", err)
				continue
			}
			resp, err := client.Provision(ctx, &shard.ShardProvisionRequest{ShardConfig: outerBlob, ConfigSig: sig})
			if err != nil {
				glog.Warningf("Provision attempt failed: %v", err)
			}
			// TODO(Martin2112): Check signature.
			if err := proto.Unmarshal(resp.ProvisionedConfig, cfg); err != nil {
				glog.Warningf("Failed to unmarshal received config: %v", err)
				continue
			}
			s.config = cfg
			s.provOK = true
		}
	}
}
