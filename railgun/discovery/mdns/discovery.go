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

package mdns

import (
	"fmt"
	"net"
	"time"

	"github.com/google/trillian-examples/railgun/discovery"
	"github.com/hashicorp/mdns"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	serviceSuffix  = "_railgun._grpc._tcp"
	chanBufferSize = 10
)

type ServiceParams struct {
	Host      string
	Service   string
	NodeID    string
	Port      int
	Addresses []net.IP
	Info      []string
}

type mDNSDiscoverer struct {
	sName  string
	dns    *mdns.MDNSService
	server *mdns.Server
}

type mDNSResult struct {
	s *mdns.ServiceEntry
}

func NewMDNSDiscoverer(params ServiceParams) (discovery.Discoverer, error) {
	// Create the service export.
	sName := fmt.Sprintf("_%s%s", params.Service, serviceSuffix)
	dns, err := mdns.NewMDNSService(params.NodeID, sName, "", params.Host, params.Port, params.Addresses, params.Info)
	if err != nil {
		return nil, err
	}

	// Start the DNS server immediately.
	server, err := mdns.NewServer(&mdns.Config{Zone: dns})
	if err != nil {
		return nil, err
	}
	return &mDNSDiscoverer{sName: sName, dns: dns, server: server}, nil
}

func (m *mDNSDiscoverer) Register(nodeID, service string) error {
	// Not needed for this type of discovery service - registration done at create time.
	return status.Error(codes.Unimplemented, "this implementation does not support Register()")
}

func (m *mDNSDiscoverer) Unregister(nodeID, service string) error {
	return status.Error(codes.Unimplemented, "this implementation does not support Unregister()")
}

func (m *mDNSDiscoverer) Lookup(service string, timeout time.Duration) ([]discovery.ServiceResult, error) {
	result := make([]discovery.ServiceResult, 0, chanBufferSize)
	entriesCh := make(chan *mdns.ServiceEntry, chanBufferSize)
	defer close(entriesCh)
	go func() {
		for s := range entriesCh {
			result = append(result, &mDNSResult{s: s})
		}
	}()

	// Use MDNS defaults but with our timeout.
	params := mdns.DefaultParams(m.sName)
	params.Timeout = timeout
	params.Entries = entriesCh
	if err := mdns.Query(params); err != nil {
		return nil, err
	}

	return result, nil
}

func (m *mDNSDiscoverer) Close() error {
	return m.server.Shutdown()
}

func (r *mDNSResult) GetName() string {
	return r.s.Name
}

func (r *mDNSResult) GetHost() string {
	return r.s.Host
}

func (r *mDNSResult) GetAddressV4() net.IP {
	return r.s.AddrV4
}

func (r *mDNSResult) GetAddressV6() net.IP {
	return r.s.AddrV6
}
