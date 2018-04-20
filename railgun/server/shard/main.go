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

package main

import (
	"flag"
	"fmt"
	"net"

	"time"

	"github.com/golang/glog"
	"github.com/google/trillian-examples/railgun/discovery/mdns"
	"github.com/google/trillian-examples/railgun/shard"
	"github.com/google/trillian-examples/railgun/storage"
	"github.com/google/trillian-examples/railgun/storage/boltdb"
	"github.com/google/trillian/crypto/keys/pem"
	"github.com/google/trillian/util"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

var (
	port          = flag.Int("port", 0, "Port to use for exporting gRPC services")
	serviceName   = flag.String("service_name", "railgun-test", "A string that identifies this deployment")
	publicKeyPath = flag.String("coordinator_pubkey_file", "", "The file that holds the public key of the coordinator")
	boltDBPath    = flag.String("boltdb_path", "", "The file to be used to store all data")
	reflect       = flag.Bool("grpc_reflection", false, "If true, gRPC reflection will be registered on the server")
	tokenExpiry   = flag.Duration("token_expiry", 5*time.Second, "Duration that provision tokens will be valid for")
)

func main() {
	flag.Parse()
	if complainAboutFlags() {
		flag.Usage()
		return
	}

	authorizedKey, err := pem.ReadPublicKeyFile(*publicKeyPath)
	if err != nil {
		glog.Fatalf("Could not read authorized server public key file: %v", err)
	}

	db, err := boltdb.OpenDB(*boltDBPath, nil)
	if err != nil {
		glog.Fatalf("Could not open boltdb storage using: %v", err)
	}
	defer db.Close()
	shardStorage := boltdb.NewShardStorage(db)
	cfg, err := shardStorage.GetShardConfig()
	var nodeUUID uuid.UUID
	switch {
	case status.Code(err) == codes.NotFound:
		// Nothing found in storage, this is a new shard.
		cfg, err := storage.NewShardConfig(authorizedKey)
		if err != nil {
			glog.Fatalf("Failed to create shard config: %v", err)
		}
		if err := shardStorage.CreateShardConfig(cfg); err != nil {
			glog.Fatalf("Failed to write new shard config: %v", err)
		}

	case err != nil:
		glog.Fatalf("Failed to read shard config: %v", err)

	default:
		if cfg.Uuid == nil {
			glog.Fatal("Inconsistent config - no UUID assigned for shard")
		}

		nodeUUID, err = uuid.FromBytes(cfg.Uuid)
		if err != nil {
			glog.Fatalf("UUID did not parse: %v", err)
		}

		glog.Infof("Restarting with UUID: %v", nodeUUID.String())
	}

	// Start bringing up services. First set up a gRPC server.
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		glog.Fatalf("Failed to listen: %v", err)
	}
	server := shard.NewShardServiceServer(shardStorage, authorizedKey, new(util.SystemTimeSource), shard.Opts{TokenExpiry: *tokenExpiry})
	grpcServer := grpc.NewServer()
	shard.RegisterShardServiceServer(grpcServer, server)
	if *reflect {
		reflection.Register(grpcServer)
	}
	go util.AwaitSignal(grpcServer.Stop)

	// Then setup and register with discovery service.
	disco, err := mdns.NewMDNSDiscoverer(mdns.ServiceParams{
		Service: *serviceName,
		NodeID:  nodeUUID.String(),
		Port:    8080,
	})
	if err != nil {
		glog.Fatalf("Failed to start discovery service: %v", err)
	}
	defer disco.Close()

	// Now we're ready to start handling requests.
	grpcServer.Serve(lis)
}

func complainAboutFlags() bool {
	type strFlag struct {
		n string
		f *string
	}
	errors := 0
	for _, sf := range []strFlag{
		{n: "serviceName", f: serviceName},
		{n: "boltDBPath", f: boltDBPath},
		{n: "publicKeyPath", f: publicKeyPath},
	} {
		if len(*sf.f) == 0 {
			glog.Warningf("Error: --%s must be set.\n", sf.n)
			errors++
		}
	}

	return errors > 0
}
