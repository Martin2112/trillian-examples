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

// See README.md at top level package for details on how to run this example.

package main

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"flag"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
	"github.com/golang/glog"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/trillian"
	"github.com/google/trillian-examples/embedded/storage/boltdb"
	tcrypto "github.com/google/trillian/crypto"
	"github.com/google/trillian/crypto/keys/der"
	_ "github.com/google/trillian/crypto/keys/der/proto" // Register Key ProtoHandler
	"github.com/google/trillian/crypto/keyspb"
	"github.com/google/trillian/crypto/sigpb"
	"github.com/google/trillian/extension"
	"github.com/google/trillian/log"
	"github.com/google/trillian/merkle/hashers"
	_ "github.com/google/trillian/merkle/rfc6962" // Register RFC6962 LogHasher
	"github.com/google/trillian/monitoring"
	"github.com/google/trillian/quota"
	"github.com/google/trillian/server"
	"github.com/google/trillian/storage"
	"github.com/google/trillian/util"
)

var (
	dbFile          = flag.String("boltdb_file", "/tmp/example.bolt", "The path to the file used to store our data")
	numLeaves       = flag.Int("leaves", 10, "The number of leaves to queue")
	sequenceEvery   = flag.Int("sequence_every", 10, "The tree will be updated each time this many leaves are added")
	boltTimeout     = flag.Duration("bolt_timeout", 2*time.Second, "The timeout option to pass to boltdb")
	maxRootDuration = flag.Duration("max_root_duration", time.Minute, "The max root duration to pass to sequencing")
	leafIndex       = flag.Int64("leaf_index", 1, "The log index to retrieve")
	proofIndex      = flag.Int64("proof_index", 1, "The log index to retrieve a proof for")
	proofTreeSize   = flag.Int64("proof_tree_size", 10, "The log tree size to use for an inclusion / consistency proof")
	proofTreeSize2  = flag.Int64("proof_tree_size2", 0, "The second log tree size to use for a consistency proof")
	treeID          = flag.Int64("tree_id", 0, "The id of the log tree to be used")
)

type cmdFunc func(ctx context.Context, registry extension.Registry) error

func main() {
	flag.Parse()

	cmdMap := map[string]cmdFunc{
		"list_trees":        listTreesCmd,
		"queue_leaves":      queueLeavesCmd,
		"print_tree":        printTreeCmd,
		"print_root":        printRootCmd,
		"get_leaf":          getLeafCmd,
		"inclusion_proof":   inclusionProofCmd,
		"consistency_proof": consistencyProofCmd,
	}

	// Create / open the underlying database.
	db, err := boltdb.OpenDB(*dbFile, &bolt.Options{Timeout: *boltTimeout})
	if err != nil {
		glog.Exitf("Failed to open boltdb: %v", err)
	}
	defer db.Close()

	// Set up a log / admin storage using the database (no metrics for the moment).
	ctx := context.Background()
	mf := monitoring.InertMetricFactory{}
	as := boltdb.NewAdminStorage(db)
	ls := boltdb.NewLogStorage(db, mf)

	// Create a log server. This isn't registered / exported using gRPC in this case but it just
	// takes protos in and out so no reason why we can't use it standalone. We can safely mix
	// direct calls to the log storage with server calls because we know we're the only user of both.
	registry := extension.Registry{
		LogStorage:    ls,
		AdminStorage:  as,
		MetricFactory: mf,
	}

	if len(flag.Args()) == 0 {
		glog.Exit("Need to specify a command")
	}

	cmd, ok := cmdMap[flag.Args()[0]]
	if ok {
		if err := cmd(ctx, registry); err != nil {
			fmt.Println(err)
		}
		return
	}
	fmt.Printf("Unknown command: %s\n", flag.Args()[0])
}

func createSignerOrDie() (*tcrypto.Signer, *ecdsa.PrivateKey, []byte) {
	ecdsaKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		glog.Exitf("Failed to generate ECDSA key: %v", err)
	}
	pubKeyDER, err := x509.MarshalPKIXPublicKey(ecdsaKey.Public())
	if err != nil {
		glog.Exitf("Failed to marshal public key to DER: %v", err)
	}
	signer := tcrypto.NewSigner(6962, ecdsaKey, crypto.SHA256)
	return signer, ecdsaKey, pubKeyDER
}

func makeLeafOrDie(li int64, h hashers.LogHasher) *trillian.LogLeaf {
	lv := fmt.Sprintf("test data %d", li)
	lih := sha256.Sum256([]byte(lv))
	lmh, err := h.HashLeaf([]byte(lv))
	if err != nil {
		glog.Exitf("HashLeaf()=%v", err)
	}
	return &trillian.LogLeaf{
		LeafIndex:        li,
		LeafValue:        []byte(lv),
		LeafIdentityHash: lih[:],
		MerkleLeafHash:   lmh,
	}
}

func createLogTreeOrDie(ctx context.Context, as storage.AdminStorage, mKey, pubKey []byte, tt trillian.TreeType) *trillian.Tree {
	mKeyProto, err := ptypes.MarshalAny(&keyspb.PrivateKey{Der: mKey})
	if err != nil {
		glog.Exitf("MarshalAny(privateKey)=%v", err)
	}
	tree := &trillian.Tree{
		TreeType:           tt,
		HashStrategy:       trillian.HashStrategy_RFC6962_SHA256,
		HashAlgorithm:      sigpb.DigitallySigned_SHA256,
		SignatureAlgorithm: sigpb.DigitallySigned_ECDSA,
		PrivateKey:         mKeyProto,
		PublicKey:          &keyspb.PublicKey{Der: pubKey},
		TreeState:          trillian.TreeState_ACTIVE,
		MaxRootDuration:    ptypes.DurationProto(time.Minute),
	}
	tree, err = storage.CreateTree(ctx, as, tree)
	if err != nil {
		glog.Exitf("CreateTree()=%v", err)
	}
	glog.Infof("Created tree with id: %x", tree.TreeId)
	return tree
}

func queueLeavesCmd(ctx context.Context, registry extension.Registry) error {
	// Set up keys and a Signer.
	signer, privKey, pubKey := createSignerOrDie()
	mKey, err := der.MarshalPrivateKey(privKey)
	if err != nil {
		return err
	}

	// We need a hasher that supports CT formats.
	h, err := hashers.NewLogHasher(trillian.HashStrategy_RFC6962_SHA256)
	if err != nil {
		return err
	}

	// Create a test log tree. Note this isn't how a real application would do key handling.
	// Don't cut and paste this code anywhere important!
	tree := createLogTreeOrDie(ctx, registry.AdminStorage, mKey, pubKey, trillian.TreeType_LOG)

	logServer := server.NewTrillianLogRPCServer(registry, util.SystemTimeSource{})

	// Initialize the log tree.
	_, err = logServer.InitLog(ctx, &trillian.InitLogRequest{LogId: tree.TreeId})
	if err != nil {
		return err
	}

	// Set up a Sequencer.
	seq := log.NewSequencer(h, util.SystemTimeSource{}, registry.LogStorage, signer, monitoring.InertMetricFactory{}, quota.Noop())

	// Queue some work.
	for l := 0; l < *numLeaves; l++ {
		leaves := []*trillian.LogLeaf{makeLeafOrDie(int64(l), h)}
		_, err := registry.LogStorage.QueueLeaves(ctx, tree, leaves, time.Now())
		if err != nil {
			return err
		}

		// Periodically run the sequencing.
		if l > 0 && l%*sequenceEvery == 0 {
			count, err := seq.IntegrateBatch(ctx, tree, *sequenceEvery, 0, *maxRootDuration)
			if err != nil {
				return err
			}

			glog.Infof("%d leaves were sequenced", count)
		}
	}

	// Sequence anything that's left over in the queue.
	done := false
	for !done {
		count, err := seq.IntegrateBatch(ctx, tree, *sequenceEvery, 0, *maxRootDuration)
		if err != nil {
			return err
		}

		if count == 0 {
			done = true
		}

		glog.Infof("%d leaves were sequenced (after queueing)", count)
	}
	return nil
}

func printTreeCmd(ctx context.Context, registry extension.Registry) error {
	tree, err := storage.GetTree(ctx, registry.AdminStorage, *treeID)
	if err != nil {
		return err
	}

	fmt.Printf("Tree: %v", tree)

	return nil
}

func printRootCmd(ctx context.Context, registry extension.Registry) error {
	logServer := server.NewTrillianLogRPCServer(registry, util.SystemTimeSource{})
	resp, err := logServer.GetLatestSignedLogRoot(ctx, &trillian.GetLatestSignedLogRootRequest{LogId: *treeID})
	if err != nil {
		return err
	}

	fmt.Printf("Latest Root: %v\n", resp.SignedLogRoot)
	return nil
}

func inclusionProofCmd(ctx context.Context, registry extension.Registry) error {
	logServer := server.NewTrillianLogRPCServer(registry, util.SystemTimeSource{})
	proof, err := logServer.GetInclusionProof(ctx, &trillian.GetInclusionProofRequest{
		LogId:     *treeID,
		LeafIndex: *proofIndex,
		TreeSize:  *proofTreeSize})
	if err != nil {
		return err
	}

	glog.Infof("Proof for leaf %d at tree size %d: %v", *proofIndex, *proofTreeSize, proof.Proof)
	return nil
}

func consistencyProofCmd(ctx context.Context, registry extension.Registry) error {
	logServer := server.NewTrillianLogRPCServer(registry, util.SystemTimeSource{})
	proof, err := logServer.GetConsistencyProof(ctx, &trillian.GetConsistencyProofRequest{
		LogId:          *treeID,
		FirstTreeSize:  *proofTreeSize,
		SecondTreeSize: *proofTreeSize2,
	})
	if err != nil {
		return err
	}

	glog.Infof("Proof from tree size %d to %d: %v", *proofTreeSize, *proofTreeSize2, proof.Proof)
	return nil
}

func getLeafCmd(ctx context.Context, registry extension.Registry) error {
	logServer := server.NewTrillianLogRPCServer(registry, util.SystemTimeSource{})
	leaf, err := logServer.GetLeavesByIndex(ctx, &trillian.GetLeavesByIndexRequest{
		LogId:     *treeID,
		LeafIndex: []int64{*leafIndex},
	})
	if err != nil {
		return err
	}

	glog.Infof("Leaf: %v", leaf.Leaves[0])
	return nil
}

func listTreesCmd(ctx context.Context, registry extension.Registry) error {
	trees, err := storage.ListTrees(ctx, registry.AdminStorage, false)
	if err != nil {
		return err
	}

	for _, tree := range trees {
		fmt.Printf("%20d %s %s\n", tree.TreeId, tree.TreeType, tree.TreeState)
	}
	fmt.Printf("%d Trees\n", len(trees))
	return nil
}
