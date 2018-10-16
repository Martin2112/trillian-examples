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

package boltdb

import (
	"bytes"
	"context"
	"crypto"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/golang/glog"
	"github.com/google/trillian"
	"github.com/google/trillian/merkle"
	"github.com/google/trillian/merkle/hashers"
	"github.com/google/trillian/merkle/rfc6962"
	"github.com/google/trillian/storage"
	"github.com/google/trillian/storage/cache"
)

func TestNodeRoundTrip(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	s := newTreeStorage(db)
	ctx := context.Background()

	const writeRevision = int64(100)
	nodesToStore := createSomeNodes()
	nodeIDsToRead := make([]storage.NodeID, len(nodesToStore))
	for i := range nodesToStore {
		nodeIDsToRead[i] = nodesToStore[i].NodeID
	}

	{
		runTreeTX(ctx, s, 6962, t, func(tx *treeTX) error {
			forceWriteRevision(writeRevision, tx)

			// Need to read nodes before attempting to write
			if _, err := tx.GetMerkleNodes(ctx, 99, nodeIDsToRead); err != nil {
				t.Fatalf("Failed to read nodes: %s", err)
			}
			if err := tx.SetMerkleNodes(ctx, nodesToStore); err != nil {
				t.Fatalf("Failed to store nodes: %s", err)
			}
			return nil
		})
	}

	{
		runTreeTX(ctx, s, 6962, t, func(tx *treeTX) error {
			readNodes, err := tx.GetMerkleNodes(ctx, 100, nodeIDsToRead)
			if err != nil {
				t.Fatalf("Failed to retrieve nodes: %s", err)
			}
			if err := nodesAreEqual(readNodes, nodesToStore); err != nil {
				t.Fatalf("Read back different nodes from the ones stored: %s", err)
			}
			return nil
		})
	}
}

// This test ensures that node writes cross subtree boundaries so this edge case in the subtree
// cache gets exercised. Any tree size > 256 will do this.
func TestLogNodeRoundTripMultiSubtree(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	s := newTreeStorage(db)
	ctx := context.Background()

	const writeRevision = int64(100)
	nodesToStore, err := createLogNodesForTreeAtSize(871, writeRevision)
	if err != nil {
		t.Fatalf("failed to create test tree: %v", err)
	}
	nodeIDsToRead := make([]storage.NodeID, len(nodesToStore))
	for i := range nodesToStore {
		nodeIDsToRead[i] = nodesToStore[i].NodeID
	}

	{
		runTreeTX(ctx, s, 4190, t, func(tx *treeTX) error {
			forceWriteRevision(writeRevision, tx)

			// Need to read nodes before attempting to write
			if _, err := tx.GetMerkleNodes(ctx, writeRevision-1, nodeIDsToRead); err != nil {
				t.Fatalf("Failed to read nodes: %s", err)
			}
			if err := tx.SetMerkleNodes(ctx, nodesToStore); err != nil {
				t.Fatalf("Failed to store nodes: %s", err)
			}
			return nil
		})
	}

	{
		runTreeTX(ctx, s, 4190, t, func(tx *treeTX) error {
			readNodes, err := tx.GetMerkleNodes(ctx, 100, nodeIDsToRead)
			if err != nil {
				t.Fatalf("Failed to retrieve nodes: %s", err)
			}
			if err := nodesAreEqual(readNodes, nodesToStore); err != nil {
				missing, extra := diffNodes(readNodes, nodesToStore)
				for _, n := range missing {
					t.Errorf("Missing: %s %s", n.NodeID.String(), n.NodeID.CoordString())
				}
				for _, n := range extra {
					t.Errorf("Extra  : %s %s", n.NodeID.String(), n.NodeID.CoordString())
				}
				t.Fatalf("Read back different nodes from the ones stored: %s", err)
			}
			return nil
		})
	}
}

func TestNodeReadHorizon(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	s := newTreeStorage(db)
	ctx := context.Background()

	const writeRevision = int64(100)
	nodesToStore, err := createLogNodesForTreeAtSize(871, writeRevision)
	if err != nil {
		t.Fatalf("failed to create test tree: %v", err)
	}
	nodeIDsToRead := make([]storage.NodeID, len(nodesToStore))
	for i := range nodesToStore {
		nodeIDsToRead[i] = nodesToStore[i].NodeID
	}

	{
		runTreeTX(ctx, s, 4190, t, func(tx *treeTX) error {
			forceWriteRevision(writeRevision, tx)

			// Need to read nodes before attempting to write
			if _, err := tx.GetMerkleNodes(ctx, writeRevision-1, nodeIDsToRead); err != nil {
				t.Fatalf("Failed to read nodes: %s", err)
			}
			if err := tx.SetMerkleNodes(ctx, nodesToStore); err != nil {
				t.Fatalf("Failed to store nodes: %s", err)
			}
			return nil
		})
	}

	{
		runTreeTX(ctx, s, 4190, t, func(tx *treeTX) error {
			readNodes, err := tx.GetMerkleNodes(ctx, writeRevision-1, nodeIDsToRead)
			if err != nil {
				t.Fatalf("Failed to retrieve nodes: %s", err)
			}
			// We wrote at revision 100 so reading at 99 should give nothing.
			if err := nodesAreEqual(readNodes, nil); err != nil {
				missing, extra := diffNodes(readNodes, nodesToStore)
				for _, n := range missing {
					t.Errorf("Missing: %s %s", n.NodeID.String(), n.NodeID.CoordString())
				}
				for _, n := range extra {
					t.Errorf("Extra  : %s %s", n.NodeID.String(), n.NodeID.CoordString())
				}
				t.Fatalf("Read back different nodes from the ones stored: %s", err)
			}
			return nil
		})
	}
}

func TestNodeReadHorizonRightNode(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()
	s := newTreeStorage(db)
	ctx := context.Background()

	// We write two nodes at revision 10 and then another version of the first one at revision
	// 20.
	const writeRevision1 = int64(10)
	const writeRevision2 = int64(20)

	nodesToStore := createSomeNodes()[:2]
	nodesToStore[0].Hash = []byte("Node0@10")
	nodesToStore[1].Hash = []byte("Node1@10")

	nodeIDsToRead := []storage.NodeID{nodesToStore[0].NodeID, nodesToStore[1].NodeID}

	{
		runTreeTX(ctx, s, 4190, t, func(tx *treeTX) error {
			forceWriteRevision(writeRevision1, tx)

			// Need to read nodes before attempting to write
			if _, err := tx.GetMerkleNodes(ctx, writeRevision1-1, nodeIDsToRead); err != nil {
				t.Fatalf("Failed to read nodes: %s", err)
			}
			if err := tx.SetMerkleNodes(ctx, nodesToStore); err != nil {
				t.Fatalf("Failed to store nodes: %s", err)
			}
			return nil
		})
	}

	// Now update one node at write revision 20.
	nodesToStore2 := nodesToStore[:1]
	nodesToStore2[0].Hash = []byte("Node0@20")
	nodeIDsToRead = []storage.NodeID{nodesToStore[0].NodeID}

	{
		runTreeTX(ctx, s, 4190, t, func(tx *treeTX) error {
			forceWriteRevision(writeRevision2, tx)

			// Need to read nodes before attempting to write
			if _, err := tx.GetMerkleNodes(ctx, writeRevision2-1, nodeIDsToRead); err != nil {
				t.Fatalf("Failed to read nodes: %s", err)
			}
			if err := tx.SetMerkleNodes(ctx, nodesToStore2); err != nil {
				t.Fatalf("Failed to store nodes: %s", err)
			}
			return nil
		})
	}

	// Read back the two nodes at revision 20.
	nodeIDsToRead = []storage.NodeID{nodesToStore[0].NodeID, nodesToStore[1].NodeID}

	{
		runTreeTX(ctx, s, 4190, t, func(tx *treeTX) error {
			readNodes, err := tx.GetMerkleNodes(ctx, writeRevision2, nodeIDsToRead)
			if err != nil {
				t.Fatalf("Failed to retrieve nodes: %s", err)
			}
			if err := nodesAreEqual(readNodes, nodesToStore); err != nil {
				missing, extra := diffNodes(readNodes, nodesToStore)
				for _, n := range missing {
					t.Errorf("Missing: %s %s", n.NodeID.String(), n.NodeID.CoordString())
				}
				for _, n := range extra {
					t.Errorf("Extra  : %s %s", n.NodeID.String(), n.NodeID.CoordString())
				}
				t.Fatalf("Read back different nodes from the ones stored: %s", err)
			}
			return nil
		})
	}
}

func forceWriteRevision(rev int64, tx *treeTX) {
	tx.writeRevision = rev
}

func createSomeNodes() []storage.Node {
	r := make([]storage.Node, 4)
	for i := range r {
		r[i].NodeID = storage.NewNodeIDWithPrefix(uint64(i), 8, 8, 8)
		h := sha256.Sum256([]byte{byte(i)})
		r[i].Hash = h[:]
		glog.Infof("Node to store: %v\n", r[i].NodeID)
	}
	return r
}

func createLogNodesForTreeAtSize(ts, rev int64) ([]storage.Node, error) {
	tree := merkle.NewCompactMerkleTree(rfc6962.New(crypto.SHA256))
	nodeMap := make(map[string]storage.Node)
	for l := 0; l < int(ts); l++ {
		// We're only interested in the side effects of adding leaves - the node updates
		if _, _, err := tree.AddLeaf([]byte(fmt.Sprintf("Leaf %d", l)), func(depth int, index int64, hash []byte) error {
			nID, err := storage.NewNodeIDForTreeCoords(int64(depth), index, 64)
			if err != nil {
				return fmt.Errorf("failed to create a nodeID for tree - should not happen d:%d i:%d",
					depth, index)
			}

			nodeMap[nID.String()] = storage.Node{NodeID: nID, NodeRevision: rev, Hash: hash}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	// Unroll the map, which has deduped the updates for us and retained the latest
	nodes := make([]storage.Node, 0, len(nodeMap))
	for _, v := range nodeMap {
		nodes = append(nodes, v)
	}

	return nodes, nil
}

func nodesAreEqual(lhs []storage.Node, rhs []storage.Node) error {
	if ls, rs := len(lhs), len(rhs); ls != rs {
		return fmt.Errorf("different number of nodes, %d vs %d", ls, rs)
	}
	for i := range lhs {
		if l, r := lhs[i].NodeID.String(), rhs[i].NodeID.String(); l != r {
			return fmt.Errorf("NodeIDs are not the same,\nlhs = %v,\nrhs = %v", l, r)
		}
		if l, r := lhs[i].Hash, rhs[i].Hash; !bytes.Equal(l, r) {
			return fmt.Errorf("Hashes are not the same for %s,\nlhs = %v,\nrhs = %v", lhs[i].NodeID.CoordString(), l, r)
		}
	}
	return nil
}

func diffNodes(got, want []storage.Node) ([]storage.Node, []storage.Node) {
	var missing []storage.Node
	gotMap := make(map[string]storage.Node)
	for _, n := range got {
		gotMap[n.NodeID.String()] = n
	}
	for _, n := range want {
		_, ok := gotMap[n.NodeID.String()]
		if !ok {
			missing = append(missing, n)
		}
		delete(gotMap, n.NodeID.String())
	}
	// Unpack the extra nodes to return both as slices
	extra := make([]storage.Node, 0, len(gotMap))
	for _, v := range gotMap {
		extra = append(extra, v)
	}
	return missing, extra
}

// Convenience methods to avoid copying out "if err != nil { blah }" all over the place
func runTreeTX(ctx context.Context, b *boltTreeStorage, treeID int64, t *testing.T, f func(tx *treeTX) error) {
	t.Helper()
	hasher, err := hashers.NewLogHasher(trillian.HashStrategy_RFC6962_SHA256)
	if err != nil {
		t.Fatalf("Failed to create hasher: %v", err)
	}
	stCache := cache.NewLogSubtreeCache(defaultLogStrata, hasher)
	ttx, err := b.beginTreeTX(ctx, treeID, hasher.Size(), stCache, false)
	if err != nil {
		t.Fatalf("beginTreeTX(): %v", err)
	}
	defer ttx.Close()

	if err != nil && err != storage.ErrTreeNeedsInit {
		t.Fatalf("Unexpected error setting up tx: %v", err)
	}

	if err := f(&ttx); err != nil {
		t.Errorf("Bolt tx failed: %v", err)
	}

	if err := ttx.Commit(); err != nil {
		t.Errorf("Bolt tx Commit() failed: %v", err)
	}
}
