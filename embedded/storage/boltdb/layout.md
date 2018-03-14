# BoltDB Bucket Layout

## Trees

There is a single bucket holding the tree definitions called `Tree`.

Within this bucket the non bucket keys are hex treeIDs and the values are 
marshalled `trillian.Tree` protos.

## Tree Roots

There is a single bucket for tree roots called `TreeRoot`.

## Subtrees

Subtree protos are in a single bucket per tree. The bucket name is `Subtree_XXXX`
where XXXX is the treeID in hex.

Within the bucket the keys are subtree paths and the values are marshalled `storagepb.Subtree` protos.

## Leaves

