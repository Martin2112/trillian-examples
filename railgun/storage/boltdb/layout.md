# BoltDB Bucket Layout

## Subtrees

Subtree protos are under a bucket per tree. The bucket name is `Subtree_XXXX`
where XXXX is the treeID in hex.

Within the bucket the keys are subtree paths and the values buckets. Within these
buckets the keys are big-endian representations of the write revision number and
values are marshalled `storagepb.Subtree` protos.

## Trees

There is a single bucket holding the tree definitions called `Tree`.

Within this bucket the non bucket keys are hex treeIDs and the values are 
marshalled `trillian.Tree` protos.

For each tree there is then also a top level `Tree_XXX` bucket where XXX is the hex
string version of the `treeID`. Under this there are several other buckets,
which hold data represented by tables / indices in the SQL Log schema.

### Tree Heads

Within each `Tree_XXX` bucket there is a `TreeHead` bucket. The keys are tree revision numbers
and the values are marshalled `SignedLogRoot` protos. Note that there is only
one tree revision stored in the `Subtree` structures. Keys in these buckets are 8
byte big-endian representation of the tree revision so they maintain a usable
iteration order.

### Leaves

