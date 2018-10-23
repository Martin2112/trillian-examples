# Trillian Embedded Example

## Intro / Rationale

The focus in our main repo is on scaling up to run systems that are distributed
and fault tolerant. But what if you want to scale down and do something simple
like create and read a Verifiable Log from within a standalone process. You
don't want to be creating clusters and running database servers in that case.

This is an example using storage in [BoltDB](https://github.com/boltdb/bolt).
This is a pure go nested key / value store. There are no external storage
dependencies that need to be set up to run it.

## Really Important Notes!

BoltDB is not a multi user database and has no replication. The storage in this
example **cannot** be used to run CT log servers. This is **not supported** and
**won't work**.

The `trillian.Tree` protos written by this code include private key material so
anyone with read access to the database file can obtain the keys. For a demo this doesn't
really matter but real applications should use better ways of handling keys. There
are examples in our main repos and there is no hard requirement that the keys
themselves must be written to `Tree`. For example it's possible to use an identifier for
a key that is stored elsewhere in a secured vault.

## Some Possible Example Applications

* Change detection / tamper evidence
* Audit logging
* Highly sharded lightweight log services

## Running The Example

We'll create a log tree and add some dummy test data to it. Then we'll query the
log for various things. The example command to add leaves can be run multiple times, each
run will create a new tree within the database.

By default the database file will be `/tmp/example.bolt` but it can be specified
with the `--boltdb_file` flag.

### Add Data To a Log Tree

```
go run github.com/google/trillian-examples/embedded/cmd/example -alsologtostderr -leaves=100 queue_leaves
```

Make a note of the ID of the tree that gets created as a random one is used each time. It will be
printed in log lines similar to this:

```
4821695058304186983: sequenced 10 leaves, size 70, tree-revision 7
```

Set a variable for later to make things easer:

```
TREE_ID=4821695058304186983
```

### Get the Latest Tree Root

```
go run github.com/google/trillian-examples/embedded/cmd/example -alsologtostderr -tree_id=$TREE_ID print_root
```

### Get an Inclusion Proof

```
go run github.com/google/trillian-examples/embedded/cmd/example -alsologtostderr -tree_id=$TREE_ID -proof_index=3 --proof_tree_size=80 inclusion_proof
```

### Get a Consistency Proof

```
go run github.com/google/trillian-examples/embedded/cmd/example -alsologtostderr -tree_id=$TREE_ID -proof_index=3 --proof_tree_size=1 --proof_tree_size2=74 consistency_proof
```

### Get a Leaf

```
go run github.com/google/trillian-examples/embedded/cmd/example -alsologtostderr -tree_id=$TREE_ID --leaf_index=27 get_leaf
```

### List the Trees in the Database

```
go run github.com/google/trillian-examples/embedded/cmd/example -alsologtostderr list_trees
```

### Clean Up

When finished playing with the examples the database file can be deleted.

```
rm /tmp/example.boltdb
```