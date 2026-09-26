# Nodetool clearsnapshot

**clearsnapshot** `[-t tag] [<keyspace>]`- This command removes snapshots. By default all the snapshots will be removed unless a `snapshot_name` is provided.

For example:

All snapshots.

```shell
nodetool clearsnapshot
```

A specific snapshot name.

```shell
nodetool clearsnapshot -t <snapshot_name>
```

[Nodetool Reference](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool.md)
