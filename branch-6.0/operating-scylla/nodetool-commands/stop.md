# Nodetool stop compaction

Stops a compaction operation. This command is usually used to stop compaction that has a negative impact on the performance of a node.

Usage

```sh
nodetool <options> stop -- <compaction_type>
```

Supported compaction types: COMPACTION, CLEANUP, SCRUB, RESHAPE

Stopping a compaction by id (`--id <id>`) is not implemented.

For example:

```sh
nodetool stop COMPACTION

nodetool stop RESHAPE
```

[Nodetool Reference](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/nodetool.md)
