# Nodetool stop compaction

Stops a compaction operation. This command is usually used to stop compaction that has a negative impact on the performance of a node.

Usage

```sh
nodetool <options> stop -- <compaction_type>
```

#### Versionadded
Added in version version: 4.5 `compaction type`

Supported compaction types: COMPACTION, CLEANUP, VALIDATION, SCRUB, RESHARD, RESHAPE

For example:

```sh
nodetool stop compaction

nodetool stop compaction RESHAPE
```

[Nodetool Reference](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool.md)
