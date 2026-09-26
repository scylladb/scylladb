# How to flush old tombstones from a table

**Audience: Devops professionals, architects**

## Description

If you have large partitions with lots of tombstones, you can use this workaround to flush the old tombstones.
To avoid data resurrection, make sure that tables are repaired (either with nodetool repair or ScyllaDB Manager) before the `gc_grace_seconds` threshold is reached.
After the repair finishes, any tombstone older than the previous repair can be flushed.

#### NOTE
Use [this article](https://opensource.docs.scylladb.com/branch-6.1/troubleshooting/large-partition-table.md) to help you find large partitions.

### Steps:

1. Run nodetool repair to synchronize the data between nodes. Alternatively, you can use ScyllaDB Manager to run a repair.

```sh
nodetool repair <options>;
```

1. Set  the `gc_grace_seconds` to the time since last repair was started -  For instance, if the last repair was executed one day ago, then set `gc_grace_seconds` to one day (86400sec). For more information, please refer to [this KB article](https://opensource.docs.scylladb.com/branch-6.1/kb/gc-grace-seconds.md).

#### NOTE
To prevent the compaction of unsynched tombstones, it is important to get the timing correctly. If you are not sure what time should set, please contact [ScyllaDB support](https://www.scylladb.com/product/support/).

```sh
ALTER TABLE <keyspace>.<mytable> WITH gc_grace_seconds = <newTimeValue>;
```

#### NOTE
Steps 3 & 4 should be repeated on ALL nodes affected with large tombstone partitions.

1. Run nodetool flush

```sh
nodetool flush <keyspace> <mytable>;
```

1. Run compaction (this will remove big partitions with tombstones from specified table)

#### NOTE
By default, major compaction runs on all the keyspaces and tables, so if we want to specyfy e.g. only one table, we should point at it using arguments: `<keyspace>.<mytable>`. For more information, please refer to [this article](https://docs.scylladb.com/operating-scylla/nodetool-commands/compact/).

```sh
nodetool compact <keyspace> <mytable>;
```

1. Alter the table and change the grace period back to the original `gc_grace_seconds` value.

```sh
ALTER TABLE <keyspace>.<mytable> WITH gc_grace_seconds = <oldValue>;
```
