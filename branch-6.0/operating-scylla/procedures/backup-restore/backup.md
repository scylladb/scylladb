# Backup your Data

Even though ScyllaDB is a fault-tolerant system, it is recommended to regularly back up the data to external storage.

* Backup is a per-node procedure. Make sure to back up each node in your
  cluster. For cluster-wide backup and restore, see [ScyllaDB Manager](https://manager.docs.scylladb.com/stable/restore/).
* Backup works the same for non-encrypted and encrypted SStables. You can use
  [Encryption at Rest](https://enterprise.docs.scylladb.com/stable/operating-scylla/security/encryption-at-rest.html)
  available in ScyllaDB Enterprise without affecting the backup procedure.

You can choose one of the following:

* [Full Backup](#backup-full-backup-snapshots) - Copy the entire data of a node.
* [Incremental Backup](#backup-incremental-backup) - Updates (delta) - flushed SSTables.

<a id="backup-full-backup-snapshots"></a>

## Full Backup - Snapshots

Snapshots are taken using [nodetool snapshot](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/nodetool-commands/snapshot.md). First, the command flushes the MemTables from memory to SSTables on disk, and afterward, it creates a hard link for each SSTable in each keyspace.
With time, SSTables are compacted, but the hard link keeps a copy of each file. This takes up an increasing amount of disk space. It is important to clear space by [clean unnecessary snapshots](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/procedures/backup-restore/delete-snapshot.md).

**Procedure**

1. Data can only be restored from a snapshot of the table schema, where data exists in a backup. Backup your schema with the following command:
<br/>
`$: cqlsh -e "DESC SCHEMA WITH INTERNALS" > <schema_name.cql>`
<br/>

For example:

`$: cqlsh -e "DESC SCHEMA WITH INTERNALS" > db_schema.cql`
<br/>

#### WARNING
To get a proper schema description, you need to use cqlsh at least in version `6.0.19`. Restoring a schema backup created by
an older version of cqlsh may lead to data resurrection or data loss. To check the version of your cqlsh, you can use `cqlsh --version`.

<br/>
2. Take a snapshot, including every keyspace you want to backup.
<br/>
`$ nodetool snapshot <KEYSPACE_NAME>`
<br/>
For example:
<br/>
`$ nodetool snapshot mykeyspace`
<br/>
The snapshot is created under Scylla data directory `/var/lib/scylla/data`
<br/>
It will have the following structure:
<br/>
`keyspace_name/table_name-UUID/snapshots/snapshot_name`
<br/>
For example:
<br/>
`/mykeyspace/team_roster-91cd2060f99d11e6a47a000000000000/snapshots/1487847672222`
<br/>

From one of the nodes, recreate the schema. Repeat these steps for each node in the cluster.

<a id="backup-incremental-backup"></a>

## Incremental Backup

Enabling the incremental backup (disabled by default) will create a hard-link from each SSTable, right after it is **flushed**, to a backups directory.
<br/>
For a complete point in time backup, the following is required:
<br/>
> * A snapshot
> * All incremental backups and commit logs from the time of the snapshot.
> * Make sure to delete unnecessary incremental backups. Scylla does not do this automatically.

**Procedure**

1. In the `/etc/scylla/scylla.yaml` file set the `incremental backups` parameters to `true` and restart the Scylla service. Snapshot are created under Scylla data directory `/var/lib/scylla/data`
<br/>
with the following structure:
<br/>
`keyspace_name/table_name-UUID/backups/backups_name`
<br/>
For example:
<br/>
`/mykeyspace/team_roster-91cd2060f99d11e6a47a000000000000/backups/1437827672721`
<br/>

## Additional Resources

* [Scylla Snapshots](https://opensource.docs.scylladb.com/branch-6.0/kb/snapshots.md)
