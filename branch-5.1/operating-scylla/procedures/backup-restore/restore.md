# Restore from a Backup and Incremental Backup

Restoring a keyspace from a backup requires all snapshot files of the tables, and (if available) incremental backup files taken after the snapshot. Before restoring from backup, the table data must be truncated, making sure that the existing data does not overwrite the restored data.

#### NOTE
For cluster wide backup and restore, see [ScyllaDB Manager](https://manager.docs.scylladb.com/).

#### NOTE
The following procedure assumes data is restored to the same cluster that was backed-up:

- same number of nodes
- same IPs
- same token range per node

The procedure restores each node using the backup file of the **same node**.
If this is not the case, one should use other restoration methods tools like [sstableloader](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/procedures/cassandra-to-scylla-migration-process.md). This procedure is much slower than restoring to the same topology cluster.

<a id="restore-procedure"></a>

## Procedure

From **one** of the nodes, recreate the schema.
<br/>

`cqlsh -e "SOURCE '/path_to_schema/<schema_name.cql>'"`

For example:
<br/>

`cqlsh -e "SOURCE 'centos/db_schema.cql'"`

### Repeat the following steps for each node in the cluster:

#### NOTE
If you are restoring [encrypted backup files](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/encryption-at-rest.md), make sure Scylla Enterprise has the same keys used by Scylla to encrypt the data before starting the restore process.

#### NOTE
Best practise is **not** to restore [Materialized Views (MV)](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/materialized-views.md) and [Secondary Indexes (SI)](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/secondary-indexes.md) SSTables.
It is recommended to:

- Drop the MV and SI using DROP MATERIALIZED VIEW or DROP INDEX
- Restore the base table only (see below)
- Recreate the  MV or SI, using the original description from the CQL backup, using CREATE MATERIALIZED VIEW or CREATE INDEX

1. Run the [nodetool drain](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/nodetool-commands/drain.md) command to ensure the data is flushed to the SSTables
2. Shut down the node

   Supported OS
   ```shell
   sudo systemctl stop scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl stop scylla
   ```

   (without stopping *some-scylla* container)
3. Delete all the files in the commitlog. Deleting the commitlog will prevent the newer insert from overriding the restored data.

   `sudo rm -rf /var/lib/scylla/commitlog/*`
4. Delete all the files in the keyspace_name_table. Note that by default the snapshots are created under Scylla data directory `/var/lib/scylla/data/keyspace_name/table_name-UUID/`.

   Make sure NOT to delete the existing snapshots in the process.

   For example:
   ```shell
   sudo ll /var/lib/scylla/data/mykeyspace/team_players-6e856600017f11e790f4000000000000

   -rw-r--r-- 1 scylla   scylla     66 Mar  5 09:19 nba-team_players-ka-1-CompressionInfo.db
   -rw-r--r-- 1 scylla   scylla    669 Mar  5 09:19 nba-team_players-ka-1-Data.db
   -rw-r--r-- 4 scylla   scylla     10 Mar  5 08:46 nba-team_players-ka-1-Digest.sha1
   -rw-r--r-- 1 scylla   scylla     24 Mar  5 09:19 nba-team_players-ka-1-Filter.db
   -rw-r--r-- 1 scylla   scylla    218 Mar  5 09:19 nba-team_players-ka-1-Index.db
   -rw-r--r-- 1 scylla   scylla     38 Mar  5 09:19 nba-team_players-ka-1-Scylla.db
   -rw-r--r-- 1 scylla   scylla   4446 Mar  5 09:19 nba-team_players-ka-1-Statistics.db
   -rw-r--r-- 1 scylla   scylla     89 Mar  5 09:19 nba-team_players-ka-1-Summary.db
   -rw-r--r-- 4 scylla   scylla    101 Mar  5 08:46 nba-team_players-ka-1-TOC.txt
   drwx------ 5 scylla   scylla     69 Mar  6 08:14 snapshots
   drwx------ 2 scylla   scylla      6 Mar  5 08:40 upload

   sudo rm -f  /var/lib/scylla/data/mykeyspace/team_players-6e856600017f11e790f4000000000000/*

   rm: cannot remove ‘/var/lib/scylla/data/nba/team_roster-c019f8108fda11e8b16a000000000001/snapshots’: Is a directory
   rm: cannot remove ‘/var/lib/scylla/data/nba/team_roster-c019f8108fda11e8b16a000000000001/upload’: Is a directory

   sudo ll /var/lib/scylla/data/mykeyspace/team_players-6e856600017f11e790f4000000000000/

   drwx------ 5 scylla   scylla     69 Mar  6 08:14 snapshots
   drwx------ 2 scylla   scylla      6 Mar  5 08:40 upload
   ```
5. Select the snapshot you want to restore (usually the most recent one)
   ```shell
   /var/lib/scylla/data/keyspace_name/table_name-UUID/snapshots/<snapshot_name>
   ```

   For example:
   ```shell
   cd /var/lib/scylla/data/mykeyspace/team_players-6e856600017f11e790f4000000000000/snapshots/1487847672222
   ```
6. Copy the snapshots directory content to the `/var/lib/scylla/data/keyspace_name/table_name-UUID/` directory

   For example:
   ```shell
   sudo cp -r * /var/lib/scylla/data/mykeyspace/team_players-6e856600017f11e790f4000000000000
   ```
7. If you have incremental backup files, copy them from the **backups** folder `/var/lib/scylla/data/keyspace_name/table_name-UUID/backups/<backups_name>` to  the `/var/lib/scylla/data/keyspace_name/table_name-UUID/` directory

   For example:
   ```shell
   sudo cp -r * /var/lib/scylla/data/mykeyspace/team_players-6e856600017f11e790f4000000000000
   ```
8. Make sure that all files are owned by the `scylla` user and group:
   ```shell
   sudo chown -R scylla:scylla /var/lib/scylla/data/mykeyspace/team_players-6e856600017f11e790f4000000000000
   ```
9. Start the node

   Supported OS
   ```shell
   sudo systemctl start scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl start scylla
   ```

   (with *some-scylla* container already running)
10. Run `nodetool repair` command to guarantee that your data is consistent with other nodes.

After performing the above on all nodes, repair the cluster with nodetool repair. This makes sure that the data is consistent on all nodes and between each node.
