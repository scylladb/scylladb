# Backup and restore for strongly consistent tablets

This document discusses backup and restore for strongly consistent tablets (SC).
The current plan is to reuse existing backup and restore mechanisms for
eventually consistent tablets (EC), as there seem to be no major blockers. We
still need to deal with some technical details described here.

There is a draft PR (a proof of concept) based on this document:
https://github.com/scylladb/scylladb/pull/32596.

## Background

### Backup snapshots

A **backup snapshot** hard-links a table's sstables under `snapshots/<name>`
after a flush, with a manifest listing each sstable's token range and tablet ID.
Not to be confused with a **Raft snapshot**. `nodetool snapshot` takes a backup
snapshot on one node. The cluster-wide backup snapshot
(`/storage_service/tablets/snapshots`) is a global topology operation,
serialized with tablet migrations and resizes, that takes it on every node.

### Current backup mechanisms for EC

A backup uploads the sstables of a backup snapshot to a **backup location**: an
object storage endpoint, bucket and prefix (an S3, GCS or Azure bucket in
practice). There are two mechanisms.

A **per-replica backup** uploads one node's backup snapshot of one table, so
every replica's copy of the data is backed up separately. `nodetool backup`
(`/storage_service/backup`) does it for one node.

A **cluster backup** (`/storage_service/tablets/backup`) uploads a cluster-wide
backup snapshot. One coordinator decides which node uploads which sstables, asks
each node to upload its share (an RPC per node and table), and writes one
manifest per backup location. The API takes one location per datacenter, and in
practice every datacenter has its own bucket, so there is one manifest per
datacenter.

The cluster backup de-duplicates per sstable. For each tablet, the sstables a
repair proved equal on every replica (their `repaired_at` at or after the
tablet's) are uploaded from one replica only, the first in node order, and the
other replicas are listed as that copy's `additional_nodes`. The sstables not
repaired yet are uploaded from every replica.

A cluster backup can be restored only with the tablet-aware restore (described
below).

### Current restore mechanisms for EC

**Load and stream.** The sstables sit on one node, the loading node. It reads
them partition by partition and sends each partition's mutations to the replicas
that own it. The mutations travel over one streaming RPC connection per target
replica. A receiving replica writes them straight into new sstables of its own.
`nodetool refresh --load-and-stream` does it for the sstables an operator placed
in the table's `upload` directory. `nodetool restore`
(`/storage_service/restore`) is the same operation fed from object storage: the
node downloads the backed-up sstables from the bucket and load-and-streams them
as they arrive, keeping no copy on disk. Both take:
- `--scope` (`node`, `rack`, `dc`, `all` -- the default), which limits the
  target replicas to those on the loading node, in its rack or in its
  datacenter;
- `--primary-replica-only`, which streams each tablet's partitions to a single
  replica among those in the scope, chosen deterministically from the tablet ID,
  so the loads run from every node all land on the same replica and the other
  replicas stay empty until a repair copies the data to them (it can't be
  combined with `--scope node`).

When every node loads its own backup at once, which is how an operator restores
a full backup by hand, each node's backup holds a copy of every partition the
node replicated. With the default scope `all`, every partition would be streamed
to each replica once per loading node that holds it, i.e. as many times as it
has replicas (the sum of the RFs of all datacenters).

`--scope` avoids that duplication. With `node`, each node streams its copy to
itself only, so every replica gets its own backup back. With `rack` or `dc`, a
node's copies reach only the replicas in its rack or datacenter, for loads run
from one node per rack or per datacenter. The targets are chosen per partition,
i.e. per tablet. The narrower scopes are safe only for such coordinated loads,
where every partition is also loaded by one of its replicas: a partition with no
replica in the loading node, rack or datacenter is streamed to nobody, and a
repair can't recover data no replica received. Sstables that no other node loads
must therefore be loaded with `all`, alone or with `--primary-replica-only`
followed by a repair, which is what Scylla Manager does.

**Tablet-aware restore** (`/storage_service/tablets/restore`): a cluster-wide
restore of one table from the manifests of a per-replica or a cluster backup.
The topology coordinator puts every tablet through the `restore` transition
stage, in which each replica downloads the sstables backed up for its tablet's
token range from its own datacenter and rack, and attaches them. No streaming.

Both mechanisms are restarted until they succeed. A tablet-aware restore that
fails on a tablet clears it with an error and the request fails. Re-running it
restores the remaining tablets and skips the sstables already downloaded. A
load-and-stream that fails is simply run again: streaming the same mutations
twice changes nothing (same data, same timestamps), but nothing is skipped, so a
retry streams everything again.

Unless a cluster backup is restored with the tablet-aware restore, a full repair
is recommended after a restore, because the replicas may end up with different
data: the backup copies of a tablet differ unless a repair proved them equal,
and `--primary-replica-only` leaves all but one replica empty. Running the
repair automatically is planned (SCYLLADB-1997).

**Question**: this document currently assumes that we want to support all
mechanisms listed above for SC (with minor changes and limitations described
below), but we should think whether that's necessary. Supporting all of them
seems not that difficult, so we probably should care mostly about aligning
future plans for EC and SC, not about reducing the scope for SC.

### Scylla Manager

Scylla Manager is Scylla's tool for scheduling and orchestrating cluster
maintenance, in particular repairs, backups and restores. Scylla Cloud uses it
for every cluster and it is the recommended way elsewhere, but an operator can
also drive the nodetool commands and REST APIs directly.

Note that there is also the "What SC changes for Scylla Manager" section below.

#### Backup

Scylla Manager's backup is a per-replica backup. It takes a backup snapshot on
every node, dumps the schema and uploads both to the backup location given per
task with `--location`, with rclone or with the native `nodetool backup`.

The cluster backup API is planned to be driven by Scylla Manager as well
(SCYLLADB-3585).

**Question:** will Scylla Manager use cluster backup by 2027.1? Using only
cluster backup for SC could simplify.

#### Restore

Manager restores a table either with the tablet-aware restore or with
load-and-stream. It picks the path per table and there is no switch to choose
it. The tablet-aware restore is used when every node supports the API (2026.3),
the backup was taken with Scylla 2026.1.1 or later, both the backed-up and the
target keyspace use tablets, and the target cluster has a single DC. Every other
table goes through load-and-stream.

**Question:** will the single DC requirement for the tablet-aware restore
(blocked by SCYLLADB-5862) be dropped by 2027.1? This is the only requirement
that might not be satisfied for SC. Using only the tablet-aware restore for SC
could simplify.

The load-and-stream path splits the backed-up sstables into batches, downloads
each batch to a node with free disk space (any node of the datacenter, not
necessarily the one that backed the sstables up) and runs load-and-stream, or
`/storage_service/restore`, with `primary_replica_only=true`. Note that with
`primary_replica_only=true` and every backup copy streamed, before the repair
the primary replica of each tablet holds the union of all its copies and the
other replicas hold nothing restored.

#### Repair

After the data is restored, with either path, Manager repairs the restored
tables and re-creates Materialized Views. The repair stage is Manager's own,
outside the Scylla restore task.

#### Tombstone GC

Manager guards tombstone GC around the restore. Before loading any data it sets
`tombstone_gc = {'mode': 'disabled'}` on every restored table, and after the
repair it sets the table's original mode back. Without this, compaction could
purge a tombstone past `gc_grace_seconds` before a later batch delivered the row
it shadowed (compaction only sees the sstables already present), and the row
would come back.

### SC basics

An SC table replicates each tablet through a Raft group. We will initially
support only global consistency - a group per tablet; each tablet replica is a
group member. The current plan is GA in 2027.1.

Raft always keeps the data consistent between the tablet replicas within a
group. External mechanisms like repair and hints are never used.

Raft requires a quorum of replicas to elect a leader. Without a leader, Raft is
blocked.

The main use case for global consistency is a single DC (RF>0 in one DC only)
because otherwise the Raft members must continually communicate across DCs (huge
latency, high network cost). Nevertheless, from what I know, we still want to
support RF>0 in multiple DCs.

**Question:** Do we need to support backup and restore for global consistency in
multi-DC for GA? It's supposed to be a niche use case, and it introduces some
complications, as described below.

In the future, we will support local consistency - a group per tablet and DC.
This means that Raft won't keep the data consistent across DCs. We will need
some additional mechanism that will keep DCs eventually consistent.

## What SC changes for backup and restore

- **It's always enough to back up one (and any) replica**. Any state on any
  replica always represents a committed prefix of the Raft log, which is
  consistent among all replicas (they only might apply it at different times).

  This makes cluster backup a perfect fit for SC. It will be even simpler and
  more efficient because one copy of every sstable is always sufficient (no need
  to look at `repaired_at`).

  There will still be some redundancy for global consistency with multiple DCs
  because cluster backup saves one copy of each tablet per backup location, i.e.
  per DC when every DC has its own bucket. This should be acceptable and is hard
  to improve: in Scylla Cloud a node can only reach its own DC's bucket, and a
  single copy would be lost together with the DC that holds it (removed, or its
  RF changed to 0).

  Per-replica backup can be used, but it will be wasteful, or will have to be
  used for one replica per tablet to not be wasteful.

  It would be perfect to conclude that we will never have to deal with redundant
  per-replica backups for SC, but for now this document shortly explains how to
  deal with them.
- **Restore must give all replicas the same data**. Otherwise, the state would
  become inconsistent, which would later break linearizability of client
  requests. Nothing will reconcile the data after restore (except a Raft
  snapshot transfer, but that's not guaranteed and shouldn't be relied on). This
  means the following:
    - tablet-aware restore:
        - works out of the box with cluster backup and single DC,
        - works with cluster backup and multiple DCs too, but it must restore
          every DC from one location; alternatively, we could try to coordinate
          the snapshot to one applied index per tablet, which would make the
          per-DC copies identical and let each DC restore from its own location
          like EC does,
        - with per-replica backup, it must choose one backup copy for all
          replicas,
    - load-and-stream:
        - mustn't support `--primary-replica-only`, as it makes the replicas
          inconsistent,
        - is safe only with `--scope all`, so perhaps we should reject other
          scopes.
- **Repair after restore is never needed**.
- **Flush is not enough to ensure the backup snapshot contains all committed
  writes**. The reason is that a write is acked when committed by Raft, not when
  applied (happens later), and a committed write is present only in the
  commitlog, not in a memtable. We have three options:
    - do nothing and accept that a backup snapshot might miss some latest
      writes; that's the simplest option and might be acceptable since backups
      are done once in a while and miss a lot of data anyway,
    - execute `wait_for_apply(commit_idx)` before flush per tablet, which
      ensures that the snapshot will contain all locally committed writes
      (similar to EC),
    - execute `read_barrier()` before flush per tablet, which ensures that the
      snapshot will contain all globally committed writes; slowest, expensive
      since executed for each tablet, and requires Raft quorum (we can make it
      best-effort and not block without a quorum), but provides the strongest
      guarantees.

  Whichever wait we choose, it should be best effort: a tablet whose apply
  doesn't finish within a timeout (30 seconds for now) is skipped with a
  warning, so that a stuck Raft server can't block a snapshot forever. Such a
  snapshot might miss the latest writes of that tablet.

## What SC changes for Scylla Manager

This mostly depends on what mechanisms Scylla Manager will use when we ship
backup and restore for SC (the current plan is 2027.1).

Backup:
- If cluster backup is used, no changes needed, also for multi-DC: the backup
  saves one copy per location as for EC, and the multi-DC constraint (all
  replicas must restore the same data) is handled on the restore side (see
  below).
- If per-replica backup is used, no changes are needed for correctness either,
  but backing up all replicas will be wasteful. We could avoid that on the
  Scylla Manager side by backing up only one replica per tablet, but at this
  point we would basically reimplement cluster backup on the Scylla Manager
  side. We probably don't want to go in this direction.

Restore:
- If tablet-aware restore is always used, no or almost no changes are needed.
  Scylla Manager will have to point every datacenter at one location to make
  restore work with global consistency for multi-DC, assuming we want to support
  that and we won't handle this on the Scylla side. If for some reason all
  replicas were backed up redundantly, tablet-aware restore on the Scylla side
  will ignore all but one backup copy.
- If load-and-stream is used for multi-DC, it mustn't use `primary_replica_only`
  and ensure all replicas get the same data, perhaps with `--scope all`. If for
  some reason all replicas were backed up redundantly, Scylla Manager could
  limit load-and-stream to one replica per tablet (the per-node manifests list
  each sstable's tablet).

Repair must always be skipped for SC no matter what Scylla Manager uses.
Alternatively, we can make repair a no-op for SC on the Scylla side (arguably
ugly, but contained in Scylla).

The tombstone GC guard should stay as it is for SC. The reason is the same as
for EC.

## Assumptions needed for safety

- **No entries added to Raft during restore**. This includes client writes and
  TRUNCATE TABLE. Raft's own entries (like a new leader's dummy entry) don't
  touch the data. Tablet migrations and resizes aren't a problem, as the
  topology coordinator serializes them with the restore. We don't want Raft
  servers to change the data during restore, which happens outside Raft. Some
  dangers we want to avoid:
    - the leader might have outdated state, and thus writes might break
      linearizability,
    - the leader might initiate a Raft snapshot transfer concurrent with
      restore.

  Restore is normally an offline operation, so this is mostly about protecting
  against mistakes. We might add some guardrails or warnings in the docs.
- **No client reads during restore**. Restore is outside Raft, so such reads
  would break linearizability. Again, not a practical issue since restore is an
  offline operation.
- **We support restore only to an empty (new or truncated) table** just like for
  EC (where it is documented but not enforced). Tablet-aware restore can easily
  enforce it. It will be harder for load-and-stream, but maybe we don't have to
  do it. Again, we want Raft servers to be idle during restore.
- **Writes after restore must have higher timestamps than all restored data**.
  The SC leader stamps writes with `max(now, last_timestamp + 1)`, where
  `last_timestamp` is read from the tablet's data once, when the leader is
  elected. The restore attaches the data under leaders elected over the empty
  table, so `last_timestamp` knows nothing about the restored data and the first
  write after the restore is stamped with `now`. If the restored data has higher
  timestamps (from a clock that ran ahead on the backed-up cluster), the write
  is shadowed by it until the leader changes, when the new leader seeds
  `last_timestamp` from the data again. This should never happen in practice,
  but we can easily prevent it for the tablet-aware restore if we want.
  Load-and-stream has no point at which a replica knows the load-and-stream is
  over, so the gap stays there (as it does for EC, whose coordinators stamp
  writes with `now`).

## What to do with the Raft server during a restore

The current idea is the simplest one -- just leave the tablets' Raft servers be.
Under the assumptions above, they keep running while the tablet is being
restored, but they never do anything meaningful. Once restore completes, all
replicas have consistent state, so Raft invariants aren't broken.

If for some reason this idea turns out to be unsafe, we can try to block Raft
servers during restore, or have them temporarily destroyed.

Another approach is to make restore go through Raft (applied as a command or
replicated through a snapshot transfer), but that will be much more complicated,
and will diverge the EC and SC code paths (much harder to test and maintain).

## Raft snapshot transfer (SCYLLADB-3763, SCYLLADB-2573)

The Raft snapshot transfer will be implemented by transferring sstables. It will
be optimized: sstables already present on every replica will be excluded. We
should ensure that restored sstables will always be excluded. It might work out
of the box, or with some minor tweaks.

## TRUNCATE and DROP with `auto_snapshot`

TRUNCATE and DROP KEYSPACE/TABLE with `auto_snapshot` must create a backup
snapshot. DROP does, like for any table. TRUNCATE isn't supported for SC tables
yet (https://github.com/scylladb/scylladb/pull/31772).

We should ideally also wait for all committed writes before the flush. It will
be effective for TRUNCATE, but for DROP the schema change unregisters the table
and its tablet map first, so the Raft groups are already stopping before the
flush and thus there is nothing to wait for. Waiting earlier would mean waiting
inside the group0 schema apply, which will probably be complicated and
undesired. One option is to just accept that the backup snapshot triggered by
DROP might miss some committed writes.

TRUNCATE goes through every Raft group, so it must trigger a backup snapshot
separately for each tablet.

## Recovery from majority loss

This section is about the problem of permanently losing quorum for tablet Raft
groups, but not losing all replicas. Typically, 2/3 replicas are dead for RF=3.

The main difference compared to EC is that without Raft quorum tablet migrations
will get permanently stuck from now on because they perform Raft configuration
changes requiring quorum. This will include tablet migrations performed during
replacement/removal of dead nodes.

It should be possible to solve this problem in an automatic way, but that's
outside the scope for now. The current idea for GA is to rely on backup and
restore. The procedure may look like this:

1. Back up the tables from the surviving replicas first. With Scylla Manager,
   run an ad-hoc backup task with `sctool` (Scylla Manager's command line tool);
   it starts right away and skips the down nodes. By hand, take a snapshot and
   run `nodetool backup` on each surviving node.

   The rest of the procedure doesn't depend on this step, but it minimizes the
   data loss. The backup holds what the surviving replicas had applied (the
   snapshot waits for the writes a replica knows are committed, but a replica
   may hold a committed entry in its log without knowing it is committed yet).
   So the writes no surviving replica has applied will be lost, and with several
   surviving replicas of a tablet, the restore attaches one copy, so the writes
   that copy lacks are lost too. In most cases, there will be no such writes.
2. Drop the keyspace.
3. Replace/remove the dead nodes. Note that if the keyspace wasn't dropped
   first, the tablet migrations onto the new nodes would get stuck due to the
   quorum loss.
4. Re-create the keyspace and the tables. A strongly consistent table can't be
   resized yet, so the tablet-aware restore requires the table to have the
   backup's tablet count (the manifests record it); create the table with it.
5. Restore the backup.

If group0 also lost quorum, it should be recovered first just like without SC.

Setting up a new cluster and restoring the data to it is always an alternative
option, perhaps the best one after some disasters.

## Out of scope for now

The features below will be related to backup and restore for SC, so it's worth
highlighting them here.

### Local consistency

Probably no changes needed except for allowing per-DC backup and per-DC restore,
and relying on the mechanism that reconciles data across DCs (whatever that will
be exactly).

### Point-in-time-recovery (SCYLLADB-1067)

This feature is currently in progress only for EC. It adds continuous backup of
the commitlog on top of the periodic backups, and a restore to a point in time.
We might implement it for SC as well. The Raft logs of SC tables are already in
the commitlog, but as Raft entries rather than plain mutations. PITR's replay
can reuse the startup replay's decoding of Raft entries and its bookkeeping of
superseded ones, but it has to apply the committed entries itself, as the
backed-up Raft groups no longer exist, and it must give every replica the same
replay.

### Thin replication (SCYLLADB-204)

With thin replication, a thin replica keeps only the data not yet applied by
every replica, so its copy is partial, and shouldn't be restored. Cluster backup
must not pick it as the kept copy. Per-replica backup should be rejected on a
thin replica. Also, restore should exclude thin replicas.
