# Cluster-wide compaction of tablet tables

## Motivation

A major compaction of a table, e.g. with `nodetool compact`, compacts every tablet
replica of the table on each node at once. It may therefore temporarily need as much
free disk space as the table takes, which makes it unusable for large tables. This is
especially true for TWCS, whose major compaction isn't incremental.

A cluster-wide compaction compacts a table tablet by tablet instead. The topology
coordinator schedules the tablets so that only a bounded number of tablet replicas is
compacted on each shard at any time. The temporary space overhead of compacting the
whole table is then bounded by the size of that many tablets per shard: with the
default of a single tablet per shard and about 100 tablets per shard, about 1% of a
shard's capacity.

A per-node major compaction is also at the mercy of tablet topology operations. A
migration (tablet cleanup), a merge or split, or an incremental repair (component
rewrite) of a tablet stops the compaction of that tablet, and the per-node major moves
on to the next tablet without coming back to it. A tablet that migrates to another node
is then not compacted there either, unless the user runs a major on that node too. So
a major compaction run node by node can miss tablets, and the user can't tell which.

A cluster-wide compaction compacts each tablet under a tablet transition, which the
topology coordinator serializes with the tablet's other transitions, so the tablet isn't
migrated, repaired, merged or split while it is being compacted. Its state is kept per
tablet in `system.tablets`, so a tablet that migrates before its turn is compacted at
its new location, and the state survives splits and merges. Every tablet of the table
is thus compacted once, whatever topology operations run meanwhile (see Limitations and
future improvements for what still fails the request).

The work is driven by a global topology request and tracked by a virtual task. The
user interface follows tablet repair (`nodetool cluster repair`).

The machinery is generic in the type of compaction: the request, the tablet transition
and the RPC carry a compaction type and its options. Only major compaction is
supported for now. Other types, such as reshape, upgrade or scrub, can be added later
(see Extending to other compaction types).

## User interface

### REST API

`POST /storage_service/tablets/compaction`, with these parameters:

| Parameter | Meaning |
|---|---|
| `ks` | Keyspace name. Optional when `table_id` is given; if given with it, it must be the table's keyspace. |
| `table` | Table name. |
| `table_id` | Table id, instead of `table`. Giving both `table` and `table_id`, or neither, is rejected. |
| `recursive` | `true` also compacts the tables derived from the table that aren't co-located with it (see below). |
| `type` | The type of compaction, `major` by default. Only `major` is supported for now. |
| `force_flush` | `true` flushes all tables on every node before compacting, regardless of `compaction_flush_all_tables_before_major_seconds` (see Flushing all tables). An option of every compaction type. |
| `await_completion` | `true` waits for the compaction to complete, and fails if it failed. |

Any other query parameter is an option of the compaction type, like the existing
compaction APIs take e.g. `scrub_mode`. A new type or option then doesn't need a new
API. Options that the type doesn't support are rejected, which also catches misspelled
parameters. Major compaction takes no options of its own for now. `force_flush` is an
option of every type, carried with the type's options.

The call queues a `compact_tablets` global topology request and returns its id.
The id is also the id of the task that tracks it. A call for tables that a queued or
ongoing request already compacts, with the same type and options, joins that request.
A call that overlaps such a request only partially, or with another type or options,
is rejected, since two requests compacting the same table would keep recompacting each
other's tablets.

The API requires the `TABLET_CLUSTER_COMPACTION` cluster feature.

### nodetool

    nodetool cluster compact [--type major] [--recursive] [--force-flush] [<keyspace> [<table>...]]
    nodetool cluster compact [--type major] [--recursive] [--force-flush] --table-id <id>... [<keyspace>]

It compacts the given tables, all tables of the keyspace, or all tables of all tablet
keyspaces. It runs them one after the other, waits for the task of each, and fails if
any of them failed. When compacting all tables, it skips the tables that are co-located
with a base table, since those are compacted together with their base table.

`--type` is passed through to the API's `type` parameter, `major` by default. The
server validates it, so a new type can be run without changing nodetool.
`--force-flush` sets the `force_flush` option.

### Concurrency limit

There is no option of its own. The number of tablets compacted at a time on a shard
is limited by the existing per-shard streaming budget of tablet transitions, set by
the `tablet_streaming_read_concurrency_per_shard` and
`tablet_streaming_write_concurrency_per_shard` node configuration options (2 by
default), as read by the topology coordinator. See Scheduling.

## State

The state lives in the system tables managed by group 0, so it survives coordinator
changes.

| Table | Column | Meaning |
|---|---|---|
| `system.topology_requests` | `cluster_compaction_table_id` | The table the request was made for. |
| | `cluster_compaction_table_ids` | All tables the request compacts (see Recursive compaction). |
| | `cluster_compaction_type` | The type of compaction, e.g. `major`. |
| | `cluster_compaction_options` (`map<text, text>`) | The options of the compaction type. |
| | `cluster_compaction_first_error` | The error of the first tablet that failed to compact, if any. |
| `system.topology` | `ongoing_cluster_compaction_requests` (static `set<timeuuid>`) | Requests picked up by the coordinator and being processed. |
| `system.tablets` | `cluster_compaction_request_id` | The id of the last request that processed the tablet. |
| | `cluster_compaction_failed` | Whether that request failed to compact the tablet. |

The type and options are kept only in the request. The tablet transition doesn't
carry them: the coordinator looks them up through the request of the tablet's table,
and passes them to the replicas in the RPC. In C++, the id stored in the tablets is a
`locator::cluster_compaction_id`, a tagged UUID equal to the id of the request.

A tablet is pending for request R as long as its `cluster_compaction_request_id`
differs from R. This has several advantages:
- nothing is written for the pending tablets up front;
- nothing has to be cleaned up when a request is aborted;
- no clock is compared across nodes.

Tablet resize keeps the state correct:
- **Split:** both new tablets inherit the parent's `tablet_info`, and with it its
  state.
- **Merge:** the merged tablet keeps `cluster_compaction_request_id` only when both
  siblings agree on it, so otherwise it is compacted again. The exception is a
  failure of either sibling, which sticks, since failures aren't retried.

The new `system.tablets` columns are written only once the `TABLET_CLUSTER_COMPACTION`
feature is enabled, since nodes that don't know them would fail to apply the
mutation.

## Scheduling

When the topology coordinator picks the request from the queue, it only adds the
request to `ongoing_cluster_compaction_requests` and returns to idle. Requests for
different tables can therefore be ongoing at the same time.

From then on, in every round, the load balancer
(`load_balancer::make_compaction_plan()`) does the following:
- It computes the streaming load of each shard, of the transitions that are already
  running, including tablet compactions, and of the migrations and repairs of the same
  plan, like `make_repair_plan()` does.
- It goes over the ongoing requests, oldest first. For each of their tables, it starts
  the `tablet_compaction` transition of tablets that:
  - are pending for the request;
  - have no other transition;
  - aren't being migrated or repaired in the same plan;
  - fit in the streaming budget of the shards of all of their replicas
    (`can_accept_load()`).
- It schedules tablets with replicas that are down too, so that they fail right away
  rather than hold up the request.
- It skips tables that are being migrated as a whole (`is_migrating_table`), and
  retries them in a later round.
- It runs even when load balancing is disabled.

A request is complete once every tablet of its tables has been processed, that is,
compacted or failed. The load balancer then reports its completion. The coordinator
removes the request from `ongoing_cluster_compaction_requests` and marks it done. The
request is marked failed if any tablet failed, or if any of its tables was dropped.

### Shared budget

A tablet compaction doesn't stream, but it reads and writes the whole tablet on each
of its replicas, much like repair. So `get_migration_streaming_info()` describes a
`tablet_compaction` transition like a `repair` one: it reads from and writes to all of
the tablet's replicas, with a weight of 2
(`tablet_migration_stream_weight_tablet_compaction`). It counts toward both the read
and the write load of each replica shard, against
`tablet_streaming_read_concurrency_per_shard` and
`tablet_streaming_write_concurrency_per_shard`, and `is_streaming()` counts the
transition while it runs. The weight is per transition kind, so it applies to every
compaction type.

As for every transition, `can_accept_load()` admits a transition on an idle shard
whatever its weight, and otherwise only while the shard's load plus the weight stays
within the limit. With the default limits of 2, this means:
- A shard compacts a single tablet at a time.
- A shard that compacts a tablet doesn't take part in migrations or repairs meanwhile,
  and a shard that streams a tablet, or repairs one, doesn't start a tablet
  compaction.

Sharing the budget, rather than having a separate limit, keeps the total load that
tablet transitions put on a shard bounded by a single set of limits, and lets
migrations and repairs take running compactions into account. Raising the limits to
4 allows 2 tablet compactions per shard, but also more concurrent migrations and
repairs.

Repairs are planned before tablet compactions in each round, so when both are pending
for the same shards, repairs take the budget first.

### Why a weight of 2

A major compaction loads a shard much like repair does, rather than like a
migration: it reads and writes the whole tablet on each of its replicas, and on top
of that I/O it spends CPU on processing the mutations in the tablet's sstables, which
it merges and rewrites. It is therefore given repair's weight of 2, rather than the
weight of 1 of a migration. With the default limits, that means a single major
compaction per shard at a time, which also keeps the temporary space overhead at its
lowest: the size of a single tablet per shard. The other compaction types that are
expected to be added, reshape, upgrade and scrub, rewrite the tablet's sstables too.

A weight of 1 was considered. It would allow 2 major compactions per shard, or a
major compaction alongside a migration, with the default limits, at the cost of twice
the space overhead and of more I/O and CPU contention with user requests and the
other tablet transitions. Operators who want more concurrency can raise the limits
instead, which keeps the relative cost of each kind of transition consistent.

## The `tablet_compaction` tablet transition

The transition kind and its only stage are both named `tablet_compaction`. Like
`repair` and `restore`, the transition doesn't change the tablet's replicas, so reads
and writes keep using the current ones. It only serializes the compaction with the
tablet's other transitions (invariant INV-TABL-2), so the tablet isn't migrated,
repaired or merged while it is being compacted.

The topology coordinator handles the stage by first running a global tablet token
metadata barrier, and then sending the `TABLET_COMPACTION` RPC to all replicas in
parallel, with the id, the compaction type and the options of the request of the
tablet's table. The barrier makes sure that no replica still runs the compaction of an
earlier transition of the tablet, aborted with its request (see Task tracking): the
replica's operation registry would otherwise let the new RPC join that operation and
inherit its failure, and the old RPC could run under the new transition. The tablets of a co-location group are compacted one after the
other. A replica that is down fails the tablet right away, without being sent the
RPC, which has no timeout.

On the replica, `storage_service::compact_tablet()` validates the type and options
first, with `storage_service::validate_tablet_compaction()`, since the coordinator may
run a newer version that supports more types. After `do_tablet_operation()`'s read
barrier, it flushes all tables of the node once per request (see Flushing all
tables). It then dispatches on the type. For a major compaction,
`table::major_compact_tablet()` does the following:
- It flushes the tablet's storage group.
- For each compaction group view in turn, it runs off-strategy compaction, so that
  sstables in the maintenance set are compacted too, and then a major compaction. The
  overhead is thus bounded by the size of the tablet.
  - Major compaction only compacts the main set. Tablet repair and file-based tablet
    streaming don't write into the maintenance set, but a tablet can still have
    sstables there after a restart, which moves sstables written by repair to the
    maintenance set (SCYLLADB-5476), or after a tablet rebuild with mutation-based
    streaming (SCYLLADB-5477). Once both are fixed, tablets no longer use the
    maintenance set, and the off-strategy step can be dropped.
- It fails unless the tablet was really compacted. To that end,
  `perform_major_compaction()` takes a `throw_if_stopping` flag, so that a compaction
  that doesn't run to completion fails instead of returning success. This covers a
  compaction stopped by the node's shutdown, by `nodetool stop`, or by compaction
  being disabled. It also fails if off-strategy compaction leaves sstables in the
  maintenance set.
- It stops the tablet's compactions when the transition goes away, through a tablet
  metadata guard.

The transition is cleared whether the compaction succeeded or failed. On success, the
tablet records the request in `cluster_compaction_request_id`. On failure, it also sets
`cluster_compaction_failed`, and the coordinator records the first error in the
request's `cluster_compaction_first_error`.

### Flushing all tables

Like a major compaction of a keyspace or of all keyspaces on a node
(`compaction::maybe_flush_commitlog()`), a cluster-wide compaction first flushes all
tables, on all shards, forcing new commitlog segments, so that it can purge as many
tombstones as possible: a tombstone can't be purged while the data it shadows may
still be replayed from the commitlog. Unless the request's `force_flush` option is
`true`, it does so only if `compaction_flush_all_tables_before_major_seconds` (a day by
default) elapsed since all tables were last flushed, and never if it is 0.

Each node does it once per request, before compacting the request's first tablet on
that node:
- `storage_service::flush_before_cluster_compaction()` keeps, on shard 0, a shared
  future of the flush of each ongoing request, which all the request's tablet
  compactions on the node wait for. It forgets the requests that are no longer
  ongoing.
- It is called after `do_tablet_operation()`'s group 0 read barrier, so the node
  knows that the request is ongoing, and doesn't forget its flush too early.
- A failed flush fails the tablet compaction, and the next tablet compaction of the
  request on the node tries again.

Doing it on the replicas, rather than on the coordinator before it starts scheduling
the request's tablets, doesn't block the topology coordinator while all nodes flush,
and only the nodes that hold replicas of the request's tables flush. `force_flush` is
carried with the other options of the request, so a request with another
`force_flush` doesn't join an ongoing one.

## Failure policy

The compaction is best effort and never retries:
- A tablet that fails is recorded as failed and isn't scheduled again.
- The other tablets keep being compacted. For example, a down node fails only the
  tablets it is a replica of.
- Once every tablet has been processed, the request fails if any tablet failed. Its
  error gives the number of failed tablets, a sample of them, and the first error.

## Co-location and recursive compaction

The request is made for the base table of a co-location group. The base table's
tablets keep the state and the transitions of the whole group, so all tables of the
group are compacted. As with repair, a request for another table of the group is
rejected, so that compacting all tables of a keyspace compacts each group only once.

With `recursive`, the request also compacts the tables derived from the table that
aren't co-located with it:
- its materialized views, including those backing secondary indexes;
- its CDC log table.

More precisely, it adds the base tables of the co-location groups of those derived
tables. A single request, and therefore a single task, carries the ids of all of
these tables in `cluster_compaction_table_ids`.

## Task tracking

The existing virtual task of the `global_topology_requests` task manager module, which
already tracks keyspace RF changes, also tracks `compact_tablets` requests. The
task id is the request id.
- **Scope:** the table the request was made for.
- **Progress:** the number of tablets of all the request's tables that it has
  processed (compacted or failed), out of all of them.
- **Error while running:** the first error of a tablet that failed, if any.
- **Abort:** removes the request from `ongoing_cluster_compaction_requests` and marks it
  done with an error, so the load balancer starts no more of its tablets. A request
  that is still queued is dropped when the coordinator finds it done.
- **Cancellation:** the compaction of a single large tablet can take long, so the
  tablets being compacted are canceled too. The topology coordinator ends the
  `tablet_compaction` transitions of tables that no longer have an ongoing request,
  without recording the request in their tablets, and drops the state it keeps for
  them. Ending a transition aborts the compaction on the replicas through their tablet
  metadata guards. The compaction RPCs then fail, which is logged, and their result is
  ignored. The coordinator ends the transitions itself, rather than the abort deleting
  them, because it would otherwise keep the state of a finished compaction RPC, which
  a later compaction of the same tablet would take as its own.
- **Replica side:** the tablet metadata guard on the tablet's shard only notices
  changes made after it was created, so `storage_service::compact_tablet()` checks
  the transition again in the guard's view of the tablet map. A compaction released
  after the transition went away fails rather than runs.

## Rolling upgrade

| Feature | Gates |
|---|---|
| `TABLET_CLUSTER_COMPACTION` | The API, and therefore the creation of requests and every write of the new `system.tablets` columns. |

## Extending to other compaction types

Adding a compaction type, e.g. scrub, takes:
- **Validation:** accept the type, and its options, in
  `storage_service::validate_tablet_compaction()`. It is used both when the request is
  created and on the replica.
- **Replica dispatch:** run the type's compaction of the tablet in
  `storage_service::compact_tablet()`, with a sibling of `table::major_compact_tablet()`
  named after the type, e.g. `table::scrub_tablet()`. It has to
  fail unless the tablet was really compacted, and stop when the tablet metadata
  guard's abort source fires, like `table::major_compact_tablet()`.
- **Options:** they come through the REST API as query parameters, through the
  request's `cluster_compaction_options`, and through the RPC, as strings. No API,
  schema or IDL change is needed.
- **A cluster feature of its own:** a coordinator of a newer version sending a type
  that a replica of an older version doesn't know would fail every tablet on that
  replica. So the request creation must be gated until all nodes support the type.
- **nodetool:** nothing, since `--type` is passed through. Options would need new
  command-line flags.
- **Flushing all tables:** nothing; `force_flush` and the flush apply to every type.

The transition's streaming weight, 2, is shared by all types.

## Tests

- `test/boost/tablets_test.cc`: `test_tablet_cluster_compaction_request_id` covers the
  persistence of the new tablet columns and the merge rules.
- `test/boost/sstable_compaction_test.cc`: `major_compaction_throws_if_not_run_test`
  covers a major compaction that doesn't run to completion failing with
  `throw_if_stopping`.
- `test/cluster/tasks/test_tablet_compaction.py` covers:
  - the end-to-end compaction and the task's progress;
  - the per-shard limit of a single tablet with explicit streaming limits of 2,
    observed while an error injection holds the replicas' compactions;
  - aborting the task, which cancels the compactions held by an error injection, so
    that no tablet is compacted;
  - a tablet that fails, which isn't retried, while the other tablets are compacted and
    the task fails;
  - `table_id`, and parameter validation, including the rejection of unsupported
    compaction types and options, and of an invalid `force_flush`;
  - `recursive` with a materialized view and a secondary index;
  - flushing all tables once per request on each node: skipped when all tables were
    flushed recently, and forced on every shard with `force_flush`.
- `test/nodetool/test_cluster_compact.py` covers the nodetool command line, including
  passing `--type` through to the server, and `--force-flush`.

## Limitations and future improvements

1. **Priority.** Migrations and repairs are planned before tablet compactions in each
   round, so a shard that keeps migrating or repairing tablets can delay the compaction
   of its tablets for as long as that goes on. Nothing reserves part of the budget for
   tablet compactions. The [Priority-based tablet scheduler
   SEP](https://scylladb.atlassian.net/wiki/spaces/RND/pages/505020438/Priority-based+tablet+scheduler)
   proposes to plan all kinds of transitions in priority order, with cluster-wide
   compaction as the lowest priority, and to age waiting candidates so that none
   starves.

2. **A running compaction holds its shards' budget.** With the default limits, a shard
   compacting a tablet starts no migration or repair until that compaction ends, and a
   migration of the tablet being compacted waits for it too. So urgent work, e.g.
   draining a node being decommissioned or rebuilding replicas after a node is lost,
   can wait for as long as it takes to compact one tablet on each of its shards, which
   can be minutes for a large tablet. The SEP above proposes preempting running
   transitions that are cheap to interrupt for higher-priority work. A tablet
   compaction is one: canceling it only leaves the tablet pending, to be compacted
   again later, which is what aborting a request already does (see Task tracking). The
   preemption policy is left to the SEP.

3. **Failures aren't retried.** A tablet whose replica is down when it is scheduled,
   or whose compaction is interrupted, e.g. by the node restarting, `nodetool stop` or
   compaction being disabled, is recorded as failed for the request, and the request
   fails. Running it again creates a new request, which compacts every tablet of the
   table again, not only the failed ones. A future improvement is an option to compact
   only the tablets that a given request failed, or that weren't compacted since a
   given request or time.

4. **Per-node major compaction is unchanged.** `nodetool compact` on a tablet table
   remains best effort while topology operations run, and still doesn't report which
   tablets it skipped.
