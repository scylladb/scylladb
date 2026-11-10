# Tablet repair intensity

This document describes how an operator controls the load tablet repair puts on a cluster,
through two cluster config options: tablet_repair_max_per_node and tablet_repair_intensity.

## Problem

Tablet repair, whether requested by a user (for example by Scylla Manager) or started by
automatic repair, is scheduled by the topology coordinator. Each tablet repair is a
transition of kind repair. The scheduler accounts it as streaming load of weight 2 on every
replica shard, against a per-shard limit of tablet_streaming_read_concurrency_per_shard and
tablet_streaming_write_concurrency_per_shard (default 2), and an idle shard always accepts
one. The result is one tablet repair per shard, on every shard of every node, at the same
time, and as soon as one tablet repair finishes the next one starts.

None of the existing knobs makes this gentler:

- The streaming concurrency limits cannot go below one tablet per shard.
- Scylla Manager's --intensity and --parallel apply to vnode repair only. The tablet repair
  API takes no such parameters, and automatic repair does not go through Manager at all.
- stream_io_throughput_mb_per_sec caps the streaming IO class, which also slows tablet
  migration and node operations.

## Model

Repair delays user requests in two dimensions:

- Space: how many shards of a node are repairing at the same time.
- Time: what share of the time a node spends repairing.

The share of user work exposed to repair is roughly (k / S) * (p / 100), where k is the
number of repairing shards per node, S the number of shards per node, and p the percentage
of time the node spends in repair. Today k = S and p = 100. There is one option for each
factor, both enforced by the repair scheduler on the topology coordinator:

| Option | Controls | Default |
|---|---|---|
| tablet_repair_max_per_node | k | 0, no per-node limit |
| tablet_repair_intensity | p | 100, no rest between repairs |

Both defaults keep the behavior from before the options existed. Neither option changes how a
tablet repair runs once it has started: a repair in progress holds its tablet in transition,
its repair memory and its input sstables for as short a time as before. The options only
decide when the next repair starts.

## Options

Both are native cluster config options with the node-oriented scopes (CLUSTER, DATACENTER,
RACK, NODE). They are replicated through group0, so every node sees the same value, and they
take effect at the next scheduling decision without a restart. A datacenter that serves
latency-sensitive traffic can repair more gently than the rest of the cluster:

```cql
ALTER CLUSTER WITH tablet_repair_max_per_node = 2;
ALTER CLUSTER WITH tablet_repair_intensity = 50;
ALTER DATACENTER dc_serving WITH tablet_repair_intensity = 10;
ALTER NODE 0a2c9b38-6e57-4d1f-8f2c-2b1f8e3d9c11 WITH tablet_repair_max_per_node = 1;
```

### tablet_repair_max_per_node

The maximum number of tablet repairs a node takes part in at the same time, as the repair
master or as a follower. 0 means no per-node limit, so only the per-shard streaming limit
applies.

Only transitions of kind repair count and are limited: user-requested and automatic repair.
Repairs that are part of node operations (the rebuild_repair stage, restore) are not, so a
low limit never slows down a replace or a decommission.

### tablet_repair_intensity

The percentage of time a node spends in tablet repair, from 1 to 100. After the last tablet
repair a node took part in finishes, having taken t, the node takes part in no new tablet
repair until it has rested for t * (100 - p) / p. At 25, a repair of one minute is followed
by three minutes of rest, so repairing a table takes about four times longer and the node
spends a quarter of the time on repair.

A tablet is repaired only once every replica node has rested, each with its own value, so a
repair that crosses datacenters waits for the gentlest setting among them. The rest is
computed from the current value whenever the scheduler runs, so a change applies at once:
raising the value to 100 releases every resting node.

## Implementation

Both options live in make_repair_plan in the tablet allocator, which already builds a per-node
load map with the streaming load of the transitions in progress and of the migrations in the
plan being built.

### Per-node limit

The node entry of the load map gains a counter of tablet repairs.

- When the scheduled load is collected, every transition of kind repair in a streaming stage
  increments the counter of each of its replica hosts.
- A candidate tablet is accepted only if it passes the existing per-shard check and every
  replica host has a counter below its limit. On acceptance, the counters of all its replica
  hosts are incremented.

### Rest between repairs

The tablet allocator keeps, per host, when the last tablet repair the host took part in
finished and how long it took. The topology coordinator records it when the repair RPC to
the repair master returns, for every replica of the tablet.

- A candidate tablet is accepted only if, for every replica host, now is past the end of its
  last repair plus its duration scaled by (100 - p) / p, with p resolved for that host.
- The history lives in memory on the coordinator and is cleared when it loses leadership, so
  a new coordinator starts without rest. It is bounded by the number of nodes.
- With several repairs per node in flight, the rest starts from the last one to finish, so a
  node starts its next repairs in batches: run k repairs, rest, run k more. The duty cycle
  holds per node, not per shard.

The limit and the rest of a host are resolved through the cluster config manager with the
host's datacenter, rack and host id, once per host per plan. Candidates are already sorted by
the time since their last repair, so under either option the tablets that waited longest go
first. No new transition stage, system.tablets column or RPC is needed.

### Why not throttle inside the repair

An earlier version paused the repair master between row-level rounds for the same duty cycle.
That gives a finer grain, but every pause holds the tablet in transition, the repair memory
reservation on the master and the followers, the input sstables marked as being repaired and
the tablet's major-compaction lock, for up to a hundred times longer at low settings. Resting
between repairs holds nothing while idle. Both variants lower the share of time a node spends
repairing rather than the load while it repairs, so the finer grain bought little.

Scheduling group shares were not used because they only throttle under contention and do not
address the compaction and cache effects that follow a repair; spacing repairs out in time
does, by giving the node time to absorb them.

## Costs

- The time to repair a table grows by up to (S / k) * (100 / p). An operator either sizes the
  repair window for it or repairs more often: with incremental repair the work is
  proportional to the data written since the last repair, so frequent small repairs cost the
  same in total as rare large ones.
- A resting node also holds back repairs of tablets whose other replicas are not resting, so
  one gently configured node slows down the repair of every table it replicates.
- Neither option shortens the latency impact of a repair while it runs; they make repairs
  less frequent.

## Observability

- scylla_load_balancer_repairs_skipped_cooldown: number of times the scheduler skipped a
  candidate tablet because a replica node was resting.
- The load balancer logs at debug level when it skips a tablet because a host is at its
  tablet_repair_max_per_node limit or resting.

## Compatibility

Every registered cluster config option has a cluster feature, and the option can only be set
once all nodes know it. A node that does not know the options behaves as with the defaults.
When the topology coordinator moves to another node, the new coordinator rebuilds the repair
counters from the transitions in progress and starts with an empty repair history.

## Testing

- test/boost/tablets_test.cc, test_tablet_repair_max_per_node: the scheduler respects the
  cluster and node limits, counting both the repairs in the plan and those in progress.
- test/boost/tablets_test.cc, test_tablet_repair_intensity: a recorded repair keeps a node
  out of the plan for the scaled duration, with the node's own value, and not at the default.
- test/cluster/test_tablet_repair_intensity.py: on a live cluster, the per-node limit bounds
  the tablets in the repair stage at once, and at intensity 1 the scheduler skips candidates
  while nodes rest until the value is removed, for cluster and node scope values.
