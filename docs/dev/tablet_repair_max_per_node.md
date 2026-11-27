# Limiting tablet repairs per node

This document describes tablet_repair_max_per_node, a cluster config option that limits the
number of tablet repairs a node takes part in at the same time.

## Problem

Tablet repair, whether requested by a user (for example by Scylla Manager) or started by
automatic repair, is scheduled by the topology coordinator. Each tablet repair is a
transition of kind repair. The scheduler accounts it as streaming load of weight 2 on every
replica shard, against a per-shard limit of tablet_streaming_read_concurrency_per_shard and
tablet_streaming_write_concurrency_per_shard (default 2), and an idle shard always accepts
one. The result is one tablet repair per shard, on every shard of every node, at the same
time. Repair runs at a low priority, but its IO and CPU still raise the latency of the user
requests served by the same shard, and with every shard repairing no request avoids it.

None of the existing knobs lowers the number of repairing shards:

- The streaming concurrency limits cannot go below one tablet per shard.
- Scylla Manager's --intensity and --parallel apply to vnode repair only. The tablet repair
  API takes no such parameters, and automatic repair does not go through Manager at all.
- stream_io_throughput_mb_per_sec caps the streaming IO class, which also slows tablet
  migration and node operations.

## The option

tablet_repair_max_per_node is the maximum number of tablet repairs a node takes part in at
the same time, as the repair master or as a follower. The default 0 means no per-node limit,
so only the per-shard streaming limit applies, as before the option existed. The smallest
limit is 1: the option cannot stop tablet repair on a node.

It is a native cluster config option with the node-oriented scopes (CLUSTER, DATACENTER,
RACK, NODE). It is replicated through group0, so every node sees the same value, and it takes
effect at the next scheduling decision without a restart. A datacenter that serves
latency-sensitive traffic can have a lower limit than the rest of the cluster:

```cql
ALTER CLUSTER WITH tablet_repair_max_per_node = 2;
ALTER DATACENTER dc_serving WITH tablet_repair_max_per_node = 1;
ALTER NODE 0a2c9b38-6e57-4d1f-8f2c-2b1f8e3d9c11 WITH tablet_repair_max_per_node = 1;
```

Only transitions of kind repair count and are limited: user-requested and automatic repair.
Repairs that are part of node operations (the rebuild_repair stage, restore) are neither
counted nor limited, so a low limit never slows down a replace or a decommission.

The option does not change how a tablet repair runs once it has started. It only decides
when the next one starts.

## Implementation

The limit is enforced in make_repair_plan in the tablet allocator, which already builds a
per-node load map with the streaming load of the transitions in progress and of the
migrations in the plan being built. The node entry of the load map gains a counter of tablet
repairs.

- When the scheduled load is collected, every transition of kind repair in a streaming stage
  increments the counter of each of its replica hosts.
- A candidate tablet is accepted only if every replica host has a counter below its limit and
  it passes the existing per-shard check. On acceptance, the counters of all its replica hosts
  are incremented.
- The limit of a host is resolved through the cluster config manager with the host's
  datacenter, rack and host id, once per host per plan.

No new transition stage, system.tablets column or RPC is needed.

### Reservation

Candidates are sorted with user requests first, then by the time since their last repair,
then by tablet id. A tablet needs all its replica hosts below their limit in the same plan.
Without further rules, tablets later in the order that share only some of those hosts could
take them one by one as they become free, and the earlier tablet would wait for as long as
such tablets exist.

To bound the wait, the first candidate in the order that is rejected by the per-node limit
reserves one repair slot on each of its replica hosts for the rest of the plan: the slot is
counted like a repair when later candidates are checked against the limit. A host of the
tablet that is at its limit starts no new repair, and a host below its limit keeps one slot
free, so the tablet is scheduled once a repair in progress finishes on each host that is at
its limit. Later candidates can still use the other free slots of those hosts. The tablet id
in the sort order keeps the reservation on the same tablet from one plan to the next when
repair times are equal.

There is one reservation per plan, so at most one slot per host is held back at a time. A
candidate rejected only by the per-shard streaming limit reserves nothing, as before.

## Costs

- With S shards per node and a limit of k, repairing a table takes up to S / k times longer.
  An operator either sizes the repair window for it or repairs more often: with incremental
  repair the work is proportional to the data written since the last repair.
- A node with a low limit also holds back repairs of tablets whose other replicas have a
  higher limit.
- The option does not shorten the latency impact of a repair on the shards that run it. It
  lowers the number of shards that repair at the same time.

## Observability

The load balancer logs at debug level when it skips a tablet because a host is at its
tablet_repair_max_per_node limit, counting a slot reserved for an earlier candidate.

## Compatibility

Every registered cluster config option has a cluster feature, and the option can only be set
once all nodes know it. A node that does not know the option behaves as with the default.
When the topology coordinator moves to another node, the new coordinator rebuilds the repair
counters from the transitions in progress.

## Testing

- test/boost/tablets_test.cc, test_tablet_repair_max_per_node: the scheduler respects the
  cluster and node limits, counting both the repairs in the plan and those in progress.
- test/boost/tablets_test.cc, test_tablet_repair_max_per_node_reserves_hosts: a tablet
  rejected by the limit reserves its replica hosts against tablets repaired more recently.
- test/boost/tablets_test.cc, test_tablet_repair_max_per_node_reserves_one_slot: with a limit
  above 1 the reservation takes one slot per host and the other slots stay usable.
- test/cluster/test_tablet_repair_max_per_node.py: on a live cluster, the per-node limit
  bounds the number of tablets in the repair stage at once.
