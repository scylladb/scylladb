# Counters

Counters are special kinds of cells which value can only be incremented, decremented, read and (with some limitations) deleted. In particular, once deleted, that counter cannot be used again. For example:

```cql
> UPDATE cf SET my_counter = my_counter + 6 WHERE pk = 0
> SELECT * FROM cf;
 pk | my_counter
----+------------
  0 |          6

(1 rows)
> UPDATE cf SET my_counter = my_counter - 1 WHERE pk = 0
> SELECT * FROM cf;
 pk | my_counter
----+------------
  0 |          5

(1 rows)
> DELETE my_counter FROM cf WHERE pk = 0;
> SELECT * FROM cf;
 pk | my_counter
----+------------

(0 rows)
> UPDATE cf SET my_counter = my_counter + 3 WHERE pk = 0
> SELECT * FROM cf;
 pk | my_counter
----+------------

(0 rows)
```

## Counters representation
Counters are represented as sets of, so called, shards which are triples containing:
* counter id – uuid identifying the writer owning that shard (see below)
* logical clock – incremented each time the owning writer modifies the shard value
* current value – sum of increments and decrements done by the owning writer

During each write operation one of the replicas is chosen as a leader. The leader reads its shard, increments logical clock, updates current value and then sends the new version of its shard to the other replicas.

Shards owned by the same writer are merged (see below) so that each counter cell contains only one shard per counter id. Reading the actual counter value requires summing values of all shards.

### Counter id

The counter id is a 128-bit UUID that identifies which writer owns a shard. How it is assigned depends on whether the table uses vnodes or tablets.

**Vnodes:** the counter id is the host id of the node that owns the shard. Each node in the cluster gets a unique counter id, so the number of shards in a counter cell grows with the number of distinct nodes that have ever written to it.

**Tablets:** the counter id is rack-based rather than node-based. It is a deterministic type-3 (name-based) UUID derived from the string `"<datacenter>:<rack>"`. All nodes in the same rack share the same counter id.

During tablet migration, since there are two active replicas in a rack and in order to avoid conflicts, the node that is a *pending replica* uses the **negated** rack UUID as its counter id.

This bounds the number of shards in a counter cell to at most `2 × (number of racks)` regardless of node replacements.

### Merging and reconciliation
Reconciliation of two counters requires merging all shards belonging to the same counter id. The rule is: the shard with the highest logical clock wins.

Since support of deleting counters is limited so that once deleted they cannot be used again, during reconciliation tombstones win with live counter cell regardless of their timestamps.

### Digest
Computing a digest of counter cells needs to be done based solely on the shard contents (counter id, value, logical clock) rather than any structural metadata.

## Writes
1. Counter update starts with a client sending counter delta as a long (CQL3 `bigint`) to the coordinator.
2. CQL3 creates a `CounterMutation` containing a `counter_update` cell which is just a delta.
3. Coordinator chooses the leader of the counter update and sends it the mutation. The leader is always one of the replicas owning the partition the modified counter belongs to.
4. Now, the leader needs to transform counter deltas into shards. To do that it reads the current value of the shard it owns, and produces a new shard with the value modified by the delta and the logical clock incremented.
5. The mutation with the newly created shard is both used to update the memtable on the leader as well as sent to the other nodes for replication.

### Choosing leader
Choosing a replica which becomes a leader for a counter update is completely at the coordinator discretion. It is not a static role in any way and any concurrent update could be forwarded to a different leader. This means that all problems related to leader election are avoided.

The coordinator chooses the leader using the following algorithm:

1. If the coordinator can be a leader it chooses itself.
2. Otherwise, a random replica from the local DC is chosen.
3. If there is no eligible node available in the local DC the replica closest to the coordinator (according to the snitch) is chosen.

## Reads
Querying counter values is much simpler than updating it. First part of the read operation is performed as for all other cell types. When counter cells from different sources are being reconciled their shards are merged. Once the final counter cell value is known and the `CounterCell` is serialised, current values of all shards are summed up and the output of serialisation is a long integer.
