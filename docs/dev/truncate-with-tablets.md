# Background

The CQL TRUNCATE query is obviously data-modifying since it deletes user data, however,
unlike DELETE queries, it is not based on a tombstone that carries a write-timestamp,
with which we can determine for each cell, if it was logically written before
or after the tombstone timestamp.

Instead, truncate propagates in the cluster using an out-of-band RPC and is currently
applied locally in each node using the local wall clock of each node (not synchronized
with each other), typically after flushing the tables and taking a snapshot of them,
if auto-snapshot is enabled.

This presents a problem when tablets are enabled since there could on-going tablet
transitions in the background, that the user has no control on, like tablet migration,
with which a tablet might escape truncate by migrating between nodes just at the wrong
time so it won’t be considered for truncate by any node.

# Introduction

This design proposal relates to [Make TRUNCATE TABLE safe with tablets #16411](https://github.com/scylladb/scylladb/issues/16411).

Originally, the plan was to use a Truncate tombstone as per [Truncate must be based on a tombstone #11230](https://github.com/scylladb/scylladb/issues/11230).

This document lists the currently proposed solutions in order to reach consensus on how
truncate should be implemented.

# Proposed solutions

## 1. Short term mitigation

We can pause any background load balancing operations (if any are running) and execute the
truncate as it is currently implemented. We would issue a RPC which performs a memtable flush,
then take a snapshot of the SSTables if auto-snapshot is enabled, and delete the SSTables
belonging to the given table and associated views.

## 2. Truncate as a topology operation

We need a barrier that will wait on any on-going tablet transitions on the truncated table
AND on its subordinate views (materialized views and secondary indexes), and prevent any new
ones from taking place, similar to safe repair (See [Serialize repair with tablet migration #18641](https://github.com/scylladb/scylladb/pull/18641))

At this point we can build a transition plan, using a new, “truncate” tablet transition type,
for all tablet replicas for the truncate table and its view.

Open questions: should flush and snapshot happen as they do today, before preparing and executing
the truncate transition plan, or should they be performed inline, by each tablet, as it is truncated.
For the latter, we’ll need to pass the snapshot tag (a.k.a name) so the distributed snapshots will
use the same tag. This is harder, but we want to support a distributed snapshot anyhow in the future
for integrating backup with tablets (and the object storage stack)

## 3. Truncate with tombstones stored in SSTables

We need to implement a new type of token-range tombstone which will be stored in SSTables.
The advantage of this solution is that it fits well into the current tombstone logic since we already
have several kinds of tombstones: cell, record, range and partition. However, this also requires
changes in many places in the current code:

	- a new mutation fragment type
	- the commit log
	- row cache
	- memtable
	- SSTable writers and readers

Also, matching and filtering of SSTables will need to change.

While this solution seems to require the most amount of work, it has an advantage that token
range tombstones can also be used with business logic partitions.

## 4. Truncate with tombstones stored in table schema

With this solution we would store the tombstone in the table schema. The advantages are that
it would  be easier to integrate with the readers since they already have access to the schema.
And it could be propagated with the current schema change mechanisms. This solution allows
incorporating into the backup and restore procedure so that a table can be safely backed up
during truncate. This solution does not require all nodes to be up.

# Accepted solution

After a short discussion, the [Short term mitigation](#1-short-term-mitigation) solution was selected.
