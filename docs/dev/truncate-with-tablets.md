# Background

The CQL TRUNCATE query is obviously data-modifying since it deletes user data, however,
unlike DELETE queries, it is not based on a tombstone that carries a write-timestamp,
with which we can determine for each cell, if it was logically written before
or after the tombstone timestamp.

Instead, truncate propagates in the cluster using an out-of-band RPC and is currently
applied locally in each node using the local wall clock of each node (not synchronized
with each other), typically after flushing the tables and taking a snapshot of them,
if auto-snapshot is enabled.

This presents a problem when tablets are enabled since there could be on-going tablet
transitions in the background, that the user has no control over, like tablet migration,
with which a tablet might escape truncate by migrating between nodes just at the wrong
time so it won’t be considered for truncate by any node.

This document lists the currently proposed solutions and lists the selected proposal.

# Proposed solutions

## 1. Short term mitigation (disable/enable load balancing)

We can disable any background load balancing operations (if any are running) and execute the
truncate as it is currently implemented. We would issue a RPC which performs a memtable flush,
then take a snapshot of the SSTables if auto-snapshot is enabled, and delete the SSTables
belonging to the given table and associated views. After truncate is complete, we should
re-enable load balancing.

A problem with this solution is that a truncate operation which is issued while another
truncate is already running could potentially run into the same problem with resurecting
data which is in migration. Consider the following scenario:

- a truncate table command is received (TT1)
- load balancing is disabled as a result of TT1
- truncating the table and views for TT1 is started
- another truncate table command is received (TT2)
- TT2 does not disable load balancing, as it is already disabled
- truncating the table for TT1 completes
- TT1 enables load balancing
- TT2 starts executing the truncate operation while load balancing is enabled potentially
causing the problem with tablet migration resurecting truncated data

Another potential problem is that load balancing can be manually enabled by the admin while
truncate is still running, causing the same problem with resurrected data due to migration.

If the node which controls the truncate operation crashes before truncate is complete,
it will leave load balancing disabled.

## 2. Short term mitigation (holding ERM pointer)

Holding an effective replication map pointer on any node will pause load balancing until
the pointer is released. Therefor, obtaining an ERM pointer before truncate starts will
pause load balancing operations until truncate is complete, when the ERM pointer can be
released, which will resume load balancing.

This solution does not suffer from the problems with concurrent truncate operations, or
with manual load balancing disable/enable by an admin.

## 3. Truncate as a global topology operation

Implementing truncate as a global topology operation ensures that no other tablet
transitions are executed concurrently. This way, we avoid problems with truncate leaving
behind resurrected data.

Truncate should flush and make snapshots as it does today, while executing the truncate
operation.

## 4. Truncate with tombstones stored in SSTables

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
range tombstones have a wider applicability. However, further discussion is needed to clarify
the details about the use of these tombstones, how they should look like and how they should
propagate.

## 5. Truncate with tombstones stored in table schema

With this solution we would store the tombstone in the table schema. The advantages are that
it would  be easier to integrate with the readers since they already have access to the schema.
And it could be propagated with the current schema change mechanisms. This solution allows
incorporating into the backup and restore procedure so that a table can be safely backed up
during truncate. This solution does not require all nodes to be up.

# Accepted solution

While the __Short term mitigation (holding ERM pointer)__ is the simplest solution, it has
a problem with livelock, where it is possible for TRUNCATE to never acquire the ERM pointer
due to other processes acquiring it before TRUNCATE.

Because of this, __Truncate as a global topology operation__ solution was chosen to be
implemented, as the next best in terms of required effort.
