# Timestamp conflict resolution

The fundamental rule for ordering cells that insert, update, or delete data in a given row and column
is that the cell with the highest timestamp wins.

However, it is possible that multiple such cells will carry the same `TIMESTAMP`.
In this case, conflicts must be resolved in a consistent way by all nodes.
Otherwise, if nodes would have picked an arbitrary cell in case of a conflict and they would
reach different results, reading from different replicas would detect the inconsistency and trigger
read-repair that will generate yet another cell that would still conflict with the existing cells,
with no guarantee for convergence.

The first tie-breaking rule when two cells have the same write timestamp is that
dead cells win over live cells; and if both cells are deleted, the one with the later deletion time prevails.

If both cells are alive, their expiration time is examined.
Cells that are written with a non-zero TTL (either implicit, as determined by
the table's default TTL, or explicit, `USING TTL`) are due to expire
TTL seconds after the time they were written (as determined by the coordinator,
and rounded to 1 second resolution). That time is the cell's expiration time.
When cells expire, they become tombstones, shadowing any data written with a write timestamp
less than or equal to the timestamp of the expiring cell.
Therefore, cells that have an expiration time win over cells with no expiration time.

If both cells have an expiration time, the one with the latest expiration time wins;
and if they have the same expiration time (in whole second resolution),
their write time is derived from the expiration time less the original time-to-live value
and the one that was written at a later time prevails.

Finally, if both cells are live and have no expiration, or have the same expiration time and time-to-live,
the cell with the lexicographically bigger value prevails.

Note that when multiple columns are inserted (`INSERT`) or updated (`UPDATE`) using the same timestamp,
selecting (`SELECT`) those columns might return a result that mixes cells from either upsert.
This may happen when both upserts have no expiration time, or both their expiration time and TTL are the
same, respectively (in whole second resolution). In such a case, cell selection would be based on the cell values
in each column, independently of each other.
