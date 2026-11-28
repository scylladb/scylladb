.. highlight:: cql

.. _synthetic-timestamps:

Synthetic Write Timestamps
==========================

Every cell, row marker, and tombstone carries a write timestamp. Unless the
client provides one, the coordinator uses the current time in microseconds
since the Unix epoch. With ``USING TIMESTAMP`` (or the timestamp field of the
CQL binary protocol), the client can instead provide *synthetic* timestamps:
values that have no relationship to real time, such as a version number kept
by the application, a logical or hybrid clock, or values that, if they were
interpreted as microseconds since the epoch, would be in the distant past,
negative, or in the future.

ScyllaDB supports synthetic timestamps, subject to the requirements and
limitations below.

What works
----------

Within a table, write timestamps are only ever compared with each other, so
the following work the same as with wall-clock timestamps:

* Conflict resolution between writes (see :ref:`update ordering <update-ordering>`),
  regardless of the order in which the writes arrive.
* Deletions of all kinds (cell, row, range, and partition tombstones):
  a deletion shadows the writes with a lower or equal timestamp, including
  writes that arrive after it.
* Memtable flushes and compaction, with all compaction strategies.
* Tombstone garbage collection, in all ``tombstone_gc`` modes.
* Read repair, row-level repair, and full and incremental tablet repair.
* Hinted handoff, the batchlog, streaming, and tablet migration.
* :ref:`BATCH <batch_statement>` statements, logged and unlogged.
* Materialized view updates, which inherit their timestamps from the base
  table writes.
* Appending to a list, as long as the timestamp is within the range of a
  ``timeuuid`` (see :ref:`Limitations <synthetic-timestamps-limitations>`).

Some machinery does run on the wall clock, but it never derives anything
from the write timestamp:

* A tombstone's *deletion time*, which determines when it may be garbage
  collected (after ``gc_grace_seconds``, or after a repair), is the time at
  which the deletion arrived at the replica.
* A TTL is counted from the time the write arrived at the replica. A write
  ``USING TTL 60`` expires 60 seconds after it was made, whatever its
  timestamp.

Requirements
------------

**Every write must carry a timestamp.** A write without ``USING TIMESTAMP``
(and without a protocol-level timestamp) gets a wall-clock timestamp from the
coordinator, which has no meaningful order relative to synthetic timestamps.
For the same reason, do not mix synthetic timestamps with
:doc:`Lightweight Transactions </features/lwt>` (which always use their own
wall-clock based timestamps and reject ``USING TIMESTAMP``), or with
:doc:`counters </features/counters>` (which do not support ``USING TIMESTAMP``),
on the same data.

**Timestamps must be in range.** Any signed 64-bit integer is a valid
timestamp, except the smallest one, -2\ :sup:`63`, which is reserved.
However, by default ScyllaDB rejects ``USING TIMESTAMP`` values more than
three days in the future when interpreted as microseconds since the epoch.
To use larger values, set the ``restrict_future_timestamp`` configuration
option to ``false``.

**Do not write data older than a tombstone that may already have been
garbage-collected.** This is the same rule that applies to wall-clock
timestamps: once a tombstone has been garbage-collected, a write that arrives
afterwards with a lower timestamp is no longer shadowed by it, and becomes
visible. With wall-clock timestamps this is rare, because late writes carry
old timestamps only through clock skew or replay. With synthetic timestamps,
it is up to the application to never issue a write with a timestamp at or
below that of a deletion of the same data more than ``gc_grace_seconds`` (or,
with ``tombstone_gc`` mode ``repair``, more than one repair cycle) after the
deletion. See :ref:`tombstones GC <ddl-tombstones-gc>`.

.. _synthetic-timestamps-limitations:

Limitations
-----------

The following features compare write timestamps with the wall clock, or
generate writes with wall-clock timestamps, and are not supported together
with synthetic timestamps:

* **Dropping a column without USING TIMESTAMP.**
  ``ALTER TABLE ... DROP`` hides the column's cells whose timestamp is at
  or below the drop time, which is the wall-clock time unless
  ``USING TIMESTAMP`` is given. With synthetic timestamps, always drop
  columns with ``ALTER TABLE ... DROP ... USING TIMESTAMP``, giving a
  timestamp from the application's timeline. If the column is re-added,
  writes to it must use timestamps above the drop timestamp.
* **Change Data Capture.** CDC selects the stream for a write by its
  timestamp, and rejects writes with timestamps more than a few seconds away
  from the current time. Even apart from that, CDC log entries are keyed and
  ordered by a ``timeuuid`` derived from the write timestamp, and CDC readers
  consume the log by time windows that trail the current time; with
  synthetic timestamps the log is not a sequence of changes in the order
  they were made. See :doc:`CDC </features/cdc/index>`.
* **Strongly consistent keyspaces.** Writes to tables in strongly consistent
  keyspaces are ordered by the Raft leader, which assigns their timestamps
  itself; ``USING TIMESTAMP`` is rejected.
* **Per-row TTL** (the expiration-time column, see
  :doc:`ScyllaDB CQL Extensions </cql/cql-extensions>`) **and Alternator TTL.**
  Expired rows are deleted with a wall-clock timestamp, which is meaningless
  relative to synthetic timestamps. The usual ``USING TTL`` and
  ``default_time_to_live`` are not affected.
* **PRUNE MATERIALIZED VIEW.** View rows are pruned with a wall-clock
  timestamp.
* **Prepending to a list.** Prepending requires a timestamp between
  January 1st 2010 and about the year 2437, when interpreted as
  microseconds since the epoch.
* **Appending to a list with a timestamp outside the timeuuid range.** List
  elements are keyed by a ``timeuuid`` derived from the write timestamp, so
  the timestamp, interpreted as microseconds since the epoch, must fall
  between October 15th 1582 and about the year 5236. Frozen lists, sets,
  and maps are not affected.
* **The largest timestamp**, 2\ :sup:`63`-1. A tombstone with this
  timestamp is never garbage-collected.

Performance considerations
--------------------------

The following work correctly with synthetic timestamps, but may perform
worse:

* :ref:`Time-window compaction (TWCS) <TWCS>` assigns sstables to time
  windows by their write timestamps. With synthetic timestamps, the windows
  are meaningless: data may all fall into a single window, or be spread over
  many small ones. Use a different compaction strategy.
* Tombstones may be retained longer by compaction when write timestamps do
  not follow the order in which writes arrive, because compaction must keep
  a tombstone while there may be older data that it shadows.
* For reads at a local consistency level, read repair is normally limited to
  the local datacenter when the mismatching data was written recently. This
  optimization compares the write timestamp with the current time, so with
  synthetic timestamps read repair also contacts remote datacenters.
