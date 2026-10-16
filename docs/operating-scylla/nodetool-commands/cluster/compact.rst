Nodetool cluster compact
========================

**cluster compact** - Compacts tablet tables cluster-wide, tablet by tablet. It only compacts
keyspaces with tablets enabled (default). To compact keyspaces with tablets disabled
(vnodes-based), see :doc:`nodetool compact </operating-scylla/nodetool-commands/compact/>`.

Running ``cluster compact`` on a **single node** compacts all tablets of the specified tables on
all of their replicas. Unlike :doc:`nodetool compact </operating-scylla/nodetool-commands/compact/>`,
which compacts all the data of a table on a node at once, ``cluster compact`` compacts a limited
number of tablets per shard at a time, so its temporary space overhead is bounded by the size of
a few tablets per shard. Like tablet repair, the compaction of a tablet counts against the
``tablet_streaming_read_concurrency_per_shard`` and ``tablet_streaming_write_concurrency_per_shard``
configuration options of the shards of its replicas, so with their defaults a shard compacts a
single tablet at a time, and doesn't migrate or repair tablets meanwhile.

A table which is the base table of a co-location group is compacted together with the tables
co-located with it. A co-located table can't be compacted on its own.

The compaction is best effort: a tablet which fails to compact, e.g. because one of its replicas
is down, isn't retried. The other tablets are compacted all the same, and the compaction is then
reported as failed, with the number of failed tablets and the first error.

Each table is compacted by a cluster task, which can be followed and aborted with
:doc:`nodetool tasks </operating-scylla/nodetool-commands/tasks/index>`.

Syntax
------

.. code-block:: console

   nodetool cluster compact [--type major] [--recursive] [--force-flush] [<keyspace> [<table> ...]]
   nodetool cluster compact [--type major] [--recursive] [--force-flush] --table-id <table_id> [--table-id <table_id> ...] [<keyspace>]

ScyllaDB nodetool cluster compact command supports the following options:

- ``--type`` the type of compaction to run. Only ``major``, the default, is supported for now.
  Other types are rejected.

- ``--recursive`` also compacts the tables derived from each table which aren't co-located with it:
  its materialized views, secondary indexes and CDC log table.

- ``--force-flush`` flushes all tables on every node before compacting, regardless of the
  ``compaction_flush_all_tables_before_major_seconds`` configuration option. Without it, all tables
  are flushed, forcing new commitlog segments, only if that interval elapsed since they were last
  flushed, so that the compaction can purge as many tombstones as possible.

- ``--table-id`` compacts the table with the given id, in place of table names. If a keyspace is
  given, the table must belong to it.

For example:

::

   nodetool cluster compact ks
   nodetool cluster compact ks tbl1 tbl2
   nodetool cluster compact --recursive ks tbl1
   nodetool cluster compact --table-id 5e1bd3a0-4a9e-11f0-9f7a-0123456789ab

Without a keyspace, all tablet keyspaces are compacted.
