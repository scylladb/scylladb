Nodetool compact
================

Forces a (major) compaction on one or more tables.
Compaction is an optimization that reduces the cost of IO and CPU over time by merging rows in the background.

By default, major compaction runs on all the ``keyspaces`` and tables.
Major compactions will take all the SSTables for a column family and merge them into a **single SSTable per shard**.
If a keyspace is provided, the compaction will run on all of the tables within that keyspace. If one or more tables are provided as command-line arguments, the compaction will run only on those tables.

.. caution:: It is always best to allow ScyllaDB to automatically run minor compactions using a :doc:`compaction strategy </kb/compaction>`. Using Nodetool to run compaction can quickly exhaust all resources, increase operational costs, and take up valuable disk space. For this reason, major compactions should be avoided and are not recommended for any production system.  

.. note::
   For tablet keyspaces, use :doc:`nodetool cluster compact </operating-scylla/nodetool-commands/cluster/compact>`
   instead. It compacts a few tablets per shard at a time, so it needs far less free disk space,
   and it isn't interrupted by tablet migrations, merges, splits or repairs.


Syntax
-------
.. code-block:: console

   nodetool [options] compact [<keyspace> [<cfnames>]...]

Options
--------

The following options are available in Cassandra's nodetool, but are NOT implemented in ScyllaDB's nodetool:

* ``-st`` or ``--start-token``
* ``-et`` or ``--end-token``
* ``--user-defined``
* ``--split-output``

Examples
---------

.. code-block:: shell

   nodetool compact
   nodetool compact keyspace1
   nodetool compact keyspace1 standard1

See Also
--------

.. include:: nodetool-index.rst

:doc:`Compaction Overview </kb/compaction>`

:doc:`CQL compaction Reference </cql/compaction>`

:doc:`How to choose a Compaction Strategy </architecture/compaction/compaction-strategies>`
