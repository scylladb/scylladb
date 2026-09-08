Nodetool toppartitions
======================

**toppartitions** <keyspace> <table> <duration> - Samples cluster writes and reads and reports the most active partitions in a specified table and time frame.

**toppartitions**  [<--ks-filters ks>] [<--cf-filters tables>] [<-d duration>] [<-s|--capacity capacity>] [<-a|--samplers samplers>]

.. note::

   The command needs to be run while there are writes and reads operations.

==========  ===================================
Parameter   Description
==========  ===================================
keyspace    The keyspace name
----------  -----------------------------------
table       The table name
----------  -----------------------------------
duration    The duration in milliseconds (requires a ``-d`` prefix)
----------  -----------------------------------
ks-filters  List of keyspaces
----------  -----------------------------------
cf-filters  List of Tables (Column Families)
----------  -----------------------------------
capacity    The capacity of the sampler; higher values increase accuracy at the cost of memory (default 256 keys).
----------  -----------------------------------
samplers    The samplers to use; ``WRITES``, ``READS``, or both (default).
==========  ===================================

For example:

.. code-block:: shell

   nodetool toppartitions nba team_roster 5000

Additional examples:

* listing the top partitions from *all* tables in *all* keyspaces ``nodetool toppartitions``
* listing the top partitions for the last 1000 ms ``nodetool toppartitions -d 1000``
* listing the top partitions from a list of tables ``nodetool toppartitions --cf-filters ks:t,system:status``
* listing the top partitions from all tables from a list of keyspaces ``nodetool toppartitions --ks-filters ks,system``
* combining lists of keyspaces and tables ``nodetool toppartitions --ks-filters ks --cf-filters system:local``

Example output:

.. code-block:: shell

   WRITES Sampler:
     Cardinality: ~5 (256 capacity)
     Top 10 partitions:
	    Partition                Count       +/-
            Russell Westbrook         100         0
	    Jermi Grant               25          0
	    Victor Oladipo            17          0
	    Andre Roberson            1           0
	    Steven Adams              1           0

   READS Sampler:
     Cardinality: ~5 (256 capacity)
     Top 10 partitions:
	    Partition                Count       +/-
            Russell Westbrook         100         0
	    Victor Oladipo            17          0
	    Jermi Grant               12          0
	    Andre Roberson            5           0
	    Steven Adams              1           0

In this example, we can see that for the ``Partition`` with ``Partition Key`` "Russell Westbrook", 100 writes and 100 read operations were done during the set duration.

For each of the samplers (WRITES and READS in this specific example), nodetool toppartitions reports:

* The cardinality of the sampled operations (number of unique operations in the sample set).

* The number of partitions in a specified table that had the most traffic in a specified time period.

Output
------

=============  =============================================================================================
Parameter      Description
=============  =============================================================================================
Partition      The Partition Key, prefixed by the Keyspace and table (ks:cf)
-------------  ---------------------------------------------------------------------------------------------
Count          The number of operations of the specified type that occurred during the specified time period
-------------  ---------------------------------------------------------------------------------------------
+/-            The margin of error for the Count statistic
-------------  ---------------------------------------------------------------------------------------------
Write Count    The total number of writes since last boot
-------------  ---------------------------------------------------------------------------------------------
Write Latency  The average read latency
=============  =============================================================================================

To know which node hold the partition key use the nodetool :doc:`getendpoints </operating-scylla/nodetool-commands/getendpoints/>` command.

If the margin of error column (+/-) approaches the Count column, the measurement is inaccurate. You can increase accuracy by specifying
the ``--capacity`` option.

For example:

.. code-block:: shell

   nodetool getendpoints nba team_roster Russell Westbrook

Example output:

.. code-block:: shell

   10.0.0.72

Querying over CQL
-----------------

The same sampler is available remotely as the ``system.toppartitions`` virtual table, for superusers only.
The sampling window is the statement timeout, set with ``USING TIMEOUT``; ``capacity`` and ``list_size``
(nodetool's ``-s`` and ``-k``) are passed as equality restrictions, and ``kind`` selects the sampler (``-a``).
``kind`` follows ``capacity`` and ``list_size`` in the clustering key, so restricting it requires either
both of them or ``ALLOW FILTERING``.
Every page starts a new sampling window, so fetch the result in a single page (``PAGING OFF`` in cqlsh).

.. code-block:: cql

   SELECT kind, rank, partition_key, count, error FROM system.toppartitions
   WHERE keyspace_name = 'ks' AND table_name = 't'
     AND capacity = 64 AND list_size = 5
   USING TIMEOUT 3000ms;

Example output (``t`` has an ``int`` partition key, so ``partition_key`` is the stringified integer):

.. code-block:: text

    kind  | rank | partition_key | count | error
   -------+------+---------------+-------+-------
    write |    0 |             0 |  4102 |     0
    write |    1 |             1 |    92 |     0
    write |    2 |             6 |    92 |     0
    write |    3 |             2 |    91 |     0
    write |    4 |             8 |    91 |     0
     read |    0 |             0 |  4102 |     0
     read |    1 |             1 |    92 |     0
     read |    2 |             6 |    92 |     0
     read |    3 |             2 |    91 |     0
     read |    4 |             8 |    91 |     0

   -- several tables, writes only
   SELECT * FROM system.toppartitions
   WHERE keyspace_name = 'ks' AND table_name IN ('t1', 't2')
     AND capacity = 256 AND list_size = 10 AND kind = 'write'
   USING TIMEOUT 5s;

   -- all tables in all keyspaces
   SELECT * FROM system.toppartitions USING TIMEOUT 10s;

Additional Information
----------------------

* :ref:`Catching a Hot Partition <tracing-catching-a-hot-partition>` - Information on how to locate a hot partition
* .. include:: nodetool-index.rst
