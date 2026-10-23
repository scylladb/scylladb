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

Running over CQL
----------------

The same sampler is available remotely with the ``EXECUTE COMMAND toppartitions`` statement, for superusers only.
It samples the node the statement is sent to, blocks for ``duration`` milliseconds, and returns one row
per sampled partition. All parameters are named and optional:

* ``keyspace_name``, ``table_name``: sample one keyspace or one table (default: all tables)
* ``duration``: sampling window in milliseconds (``-d``, default 5000, max 60000)
* ``capacity``: sampler capacity (``-s``, default 256, max 4096)
* ``list_size``: partitions reported per sampler (``-k``, default 10 or ``capacity`` if smaller, at most ``capacity``)
* ``kind``: ``'read'`` or ``'write'`` to run a single sampler (``-a``, default both)

.. code-block:: cql

   EXECUTE COMMAND toppartitions WITH keyspace_name = 'ks' AND table_name = 't'
       AND duration = 3000 AND capacity = 64 AND list_size = 5;

Example output (``t`` has an ``int`` partition key, so ``partition_key`` is the stringified integer):

.. code-block:: text

    keyspace_name | table_name | kind  | rank | partition_key | count | error
   ---------------+------------+-------+------+---------------+-------+-------
               ks |          t |  read |    0 |             0 |  4102 |     0
               ks |          t | write |    0 |             0 |  4102 |     0

Additional Information
----------------------

* :ref:`Catching a Hot Partition <tracing-catching-a-hot-partition>` - Information on how to locate a hot partition
* .. include:: nodetool-index.rst
