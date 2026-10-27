Nodetool toppartitions
======================

**toppartitions** <keyspace> <table> <duration> - Samples the writes and reads on the node and reports the most active partitions in the specified table(s) and time frame.

**toppartitions** ``[--ks-filters <keyspaces>] [--cf-filters <tables>] [-d|--duration <duration>] [-s|--capacity <capacity>] [-k|--size <size>] [-a|--samplers <samplers>]``

.. note::

   The command needs to be run while there are writes and reads operations.

==========  ===================================
Parameter   Description
==========  ===================================
keyspace    The keyspace name
----------  -----------------------------------
table       The table name
----------  -----------------------------------
duration    The duration in milliseconds. Given positionally together with keyspace and table, or with ``-d``/``--duration`` (default 5000) when no positional arguments are used.
----------  -----------------------------------
ks-filters  List of keyspaces
----------  -----------------------------------
cf-filters  List of Tables (Column Families)
----------  -----------------------------------
capacity    The capacity of the sampler; higher values increase accuracy at the cost of memory (default 256 keys).
----------  -----------------------------------
size        The number of top partitions to list (default 10); must be smaller than capacity.
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

   READS Sampler:
     Cardinality: ~5 (256 capacity)
     Top 10 partitions:
           Partition                               Count       +/-
           (nba:team_roster) Russell Westbrook       100         0
           (nba:team_roster) Victor Oladipo           17         0
           (nba:team_roster) Jermi Grant              12         0
           (nba:team_roster) Andre Roberson            5         0
           (nba:team_roster) Steven Adams              1         0

   WRITES Sampler:
     Cardinality: ~5 (256 capacity)
     Top 10 partitions:
           Partition                               Count       +/-
           (nba:team_roster) Russell Westbrook       100         0
           (nba:team_roster) Jermi Grant              25         0
           (nba:team_roster) Victor Oladipo           17         0
           (nba:team_roster) Andre Roberson            1         0
           (nba:team_roster) Steven Adams              1         0

In this example, we can see that for the ``Partition`` with ``Partition Key`` "Russell Westbrook", 100 writes and 100 read operations were done during the set duration.

For each of the samplers (WRITES and READS in this specific example), nodetool toppartitions reports:

* The cardinality of the sampled operations (number of distinct partitions sampled, bounded by the capacity).

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

Additional Information
----------------------

* :ref:`Catching a Hot Partition <tracing-catching-a-hot-partition>` - Information on how to locate a hot partition
* .. include:: nodetool-index.rst
