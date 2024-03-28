Large Partitions Hunting
========================



This document describes how to catch large partitions.

What Should Make You Want To Start Looking For A Large Partition?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Any of the following:

* Latencies on a single shard become very long (look at the "Scylla Overview Metrics" dashboard of `ScyllaDB Monitoring Stack <https://monitoring.docs.scylladb.com/stable/>`_).
* Oversized allocation warning messages in the log:

  .. code-block:: none

     seastar_memory - oversized allocation: 2842624 bytes, please report

* A warning of "Writing large (partition|row|cell)" is issued when writing to a table (usually happens during a compaction):

  .. code-block:: none

     WARN  2022-09-22 17:33:11,075 [shard 1]large_data - Writing large partition Some_KS/Some_table: PK[/CK[/COL]] (SIZE bytes) to SSTABLE_NAME

  In this case, refer to :ref:`Troubleshooting Large Partition Tables <large-partition-table-configure>` for more information.

What To Do When You Suspect You May Have A Large Partition?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For each table you suspect run:

.. code-block:: console

   nodetool flush <keyspace name> <table name>
   nodetool cfstats <keyspace name>.<table name> | grep "Compacted partition maximum bytes"


For example:

.. code-block:: console

   nodetool cfstats demodb.tmcr | grep "Compacted partition maximum bytes"

                   Compacted partition maximum bytes: 1188716932

Using system tables to detect large partitions, rows, or cells
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Large rows and large cells are listed in the ``system.large_rows`` and ``system.large_cells`` tables, respectively.  See :doc:`Scylla Large Rows and Cells Tables </troubleshooting/large-rows-large-cells-tables/>` for more information.


When Compaction Creates an Error
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When compaction or writing to a table results in a "Writing a partition with too many rows" warning:

This warning indicates that there is a huge multi-row partition (based on the number of rows) and it is orthogonal
to the size-based warnings. The warning is controlled by ``compaction_rows_count_warning_threshold``, which is set in the scylla.yaml.
See :ref:`Troubleshooting Large Partition Tables <large-partition-table-configure>` for more information.