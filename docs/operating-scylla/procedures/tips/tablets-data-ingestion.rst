Ingesting Data into Tables with Tablets
=======================================

This page describes how to prepare a cluster for loading a large amount of
data into tables which use :doc:`tablets </architecture/tablets>`, for example
with :ref:`nodetool refresh --load-and-stream <nodetool-refresh-load-and-stream>`.

Why tune for ingestion
----------------------

Without preparation, ingestion competes with background tablet operations for
the cluster's resources:

* Ingestion tools often don't upload data uniformly across nodes, so the load
  becomes imbalanced while data is loaded. The tablet load balancer then
  migrates tablets to restore the balance.
* A new table is empty, so ScyllaDB starts it with few tablets. As data
  arrives, the table goes through repeated cycles of tablet splits until its
  tablets reach the target size. Splits are expensive in terms of I/O and CPU,
  like SSTable compaction.
* Tablet migrations for load balancing consume I/O bandwidth. In addition,
  ``nodetool refresh --load-and-stream`` waits for tablet migrations, splits
  and merges to complete before it streams, so they insert pauses into the
  upload.

To improve the efficiency of ingestion, we recommend giving the load balancer
hints about the expected size of the tables, and disabling tablet load
balancing for the time of the ingestion.

Before ingestion
----------------

#. Set a size hint for each table which you are going to load, with the
   ``expected_data_size_in_gb`` tablet option. The value is the expected size
   of the table's data, before replication. For example, for a table which is
   going to hold 500 GB of data:

   .. code-block:: cql

      ALTER TABLE my_keyspace.my_table WITH tablets = {'expected_data_size_in_gb': 500};

   You can also set the option when creating the table. See
   :ref:`Per-table tablet options <cql-per-table-tablet-options>`.

   .. warning::

      ``ALTER TABLE ... WITH tablets = {...}`` replaces all tablet options of
      the table. Options which are set on the table but missing from the
      statement are removed. If the table already has tablet options, such as
      ``min_tablet_count``, repeat them in the statement, for example:

      .. code-block:: cql

         ALTER TABLE my_keyspace.my_table WITH tablets = {'min_tablet_count': 64, 'expected_data_size_in_gb': 500};

      Use ``DESCRIBE TABLE`` to see the current options.

#. Wait for ScyllaDB to split the tables according to the hints:

   .. code-block:: console

      curl -X POST "http://127.0.0.1:10000/storage_service/quiesce_topology"

   Run it on any one node. The call returns after the tablet splits, merges
   and migrations which the cluster needs are complete. For large tables this
   can take a long time.

   ScyllaDB splits the tables only while both tablet load balancing and tablet
   splits and merges are enabled. If either was disabled, enable it before this
   step, see
   :doc:`Disabling Tablet Background Operations </operating-scylla/procedures/maintenance/tablet-background-operations>`.
   Otherwise, the call returns without splitting the tables.

   Do not run this step during a rolling upgrade. Until all nodes run the same
   version, the call can return before the tables are split.

#. Disable tablet load balancing:

   .. code-block:: console

      curl -X POST "http://127.0.0.1:10000/storage_service/tablets/balancing?enabled=false"

   Run it on any one node. The setting applies to the whole cluster.

   Do it only after the previous step: while load balancing is disabled,
   ScyllaDB does not split tables. See
   :doc:`Disabling Tablet Background Operations </operating-scylla/procedures/maintenance/tablet-background-operations>`
   for what else load balancing affects.

After ingestion
---------------

#. Enable tablet load balancing again:

   .. code-block:: console

      curl -X POST "http://127.0.0.1:10000/storage_service/tablets/balancing?enabled=true"

   Run it on any one node. The setting applies to the whole cluster.

#. Drop the size hints. ScyllaDB then sizes each table according to the data
   it actually holds:

   .. code-block:: cql

      ALTER TABLE my_keyspace.my_table WITH tablets = {};

   This statement removes all tablet options of the table. To keep the other
   options, repeat them in the statement and leave out only the hint, for
   example:

   .. code-block:: cql

      ALTER TABLE my_keyspace.my_table WITH tablets = {'min_tablet_count': 64};
