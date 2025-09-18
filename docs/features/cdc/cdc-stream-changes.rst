==================
CDC Stream Changes
==================

Stream IDs used for CDC log entries change over time. A single base partition key might be mapped to one stream in the log today, but to a different stream tomorrow. If you build a query which follows changes made to your favorite partition by using a ``WHERE`` clause to specify the proper stream ID, you might need to update the query due to CDC streams changes.

The good news is, it is easy to learn what the used stream IDs are (:doc:`Querying CDC Streams </features/cdc/cdc-querying-streams/>`). The method depends on whether the keyspace is vnode-based or tablets-based.

-----------------------------------------------
CDC Stream Generations (vnodes-based keyspaces)
-----------------------------------------------

In a vnode-based keyspace, stream IDs will change if you join a new node to the cluster or by running the ``checkAndRepairCdcStreams`` command (:doc:`Nodetool checkAndRepairCdcStreams </operating-scylla/nodetool-commands/checkandrepaircdcstreams>`).

.. note::
    Stream IDs are chosen to maintain the following invariant:

    * given a base write with partition key ``pk``, the corresponding log table entries will have partition key ``s_id`` such that the token of ``pk`` is in the same vnode as the token of ``s_id``.

    Since adding a node to the cluster splits vnodes into smaller vnodes, we need to (in general) change the used stream IDs.

These sets of stream IDs are called **CDC stream generations** (also referred to simply as *CDC generations*). 

A CDC generation consists of:

* a timestamp, describing the point in time from which this generation starts operating,
* a set of stream IDs,
* a mapping from the set of tokens (in the entire token ring) to the set of stream IDs in this generation. 

This is the mapping used to decide on which stream IDs to use when making writes, as explained in the :doc:`./cdc-streams` document. It is a global property of the cluster: it doesn't depend on the table you're making writes to.

.. caution::
   The tables mentioned in the following sections: ``system_distributed.cdc_generation_timestamps`` and ``system_distributed.cdc_streams_descriptions_v2`` have been introduced in ScyllaDB 4.4. It is highly recommended to upgrade to 4.4 for efficient CDC usage. The last section explains how to run the below examples in ScyllaDB 4.3.

When CDC generations change
---------------------------

When you start a fresh cluster, the first generation is created. It has a timestamp chosen using the local clock of the node; it is stored in the ``time`` column of the ``system_distributed.cdc_generation_timestamps`` table. The stream IDs used in this generation are stored in the ``streams`` column of the ``system_distributed.cdc_streams_descriptions_v2`` table. Whenever you bootstrap a new node, you will see a new row appear in ``cdc_generation_timestamps`` containing the new generation's timestamp and a new partition in ``cdc_streams_descriptions_v2`` containing the stream IDs of that new generation.

The ``cdc_generation_timestamps`` table is a single-partition table; all timestamps are stored in the ``key = 'timestamps'`` partition.

.. _next-gen:

Example: The Next Generation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#. Start a new cluster. Query the ``cdc_generation_timestamps`` table to see the available generations:

   .. code-block:: cql

      SELECT time FROM system_distributed.cdc_generation_timestamps WHERE key = 'timestamps';

   returns:

   .. code-block:: none

        time
       ---------------------------------
        2020-03-25 12:44:43.006000+0000

       (1 rows)

   this is the timestamp of the first generation.

#. Create a table and insert a row:

   .. code-block:: cql

      CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true};
      INSERT INTO ks.t (pk, ck, v) values (0,0,0);

#. Bootstrap another node. After it finishes joining the cluster, query ``cdc_generation_timestamps`` again:

   .. code-block:: cql

      SELECT time FROM system_distributed.cdc_generation_timestamps WHERE key = 'timestamps';

   returns:

   .. code-block:: none

     time
    ---------------------------------
     2020-03-25 16:05:29.484000+0000
     2020-03-25 12:44:43.006000+0000

    (2 rows)

   Note that ``time`` is the clustering key column of this table. It is sorted in descending order.

#. Wait until the new generation starts operating. You can do that by using the CQL ``now()`` function to periodically check the current time of the node you're connected to:

   .. code-block:: cql

    SELECT totimestamp(now()) FROM system.local;

   returns (after waiting):

   .. code-block:: cql

     system.totimestamp(system.now())
    ----------------------------------
      2020-03-25 16:05:31.939000+0000

    (1 rows)

#. Insert a row to your table again:

   .. code-block:: cql

      INSERT INTO ks.t (pk, ck, v) values (0,0,0);

#. Query the log table:

   .. code-block:: cql
    
    SELECT "cdc$stream_id", pk FROM ks.t_scylla_cdc_log;

   returns:

   .. code-block:: none

     cdc$stream_id                      | pk
    ------------------------------------+----
     0x0521d5ce4a4a8ca552f83d88a1ae55d2 |  0
     0x166eddaa68db9a95af83968998626f7c |  0

    (2 rows)

   There are two entries with the same base partition key, but in different streams. One of them corresponds to the write made before the generation change, the other --- to the write made after the change.

After the operating CDC generation changes, all writes with timestamps greater than or equal to the new generation's timestamp will use the new stream IDs.

If the clock of the node you're connected to reports time distant from the write's timestamp, it may reject the write. If you've configured the driver to generate timestamps for you, make sure that the clock of the machine your driver is running on is not too desynchronized with the clock of the node you're connecting to. That way you can minimize the chance of writes being rejected.

The first generation's timestamp
--------------------------------

The first generation's timestamp is chosen by the first starting node by taking the current time (on the node's clock) shifted forward by a small duration (around a minute). Therefore you won't be able to perform writes to CDC-enabled tables immediately after starting the first node: there is no CDC generation operating *yet*.

Example: "could not find any CDC stream"
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Suppose a node was started at 17:59:35 UTC+1 time, as reported by the node's logs:

.. code-block:: none

   INFO  2020-02-06 17:59:35,087 [shard 0] init - ScyllaDB version 666.development-0.20200206.9eae0b57a with build-id 052adc1eb0601af2 starting ...

You immediately connected to the node using cqlsh and queried the ``cdc_generation_timestamps`` table:

.. code-block:: cql

   SELECT time FROM system_distributed.cdc_generation_timestamps WHERE key = 'timestamps';

The result was:
 
.. code-block:: none

    time
   ---------------------------------
    2020-02-06 17:00:43.100000+0000

   (1 rows)

This generation's timestamp is ``17:00:43 UTC time`` (timestamp columns in ScyllaDB always show the timestamp as a UTC time-date), which is a little more than a minute later compared to the node's startup time (which was ``16:59:35 UTC time``).

If you then immediately create a CDC-enabled table and attempt to make an insert:

.. code-block:: cql

   CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor': 3};
   CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true};
   INSERT INTO ks.t (pk, ck, v) values (0, 0, 0);

the result will be an error message:

.. code-block:: none

   ServerError: cdc::metadata::get_stream: could not find any CDC stream (current time: 2020/02/06 16:59:58). Are we in the middle of a cluster upgrade?

If you see a message like that, it doesn't necessarily mean something is wrong, as it may simply mean that the first generation hasn't started operating yet. If you wait for about a minute, you should be able to write to a CDC-enabled table.

You may also see this message if you were running a cluster with an old version of ScyllaDB (which didn't support CDC) and started a rolling upgrade.
Make sure to upgrade all nodes **before** you start doing CDC writes: one of the nodes will be responsible for creating the first CDC generation and informing other nodes about it.

---------------------------------------------
CDC Stream Changes in tablets-based keyspaces
---------------------------------------------

In a tablets-based keyspace, each base table has its own set of streams operating at any given moment, determined by the current shape of the table's tablets.

.. note::
    Stream IDs are chosen to maintain the following invariant:

    * Given a base write with partition key ``pk``, the corresponding log table entries will have partition key ``s_id`` such that the token of ``pk`` is in the same tablet as the token of ``s_id``.

When CDC streams change
-----------------------

When a CDC log table is first created, a new stream set is created for it, with a single stream ID corresponding to each tablet of the base table.
As the base table grows or shrinks, its tablets may be split or merged.
When that happens, a new CDC stream set is created for the table, with a new stream ID for each new tablet, and it replaces the old stream set.
Each stream set has an associated timestamp that indicates when the stream set starts operating.

Example: Tablet Split
^^^^^^^^^^^^^^^^^^^^^

#. Create a table and insert a row:

   .. code-block:: cql

      CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true} AND tablets = {'min_tablet_count': 2};
      INSERT INTO ks.t (pk, ck, v) values (0,0,0);

#. Query the CDC timestamps:

   .. code-block:: cql

      SELECT * FROM system.cdc_timestamps;

   returns:

   .. code-block:: cql

       keyspace_name | table_name | timestamp
      ---------------+------------+---------------------------------
                  ks |          t | 2025-09-09 14:41:36.977000+0000

      (1 rows)

#. Query the CDC streams:

   .. code-block:: cql

      SELECT * FROM system.cdc_streams;

   returns:

      .. code-block:: none

          keyspace_name | table_name | timestamp                       | stream_state | stream_id
         ---------------+------------+---------------------------------+--------------+------------------------------------
                     ks |          t | 2025-09-09 14:41:36.977000+0000 |            0 | 0xffffffffffffffff50e84c99d4000001
                     ks |          t | 2025-09-09 14:41:36.977000+0000 |            0 | 0x7fffffffffffffff55ebbda510000001
                     ks |          t | 2025-09-09 14:41:36.977000+0000 |            2 | 0xffffffffffffffff50e84c99d4000001
                     ks |          t | 2025-09-09 14:41:36.977000+0000 |            2 | 0x7fffffffffffffff55ebbda510000001

         (4 rows)

   We see that we have a single stream set with an associated timestamp and it has two streams - one for each tablet of the base table.

   The stream kind value ``0`` indicates that these streams are the current streams for this timestamps, and the stream kind value ``2`` indicates that these are newly opened streams in this timestamp.

#. Query the CDC log table:

   .. code-block:: cql

      SELECT "cdc$stream_id", pk FROM ks.t_scylla_cdc_log;

   returns:

      .. code-block:: none

          cdc$stream_id                      | pk
         ------------------------------------+----
          0xffffffffffffffff50e84c99d4000001 |  0

         (1 rows)

#. Now trigger a tablet split:

   .. code-block:: cql

      ALTER TABLE ks.t WITH tablets={'min_tablet_count':4};

#. Wait for tablet split to complete and a new CDC timestamp to be created:

   .. code-block:: cql

      SELECT * FROM system.cdc_timestamps;

   eventually a new timestamp appears:

      .. code-block:: none

          keyspace_name | table_name | timestamp
         ---------------+------------+---------------------------------
                     ks |          t | 2025-09-09 14:44:47.820000+0000
                     ks |          t | 2025-09-09 14:41:36.977000+0000

         (2 rows)

   note the timestamps are sorted in descending order to make it easy to check for new timestamps.

#. Query the CDC streams again:

   .. code-block:: cql

      SELECT * FROM system.cdc_streams;

   returns:

      .. code-block:: none

          keyspace_name | table_name | timestamp                       | stream_state | stream_id
         ---------------+------------+---------------------------------+--------------+------------------------------------
                     ks |          t | 2025-09-09 14:41:36.977000+0000 |            0 | 0xffffffffffffffff50e84c99d4000001
                     ks |          t | 2025-09-09 14:41:36.977000+0000 |            0 | 0x7fffffffffffffff55ebbda510000001
                     ks |          t | 2025-09-09 14:41:36.977000+0000 |            2 | 0xffffffffffffffff50e84c99d4000001
                     ks |          t | 2025-09-09 14:41:36.977000+0000 |            2 | 0x7fffffffffffffff55ebbda510000001
                     ks |          t | 2025-09-09 14:44:47.820000+0000 |            0 | 0xbfffffffffffffffa67123f568000001
                     ks |          t | 2025-09-09 14:44:47.820000+0000 |            0 | 0xffffffffffffffffe28e96d4f4000001
                     ks |          t | 2025-09-09 14:44:47.820000+0000 |            0 | 0x3fffffffffffffff296a1c0f24000001
                     ks |          t | 2025-09-09 14:44:47.820000+0000 |            0 | 0x7fffffffffffffff9573ad2614000001
                     ks |          t | 2025-09-09 14:44:47.820000+0000 |            1 | 0xffffffffffffffff50e84c99d4000001
                     ks |          t | 2025-09-09 14:44:47.820000+0000 |            1 | 0x7fffffffffffffff55ebbda510000001
                     ks |          t | 2025-09-09 14:44:47.820000+0000 |            2 | 0xbfffffffffffffffa67123f568000001
                     ks |          t | 2025-09-09 14:44:47.820000+0000 |            2 | 0xffffffffffffffffe28e96d4f4000001
                     ks |          t | 2025-09-09 14:44:47.820000+0000 |            2 | 0x3fffffffffffffff296a1c0f24000001
                     ks |          t | 2025-09-09 14:44:47.820000+0000 |            2 | 0x7fffffffffffffff9573ad2614000001

         (14 rows)

   Now, for the new timestamp, we have 4 new streams that are indicated as the current streams and also as newly opened streams by their stream_state value.
   We also see that the two streams from the previous timestamps are now indicated as closed by the stream_state value ``1``.

#. Insert a row to the table again:

   .. code-block:: cql

      INSERT INTO ks.t (pk, ck, v) values (0,0,0);

#. Query the log table:

   .. code-block:: cql

      SELECT "cdc$stream_id", pk FROM ks.t_scylla_cdc_log;

   returns:

      .. code-block:: none

          cdc$stream_id                      | pk
         ------------------------------------+----
          0xffffffffffffffff50e84c99d4000001 |  0
          0xffffffffffffffffe28e96d4f4000001 |  0

         (2 rows)

   The new entry is written to one of the new streams.
