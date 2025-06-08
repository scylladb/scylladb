====================
Querying CDC Streams
====================

Some use cases for CDC may require querying the log table periodically in short intervals. One way to do that would be to perform **partition scans**, where you don't specify the partition (in this case, the stream) which you want to query, for example:

.. code-block:: cql

 SELECT * FROM ks.t_scylla_cdc_log;

Although partition scans are convenient, they require the read coordinator to contact the entire cluster, not just a small set of replicas defined by the replication factor.

The recommended alternative is to query each stream separately:

.. code-block:: cql

 SELECT * FROM ks.t_scylla_cdc_log WHERE "cdc$stream_id" = 0x365fd1a9ae34373954529ac8169dfb93;

With the above approach you can, for instance, build a distributed CDC consumer, where each of the consumer nodes queries only streams that are replicated to ScyllaDB nodes in proximity to the consumer node. This allows efficient, concurrent querying of streams, without putting strain on a single node due to a partition scan.

.. caution::
   The tables mentioned in the following sections: ``system_distributed.cdc_generation_timestamps`` and ``system_distributed.cdc_streams_descriptions_v2`` have been introduced in ScyllaDB 4.4. It is highly recommended to upgrade to 4.4 for efficient CDC usage. The last section explains how to run the below examples in ScyllaDB 4.3.

   If you use CDC in ScyllaDB 4.3 and your application is constantly querying CDC log tables and using the old description table to learn about new generations and stream IDs, you should upgrade your application before upgrading to 4.4. The upgraded application should dynamically switch from using the old description table to the new description tables when the cluster is upgraded from 4.3 to 4.4. We present an example algorithm that the application can perform in the last section.

   We highly recommend using the newest releases of our client CDC libraries (`Java CDC library <https://github.com/scylladb/scylla-cdc-java>`_, `Go CDC library <https://github.com/scylladb/scylla-cdc-go>`_, `Rust CDC library <https://github.com/scylladb/scylla-cdc-rust>`_). They take care of correctly querying the stream description tables and they handle the upgrade procedure for you.

Learning about available streams
--------------------------------

.. note::
   The following subsections describe how to discover available CDC streams depending on the type of keyspace:
   - For **vnode-based keyspaces**, use the method described in :ref:`vnode-based-keyspaces`.
   - For **tablets-based keyspaces**, see :ref:`tablets-based-keyspaces`.


.. _vnode-based-keyspaces:

Vnode-based keyspaces
^^^^^^^^^^^^^^^^^^^^^

To query the log table without performing partition scans, you need to know which streams to look at. For this you can use the ``system_distributed.cdc_generation_timestamps`` and ``system_distributed.cdc_streams_descriptions_v2`` tables.

Example: querying the CDC description table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#. Retrieve the timestamp of the currently operating CDC generation from the ``cdc_generation_timestamps`` table. If you have a multi-node cluster, query the table with QUORUM or ALL consistency level so you don't miss any entry:

   .. code-block:: cql

    CONSISTENCY QUORUM;
    SELECT time FROM system_distributed.cdc_generation_timestamps WHERE key = 'timestamps';

   The query can return multiple entries:

   .. code-block:: none

     time
    ---------------------------------
     2020-03-25 16:05:29.484000+0000
     2020-03-25 12:44:43.006000+0000

    (2 rows)

   Take the highest one. In our case this is ``2020-03-25 16:05:29.484000+0000``.

#. Retrieve the list of stream IDs in the current CDC generation from the ``cdc_streams_descriptions_v2`` table. Unfortunately, to use the time-date value in a ``WHERE`` clause, you have to modify the format of the time-date a little by removing the last three 0s before the ``+``. In our case, the modified time-date is ``2020-03-25 16:05:29.484+0000``:

   .. code-block:: cql

    CONSISTENCY QUORUM;
    SELECT streams FROM system_distributed.cdc_streams_descriptions_v2 WHERE time = '2020-03-25 16:05:29.484+0000';

   The result consists of a number of rows (most likely returned in multiple pages, unless you turned off paging), each row containing a list of stream IDs, such as:

   .. code-block:: none

     streams
    --------------------------------------------------------------------------------------------------------------
     {0x7ffe0c687fcce86e0783343730000001, 0x80000000000000010d9ee5f1f4000001, 0x800555555555555653e250f2d8000001}
     {0x807ae73e07dbd4122e32d36e08000011, 0x80800000000000001facbbb618000011, 0x80838c6b76e19a1bc3581db310000011}
     {0x80838c6b76e19a1c6da83d4d14000021, 0x80855555555555566d556a0a18000021, 0x808aaaaaaaaaaaabf1008f4120000021}
     {0x80c5343222b6eee636e3ed42d0000031, 0x80c5555555555556efd251b0b8000031, 0x80caaaaaaaaaaaabb9bde28998000031}
     ...

    (256 rows)

   Save all stream IDs returned by the query. When we ran the example, the query returned 256 * 3 = 768 stream IDs.

#. Use the obtained stream IDs to query your CDC log tables:

   .. code-block:: cql

    CREATE TABLE ks.t (pk int, ck int, v int, primary key (pk, ck)) WITH cdc = {'enabled': true};
    INSERT INTO ks.t (pk, ck, v) values (0, 0, 0);
    SELECT * FROM ks.t_scylla_cdc_log WHERE "cdc$stream_id" = 0x7ffe0c687fcce86e0783343730000001;
    SELECT * FROM ks.t_scylla_cdc_log WHERE "cdc$stream_id" = 0x80000000000000010d9ee5f1f4000001;
    ...

   Each change will be present in exactly one of these stream IDs. When we ran the example, it was:

   .. code-block:: cql

    SELECT * FROM ks.t_scylla_cdc_log WHERE "cdc$stream_id" = 0xced00000000000009663c8dc500005a1;

   .. code-block:: none

     cdc$stream_id                      | cdc$time                             | cdc$batch_seq_no | cdc$deleted_v | cdc$end_of_batch | cdc$operation | cdc$ttl | ck | pk | v
    ------------------------------------+--------------------------------------+------------------+---------------+------------------+---------------+---------+----+----+---
     0xced00000000000009663c8dc500005a1 | 7a370e64-819f-11eb-c419-1f717873d8fa |                0 |          null |             True |             2 |    null |  0 |  0 | 0

    (1 rows)


Query all streams to read the entire CDC log.

Reacting to topology changes
----------------------------

As explained in :doc:`./cdc-stream-changes`, the set of used CDC stream IDs changes whenever you bootstrap a new node. You should then query the CDC description table to read the new set of stream IDs and the corresponding timestamp.

If you're periodically querying streams and you don't want to miss any writes that are sent to the old generation, you should query it at least one time **after** the old generation stops operating (which happens when the new generation starts operating).

Keep in mind that time is relative: every node has its own clock. Therefore you should make sure that the old generation stops operating **from the point of view of every node** in the cluster **before** you query it one last time and start querying the new generation.

Example: switching streams
^^^^^^^^^^^^^^^^^^^^^^^^^^

Suppose that ``cdc_generation_timestamps`` contains the following entries:

.. code-block:: none

  time
 ---------------------------------
  2020-03-25 16:05:29.484000+0000
  2020-03-25 12:44:43.006000+0000

 (2 rows)

The currently operating generation's timestamp is ``2020-03-25 16:05:29.484000+0000`` --- the highest one in the above list. You've been periodically querying all streams in this generation. In the meantime, a new node is bootstrapped, hence a new generation appears:


.. code-block:: none

     time
    ---------------------------------
     2020-03-25 17:21:45.360000+0000
     2020-03-25 16:05:29.484000+0000
     2020-03-25 12:44:43.006000+0000

    (3 rows)

You should keep querying streams from generation ``2020-03-25 16:05:29.484000+0000`` until after you make sure that every node's clock moved past ``2020-03-25 17:21:45.360000+0000``. One way to do that is to connect to each node and use the ``now()`` function:

.. code-block:: none

    $ cqlsh 127.0.0.1
    Connected to  at 127.0.0.1:9042.
    [cqlsh 5.0.1 | Cassandra 3.0.8 | CQL spec 3.3.1 | Native protocol v4]
    Use HELP for help.
    cqlsh> select totimestamp(now()) from system.local;

     system.totimestamp(system.now())
    ----------------------------------
      2020-03-25 17:24:34.104000+0000

    (1 rows)
    cqlsh> 
    $ cqlsh 127.0.0.4
    Connected to  at 127.0.0.4:9042.
    [cqlsh 5.0.1 | Cassandra 3.0.8 | CQL spec 3.3.1 | Native protocol v4]
    Use HELP for help.
    cqlsh> select totimestamp(now()) from system.local;

     system.totimestamp(system.now())
    ----------------------------------
      2020-03-25 17:24:42.038000+0000

    (1 rows)

and so on. After you make sure that every node uses the new generation, you can query streams from the previous generation one last time, and then switch to querying streams from the new generation.

Differences in ScyllaDB 4.3
---------------------------

In ScyllaDB 4.3 the tables ``cdc_generation_timestamps`` and ``cdc_streams_descriptions_v2`` don't exist. Instead there is the ``cdc_streams_descriptions`` table. To retrieve all generation timestamps, instead of querying the ``time`` column of ``cdc_generation_timestamps`` using a single-partition query (i.e. using ``WHERE key = 'timestamps'``), you would query the ``time`` column of ``cdc_streams_descriptions`` with a full range scan (without specifying a single partition):

.. code-block:: cql

   SELECT time FROM system_distributed.cdc_streams_descriptions;

To retrieve a generation's stream IDs, you query the ``streams`` column of ``cdc_streams_descriptions`` as follows:

.. code-block:: cql

    SELECT streams FROM system_distributed.cdc_streams_descriptions WHERE time = '2020-03-25 16:05:29.484+0000';

All stream IDs are stored in a single row, unlike ``cdc_streams_descriptions_v2``.

.. _scylla-4-3-to-4-4-upgrade:

ScyllaDB 4.3 to ScyllaDB 4.4 upgrade
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you didn't enable CDC on any table while using ScyllaDB 4.3 or earlier, you don't need to understand this section. Simply upgrade to 4.4 (we recommend doing it as soon as you can) and implement your application to query streams as described above.

However, if you use CDC with ScyllaDB 4.3 and your application is periodically querying the old ``cdc_streams_descriptions`` table, you should upgrade your application *before* upgrading the cluster to ScyllaDB 4.4.

The upgraded application should understand both the old ``cdc_streams_descriptions`` table and the new ``cdc_generation_timestamps`` and ``cdc_streams_descriptions_v2`` tables. It should smoothly transition from querying the old table to querying the new tables as the cluster upgrades.

When ScyllaDB upgrades from 4.3 to 4.4 it will attempt to copy descriptions of all existing generations from the old table to the new tables. This copying procedure may take a while. Until it finishes, your application should keep using the old table; it should switch as soon as it detects that the procedure is finished. To detect that the procedure is finished, you can query the ``system.cdc_local`` table: if the table contains a row with ``key = 'rewritten'``, the procedure was finished; otherwise it is still in progress.

It is possible to disable the rewriting procedure. In that case only the latest generation will be inserted to the new table and your application should act accordingly (it shouldn't wait for the ``'rewritten'`` row to appear but start using the new tables immediately). It is not recommended to disable the rewriting procedure and we've purposefully left it undocumented how to do it. This option exists only for emergencies and should be used only with the assistance of a qualified ScyllaDB engineer.

In fresh ScyllaDB 4.4 clusters (that were not upgraded from a previous version) the old description table does not exist. Thus the application should check for its existence and when it detects its absence, it should use the new tables immediately.

With the above considerations in mind, the application should behave as follows. When it wants to learn if there are new generations:

1. Check if the ``system_distributed.cdc_streams_descriptions`` table exists. If not, proceed to query the new tables.
2. Otherwise, check if ``system.cdc_local`` contains a row with ``key = 'rewritten'``. If yes, proceed to query the new tables.
3. Otherwise, query the old table; the rewriting procedure is still in progress. Repeat step 2 in a few seconds; by this time the rewriting may have already finished.

You may also decide that it's safe to switch to the new tables even though not all generations have been copied from the old table. This may be the case if your application is interested only in the latest changes in the latest generation (for example, because it queries the CDC log tables in near-real time and has already seen all past changes). In this case, the application may check that the latest generation's timestamp is present in ``cdc_generation_timestamps`` and if it is, start using the new tables immediately.

Note that after upgrading the cluster to 4.4, all new generations (which are created when bootstrapping new nodes) appear only in the new tables. After upgrading your application and your cluster, and ensuring that either all generations have been rewritten to the new tables or that you're not interested in the data from old generations, it is safe to remove the old description table.

.. note::
   We highly recommend using the newest releases of our client CDC libraries (`Java CDC library <https://github.com/scylladb/scylla-cdc-java>`_, `Go CDC library <https://github.com/scylladb/scylla-cdc-go>`_, `Rust CDC library <https://github.com/scylladb/scylla-cdc-rust>`_). They take care of correctly querying the stream description tables and they handle the upgrade procedure for you.

.. _tablets-based-keyspaces:

Tablets-based keyspaces
^^^^^^^^^^^^^^^^^^^^^^^

Scylla exposes two system tables to provide information about CDC streams for CDC consumers:

- ``system.cdc_timestamps``: This table records the timestamps when CDC streams are changed.
CDC consumers use this table to learn when any changes to streams have occurred.
After discovering a relevant timestamp in ``system.cdc_timestamps``, the consumer can then query the ``system.cdc_streams`` table for that specific timestamp to get detailed information about the streams at that point in time.

- ``system.cdc_streams``: For each timestamp, this table shows the set of streams operating at that timestamp, as well as the changes from the previous timestamp (such as streams being opened or closed).
Each row includes the stream’s state (``stream_state``), which describes whether the stream is active, opened, or closed at that timestamp.

The ``stream_state`` column in ``system.cdc_streams`` formally describes the lifecycle of a stream at a given timestamp:

- **0 (active):** The stream is active and can be queried for CDC data at this timestamp.
- **1 (closed):** The stream was closed at this timestamp; no new CDC data will be written to this stream after this point.
- **2 (opened):** The stream was opened at this timestamp; CDC data for this stream starts from this point.

To list all available CDC streams for a tablets-based keyspace:

1. Retrieve the timestamps of the CDC stream sets for your table:

   .. code-block:: cql

      SELECT timestamp FROM system.cdc_timestamps WHERE keyspace_name = '<your_keyspace>' AND table_name = '<your_table>';

   The query returns all timestamps in descending order. The first timestamp is the timestamp for the currently operating CDC stream set. For example:

   .. code-block:: none

      timestamp
      ---------------------------------
      2025-09-02 15:34:42.467000+0000
      2025-09-02 15:33:27.888000+0000

      (2 rows)

2. Retrieve all CDC streams for a specific timestamp (stream_state = 0 means the stream is active at this timestamp):

   .. code-block:: cql

      SELECT stream_id FROM system.cdc_streams WHERE keyspace_name='<your_keyspace>' AND table_name='<your_table>' AND timestamp = '2025-09-02 15:34:42.467+0000' AND stream_state = 0;

   For example, the query can return:

   .. code-block:: none

      stream_id
      ------------------------------------
      0xbfffffffffffffffa15608ebf0000001
      0xffffffffffffffff372c68c25c000001
      0x3fffffffffffffff73b3f26904000001
      0x7fffffffffffffff1ef74fe610000001

      (4 rows)

   Or, you can query for the streams that were opened or closed at a specific timestamp as follows:

   .. code-block:: cql

      SELECT stream_id FROM system.cdc_streams WHERE keyspace_name='<your_keyspace>' AND table_name='<your_table>' AND timestamp = '2025-09-02 15:34:42.467+0000' AND stream_state >= 1 AND stream_state <= 2;

   returns:

   .. code-block:: none

      stream_state | stream_id
      -------------+------------------------------------
                 1 | 0xffffffffffffffffdb6cb86b34000001
                 1 | 0x7fffffffffffffff0ded3e1868000001
                 2 | 0xbfffffffffffffffa15608ebf0000001
                 2 | 0xffffffffffffffff372c68c25c000001
                 2 | 0x3fffffffffffffff73b3f26904000001
                 2 | 0x7fffffffffffffff1ef74fe610000001

      (4 rows)

3. Use the obtained stream IDs to query your CDC log tables. For each stream ID, query the table for entries with timestamps in the range between the stream's opening and closing timestamps (if exists). For example:

   .. code-block:: cql

      SELECT * FROM <your_keyspace>.<your_table>_scylla_cdc_log WHERE "cdc$stream_id" = 0xffffffffffffffffdb6cb86b34000001 AND "cdc$time" >= minTimeuuid('2025-09-02 15:33:27.888+0000') AND "cdc$time" < minTimeuuid('2025-09-02 15:34:42.467+0000');
      SELECT * FROM <your_keyspace>.<your_table>_scylla_cdc_log WHERE "cdc$stream_id" = 0x7fffffffffffffff0ded3e1868000001 AND "cdc$time" >= minTimeuuid('2025-09-02 15:33:27.888+0000') AND "cdc$time" < minTimeuuid('2025-09-02 15:34:42.467+0000');
      SELECT * FROM <your_keyspace>.<your_table>_scylla_cdc_log WHERE "cdc$stream_id" = 0xbfffffffffffffffa15608ebf0000001 AND "cdc$time" >= minTimeuuid('2025-09-02 15:34:42.467+0000');
      SELECT * FROM <your_keyspace>.<your_table>_scylla_cdc_log WHERE "cdc$stream_id" = 0xffffffffffffffff372c68c25c000001 AND "cdc$time" >= minTimeuuid('2025-09-02 15:34:42.467+0000');
      SELECT * FROM <your_keyspace>.<your_table>_scylla_cdc_log WHERE "cdc$stream_id" = 0x3fffffffffffffff73b3f26904000001 AND "cdc$time" >= minTimeuuid('2025-09-02 15:34:42.467+0000');
      SELECT * FROM <your_keyspace>.<your_table>_scylla_cdc_log WHERE "cdc$stream_id" = 0x7fffffffffffffff1ef74fe610000001 AND "cdc$time" >= minTimeuuid('2025-09-02 15:34:42.467+0000');

   .. note::
      It is necessary to restrict the timestamps range in order to avoid reading duplicated entries from the CDC log table. A single entry may be written to two different streams when switching from one stream set to another.

   Query all streams to read the entire CDC log.

Reacting to stream changes
--------------------------
As explained in :doc:`./cdc-stream-changes`, the set of used CDC stream IDs changes whenever the tablets of the base table are split or merged.

When reading rows from the CDC log table for some stream, you should check the value of ``cdc$closed_time`` to learn about the closing of the stream.
If the value is null, you can continue querying the stream.
But if the value is not null, it means that the stream is closed, or about to be closed soon, and you should query the CDC timestamps and CDC streams table to learn about the new streams that replace the currently operating stream set.

If while querying the stream you discover that ``cdc$closed_time`` is not null, you should complete querying the stream for all entries with timestamps preceding the ``cdc$closed_time`` value, and then move to querying the new streams starting from this timestamp.