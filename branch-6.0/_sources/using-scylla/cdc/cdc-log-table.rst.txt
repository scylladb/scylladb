=================
The CDC Log Table
=================

When CDC is enabled on a table, a corresponding **CDC log table** is created.
Below we explain what the log table's schema is. For now, to make the explanation simpler, we assume that the base table doesn't use any non-frozen collection or UDT columns.

Suppose you've created the following table:

.. code-block:: cql

    CREATE TABLE ks.t (
        pk1 int, pk2 int,
        ck1 int, ck2 int,
        v int,
        vs int static,
        PRIMARY KEY ((pk1, pk2), ck1, ck2)
    ) WITH cdc = {'enabled': true};

Since CDC was enabled using ``WITH cdc = {'enabled':true}``, Scylla automatically creates the following log table:

.. code-block:: cql

    CREATE TABLE ks.t_scylla_cdc_log (
        "cdc$stream_id" blob,
        "cdc$time" timeuuid,
        "cdc$batch_seq_no" int,
        "cdc$operation" tinyint,
        "cdc$ttl" bigint,
        pk1 int, pk2 int,
        ck1 int, ck2 int,
        v int,                        "cdc$deleted_v" boolean,
        vs int,                       "cdc$deleted_vs" boolean,
        PRIMARY KEY ("cdc$stream_id", "cdc$time", "cdc$batch_seq_no")
    )

The rules are:

* The log table is located in the same keyspace as the base table, hence uses the same replication strategy.
* The log table's name is the base table's name plus the "_scylla_cdc_log" suffix. For example, the ``t`` base table gets an associated ``t_scylla_cdc_log`` log table.
* Each key column in the base table has a corresponding column in the log table with the same name and type. For example, the ``ck2`` column in the base has a corresponding ``ck2`` column in the log with type ``int``.
* Each non-key column in the base table which is *atomic* (i.e. not a non-frozen collection nor a non-frozen UDT column) gets 2 corresponding columns in the log table:

  * one with the same name and type (e.g. ``v int`` or ``vs int`` above),
  * one with the original name prefixed with ``cdc$deleted_`` and of ``boolean`` type (e.g. ``v int`` has a ``cdc$deleted_v boolean`` column).

* There are additional metadata columns which don't depend on the base table columns:

  * ``cdc$stream_id`` of type ``blob``, which is the partition key,
  * ``cdc$time`` of type ``timeuuid`` and ``cdc$batch_seq_no`` of type ``int``, which together form the clustering key,
  * ``cdc$operation`` of type ``tinyint``,
  * and ``cdc$ttl`` of type ``bigint``.

Note that log columns corresponding to static columns are not themselves static. E.g. the ``vs int static`` base column has a corresponding ``vs int`` column.

Log rows
--------

When performing a modification to the base table, such as inserting or deleting a row, the change will be reflected in CDC by new entries appearing in the log table. One modification statement for the base table may cause multiple entries to appear in the log. Those depend on:

* the CDC options you use (e.g. whether pre-image is enabled or not),
* the write action you perform: insert, update, row deletion, range deletion, or partition deletion,
* types of affected columns (notably collections require complex handling).

There are different types of entries appearing in the CDC log:

* delta entries, which describe "what has changed",
* pre-image entries, which describe "what was before",
* and post-image entries, which describe "the end result".

To understand the basics:

#. start by reading about metadata columns (``cdc$stream_id``, ``cdc$time``, ``cdc$operation``, and ``cdc$ttl``) in the sections below,
#. then consult the :doc:`Basic operations in CDC <./cdc-basic-operations>` document to understand how basic modification statements, such as single UPDATEs or DELETEs, which don't involve collections, are reflected in CDC using delta rows.

Stream ID column
^^^^^^^^^^^^^^^^

The ``cdc$stream_id`` column, of type ``blob``, is the log table's partition key. Each value in this column is a **stream identifier**.

When a change is performed in the base table, a stream identifier is chosen for the corresponding log entries depending on two things:

* the base write's partition key,
* the currently operating **CDC generation** which is a global property of the Scylla cluster (similar to tokens).

Partitions in the log table are called *streams*; within one stream, all entries are sorted according to the base table writes' timestamps, using standard clustering key properties (note that ``cdc$time``, which represents the time of the write, is the first part of the clustering key).

If you want to use CDC efficiently, it's important to understand how stream IDs are managed and chosen. Consult the :doc:`./cdc-streams` document for basic definitions and properties, :doc:`./cdc-stream-generations` document to understand how streams are managed and how they change over time, and finally :doc:`./cdc-querying-streams` to learn how streams can be queried efficiently, and how to find out which streams to query. Reading these documents is not a prerequisite for understanding the rest of the log table related sections.

Time column
^^^^^^^^^^^

The ``cdc$time`` column is the first part of the clustering key. The type of this column is ``timeuuid``, which represents a so-called *time-based UUID*, also called a *version 1 UUID*. A value of this type consists of two parts: a *timestamp*, and "the rest". In the case of a CDC log entry, the timestamp is equal to the timestamp of the corresponding write (more on that below), and the rest of the ``timeuuid`` value consists of randomly generated bytes so that writes with conflicting timestamps get separate entries in the log table.

Digression: write timestamps in Scylla
++++++++++++++++++++++++++++++++++++++

Each write in Scylla has a timestamp, or possibly multiple different timestamps (which is rare), used to order the write with respect to other writes, which might be performed concurrently. The timestamp can be:

* specified by the user,
* generated by the used CQL driver,
* or generated by the server.

The first case happens when the user directly specifies the timestamp in a CQL statement with the ``USING TIMESTAMP`` clause, like in the following example:

.. code-block:: cql

    CREATE TABLE ks.t (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck));
    UPDATE ks.t USING TIMESTAMP 123 SET a = 0, b = 0 WHERE pk = 0 AND ck = 0;

The timestamp of the write above is ``123``. More precisely, each written cell has its own timestamp; in the example above, there are two cells written:

* one in row ``(pk, ck) = (0, 0)``, in column ``a``,
* one in row ``(pk, ck) = (0, 0)``, in column ``b``.

We can query the timestamp of a cell using the ``writetime`` CQL function:

.. code-block:: cql

    SELECT writetime(a), writetime(b) FROM ks.t WHERE pk = 0 AND ck = 0;

returns:

.. code-block:: none

     writetime(a) | writetime(b)
    --------------+--------------
              123 |          123

    (1 rows)

The other two cases happen when the user doesn't specify a timestamp. Then it depends on your driver's configuration whether the timestamp is generated by the driver or by the server. For example, the python driver, which is used by the ``cqlsh`` tool, has the ``use_client_timestamp`` option (``True`` by default).

Continuing the above example, the below illustrates what happens if we don't specify a timestamp:

.. code-block:: cql

    UPDATE ks.t SET a = 0 WHERE pk = 0 AND ck = 0;
    SELECT writetime(a), writetime(b) FROM ks.t WHERE pk = 0 AND ck = 0;

returns:

.. code-block:: none

     writetime(a)     | writetime(b)
    ------------------+--------------
     1584966784195982 |          123

    (1 rows)

The timestamp is generated by reading the machine's local clock (either on the client or the server, depending on your driver's configuration) and taking *the number of microseconds since the Unix epoch* (00:00:00 UTC, 1 January 1970).

It is possible for a write to have multiple timestamps, but this should rarely be needed:

.. code-block:: cql

    BEGIN UNLOGGED BATCH
        UPDATE ks.t USING TIMESTAMP 1584966784195983 SET a = 0  WHERE pk = 0 AND ck = 0;
        UPDATE ks.t USING TIMESTAMP 1584966784195984 SET b = 0  WHERE pk = 0 AND ck = 0;
    APPLY BATCH;
    SELECT writetime(a), writetime(b) FROM ks.t WHERE pk = 0 AND ck = 0;

returns:

.. code-block:: none

     writetime(a)     | writetime(b)
    ------------------+------------------
     1584966784195983 | 1584966784195984

    (1 rows)

Write timestamps in CDC
+++++++++++++++++++++++

The ``cdc$time`` column in a CDC log entry is a ``timeuuid`` which contains the timestamp of the corresponding base table write. For example:

.. code-block:: cql

    CREATE TABLE ks.t (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true};
    UPDATE ks.t SET a = 0 WHERE pk = 0 AND ck = 0;
    SELECT "cdc$time" FROM ks.t_scylla_cdc_log;

returns:

.. code-block:: none

     cdc$time
    --------------------------------------
     b223c55e-6d07-11ea-7654-24e4fb3f20b9

    (1 rows)

Unfortunately, there is no method to extract the exact timestamp in *microseconds* from the ``timeuuid`` directly in CQL. We can extract the timestamp truncated to *milliseconds*, using the ``tounixtimestamp`` CQL function:

.. code-block:: cql

    SELECT tounixtimestamp("cdc$time") FROM ks.t_scylla_cdc_log;

returns:

.. code-block:: none

     system.tounixtimestamp(cdc$time)
    ----------------------------------
                        1584969040910

    (1 rows)

To obtain an exact value in microseconds you can use the below Python snippet:

.. code-block:: python

    from uuid import UUID
    def get_timestamp(u):
        return int((UUID(u).time - 0x01b21dd213814000)/10)

For example:

.. code-block:: python

    print(get_timestamp('b223c55e-6d07-11ea-7654-24e4fb3f20b9'))

prints ``1584969040910883``. Confirm that it is indeed the write timestamp of our previous UPDATE:

.. code-block:: cql

    SELECT writetime(a) WHERE pk = 0 AND ck = 0;

returns:

.. code-block:: none

     writetime(a)
    ------------------
     1584969040910883

    (1 rows)

You can also interpret the timestamp as a UTC time-date in CQL using the ``totimestamp`` CQL function:

.. code-block:: cql

    SELECT totimestamp("cdc$time") FROM ks.t_scylla_cdc_log;

returns:

.. code-block:: none

     system.totimestamp(cdc$time)
    ---------------------------------
     2020-03-23 13:10:40.910000+0000

    (1 rows)

``timeuuid`` values are compared in Scylla using the timestamp first, and the other bytes second. Thus, given two base writes whose corresponding log entries are in the same stream, the write with the higher timestamp will have its log entries appear after the lower timestamp write's log entries. If they have the same timestamp, the ordering will be chosen randomly (because the other bytes in the ``timeuuid`` are generated randomly).

Batch sequence number column
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``cdc$batch_seq_no`` column is the second part of the clustering key. It has type ``int`` and is used to group multiple log entries which correspond to a single write, given that they have the same timestamp.

For example, suppose you perform a batch write to two different rows within the same partition:

.. code-block:: cql

    CREATE TABLE ks.t (pk int, ck int, a int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true};
    BEGIN UNLOGGED BATCH
        UPDATE ks.t SET a = 0  WHERE pk = 0 AND ck = 0;
        UPDATE ks.t SET a = 0  WHERE pk = 0 AND ck = 1;
    APPLY BATCH;
    SELECT "cdc$time", "cdc$batch_seq_no" FROM ks.t_scylla_cdc_log;

returns:

.. code-block:: none

     cdc$time                             | cdc$batch_seq_no
    --------------------------------------+------------------
     c3b851fe-6d0c-11ea-3f9b-422e11ed8da0 |                0
     c3b851fe-6d0c-11ea-3f9b-422e11ed8da0 |                1

    (2 rows)

Observe that two entries have appeared, corresponding to the two updates. They have the same ``cdc$time`` value since they were performed in a single write and had the same timestamp. To distinguish between them, we use the ``cdc$batch_seq_no`` column. It is unspecified which update has its entries come first (in the example above, it is unspecified whether the ``ck = 0`` write or the ``ck = 1`` write will have ``cdc$batch_seq_no = 0``); from Scylla's point of view, it doesn't matter.

If you use different timestamps for the batch, the entries will have different timeuuids, so they won't be grouped like above:

.. code-block:: cql

    CREATE TABLE ks.t (pk int, ck int, a int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true};
    BEGIN UNLOGGED BATCH
        UPDATE ks.t USING TIMESTAMP 1584971217889332 SET a = 0  WHERE pk = 0 AND ck = 0;
        UPDATE ks.t USING TIMESTAMP 1584971217889333 SET a = 0  WHERE pk = 0 AND ck = 1;
    APPLY BATCH;
    SELECT "cdc$time", "cdc$batch_seq_no" FROM ks.t_scylla_cdc_log;

returns:

.. code-block:: none

     cdc$time                             | cdc$batch_seq_no
    --------------------------------------+------------------
     c3b85208-6d0c-11ea-d600-dcd1bfc285c9 |                0
     c3b85212-6d0c-11ea-18fd-95fe5b0e6260 |                0

    (2 rows)

``cdc$batch_seq_no`` is also used to group the pre-image entry with the delta entry, if pre-images are enabled, and similarly for post-image.

Operation column
^^^^^^^^^^^^^^^^

The ``cdc$operation`` column, of type ``int``, distinguishes between delta rows, pre-image rows, and post-image rows. For delta rows, it distinguishes between different types of operations. Below is the list of possible values:

===== ======================================
Value Meaning
===== ======================================
0     pre-image
1     row update
2     row insert
3     row delete
4     partition delete
5     row range delete inclusive left bound
6     row range delete exclusive left bound
7     row range delete inclusive right bound
8     row range delete exclusive right bound
9     post-image
===== ======================================

Values 1-8 are for delta rows. Read about the different operations in the :doc:`./cdc-basic-operations` document.

Time-to-live column
^^^^^^^^^^^^^^^^^^^

The ``cdc$ttl`` column has type ``bigint`` and holds the TTL of the base write, if any. Example:

.. code-block:: cql

    CREATE TABLE ks.t (pk int, ck int, a int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true};
    UPDATE ks.t SET a = 0 WHERE pk = 0 AND ck = 0;
    UPDATE ks.t USING TTL 5 SET a = 0 WHERE pk = 0 AND ck = 0;
    SELECT "cdc$ttl" FROM ks.t_scylla_cdc_log;

returns:

.. code-block:: none

     cdc$ttl
    ---------
        null
           5

The first row corresponds to the first update, which didn't have a ttl specified; thus, the ``cdc$ttl`` column is null. The second update contained the ``USING TTL 5`` clause, so the corresponding CDC log entry reflected that.

TTLs are only set for *live* cells, i.e. cells that have a value. You cannot specify a TTL on a dead cell. Adding a ``USING TTL`` clause when setting cells to null has no effect, hence CDC won't show any TTL in such case, for example:

.. code-block:: cql

    CREATE TABLE ks.t (pk int, ck int, a int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true};
    UPDATE ks.t USING TTL 5 SET a = null WHERE pk = 0 AND ck = 0;
    SELECT "cdc$ttl" FROM ks.t_scylla_cdc_log;

returns:

.. code-block:: none

     cdc$ttl
    ---------
        null

Even though we have attempted to specify a TTL (``USING TTL 5``), it had no effect because the only updated columns were set to ``null`` (``SET a = null``). The UPDATE statement above is equivalent to one with the ``USING TTL`` clause removed.

This has the following consequence: if you specify a TTL with a ``USING TTL`` clause, and some of the cells set by your statement are dead (``null``) while the other are alive, CDC will record multiple entries: one for the dead cells, the other for the alive cells. Example:

.. code-block:: cql

    CREATE TABLE ks.t (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true};
    UPDATE ks.t USING TTL 5 SET a = 0, b = null WHERE pk = 0 AND ck = 0;
    SELECT "cdc$batch_seq_no", a, "cdc$deleted_a", b, "cdc$deleted_b", "cdc$ttl" FROM ks.t_scylla_cdc_log;

returns:

.. code-block:: none

     cdc$batch_seq_no | a    | cdc$deleted_a | b    | cdc$deleted_b | cdc$ttl
    ------------------+------+---------------+------+---------------+---------
                    0 | null |          null | null |          True |    null
                    1 |    0 |          null | null |          null |       5

    (2 rows)

One entry says that ``b`` was set to ``null`` (``cdc$deleted_b = True``) and doesn't have a TTL, since it's not relevant for dead cells. The other entry says that ``a`` was set to ``0`` (``a = 0``) with TTL equal to ``5`` (``cdc$ttl = 5``). The two changes were performed in a single statement and used a single timestamp, so they were grouped using the ``cdc$batch_seq_no`` column.

A note on table truncations
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Truncating the base table does not automatically truncate the log table, nor vice versa.

For example, if you truncate the base table but not the log table, your log table will keep entries that describe changes to the base table which are no longer reflected in the base table.
Furthermore, if you've enabled the ``preimage`` option, new pre-image entries appended to the log will be calculated using the base table as it appears after truncation.

Depending on your use case, this might (or might not) lead to some unexpected results.

You may want to always keep your base and log tables in sync. If that is the case, you should truncate both tables if you truncate one of them. Preferably, such truncations should not race with concurrently performed writes, thus the following procedure should be used:

#. Stop writing to the base table.
#. Consume remaining CDC data if necessary.
#. Truncate the base table.
#. Truncate the log table.
#. Resume writing to the base table.
