========================
Preimages and postimages
========================

Prerequisites: make sure you are familiar with the :doc:`CDC log table <./cdc-log-table>` and :doc:`Basic operations in CDC <./cdc-basic-operations>` documents. Whenever we discuss non-frozen collections and UDTs, knowledge from the :doc:`Advanced column types <./cdc-advanced-types>` is also helpful (but it's not required to understand parts of the document which don't refer to non-frozen collections and UDTs).

When a write is performed to the base table, a corresponding write is made to the CDC log table associated with the base table. This CDC write can have many rows.

There are three kinds of rows: preimage rows, delta rows, and postimage rows.

The row's kind can be recognized using the ``cdc$operation`` column: values ``1`` to ``8`` denote the different types of delta rows (as explained in the :doc:`CDC log table <./cdc-log-table>` document), value ``0`` denotes a preimage row, and value ``9`` denotes a postimage row.

**Delta rows** have been extensively covered in the previous documents. They always appear if CDC is enabled. On the other hand, preimage and postimage rows are optional.

The purpose of delta rows is to describe the write itself --- the `mutation` performed on the table. The design principle of CDC is that using the information gathered from delta rows you can replay the exact same write that was used to generate those rows (except when you specify the CDC ``enabled`` parameter to ``'keys'`` in CDC options, in which case only the keys of the mutations will be shown).

**Preimage rows** exist to show what the state of the row affected by the write was `prior` to the write. The amount of information shown by preimage rows can be configured: the user can choose that the preimage contains the entire row (how it was before the write was made) by passing the ``'full'`` value to the ``'preimage'`` parameter when enabling CDC on a table, or that the preimage contains only the columns that were changed by the write by passing the ``true`` value to the ``'preimage'`` parameter. We will see examples of this.

**Postimage rows** exist to show what the state of the row affected by the write is `after` the write. Postimages always describe the state of the entire row. They are constructed by combining the delta row with the full preimage row (including columns not affected by the write).

.. caution:: in order to generate preimage rows for a given write, Scylla must perform a read before making the write. This increases latencies significantly. Furthermore, the read-then-write procedure is not atomic; between the read and the write a concurrent write may be performed. If the concurrent write modifies the same row and column that the preimage had read, the preimage's value will not be consistent with the order of writes as they appear in the CDC log. Preimages will only give "sensible" results if no concurrent writes are performed to the same row. They also heavily depend on monotonicity of clocks used to generate write timestamps. We will see some examples of what can go wrong in a later section. These remarks also apply to postimages, since they are computed from preimages.

Preimage rows
-------------

As mentioned in the :doc:`CDC log table <./cdc-log-table>` document, each non-primary-key column of the base table has a corresponding column in the CDC log table with the same name, called the `value column`. The value column is used by delta rows to describe the change. Preimage rows use it to describe the previous values of the base table column in the corresponding base table rows.

Suppose that a write generates a preimage row and let ``X`` be a base table column. If the write modified the ``X`` column, or if the ``full`` option for preimages is enabled, then the value of the column ``X`` in the preimage row is equal to the value of the corresponding column of the corresponding row of the base table *before* the write was performed. We call this value the **preimage** of ``X`` for the written row; it's obtained by reading the base table before making the write. The read's consistency level depends on the write's consistency level:

.. list-table::
    :widths: 50 50
    :header-rows: 1

    * - Base write CL
      - Preimage read CL
    * - ANY
      - ONE
    * - ALL or SERIAL
      - QUORUM
    * - LOCAL SERIAL
      - LOCAL QUORUM
    * - other
      - the same as base write CL

Therefore, depending on the CL of the write, the value obtained by the read may be different as it may contact different sets of replicas. We generally recommend using ``QUORUM`` or higher with CDC so that preimages observe previously finished writes. All examples in this document assume that ``QUORUM`` writes are used. With lower consistency levels and replication factors greater than 1 the results may differ in unpredictable ways.

The preimage row is created if and only if the read returned the corresponding base table row. Thus, there will be no preimage row if the base row had no live row marker (created by an ``INSERT``) and no other live cells.

The deletion columns (``cdc$deleted_X``) are set to ``true`` for columns that have a ``null`` value in the preimage, but only for those columns that the write modified or for all columns if the ``full`` option for preimages is enabled. The deleted elements columns (``cdc$deleted_elements_X``, for non-frozen collections and UDTs) are not used by preimage rows and are set to ``null``.

Consider the following example:

.. code-block:: cql

   CREATE TABLE ks.t (pk int, ck int, v1 int, v2 map<int, int>, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': true};
   UPDATE ks.t SET v1 = 0 WHERE pk = 0 AND ck = 0;
   UPDATE ks.t SET v2 = v2 + {1:1, 2:2} WHERE pk = 0 AND ck = 0;
   UPDATE ks.t SET v2 = v2 + {2:3, 3:4} WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", "cdc$batch_seq_no", "cdc$operation", pk, ck, v1, v2 FROM ks.t_scylla_cdc_log;

returns:

.. code-block:: none

     cdc$time                             | cdc$batch_seq_no | cdc$operation | pk | ck | v1   | v2
    --------------------------------------+------------------+---------------+----+----+------+--------------
     84d11998-3eea-11eb-c97a-ef64e058e2fb |                0 |             1 |  0 |  0 |    0 |         null
     84d18fd6-3eea-11eb-81e4-bb608d1b47fc |                0 |             0 |  0 |  0 | null |         null
     84d18fd6-3eea-11eb-81e4-bb608d1b47fc |                1 |             1 |  0 |  0 | null | {1: 1, 2: 2}
     84d2059c-3eea-11eb-f572-692d1e15a146 |                0 |             0 |  0 |  0 | null | {1: 1, 2: 2}
     84d2059c-3eea-11eb-f572-692d1e15a146 |                1 |             1 |  0 |  0 | null | {2: 3, 3: 4}

There are 3 batches (each with a single ``cdc$time``) corresponding to the 3 updates:

* The first batch consists of a single row with ``cdc$operation = 1`` (denoting ``UPDATE``); this is the delta row corresponding to the first update. There is no preimage row because the base table did not have a row with ``(pk, ck) = (0, 0)`` before the update happened.
* The second batch has two rows: a preimage row (``cdc$operation = 0``) and a delta row (``cdc$operation = 1``); it corresponds to the second update. This time the preimage row appeared because the table had a row with ``(pk, ck) = (0, 0)`` before the update happened and the preimage read returned it. Column ``v1`` has ``null`` because this update did not modify the ``v1`` column and the CDC options contained ``'preimage': true``, not ``'preimage': 'full'``. Column ``v2`` has ``null`` because that's what the preimage read returned --- there was no value under this column before this update.
* The third batch also has two rows, a preimage and a delta. This time the ``v2`` column of the preimage row contains ``{1: 1, 2: 2}`` since that was the value of ``v2`` of the corresponding row in the base table before the update.

Let's see what happens if we use ``'preimage': 'full'`` instead:

.. code-block:: cql

   CREATE TABLE ks.t (pk int, ck int, v1 int, v2 map<int, int>, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': 'full'};
   UPDATE ks.t SET v1 = 0 WHERE pk = 0 AND ck = 0;
   UPDATE ks.t SET v2 = v2 + {1:1, 2:2} WHERE pk = 0 AND ck = 0;
   UPDATE ks.t SET v2 = v2 + {2:3, 3:4} WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", "cdc$batch_seq_no", "cdc$operation", pk, ck, v1, v2 FROM ks.t_scylla_cdc_log;

returns:

.. code-block:: none

     cdc$time                             | cdc$batch_seq_no | cdc$operation | pk | ck | v1   | v2
    --------------------------------------+------------------+---------------+----+----+------+--------------
     2d5df268-3eee-11eb-7927-87ffdbd439b6 |                0 |             1 |  0 |  0 |    0 |         null
     2d5e3002-3eee-11eb-148e-77c7cfe215fc |                0 |             0 |  0 |  0 |    0 |         null
     2d5e3002-3eee-11eb-148e-77c7cfe215fc |                1 |             1 |  0 |  0 | null | {1: 1, 2: 2}
     2d5e71a2-3eee-11eb-218e-3a6f0b631141 |                0 |             0 |  0 |  0 |    0 | {1: 1, 2: 2}
     2d5e71a2-3eee-11eb-218e-3a6f0b631141 |                1 |             1 |  0 |  0 | null | {2: 3, 3: 4}

The difference, compared to the previous example, is that the ``v1`` column has a value in all preimage rows: even though the second and third updates did not modify this column, the preimage shows its previous value anyway since we enabled full preimages.

Preimage rows are only created for rows modified using inserts, updates, or row deletes. They are not created for range deletes or partition deletes. For example:

.. code-block:: cql

   CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': true};
   UPDATE ks.t SET v = 0 WHERE pk = 0 AND ck = 0;
   UPDATE ks.t SET v = 0 WHERE pk = 0 AND ck = 1;
   UPDATE ks.t SET v = 0 WHERE pk = 0 AND ck = 2;

   UPDATE ks.t SET v = 1 WHERE pk = 0 AND ck = 0;
   INSERT INTO ks.t (pk, ck, v) VALUES (0, 0, 2);
   DELETE FROM ks.t WHERE pk = 0 AND ck = 0;
   DELETE FROM ks.t WHERE pk = 0 AND ck >= 1 AND ck < 2;
   DELETE FROM ks.t WHERE pk = 0;

   SELECT "cdc$time", "cdc$batch_seq_no", "cdc$operation", pk, ck, v FROM ks.t_scylla_cdc_log;

returns:

.. code-block:: none

     cdc$time                             | cdc$batch_seq_no | cdc$operation | pk | ck   | v
    --------------------------------------+------------------+---------------+----+------+------
     e4892556-3f8a-11eb-20ea-30fe49fdd40c |                0 |             1 |  0 |    0 |    0
     e48a3a0e-3f8a-11eb-129f-afaf7bdf3862 |                0 |             1 |  0 |    1 |    0
     e48b53bc-3f8a-11eb-5392-14b6b05c122b |                0 |             1 |  0 |    2 |    0

     e48c605e-3f8a-11eb-fd38-697c7087e1e0 |                0 |             0 |  0 |    0 |    0
     e48c605e-3f8a-11eb-fd38-697c7087e1e0 |                1 |             1 |  0 |    0 |    1

     e48d37d6-3f8a-11eb-66f8-c231b41871ae |                0 |             0 |  0 |    0 |    1
     e48d37d6-3f8a-11eb-66f8-c231b41871ae |                1 |             2 |  0 |    0 |    2

     e48d89ac-3f8a-11eb-cf87-59a4595c6414 |                0 |             0 |  0 |    0 |    2
     e48d89ac-3f8a-11eb-cf87-59a4595c6414 |                1 |             3 |  0 |    0 | null

     e48de5c8-3f8a-11eb-40f8-df57c38bb13f |                0 |             5 |  0 |    1 | null
     e48de5c8-3f8a-11eb-40f8-df57c38bb13f |                1 |             8 |  0 |    2 | null

     e48e239e-3f8a-11eb-d1c6-5d36bba44905 |                0 |             4 |  0 | null | null

(we've inserted the empty lines to improve readability of the example).

We first create 3 rows using ``UPDATE`` with ``(pk, ck) = (0, 0), (0, 1), (0, 2)``. The first 3 rows of the CDC log shown above correspond to these 3 updates; as expected, none of them got a preimage (because the rows didn't exist before).

We then modify the row ``(0, 0)`` using ``UPDATE``, as shown by rows 4 and 5 of the CDC log, then ``INSERT``, as shown by rows 6 and 7, and then we delete the row, which corresponds to rows 8 and 9. We can see that each operation has a preimage row.

Finally, we perform a range delete (rows 10 and 11) and a partition delete (row 12); these operations didn't get preimage rows even though the range delete removed the row ``(pk, ck) = (0, 1)`` and the partition delete removed the row ``(pk, ck) = (0, 2)``. The reason we don't generate preimages for range deletes and partition deletes is that in general these operations may affect any number of rows; obtaining preimages for all of them has an unbounded cost.

Preimages and non-atomic columns
********************************

Recall that for atomic columns in the base table, the corresponding value columns in the CDC log table have the same type. For non-frozen collections and UDTs the type is different; in particular, it is always frozen (see :doc:`Advanced column types <./cdc-advanced-types>` for details).

The preimage for a non-frozen collection/UDT column is calculated by reading the collection/UDT from the base table and "freezing" the obtained set of cells. For maps, sets and UDTs, the preimage value will "look" the same as if we manually performed a read from the base table, except that its type will be a frozen version of the base type. For example:

.. code-block:: cql

   CREATE TABLE ks.t (pk int, ck int, v set<int>, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': true};
   UPDATE ks.t SET v = {1, 2} WHERE pk = 0 AND ck = 0;
   UPDATE ks.t SET v = v + {3} WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", "cdc$batch_seq_no", "cdc$operation", pk, ck, v FROM ks.t_scylla_cdc_log;

returns:

.. code-block:: none

     cdc$time                             | cdc$batch_seq_no | cdc$operation | pk | ck | v
    --------------------------------------+------------------+---------------+----+----+--------
     920e2c86-3f94-11eb-67f7-62f63aa4f8ee |                0 |             1 |  0 |  0 | {1, 2}
     920e78d0-3f94-11eb-dd3c-388dcc1a0644 |                0 |             0 |  0 |  0 | {1, 2}
     920e78d0-3f94-11eb-dd3c-388dcc1a0644 |                1 |             1 |  0 |  0 |    {3}

The preimage of ``v`` for the second update shows ``{1, 2}``. This value has type ``frozen<set<int>>``. If we had performed a read after the first update but before the second one, we would have also obtained ``{1, 2}``, although of type ``set<int>``. The values "look" the same but formally they are different (they have different types). In particular CQL drivers may represent them differently.

For columns of type ``list<T>``, the type of the CDC value column is ``map<timeuuid, T>`` (see :doc:`Advanced column types <./cdc-advanced-types>`). Therefore, for non-frozen lists, the timeuuid keys are also exposed in preimages. To obtain the list that one would obtain by performing a standard read of the base table simply remove the keys. For example:

.. code-block:: cql

   CREATE TABLE ks.t (pk int, ck int, v list<int>, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': true};
   UPDATE ks.t SET v = [1, 2] WHERE pk = 0 AND ck = 0;
   UPDATE ks.t SET v = v + [3] WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", "cdc$batch_seq_no", "cdc$operation", pk, ck, v FROM ks.t_scylla_cdc_log;

returns:

.. code-block:: none

     cdc$time                             | cdc$batch_seq_no | cdc$operation | pk | ck | v
    --------------------------------------+------------------+---------------+----+----+------------------------------------------------------------------------------------
     a252aa44-3f95-11eb-384d-579394688d26 |                0 |             1 |  0 |  0 | {a252bb60-3f95-11eb-9a61-000000000001: 1, a252bb61-3f95-11eb-9a61-000000000001: 2}
     a252e8a6-3f95-11eb-78fe-4635649d129b |                0 |             0 |  0 |  0 | {a252bb60-3f95-11eb-9a61-000000000001: 1, a252bb61-3f95-11eb-9a61-000000000001: 2}
     a252e8a6-3f95-11eb-78fe-4635649d129b |                1 |             1 |  0 |  0 |                                          {a252e270-3f95-11eb-9a61-000000000001: 3}

The preimage of ``v`` for the second update is ``{a252bb60-3f95-11eb-9a61-000000000001: 1, a252bb61-3f95-11eb-9a61-000000000001: 2}``. Performing a standard read before the second update would return the list ``[1, 2]``. Observe that this list can also be obtained from the preimage by listing the (key, value) pairs in the order shown in the preimage map and removing the keys.

.. caution:: Unfortunately, CDC is currently not suited for working with large collections. The reason is that freezing a collection may cause a large allocation - while non-frozen collections are stored as sequences of (small) cells, freezing a collection requires all cells to be fitted in a continuous memory buffer. This issue is currently being worked on; you can track it here: https://github.com/scylladb/scylla/issues/7506. But even after this issue is resolved, remember that generating a preimage requires reading (and storing) the entire value, which may be costly with large collections.

Preimages vs concurrent writes
******************************

Preimages don't play well with concurrent writes made to the same rows and columns. The following example illustrates what can happen.

Suppose we have a table with a single row:

.. code-block:: cql

   CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': true};
   UPDATE ks.t SET v = 0 WHERE pk = 0 AND ck = 0;

and we perform the following statements concurrently. Statement 1 (which we refer to as S1):

.. code-block:: cql

   UPDATE ks.t SET v = 1 WHERE pk = 0 AND ck = 0;

Statement 2 (S2):

.. code-block:: cql

   UPDATE ks.t SET v = 2 WHERE pk = 0 AND ck = 0;

Suppose that the timestamp of S2 is greater than the timestamp of S1 (doesn't matter whether the timestamps were generated by drivers or coordinators).

The "most intuitive" result is obtained if the preimage read for S2 (denoted R2 on the picture) gets ordered after the write for S1 (denoted W1):

.. image:: preimage-ordering-1122.svg

Performing the following query:

.. code-block:: cql

   SELECT "cdc$batch_seq_no", "cdc$operation", pk, ck, v FROM ks.t_scylla_cdc_log;

would return:

.. code-block:: none

     cdc$batch_seq_no | cdc$operation | pk | ck | v
    ------------------+---------------+----+----+---
                    0 |             1 |  0 |  0 | 0

                    0 |             0 |  0 |  0 | 0
                    1 |             1 |  0 |  0 | 1

                    0 |             0 |  0 |  0 | 1
                    1 |             1 |  0 |  0 | 2

The first row corresponds to the initial update. Rows for S2 (4 and 5) got ordered after rows for S1 (2 and 3) because the timestamp of S2 is greater. The preimage for S2 observed the effect of S1 (``v = 1``) because the S2 preimage read got ordered after the S1 write.

However, the following ordering is also possible:

.. image:: preimage-ordering-1212.svg

Then, performing our query would return:

.. code-block:: none

     cdc$batch_seq_no | cdc$operation | pk | ck | v
    ------------------+---------------+----+----+---
                    0 |             1 |  0 |  0 | 0

                    0 |             0 |  0 |  0 | 0
                    1 |             1 |  0 |  0 | 1

                    0 |             0 |  0 |  0 | 0
                    1 |             1 |  0 |  0 | 2

This time the preimage for S2 *did not observe the effect of S1* (the preimage value for S2 is ``v = 0``) because the S2 preimage read got ordered before the S1 write.

One could argue that the preimage still "makes sense" because it shows an earlier value, it's just "stale". But it can get even more interesting than that:

.. image:: preimage-ordering-2211.svg

The query result is then:

.. code-block:: none

     cdc$batch_seq_no | cdc$operation | pk | ck | v
    ------------------+---------------+----+----+---
                    0 |             1 |  0 |  0 | 0

                    0 |             0 |  0 |  0 | 2
                    1 |             1 |  0 |  0 | 1

                    0 |             0 |  0 |  0 | 0
                    1 |             1 |  0 |  0 | 2

As usual, the timestamps dictate how the writes get ordered and what the end result is (if we now query ``ks.t``, we will get the row ``(pk, ck, v) = (0, 0, 2)`` - "last write wins", where "last" is defined by timestamps). However, this causes preimages to be completely inconsistent: because the write for S2 happened before the preimage read for S1, the preimage for S1 shows ``v = 2``.

Note that **this is not an "improbable edge case"**: if you perform concurrent writes to the same rows and columns with timestamps generated by different sources, these anomalies will be the common case.

If you're using preimages and you want the CDC log to give you a consistent, sequential view of changes as they happen in your table (according to their timestamp order), make sure that:

* there are no two clients writing to the same row and column concurrently,
* each client uses monotonically increasing timestamps (CQL drivers guarantee this by default).

A possible strategy may be to distribute the partition keys between your client processes so that at most one process writes to each partition key at the same time.

Postimage rows
--------------

Postimage rows use the CDC value columns to show the state of each row affected by the write as it appears after the write is applied. To enable postimages pass the ``'postimage': true`` parameter to the ``cdc`` table option.

Postimages only appear for rows that were modified using ``UPDATE`` or ``INSERT``. No postimages are generated for range deletes, partition deletes and, unlike preimages, for row deletes (there is no additional information that a postimage would give compared to the delta for row deletes).

Given a write that generates a postimage row and a column ``X`` in the base table, the value under column ``X`` in the CDC log table in the postimage row is called the **postimage** of ``X`` for the written row. It is obtained by reading the base table before making the write and applying the write to the result of the read. If preimages are enabled, the same read is used to generate both preimages and postimages; in fact, you can easily understand postimages as full preimages (including columns not affected by the write) with delta rows applied. Postimage rows show the state of all columns, not only those that were modified by the write. In particular, if you use the ``'preimage': 'full'`` option together with the ``'postimage': true`` option, then for each column not modified by the write, the value in the postimage row is the same as the value in the preimage row.

The deletion columns (``cdc$deleted_X``) and deleted elements columns (``cdc$deleted_elements_X``, for non-frozen collections and UDTs) are not used by postimage rows. They are always ``null``.

Example:

.. code-block:: cql

   CREATE TABLE ks.t (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': 'full', 'postimage': true};
   UPDATE ks.t SET v1 = 0 WHERE pk = 0 AND ck = 0;
   UPDATE ks.t SET v2 = 0 WHERE pk = 0 AND ck = 1;
   UPDATE ks.t SET v1 = 0 WHERE pk = 0 AND ck = 2;
   INSERT INTO ks.t (pk, ck, v2) VALUES (0, 0, 0);
   DELETE FROM ks.t WHERE pk = 0 AND ck = 0;
   DELETE FROM ks.t WHERE pk = 0 AND ck >= 1 AND ck < 2;
   DELETE FROM ks.t WHERE pk = 0;
   SELECT "cdc$time", "cdc$batch_seq_no", "cdc$operation", pk, ck, v1, v2 FROM ks.t_scylla_cdc_log;

Result:

.. code-block:: none

     cdc$time                             | cdc$batch_seq_no | cdc$operation | pk | ck   | v1   | v2
    --------------------------------------+------------------+---------------+----+------+------+------
     d5c0d060-3fb6-11eb-20fa-e8053e8a076f |                0 |             1 |  0 |    0 |    0 | null
     d5c0d060-3fb6-11eb-20fa-e8053e8a076f |                1 |             9 |  0 |    0 |    0 | null

     d5c10dc8-3fb6-11eb-7181-1f7a149772ed |                0 |             1 |  0 |    1 | null |    0
     d5c10dc8-3fb6-11eb-7181-1f7a149772ed |                1 |             9 |  0 |    1 | null |    0

     d5c146e4-3fb6-11eb-b10d-292fb7071c95 |                0 |             1 |  0 |    2 |    0 | null
     d5c146e4-3fb6-11eb-b10d-292fb7071c95 |                1 |             9 |  0 |    2 |    0 | null

     d5c18384-3fb6-11eb-1d0a-387f4795ae4c |                0 |             0 |  0 |    0 |    0 | null
     d5c18384-3fb6-11eb-1d0a-387f4795ae4c |                1 |             2 |  0 |    0 | null |    0
     d5c18384-3fb6-11eb-1d0a-387f4795ae4c |                2 |             9 |  0 |    0 |    0 |    0

     d5c1c01a-3fb6-11eb-1b34-8f83918c46e8 |                0 |             0 |  0 |    0 |    0 |    0
     d5c1c01a-3fb6-11eb-1b34-8f83918c46e8 |                1 |             3 |  0 |    0 | null | null

     d5c1f846-3fb6-11eb-176e-c5aafe60baf2 |                0 |             5 |  0 |    1 | null | null
     d5c1f846-3fb6-11eb-176e-c5aafe60baf2 |                1 |             8 |  0 |    2 | null | null

     d5c2251e-3fb6-11eb-f640-5bb1b9bc08bc |                0 |             4 |  0 | null | null | null

The first 3 updates (corresponding to the first 3 batches above) don't have preimages since the rows ``(pk, ck) = (0, 0), (0, 1), (0, 2)`` didn't exist, but they do have postimages since the write created those rows.

The insert (4th batch) has both a preimage (it affected an existing row) and a postimage. Observe that the postimage row shows values for both ``v1`` and ``v2`` columns, even though only ``v2`` was modified by this statement.

As expected, none of the last 3 batches - which correspond to the row delete, the range delete, and the partition delete - have a postimage, and only the row delete has a preimage.

Note how in each batch that has a postimage, the postimage can be obtained from the preimage by applying the delta (in the first 3 batches we can imagine an empty preimage). 

Example with collections:

.. code-block:: cql

   CREATE TABLE ks.t (pk int, ck int, v map<int, int>, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true};
   UPDATE ks.t SET v = {1:1, 2:2} WHERE pk = 0 AND ck = 0;
   UPDATE ks.t SET v = v + {3:3}, v = v - {2} WHERE pk = 0 AND ck = 0;
   UPDATE ks.t SET v = {4:4} WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", "cdc$batch_seq_no", "cdc$operation", pk, ck, v, "cdc$deleted_elements_v", "cdc$deleted_v" FROM ks.t_scylla_cdc_log;

Result:

.. code-block:: none

     cdc$time                             | cdc$batch_seq_no | cdc$operation | pk | ck | v            | cdc$deleted_elements_v | cdc$deleted_v
    --------------------------------------+------------------+---------------+----+----+--------------+------------------------+---------------
     7a7bbc14-406b-11eb-68ec-0c723669d5a2 |                0 |             1 |  0 |  0 | {1: 1, 2: 2} |                   null |          True
     7a7bbc14-406b-11eb-68ec-0c723669d5a2 |                1 |             9 |  0 |  0 | {1: 1, 2: 2} |                   null |          null

     7a7c05ac-406b-11eb-5663-bdda698b05ea |                0 |             0 |  0 |  0 | {1: 1, 2: 2} |                   null |          null
     7a7c05ac-406b-11eb-5663-bdda698b05ea |                1 |             1 |  0 |  0 |       {3: 3} |                    {2} |          null
     7a7c05ac-406b-11eb-5663-bdda698b05ea |                2 |             9 |  0 |  0 | {1: 1, 3: 3} |                   null |          null

     7a7c50ca-406b-11eb-964d-2bfb4e77e256 |                0 |             0 |  0 |  0 | {1: 1, 3: 3} |                   null |          null
     7a7c50ca-406b-11eb-964d-2bfb4e77e256 |                1 |             1 |  0 |  0 |       {4: 4} |                   null |          True
     7a7c50ca-406b-11eb-964d-2bfb4e77e256 |                2 |             9 |  0 |  0 |       {4: 4} |                   null |          null

The second update both added a new key-value pair ``(3, 3)`` to the map and removed the pair ``(2, 2)``, as indicated by the value column and the deleted elements column in the delta row. The postimage reflects both of these operations, showing the end result ``{1: 1, 3: 3}`` obtained by applying the update to the preimage ``{1: 1, 2: 2}``. The third update completely replaces the collection with ``{4: 4}``, as indicated by the value column and the deletion column in the delta row, and the postimage also reflects that by showing the end result in the value column.

In general, let ``X`` be a non-primary-key column of the base table and consider a write. Then:

* if ``X`` is not a non-frozen collection or UDT, the ``X`` postimage is equal to the ``X`` delta if the column was modified by the write, otherwise it is equal to the result of the preimage read.
* If ``X`` is a non-frozen collection or UDT, the postimage is equal to the result of the preimage read modified by the following procedure:

  * if ``cdc$deleted_X`` is ``True`` in the delta row, remove the entire collection,
  * add all cells shown in the ``X`` column in the delta row (if any),
  * remove all cells with keys shown in the ``cdc$deleted_elements_X`` column in the delta row (if any).

All considerations regarding concurrent writes from the previous section apply to postimages as they do to preimages. In particular, if you want your postimages to be consistent, avoid concurrent writes to the same row and column.
