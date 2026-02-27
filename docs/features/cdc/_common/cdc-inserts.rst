Inserts
-------

Digression: the difference between inserts and updates
++++++++++++++++++++++++++++++++++++++++++++++++++++++

Inserts are not the same as updates, contrary to a popular belief in Cassandra/ScyllaDB communities. The following example illustrates the difference:

.. code-block:: cql

    CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled':'true'};
    UPDATE ks.t SET v = null WHERE pk = 0 AND ck = 0;
    SELECT * FROM ks.t WHERE pk = 0 AND ck = 0;

returns:

.. code-block:: cql

     pk | ck | v
    ----+----+---

    (0 rows)

However:

.. code-block:: cql

    INSERT INTO ks.t (pk,ck,v) VALUES (0, 0, null);
    SELECT * FROM ks.t WHERE pk = 0 AND ck = 0;

returns:

.. code-block:: none

     pk | ck | v
    ----+----+------
      0 |  0 | null

    (1 rows)

.. _row-marker:

Each table has an additional invisible column called the *row marker*. It doesn't hold a value; it only holds *liveness information* (timestamp and time-to-live). If the row marker is alive, the row shows up when you query it, even if all its non-key columns are null. The difference between inserts and updates is that **updates don't affect the row marker**, while **inserts create an alive row marker**. 

Here's another example:

.. code-block:: cql

    CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled':'true'};
    UPDATE ks.t SET v = 0 WHERE pk = 0 AND ck = 0;
    SELECT * FROM ks.t;

returns:

.. code-block:: cql

     pk | ck | v
    ----+----+---
      0 |  0 | 0

    (1 rows)

The value in the ``v`` column keeps the ``(pk = 0, ck = 0)`` row alive, therefore it shows up in the query. After we delete it, the row will be gone:

.. code-block:: cql

    UPDATE ks.t SET v = null WHERE pk = 0 AND ck = 0;
    SELECT * FROM ks.t;

returns:

.. code-block:: none

     pk | ck | v
    ----+----+---

    (0 rows)

However, if we had used an ``INSERT`` instead of an ``UPDATE`` in the first place, the row would still show up even after deleting ``v``:

.. code-block:: cql

    INSERT INTO ks.t (pk, ck, v) VALUES (0, 0, 0);
    UPDATE ks.t set v = null where pk = 0 and ck = 0;
    SELECT * from ks.t;

returns:

.. code-block:: none

     pk | ck | v
    ----+----+------
      0 |  0 | null

    (1 rows)

The row marker introduced by ``INSERT`` keeps the row alive, even if there are no other non-key columns that are not ``null``. Therefore the row shows up in the query.
We can create just the row marker, without updating any columns, like this:

.. code-block:: cql

    INSERT INTO ks.t (pk, ck) VALUES (0, 0);

When specifying both key and non-key columns in an ``INSERT`` statement, we're saying "create a row marker, *and* set cells for this row". We can explicitly divide these two operations; the following:

.. code-block:: cql

    INSERT INTO ks.t (pk, ck, v) VALUES (0, 0, 0);

is equivalent to:

.. code-block:: cql

    BEGIN UNLOGGED BATCH
        INSERT INTO ks.t (pk, ck) VALUES (0, 0);
        UPDATE ks.t SET v = 0 WHERE pk = 0 AND ck = 0;
    APPLY BATCH;

The ``INSERT`` creates a row marker, the ``UPDATE`` sets the cell in the ``(pk, ck) = (0, 0)`` row and ``v`` column.

Inserts in CDC
++++++++++++++

Inserts affect the CDC log very similarly to updates; if no collections or static columns are involved, the difference lies only in the ``cdc$operation`` column:

#. Start with a basic table and perform some insert:

   .. code-block:: cql

       CREATE TABLE ks.t (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled':'true'};
       INSERT INTO ks.t (pk, ck, v1) VALUES (0, 0, 0);
       INSERT INTO ks.t (pk, ck, v2) VALUES (0, 0, NULL);

#. Confirm that the insert was performed by displaying the contents of the table:

   .. code-block:: cql

       SELECT * FROM ks.t;

   returns:

   .. code-block:: none

        pk | ck | v1 | v2
       ----+----+----+------
         0 |  0 |  0 | null

       (1 rows)

#. Display the contents of the CDC log table:

   .. code-block:: cql

      SELECT "cdc$batch_seq_no", pk, ck, v1, "cdc$deleted_v1", v2, "cdc$deleted_v2", "cdc$operation" FROM ks.t_scylla_cdc_log;

   returns:

   .. code-block:: none

        cdc$batch_seq_no | pk | ck | v1   | cdc$deleted_v1 | v2   | cdc$deleted_v2 | cdc$operation
       ------------------+----+----+------+----------------+------+----------------+---------------
                       0 |  0 |  0 |    0 |           null | null |           null |             2
                       0 |  0 |  0 | null |           null | null |           True |             2

       (2 rows)

Delta rows corresponding to inserts are indicated by ``cdc$operation = 2``.

If a static row update is performed within an ``INSERT``, it is separated from the ``INSERT``, in the same way a clustered row update is separated from a static row update. Example:

.. code-block:: cql

    CREATE TABLE ks.t (pk int, ck int, s int static, c int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true};
    INSERT INTO ks.t (pk, ck, s, c) VALUES (0, 0, 0, 0);
    SELECT "cdc$batch_seq_no", pk, ck, s, c, "cdc$operation" FROM ks.t_scylla_cdc_log;

returns:

.. code-block:: none

     cdc$batch_seq_no | pk | ck   | s    | c    | cdc$operation
    ------------------+----+------+------+------+---------------
                    0 |  0 | null |    0 | null |             1
                    1 |  0 |    0 | null |    0 |             2

    (2 rows)

There is no such thing as a "static row insert". Indeed, static rows don't have a row marker; the only way to make a static row show up is to set a static column to a non-null value. Therefore, the following statement (using the table from above):

.. code-block:: cql

    INSERT INTO ks.t (pk, s) VALUES (0, 0);

is equivalent to:

.. code-block:: cql

    UPDATE ks.t SET s = 0 WHERE pk = 0;

This is the reason why ``cdc$operation`` is ``1``, not ``2``, in the example above for the static row update.
