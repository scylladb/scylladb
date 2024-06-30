.. _updates:

Updates
-------

Updates are the most basic statements that can be performed. The following is an example of an update with CDC enabled.

#. Start with a basic table and perform some UPDATEs:

   .. code-block:: cql

       CREATE TABLE ks.t (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled':'true'};
       UPDATE ks.t SET v1 = 0 WHERE pk = 0 AND ck = 0;
       UPDATE ks.t SET v2 = null WHERE pk = 0 AND ck = 0;

#. Confirm that the update was performed by displaying the contents of the table:

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
                       0 |  0 |  0 |    0 |           null | null |           null |             1
                       0 |  0 |  0 | null |           null | null |           True |             1

       (2 rows)

Delta rows corresponding to updates are indicated by ``cdc$operation = 1``. All columns corresponding to the primary key in the log are set to the same values as in the base table, in this case ``pk = 0`` and ``ck = 0``.

Each non-key column, such as ``v1`` above, has a corresponding column in the log with the same name, but there's an additional column with the ``cdc$deleted_`` prefix (e. g. ``cdc$deleted_v1``) used to indicate whether the column was set to ``null`` in the update.

If the update sets a column ``X`` to a non-null value, the ``X`` column in the log will have that value; however, if the update sets ``X`` to a null value, the ``X`` column in the log will be ``null``, and we'll use the ``cdc$deleted_X`` column to indicate the update by setting it to ``True``. In the example above, this is what happened with the ``v2`` column.

Column deletions are a special case of an update statement. That is, the following two statements are equivalent:

.. code-block:: cql

    UPDATE <table_name> SET X = null WHERE <condition>;
    DELETE X FROM <table_name> WHERE <condition>;

Thus, in the example above, instead of

.. code-block:: cql

    UPDATE ks.t SET v2 = null WHERE pk = 0 AND ck = 0;

we could have used

.. code-block:: cql

    DELETE v2 FROM ks.t WHERE pk = 0 AND ck = 0;

and we would've obtained the same result.

Note that column deletions, (which are equivalent to updates that set a column to ``null``) *are different than row deletions*, i.e. ``DELETE`` statements that specify a clustering row but don't specify any particular column, like the following:

.. code-block:: cql

    DELETE FROM ks.t WHERE pk = 0 AND ck = 0;

You can read about row deletions in the :ref:`corresponding section <row-deletions>`.

Digression: static rows in ScyllaDB
+++++++++++++++++++++++++++++++++++

If a table in ScyllaDB has static columns, then every partition in this table contains a *static row*, which is global for the partition. This static row is different from the clustered rows: it contains values for partition key columns and static columns, while clustered rows contain values for partition key, clustering key, and regular columns. The following example illustrates how the static row can be used:

.. code-block:: cql

    CREATE TABLE ks.t (pk int, ck int, s int static, c int, PRIMARY KEY (pk, ck));
    UPDATE ks.t SET s = 0 WHERE pk = 0;
    SELECT * from ks.t WHERE pk = 0;

returns:

.. code-block:: none

     pk | ck   | s | c
    ----+------+---+------
      0 | null | 0 | null

    (1 rows)

Even though no regular columns were set, the above query returned a row; the static row.

We can still update clustered rows, of course:

.. code-block:: cql

    UPDATE ks.t SET c = 0 WHERE pk = 1 AND ck = 0;
    SELECT * from ks.t WHERE pk = 1;

returns:

.. code-block:: none

     pk | ck | s    | c
    ----+----+------+---
      1 |  0 | null | 0

    (1 rows)

Somewhat confusingly, CQL mixes the static row with the clustered rows if both appear within the same partition. Example:

.. code-block:: cql

    UPDATE ks.t SET c = 0 WHERE pk = 2 AND ck = 0;
    UPDATE ks.t SET c = 1 WHERE pk = 2 AND ck = 1;
    UPDATE ks.t SET s = 2 WHERE pk = 2;
    SELECT * from ks.t WHERE pk = 2;

returns:

.. code-block:: none

     pk | ck | s | c
    ----+----+---+---
      2 |  0 | 2 | 0
      2 |  1 | 2 | 1

    (2 rows)

From the above query result it seems as if the static column was a part of every clustered row, and setting the static column to a value within one partition is setting it "for every clustered row". **That is not the case**, it is simply CQL's way of showing the static row when both the static row and clustered rows are present: it "mixes" the static row into each clustered row. But to understand the static row, think of it as a separate row in the partition, with the property that there can be at most one static row in a partition; the static row exists if and only if at least one of the static columns are non-null.

Static rows in CDC
++++++++++++++++++

CDC separates static row updates from clustered row updates, showing them as different entries, even if you update both within one statement. Example:

.. code-block:: cql

    CREATE TABLE ks.t (pk int, ck int, s int static, c int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true};
    UPDATE ks.t SET s = 0, c = 0 WHERE pk = 0 AND ck = 0;
    SELECT "cdc$batch_seq_no", pk, ck, s, c, "cdc$operation" FROM ks.t_scylla_cdc_log;

returns:

.. code-block:: none

     cdc$batch_seq_no | pk | ck   | s    | c    | cdc$operation
    ------------------+----+------+------+------+---------------
                    0 |  0 | null |    0 | null |             1
                    1 |  0 |    0 | null |    0 |             1

    (2 rows)

CDC recognizes that logically two updates happened: one to the static row and one to the clustered row. In other words, CDC interprets the following:

.. code-block:: cql

    UPDATE ks.t SET s = 0, c = 0 WHERE pk = 0 AND ck = 0;

as follows:

.. code-block:: cql

    BEGIN UNLOGGED BATCH
        UPDATE ks.t SET s = 0 WHERE pk = 0;
        UPDATE ks.t SET c = 0 WHERE pk = 0 AND ck = 0;
    APPLY BATCH;

However, since they happened in a single statement and had a single timestamp, it grouped them using ``cdc$batch_seq_no``. The static row update can be distinguished from the clustered row update by looking at clustering key columns, in case ``ck``: they are ``null`` for static row updates and non-null for clustered row updates.
