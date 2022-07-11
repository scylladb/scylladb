Row range deletions
-------------------

Range deletions are statements in which you delete a set of rows specified by a continuous range of clustering keys. 
They work within one partition, so you must provide a single partition key when performing a range deletion. 

The following is an example of a row range deletion with CDC enabled.

#. Start with a basic table and insert some data into it.

   .. code-block:: cql

       CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled':'true'};
       INSERT INTO ks.t (pk,ck,v) VALUES (0,0,0);
       INSERT INTO ks.t (pk,ck,v) VALUES (0,1,1);
       INSERT INTO ks.t (pk,ck,v) VALUES (0,2,2);
       INSERT INTO ks.t (pk,ck,v) VALUES (0,3,3);

#. Display the contents of the table

   .. code-block:: cql

       SELECT * FROM ks.t;

   returns:

   .. code-block:: none

        pk | ck | v
       ----+----+---
         0 |  0 | 0
         0 |  1 | 1
         0 |  2 | 2
         0 |  3 | 3

       (4 rows)

#. Remove all rows where the primary key is 0 and the clustering key is in the ``(0, 2]`` range (left-opened, right-closed).

   .. code-block:: cql

       DELETE FROM ks.t WHERE pk = 0 AND ck <= 2 and ck > 0;

#. Display the contents of the table after the delete:

   .. code-block:: cql

       SELECT * FROM ks.t;

   returns:

   .. code-block:: none

        pk | ck | v
       ----+----+---
         0 |  0 | 0
         0 |  3 | 3

       (2 rows)

#. Display the contents of the CDC log table:

   .. code-block:: cql

       SELECT "cdc$batch_seq_no", pk, ck, v, "cdc$operation" FROM ks.t_scylla_cdc_log;

   returns:

   .. code-block:: none

        cdc$batch_seq_no | pk | ck | v    | cdc$operation
       ------------------+----+----+------+---------------
                       0 |  0 |  0 |    0 |             2
                       0 |  0 |  1 |    1 |             2
                       0 |  0 |  2 |    2 |             2
                       0 |  0 |  3 |    3 |             2
                       0 |  0 |  0 | null |             6
                       1 |  0 |  2 | null |             7

       (6 rows)

A range deletion appears in CDC as a batch of up to two entries into the log table: an entry corresponding to range's left bound (if specified), and an entry corresponding to the range's right bound (if specified). The appropriate values of ``cdc$operation`` are as follows:

* ``5`` denotes an inclusive left bound,
* ``6`` denotes an exclusive left bound,
* ``7`` denotes an inclusive right bound,
* ``8`` denotes an exclusive right bound.

In the example above we've used the range ``(0, 2]``, with an exclusive left bound and inclusive right bound. This is presented in the CDC log as two log rows: one with ``cdc$operation = 6`` and the other with ``cdc$operation = 7``.

The values for non-key base columns are ``null`` in these entries.

If you specify a one-sided range:

.. code-block:: cql

    DELETE FROM ks.t WHERE pk = 0 AND ck < 3;

then two entries will appear in the CDC log: one with ``cdc$operation = 5`` and ``null`` clustering key, indicating the range is not left-bounded and the other with ``cdc$operation = 8``, representing the right bound of the range:

.. code-block:: none

     cdc$batch_seq_no | pk | ck   | v    | cdc$operation
    ------------------+----+------+------+---------------
                    0 |  0 | null | null |             5
                    1 |  0 |    3 | null |             8



Range deletions with multi-column clustering keys
+++++++++++++++++++++++++++++++++++++++++++++++++

In the case of multi-column clustering keys, range deletions allow to specify an equality relation for some prefix of the clustering key, followed by an ordering relation on the column that follows the prefix. 

#. Start with a basic table:

   .. code-block:: cql

       CREATE TABLE ks.t (pk int, ck1 int, ck2 int, ck3 int, v int, primary key (pk, ck1, ck2, ck3)) WITH cdc = {'enabled':'true'};

#. Remove the rows where the primary key is 0, a prefix of the clustering key (``ck1``) is ``0``, and the following column of the clustering key (``ck2``) is in the range ``(0, 3)``:

   .. code-block:: cql

       DELETE FROM ks.t WHERE pk = 0 and ck1 = 0 AND ck2 > 0 AND ck2 < 3;

#. Display the contents of the table:

   .. code-block:: cql

       SELECT "cdc$batch_seq_no", pk, ck1, ck2, ck3, v, "cdc$operation" FROM ks.t_scylla_cdc_log;

   returns:

   .. code-block:: none

        cdc$batch_seq_no | pk | ck1 | ck2 | ck3  | v    | cdc$operation
       ------------------+----+-----+-----+------+------+---------------
                       0 |  0 |   0 |   0 | null | null |             6
                       1 |  0 |   0 |   3 | null | null |             8

       (2 rows)
