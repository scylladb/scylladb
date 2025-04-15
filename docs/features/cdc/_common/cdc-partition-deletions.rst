Partition deletion
------------------

An example for deleting a partition is as follows:

#. Start with a basic table and insert some data into it.

   .. code-block:: cql

       CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled':'true'};
       INSERT INTO ks.t (pk,ck,v) VALUES (0,0,0);
       INSERT INTO ks.t (pk,ck,v) VALUES (0,1,1);

#. Display the contents of the base table

   .. code-block:: cql

       SELECT * FROM ks.t;

   returns:

   .. code-block:: none

        pk | ck | v
       ----+----+---
         0 |  0 | 0
         0 |  1 | 1

       (2 rows)

#. Remove all rows where the primary key is equal to 0.   

   .. code-block:: cql

       DELETE FROM ks.t WHERE pk = 0;


#. Display the contents of the corresponding CDC log table:

   .. code-block:: cql

       SELECT "cdc$batch_seq_no", pk, ck, v, "cdc$operation" FROM ks.t_scylla_cdc_log;

   returns:

   .. code-block:: none

        cdc$batch_seq_no | pk | ck   | v    | cdc$operation
       ------------------+----+------+------+---------------
                       0 |  0 |    0 |    0 |             2
                       0 |  0 |    1 |    1 |             2
                       0 |  0 | null | null |             4

       (3 rows)

In the CDC log, partition deletion is identified by ``cdc$operation = 4``. Columns in the log that correspond to clustering key and non-key columns in the base table are set to ``null``.
