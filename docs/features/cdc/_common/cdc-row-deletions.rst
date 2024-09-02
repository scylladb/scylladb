.. _row-deletions:

Row deletions
-------------

Row deletions are operations where you delete an entire clustering row by using a ``DELETE`` statement, specifying the values of all clustering key columns, but **not** specifying any particular regular column.
The following is an example of a row deletion with CDC enabled.

#. Start with a basic table and insert some data into it.

   .. code-block:: cql

       CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': 'true'};
       INSERT INTO ks.t (pk, ck, v) VALUES (0,0,0);

#. Display the contents of the table

   .. code-block:: cql

       SELECT * from ks.t;

   returns:

   .. code-block:: none

        pk | ck | v
       ----+----+---
         0 |  0 | 0

       (1 rows)

#. Remove the row where the primary key 0 and the clustering key is 0.

   .. code-block:: cql

       DELETE FROM ks.t WHERE pk = 0 AND ck = 0;

#. Check that after performing a row deletion, querying the deleted row returns 0 results:

   .. code-block:: cql

       SELECT * from ks.t WHERE pk = 0 AND ck = 0;

   returns:

   .. code-block:: none

        pk | ck | v
       ----+----+------

       (0 rows)

Note that row deletions *are different than column deletions*, which are ``DELETE`` statements with some particular non-key column specified, like the following:

.. code-block:: cql

    DELETE v FROM ks.t WHERE pk = 0 AND ck = 0;

Column deletions are equivalent to UPDATEs with the column set to null, e.g.:

.. code-block:: cql

    UPDATE ks.t SET v = null WHERE pk = 0 AND ck = 0;

You can read about UPDATEs in the :ref:`corresponding section <updates>`.
