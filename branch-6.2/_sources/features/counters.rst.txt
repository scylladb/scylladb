
==================
ScyllaDB Counters
==================

.. note:: Counters are not supported in keyspaces with :doc:`tablets</architecture/tablets>` enabled.

Counters are useful for any application where you need to increment a count, such as keeping a track of:

* The number of web page views on a website.
* The number of apps/updates downloaded.
* The number of clicks on an ad or pixel.
* The number of points earned in a game.

A **counter** is a special data type (column) that only allows its value to be incremented, decremented, read, or deleted.  As a type, counters are a 64-bit signed integer. Updates to counters are atomic, making them perfect for counting. 

A table with a counter column is created empty and gets populated and initialized by updates rather than inserts. 
A new record with a counter set to null is created on the first update statement with a particular PK value and then immediately incremented by a given number (treating null as a 0). For example:

.. code-block:: none

   cqlsh:mykeyspace> CREATE TABLE cf (pk int PRIMARY KEY, my_counter counter);
   cqlsh:mykeyspace> SELECT * FROM cf;
   pk | my_counter
   ----+------------

.. code-block:: none

   cqlsh:mykeyspace> UPDATE cf SET my_counter = my_counter + 0 WHERE pk = 20;
   cqlsh:cdc_test> SELECT * FROM cf;
   pk | my_counter
   ----+------------
   20 | 0

.. code-block:: none

   cqlsh:mykeyspace> UPDATE cf SET my_counter = my_counter + 6 WHERE pk = 0;
   cqlsh:mykeyspace> SELECT * FROM cf;
   cqlsh:cdc_test> SELECT * FROM cf;
   pk | my_counter
   ---+------------
    0 | 6
   20 | 0

However, counters have limitations not present in other column types:

* The only other columns in a table with a counter column can be columns of the primary key (which cannot be updated). No other kinds of columns can be included. This limitation safeguards the correct handling of counter and non-counter updates by not allowing them in the same operation.
* All non-counters columns in the table must be part of the primary key.
* Counter columns, on the other hand, may not be part of the primary key.
* Counters may not be indexed.
* Counters may not be part of a materialized view.
* ScyllaDB does not support USING TIMESTAMP or USING TTL in the command to update a counter column.
* Once deleted, counter column values **should** not be used again. If you reuse them, proper behavior is not guaranteed.
* Counters cannot be set to a specific value other than when incrementing from 0 using the UPDATE command at initialization.
* Updates are **not** :term:`idempotent <Idempotent>`. In the case of a write failure, the client cannot safely retry the request. 


Example:
........

#. Create a table. The table can only contain the primary key and the counter column(s):

   .. code-block:: shell

      cqlsh:mykeyspace> CREATE TABLE cf (pk int PRIMARY KEY, my_counter counter);

#. Increment the count:

   .. code-block:: shell

	cqlsh:mykeyspace> UPDATE cf SET my_counter = my_counter + 3 WHERE pk = 0;
	cqlsh:mykeyspace> SELECT * FROM cf;

	pk | my_counter
	---+-------------
	0  | 3

#. Decrement the count:

   .. code-block:: shell

	cqlsh:mykeyspace> UPDATE cf SET my_counter = my_counter - 1 WHERE pk = 0;
	cqlsh:mykeyspace> SELECT * FROM cf;

	pk | my_counter
	---+-----------
	0  | 2


#. Delete the row:

   .. code-block:: shell

	cqlsh:mykeyspace> delete from cf where pk = 0;
	cqlsh:mykeyspace> select * from cf;

	pk | my_counter
	---+-----------

Remember that once deleted, counter column values should not be used again. If you reuse them, proper behavior is not guaranteed.


More Information 
................

* Read our `blog <http://www.scylladb.com/2017/04/04/counters/>`_ on counters, or see the data type :ref:`description <counters>`.
* `ScyllaDB University: Advanced Data Modeling lesson <https://university.scylladb.com/courses/data-modeling/lessons/advanced-data-modeling/>`_ - Covers advanced Data Modeling topics. We recommend that you start with the basic data modeling lesson first. It covers the application workflow, query analysis, denormalization, and more, providing practical hands-on examples. 
