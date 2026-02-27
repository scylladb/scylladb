===============================
Local Secondary Indexes
===============================

Local Secondary Indexes is an enhancement to :doc:`Global Secondary Indexes <secondary-indexes>`,
which allows Scylla to optimize workloads where the partition key of the base table and the index are the same key.

.. note::
   As of Scylla Open Source 4.0, updates for local secondary indexes are performed **synchronously**. When updates are synchronous, the client acknowledges the write
   operation only **after both** the base table modification **and** the view update are written.
   This is important to note because the process is no longer asynchronous and the modifications are immediately reflected in the index.
   In addition, if the view update fails, the client receives a write error.

Example:

.. code-block:: cql

          CREATE TABLE menus (location text, name text, price float, dish_type text, PRIMARY KEY(location, name));
          CREATE INDEX ON menus((location),dish_type);

As the same partition key is used for the base table (menus) and the Index, one node holds both.
When using a Token Aware Driver, the same node is likely the coordinator, and the query does not require any inter-node communication.

How Local Secondary Index Queries Work
......................................

Lets explore the example above, first with Global Secondary Index (GSI) and then with Local Secondary Index (LSI)

**Global Secondary Index Example**

.. code-block:: cql

          CREATE TABLE menus (location text,
                name text, price float,
                dish_type text,
                PRIMARY KEY(location, name));
                
          INSERT INTO menus (location, name, price, dish_type) VALUES ('Reykjavik', 'hakarl', 16, 'cold Icelandic starter');
          INSERT INTO menus (location, name, price, dish_type) VALUES ('Reykjavik', 'svid', 21, 'hot Icelandic main dish');
          INSERT INTO menus (location, name, price, dish_type) VALUES ('Da Lat', 'banh mi', 5, 'Vietnamese breakfast');
          INSERT INTO menus (location, name, price, dish_type) VALUES ('Ho Chi Minh', 'goi cuon', 6, 'Vietnamese hot starter');
          INSERT INTO menus (location, name, price, dish_type) VALUES ('Warsaw', 'sorrel soup', 5, 'Polish soup');

          
          CREATE INDEX ON menus(dish_type);

The create Index does **not** include the base partition key. As a result, the following query will work, but in an inefficient manner:

.. code-block:: cql

          SELECT * FROM menus WHERE location = 'Warsaw' and dish_type = 'Polish soup';

With GSI, ``dish_type`` acts as the partition key of the index table and the query requires two inter-node hops

.. image:: global-sec-index-example.png

GSI flow:

* The user provides query details to the coordinator node (1)
* An indexing subquery (2) is used  to fetch all matching base keys from the materialized view.
* The coordinator uses the resulting base key set to request appropriate rows from the base table (3).

Note, that partition keys from the base table and underlying materialized view are different, which means that their data is likely to be stored on different nodes.

**Local Secondary Index Example**
Now let's create an LSI, using the base table partition key, in this case ``location`` as partition key for the Index

.. code-block:: cql
          
          CREATE INDEX ON menus((location), dish_type);

.. code-block:: cql

          SELECT * FROM menus WHERE location = 'Warsaw' and dish_type = 'Polish soup';

The same query can be done to one node, as the Index and Base table partitions are guaranteed to be on the same node.

.. image:: local-sec-index-example.png

LSI flow:

* The user provides query details to the coordinator node (1)
* An indexing subquery (2) is used  to fetch all matching base keys from the underlying materialized view.
* The coordinator uses the resulting base key set to request appropriate rows from the base table (3), located in the **same node** as the Index

Both the base table and the underlying materialized view have the same partition keys for corresponding rows. That means that their data resides on the same node and can thus be executed locally, without having to contact another node. When using a **token aware policy**, the entire query will be done with zero inter-node communication.

.. image:: local-sec-index-token-aware-exaple.png

LSI with Token Aware driver flow:

* The user provides query details to the coordinator node (1)
* The same(2) node:

  #. Act as the Coordinator
  #. Holds the Index
  #. Holds the base table

The coordinator processes the request for the index and base table internally and returns the value to the client with zero inter-node messaging.

.. note::

   When the same table has both LSI and GSI, Scylla will automatically use the right Index for each query.

When should you use a Local Secondary Index
...........................................

* When your Index query includes the base table partition key.

More information
................

* :doc:`Global Secondary Indexes </using-scylla/secondary-indexes/>`
* :doc:`CQL Reference </cql/secondary-indexes/>` - CQL Reference for Secondary Indexes

The following courses are available from Scylla University:

* `Materialized Views and Secondary Indexes <https://university.scylladb.com/courses/data-modeling/lessons/materialized-views-secondary-indexes-and-filtering/>`_
* `Local Secondary Indexes <https://university.scylladb.com/courses/data-modeling/lessons/materialized-views-secondary-indexes-and-filtering/topic/local-secondary-indexes-and-combining-both-types-of-indexes/>`_
