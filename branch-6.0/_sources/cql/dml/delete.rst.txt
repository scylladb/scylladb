.. highlight:: cql

.. _delete_statement:

DELETE
^^^^^^

Deleting rows or parts of rows uses the ``DELETE`` statement:

.. code-block::
   
   delete_statement: DELETE [ `simple_selection` ( ',' `simple_selection` ) ]
                   : FROM `table_name`
                   : [ USING `update_parameter` ( AND `update_parameter` )* ]
                   : WHERE `where_clause`
                   : [ IF ( EXISTS | `condition` ( AND `condition` )* ) ]

For instance:

.. code-block:: cql

   DELETE FROM NerdMovies USING TIMESTAMP 1240003134000000
     WHERE movie = 'Serenity';

   DELETE phone FROM Users
     WHERE userid IN (C73DE1D3-AF08-40F3-B124-3FF3E5109F22, B70DE1D0-9908-4AE3-BE34-5573E5B09F14);

The ``DELETE`` statement deletes columns and rows. If column names are provided directly after the ``DELETE`` keyword,
only those columns are deleted from the row indicated by the ``WHERE`` clause. Otherwise, whole rows are removed.

The ``WHERE`` clause specifies which rows are to be deleted. Multiple rows may be deleted with one statement by using an
``IN`` operator. A range of rows may be deleted using an inequality operator (such as ``>=``).

``DELETE`` supports the ``TIMESTAMP`` option with the same semantics as the TIMESTAMP parameter used in the ``UPDATE`` statement.
The ``DELETE`` statement deletes data written with :ref:`INSERT <insert-statement>` or :ref:`UPDATE <update-statement>` (or :ref:`BATCH <batch_statement>`)
using a timestamp that is less than or equal to the ``DELETE`` timestamp.
For more information on the :token:`update_parameter` refer to the :ref:`UPDATE <update-parameters>` section.

In a ``DELETE`` statement, all deletions within the same partition key are applied atomically,
meaning either all columns mentioned in the statement are deleted or none.
If ``DELETE`` statement has the same timestamp as ``INSERT`` or
``UPDATE`` of the same primary key, delete operation prevails (see :ref:`update ordering <update-ordering>`).

A ``DELETE`` operation can be conditional through the use of an ``IF`` clause, similar to ``UPDATE`` and ``INSERT``
statements. Each such ``DELETE`` gets a globally unique timestamp.
However, as with ``INSERT`` and ``UPDATE`` statements, this will incur a non-negligible performance cost
(internally, Paxos will be used) and so should be used sparingly.

Please refer to the :ref:`update parameters <update-parameters>` section for more information on the :token:`update_parameter`.


Range deletions
~~~~~~~~~~~~~~~

Range deletions allow you to delete rows from a single partition, given that the clustering key is within the given range. The delete request can be determined on both ends, or it can be open-ended.

.. _open-range-deletions:


Open range deletions
~~~~~~~~~~~~~~~~~~~~

Open range deletions delete rows based on an open-ended request (>, <, >=, =<, etc.)

For example, suppose your data set for events at Madison Square Garden includes:


.. code-block:: none

   CREATE KEYSPACE mykeyspace WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '3'}  AND durable_writes = true;
   use mykeyspace ;
   CREATE TABLE events ( id text, created_at date, content text, PRIMARY KEY (id, created_at) );
   
   INSERT into events (id, created_at, content) VALUES ('concert', '2019-11-19', 'SuperM');
   INSERT into events (id, created_at, content) VALUES ('concert', '2019-11-15', 'Billy Joel');
   INSERT into events (id, created_at, content) VALUES ('game', '2019-11-03', 'Knicks v Sacramento');
   INSERT into events (id, created_at, content) VALUES ('concert', '2019-10-31', 'Dead & Company');
   INSERT into events (id, created_at, content) VALUES ('game', '2019-10-28', 'Knicks v Chicago');
   INSERT into events (id, created_at, content) VALUES ('concert', '2019-10-25', 'Billy Joel');


   SELECT * from events;

    id      | created_at | content
   ---------+------------+---------------------
       game | 2019-10-28 |    Knicks v Chicago
       game | 2019-11-03 | Knicks v Sacramento
    concert | 2019-10-25 |          Billy Joel
    concert | 2019-10-31 |      Dead & Company
    concert | 2019-11-15 |          Billy Joel
    concert | 2019-11-19 |              SuperM

   (6 rows)

And you wanted to delete all of the concerts from the month of October using an open-ended range delete. You would run:

.. code-block:: none
  
   DELETE FROM events WHERE id='concert' AND created_at <= '2019-10-31';

   SELECT * from events;

    id      | created_at | content
   ---------+------------+---------------------
       game | 2019-10-28 |    Knicks v Chicago
       game | 2019-11-03 | Knicks v Sacramento
    concert | 2019-11-15 |          Billy Joel
    concert | 2019-11-19 |              SuperM

   (4 rows)


:doc:`Apache Cassandra Query Language (CQL) Reference </cql/index>`

.. include:: /rst_include/apache-copyrights.rst


.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..     http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.
