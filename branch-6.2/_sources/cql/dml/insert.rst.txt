.. highlight:: cql

.. _insert-statement:

INSERT
^^^^^^

Inserting data for a row is done using an ``INSERT`` statement:

.. code-block::
   
   insert_statement:  INSERT INTO table_name ( `names_values` | `json_clause` )
                   :  [ IF NOT EXISTS ]
                   :  [ USING `update_parameter` ( AND `update_parameter` )* ];
   names_values: `names` VALUES `tuple_literal`
   json_clause:  JSON `string` [ DEFAULT ( NULL | UNSET ) ]
   names: '(' `column_name` ( ',' `column_name` )* ')'
   update_parameter: ( TIMESTAMP `int_value` | TTL  `int_value` | TIMEOUT `duration` )
   int_value: ( `integer` | `bind_marker` )

For example:

.. code-block:: cql

    INSERT INTO NerdMovies (movie, director, main_actor, year)
          VALUES ('Serenity', 'Joss Whedon', 'Nathan Fillion', 2005)
          USING TTL 86400 IF NOT EXISTS;

The ``INSERT`` statement writes one or more columns for a given row in a table. Note that since a row is identified by
its ``PRIMARY KEY``, at least the columns composing it must be specified. The list of columns to insert to must be
supplied when using the ``VALUES`` syntax.

Note that unlike in SQL, ``INSERT`` does not check the prior existence of the row by default: the row is created if none
existed before, and updated otherwise. Furthermore, there is no means to know which of creation or update happened.

All updates of an ``INSERT`` are applied atomically, meaning the
statement can not have a partial effect on database state.

It can, however, leave some of the columns unchanged due to the semantics
of eventual consistency on an event of a timestamp collision:

``INSERT`` statements happening concurrently at different cluster
nodes proceed without coordination. Eventually cell values
supplied by a statement with the highest timestamp will prevail (see :ref:`update ordering <update-ordering>`).

Unless a timestamp is provided by the client, ScyllaDB will automatically
generate a timestamp with microsecond precision for each
column assigned by ``INSERT``. ScyllaDB ensures timestamps created
by the same node are unique. Timestamps assigned at different
nodes are not guaranteed to be globally unique.
With a steadily high write rate timestamp collision
is not unlikely. If it happens, i.e. two ``INSERTS`` have the same
timestamp, a conflict resolution algorithm determines which of the inserted cells prevails (see :ref:`update ordering <update-ordering>`).

Please refer to the :ref:`update parameters <update-parameters>` section for more information on the :token:`update_parameter`.

.. code-block:: cql

    INSERT INTO NerdMovies  (movie, director, main_actor)
    VALUES ('Serenity', 'Anonymous', 'Unknown')
    USING TIMESTAMP  1442880000000000;
    INSERT INTO NerdMovies  (movie, director, main_actor)
    VALUES ('Serenity', 'Joseph Whedon', 'Nathan Fillion')
    USING TIMESTAMP 1442880000000000;

    SELECT movie, director, main_actor FROM NerdMovies WHERE movie = 'Serenity'

.. code-block:: none

   movie    | director      | main_actor | year
   ----------+---------------+------------+------
   Serenity | Joseph Whedon |    Unknown | null


``INSERT`` is not required to assign all columns, so if two
statements modify the same primary key but assign different
columns effects of both statements are preserved:

.. code-block:: cql

    INSERT INTO NerdMovies (movie, director, main_actor)
          VALUES ('Serenity', 'Joss Whedon', 'Nathan Fillion');
    INSERT INTO NerdMovies (movie, director, main_actor, year)
          VALUES ('Serenity', 'Josseph Hill Whedon', 2005);
    SELECT * FROM NerdMovies WHERE movie = 'Serenity'

.. code-block:: none

   ╭─────────┬───────────────────┬──────────────┬─────╮
   │movie    │director           │main_actor    │year │
   ├─────────┼───────────────────┼──────────────┼─────┤
   │Serenity │Joseph Hill Whedon │Nathan Fillion│2005 │
   ╰─────────┴───────────────────┴──────────────┴─────╯

Also note that ``INSERT`` does not support counters, while ``UPDATE`` does.

.. note:: You can use the ``IF NOT EXISTS`` condition with the ``INSERT`` statement. When this is used, the insert is only made if the row does not exist prior to the insertion. Each such ``INSERT`` gets a globally unique timestamp. Using ``IF NOT EXISTS`` incurs a non-negligible performance cost (internally, as Paxos will be used), so use ``IF NOT EXISTS`` wisely.


If :ref:`enabled <cdc-options>` on a table, you can use UPDATE, INSERT, and DELETE statements with Change Data Capture (CDC) tables.

.. to-do - add link to cdc doc


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
