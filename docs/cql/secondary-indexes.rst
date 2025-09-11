
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

.. highlight:: cql

.. _secondary-indexes:

Global Secondary Indexes
------------------------

CQL supports creating secondary indexes on tables, allowing queries on the table to use those indexes. A secondary index
is identified by a name defined by:

.. code-block::
   
   index_name: re('[a-zA-Z_0-9]+')



.. _create-index-statement:

CREATE INDEX
^^^^^^^^^^^^

Creating a secondary index on a table uses the ``CREATE INDEX`` statement:

.. code-block::
   
   create_index_statement: CREATE INDEX [IF NOT EXISTS] [ `index_name` ]
                         :     ON `table_name` '(' `index_identifier` ')'
                         :     [ USING `string` [ WITH `index_properties` ] ]
   index_identifier: `column_name`
                   :| ( FULL ) '(' `column_name` ')'
   index_properties: index_property (AND index_property)*
   index_property: OPTIONS = `map_literal`
                 :| view_property

where `view_property` is any :ref:`property <mv-options>` that can be used when creating
a :doc:`materialized view </features/materialized-views>`. The only exception is `CLUSTERING ORDER BY`,
which is not supported by secondary indexes.

If the statement is provided with a materialized view property, it will not be applied to the index itself.
Instead, it will be applied to the underlying materialized view of it.

For instance::

   CREATE INDEX userIndex ON NerdMovies (user);
   CREATE INDEX ON Mutants (abilityId);

   -- Create a secondary index called `catsIndex` on the table `Animals`.
   -- The indexed column is `cats`. Both properties, `comment` and
   -- `synchronous_updates`, are view properties, so the underlying materialized
   -- view will be configured with: `comment = 'everyone likes cats'` and
   -- `synchronous_updates = true`.
   CREATE INDEX catsIndex ON Animals (cats) WITH comment = 'everyone likes cats' AND synchronous_updates = true;

   -- Create a secondary index called `dogsIndex` on the same table, `Animals`.
   -- This time, the indexed column is `dogs`. The property `gc_grace_seconds` is
   -- a view property, so the underlying materialized view will be configured with
   -- `gc_grace_seconds = 13`.
   CREATE INDEX dogsIndex ON Animals (dogs) WITH gc_grace_seconds = 13;

   -- The view property `CLUSTERING ORDER BY` is not supported by secondary indexes,
   -- so this statement will be rejected by Scylla.
   CREATE INDEX bearsIndex ON Animals (bears) WITH CLUSTERING ORDER BY (bears ASC);

View properties of a secondary index have the same limitations as those imposed by materialized views.
For instance, a materialized view cannot be created specifying ``gc_grace_seconds = 0``, so creating
a secondary index with the same property will not be possible either.

Example::

   -- This statement will be rejected by Scylla because creating
   -- a materialized view with `gc_grace_seconds = 0` is not possible.
   CREATE INDEX names ON clients (name) WITH gc_grace_seconds = 0;

   -- This statement will also be rejected by Scylla.
   -- It's not possible to use `COMPACT STORAGE` with a materialized view.
   CREATE INDEX names ON clients (name) WITH COMPACT STORAGE;

The ``CREATE INDEX`` statement is used to create a new (automatic) secondary index for a given (existing) column in a
given table. A name for the index itself can be specified before the ``ON`` keyword, if desired. If data already exists
for the column, it will be indexed asynchronously. After the index is created, new data for the column is indexed
automatically at insertion time.

Local Secondary Index
^^^^^^^^^^^^^^^^^^^^^

:doc:`Local Secondary Indexes </features/local-secondary-indexes>` is an enhancement of :doc:`Global Secondary Indexes </features/secondary-indexes>`, which allows ScyllaDB to optimize the use case in which the partition key of the base table is also the partition key of the index. Local Secondary Index syntax is the same as above, with extra parentheses on the partition key.

.. code-block::

   index_identifier: `column_name`
                   :| ( PK ) | KEYS | VALUES | FULL ) '(' `column_name` ')'

Example:

.. code-block:: cql

          CREATE TABLE menus (location text, name text, price float, dish_type text, PRIMARY KEY(location, name));
          CREATE INDEX ON menus((location),dish_type);

More on :doc:`Local Secondary Indexes </features/local-secondary-indexes>`

.. Attempting to create an already existing index will return an error unless the ``IF NOT EXISTS`` option is used. If it
.. is used, the statement will be a no-op if the index already exists.

.. Indexes on Map Keys (supported in ScyllaDB 2.2)
.. ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. When creating an index on a :ref:`maps <maps>`, you may index either the keys or the values. If the column identifier is
.. placed within the ``keys()`` function, the index will be on the map keys, allowing you to use ``CONTAINS KEY`` in
.. ``WHERE`` clauses. Otherwise, the index will be on the map values.

.. _alter-index-statement:

ALTER INDEX
^^^^^^^^^^^

Currently, there is no statement allowing to alter a secondary index itself. However, the underlying
:doc:`materialized view </features/materialized-views>` can be altered via the ``ALTER INDEX`` statement:

.. code-block::

   alter_index_statement: ALTER INDEX `index_name` WITH `view_properties`
   view_properties: view_property (AND view_property)*

where `view_property` is any :ref:`property <mv-options>` that can be used when
altering a materialized view.

Example:

.. code-block:: cql

   CREATE TABLE ks.table (p int PRIMARY KEY, v int);
   CREATE INDEX my_index ON ks.table (v);

   -- Change the property `gc_grace_seconds` of the underlying materialized view
   -- of the secondary index. Its name value will be 13.
   ALTER INDEX ks.my_index WITH gc_grace_seconds = 13;

   -- Enable synchronous updates for the underlying materialized view.
   ALTER INDEX ks.my_index WITH synchronous_updates = true;

View properties of a secondary index have the same limitations as those imposed by materialized views.
For instance, it's forbidden to alter the default :ref:`Time to Live <time-to-live>`. The same holds
for secondary indexes:

Example::

   -- This statement will be rejected by Scylla because it's
   -- forbidden to alter the default Time to Live of a materialized view.
   ALTER INDEX ks.names WITH default_time_to_live = 3600;

.. _drop-index-statement:

DROP INDEX
^^^^^^^^^^

Dropping a secondary index uses the ``DROP INDEX`` statement:

.. code-block::
   
   drop_index_statement: DROP INDEX [ IF EXISTS ] `index_name`

The ``DROP INDEX`` statement is used to drop an existing secondary index. The argument of the statement is the index
name, which may optionally specify the keyspace of the index.

.. If the index does not exists, the statement will return an error, unless ``IF EXISTS`` is used in which case the
.. operation is a no-op.

Additional Information
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* :doc:`Global Secondary Indexes </features/secondary-indexes/>`
* :doc:`Local Secondary Indexes </features/local-secondary-indexes/>`

The following courses are available from ScyllaDB University:

* `Materialized Views and Secondary Indexes <https://university.scylladb.com/courses/data-modeling/lessons/materialized-views-secondary-indexes-and-filtering/>`_
* `Global Secondary Indexes <https://university.scylladb.com/courses/data-modeling/lessons/materialized-views-secondary-indexes-and-filtering/topic/global-secondary-indexes/>`_
* `Local Secondary Indexes <https://university.scylladb.com/courses/data-modeling/lessons/materialized-views-secondary-indexes-and-filtering/topic/local-secondary-indexes-and-combining-both-types-of-indexes/>`_

.. include:: /rst_include/apache-copyrights.rst
