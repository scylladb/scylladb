

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

.. _materialized-views:

Materialized Views
------------------

Materialized views names are defined by:

.. code-block:: cql

   view_name: re('[a-zA-Z_0-9]+')


.. _create-materialized-view-statement:

CREATE MATERIALIZED VIEW
........................

You can create a materialized view on a table using a ``CREATE MATERIALIZED VIEW`` statement:

.. code-block:: cql

   create_materialized_view_statement: CREATE MATERIALIZED VIEW [ IF NOT EXISTS ] `view_name` AS
                                     :     `select_statement`
                                     :     PRIMARY KEY '(' `primary_key` ')'
                                     :     WITH `table_options`

For instance::

    CREATE MATERIALIZED VIEW monkeySpecies_by_population AS
        SELECT * FROM monkeySpecies
        WHERE population IS NOT NULL AND species IS NOT NULL
        PRIMARY KEY (population, species)
        WITH comment='Allow query by population instead of species';

The ``CREATE MATERIALIZED VIEW`` statement creates a new materialized view. Each view is a set of *rows* that
corresponds to the rows that are present in the underlying, or base table, as specified in the ``SELECT`` statement. A
materialized view cannot be directly updated, but updates to the base table will cause corresponding updates in the
view.

Creating a materialized view has three main parts:

- The :ref:`select statement <mv-select>` that restricts the data included in the view.
- The :ref:`primary key <mv-primary-key>` definition for the view.
- The :ref:`options <mv-options>` for the view.

Attempting to create an already existing materialized view will return an error unless the ``IF NOT EXISTS`` option is
used. If it is used, the statement will be a no-op if the materialized view already exists.

.. _mv-select:

MV Select Statement
...................

The select statement of a materialized view creation defines which of the base table is included in the view. That
statement is limited in a number of ways:

- The :ref:`selection <selection-clause>` is limited to those that only select columns of the base table. In other
  words, you can't use any function (aggregate or not), casting, term, etc. Aliases are also not supported. You can,
  however, use `*` as a shortcut to selecting all columns. Further, :ref:`static columns <static-columns>` cannot be
  included in a materialized view (which means ``SELECT *`` isn't allowed if the base table has static columns).
- The ``WHERE`` clause has the following restrictions:

  - It cannot include any :token:`bind_marker`.
  - The columns that are not part of the *view table* primary key can't be restricted.
  - As the columns that are part of the *view* primary key cannot be null, they must always be at least restricted by a
    ``IS NOT NULL`` restriction (or any other restriction, but they must have one).
  - They can also be restricted by relational operations (=, >, <).

- The SELECT statement cannot include **any** of the following:

  - An :ref:`ordering clause <ordering-clause>`
  - A :ref:`limit <limit-clause>` clause
  - An :ref:`ALLOW FILTERING <allow-filtering>` clause.

.. _mv-primary-key:

MV Primary Key
..............

A view must have a primary key, and that primary key must conform to the following restrictions:

- It must contain all the primary key columns of the base table. This ensures that every row in the view corresponds to
  exactly one row of the base table.
- It can only contain a single column that is not a primary key column in the base table.

So, for instance, give the following base table definition::

    CREATE TABLE t (
        k int,
        c1 int,
        c2 int,
        v1 int,
        v2 int,
        PRIMARY KEY (k, c1, c2)
    );

then the following view definitions are allowed::

    CREATE MATERIALIZED VIEW mv1 AS
        SELECT * FROM t WHERE k IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL
        PRIMARY KEY (c1, k, c2);

    CREATE MATERIALIZED VIEW mv1 AS
        SELECT * FROM t WHERE v1 IS NOT NULL AND k IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL
        PRIMARY KEY (v1, k, c1, c2);

but the following ones are **not** allowed::

    // Error: cannot include both v1 and v2 in the primary key as both are not in the base table primary key
    CREATE MATERIALIZED VIEW mv1 AS
        SELECT * FROM t WHERE k IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL AND v1 IS NOT NULL
        PRIMARY KEY (v1, v2, k, c1, c2)

    // Error: must include k in the primary as it's a base table primary key column
    CREATE MATERIALIZED VIEW mv1 AS
        SELECT * FROM t WHERE c1 IS NOT NULL AND c2 IS NOT NULL
        PRIMARY KEY (c1, c2)


.. _mv-options:

MV Options
..........

A materialized view is internally implemented by a table, and as such, creating a MV allows the :ref:`same options than
creating a table <create-table-options>`.

Additionally, the following ScyllaDB-specific options are supported:

.. list-table::
   :widths: 20 10 10 60
   :header-rows: 1

   * - Option
     - Kind
     - Default
     - Description
   * - ``synchronous_updates``
     - simple
     - false
     - When true, view updates are applied synchronously; otherwise, view updates may be applied in the background

.. _alter-materialized-view-statement:

ALTER MATERIALIZED VIEW
.......................

After creation, you can alter the options of a materialized view using the ``ALTER MATERIALIZED VIEW`` statement:

.. code-block::
   
   alter_materialized_view_statement: ALTER MATERIALIZED VIEW `view_name` WITH `table_options`

The options that can be updated with an ALTER statement are the same as those used with a CREATE statement (see :ref:`Create table options <create-table-options>`). 

.. _drop-materialized-view-statement:

DROP MATERIALIZED VIEW
......................

Dropping a materialized view users the ``DROP MATERIALIZED VIEW`` statement:

.. code-block::
   
   drop_materialized_view_statement: DROP MATERIALIZED VIEW [ IF EXISTS ] `view_name`;

If the materialized view does not exist, the statement will return an error unless ``IF EXISTS`` is used, in which case
the operation is a no-op.


* :doc:`Apache Cassandra Query Language (CQL) Reference </cql/index>`

.. include:: /rst_include/apache-copyrights.rst
