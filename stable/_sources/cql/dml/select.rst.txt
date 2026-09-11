.. highlight:: cql

.. _select-statement:

SELECT
^^^^^^

Querying data from data is done using a ``SELECT`` statement:

.. code-block::
   
   select_statement: SELECT [ DISTINCT ] ( `select_clause` | '*' )
                   : FROM `table_name`
                   : [ WHERE `where_clause` ]
                   : [ GROUP BY `group_by_clause` ]
                   : [ ORDER BY `ordering_clause` ]
                   : [ PER PARTITION LIMIT (`integer` | `bind_marker`) ]
                   : [ LIMIT (`integer` | `bind_marker`) ]
                   : [ ALLOW FILTERING ]
                   : [ BYPASS CACHE ]
                   : [ USING TIMEOUT `timeout` ]
   select_clause: `selector` [ AS `identifier` ] ( ',' `selector` [ AS `identifier` ] )*
   selector: `column_name`
           : | CAST '(' `selector` AS `cql_type` ')'
           : | `function_name` '(' [ `selector` ( ',' `selector` )* ] ')'
           : | COUNT '(' '*' ')'
   where_clause: `relation` ( AND `relation` )*
   group_by_clause: `column_name` (',' `column_name` )*
   relation: `column_name` `operator` `term`
           : '(' `column_name` ( ',' `column_name` )* ')' `operator` `tuple_literal`
           : TOKEN '(' `column_name` ( ',' `column_name` )* ')' `operator` `term`
   operator: '=' | '<' | '>' | '<=' | '>=' | IN | CONTAINS | CONTAINS KEY
   ordering_clause: `column_name` [ ASC | DESC ] ( ',' `column_name` [ ASC | DESC ] )*
   timeout: `duration`

For instance::

    SELECT name, occupation FROM users WHERE userid IN (199, 200, 207);
    SELECT name AS user_name, occupation AS user_occupation FROM users;

    SELECT time, value
    FROM events
    WHERE event_type = 'myEvent'
      AND time > '2011-02-03'
      AND time <= '2012-01-01'

    SELECT COUNT (*) AS user_count FROM users;

    SELECT * FROM users WHERE event_type = 'myEvent' USING TIMEOUT 50ms;

The ``SELECT`` statement reads one or more columns for one or more rows in a table. It returns a result-set of the rows
matching the request, where each row contains the values for the selection corresponding to the query. Additionally,
functions, including aggregation ones, can be applied to the result.

A ``SELECT`` statement contains at least a :ref:`selection clause <selection-clause>` and the name of the table on which
the selection is on (note that CQL does **not** support joins or sub-queries, and thus a select statement only applies to a single
table). In most cases, a select will also have a :ref:`where clause <where-clause>` and it can optionally have additional
clauses to :ref:`order <ordering-clause>` or :ref:`limit <limit-clause>` the results. Lastly, :ref:`queries that require
filtering <allow-filtering>` can be allowed if the ``ALLOW FILTERING`` flag is provided.

If your ``SELECT`` query results in what appears to be missing data, see this :doc:`KB Article </kb/cqlsh-results>` for information. 

.. _selection-clause:

Selection clause
~~~~~~~~~~~~~~~~

The :token:`select_clause` determines which columns need to be queried and returned in the result-set, as well as any
transformation to apply to this result before returning. It consists of a comma-separated list of *selectors* or,
alternatively, of the wildcard character (``*``) to select all the columns defined in the table.

Selectors
`````````

A :token:`selector` can be one of the following:

- A column name of the table selected to retrieve the values for that column.
- A casting, which allows you to convert a nested selector to a (compatible) type.
- A function call, where the arguments are selector themselves.
- A call to the :ref:`COUNT function <count-function>`, which counts all non-null results.

Aliases
```````

Every *top-level* selector can also be aliased (using `AS`). If so, the name of the corresponding column in the result
set will be that of the alias. For instance::

    // Without alias
    SELECT intAsBlob(4) FROM t;

    //  intAsBlob(4)
    // --------------
    //  0x00000004

    // With alias
    SELECT intAsBlob(4) AS four FROM t;

    //  four
    // ------------
    //  0x00000004

.. note:: Currently, aliases aren't recognized anywhere else in the statement where they are used (not in the ``WHERE``
   clause, not in the ``ORDER BY`` clause, ...). You must use the original column name instead.


``WRITETIME`` and ``TTL`` function
```````````````````````````````````

Selection supports two special functions (which aren't allowed anywhere else): ``WRITETIME`` and ``TTL``. Both functions
take only one argument, and that argument *must* be a column name (so, for instance, ``TTL(3)`` is invalid).

Those functions let you retrieve meta-information that is stored internally for each column, namely:

- ``WRITETIME`` retrieves the timestamp used when writing the column. The timestamp is typically the number of *microseconds* since the `Unix epoch <https://en.wikipedia.org/wiki/Unix_time>`_ (January 1st 1970 at 00:00:00 UTC).

You can read more about the ``TIMESTAMP`` retrieved by ``WRITETIME`` in the :ref:`UPDATE <update-parameters>` section.

- ``TTL`` retrieves the remaining time to live (in *seconds*) for the value of the column, if it set to expire, or ``null`` otherwise.

You can read more about TTL in the :doc:`documentation </cql/time-to-live>` and also in `this ScyllaDB University lesson <https://university.scylladb.com/courses/data-modeling/lessons/advanced-data-modeling/topic/expiring-data-with-ttl-time-to-live/>`_. 

.. _where-clause:

The ``WHERE`` clause
~~~~~~~~~~~~~~~~~~~~

The ``WHERE`` clause specifies which rows must be queried. It is composed of relations on the columns that are part of
the ``PRIMARY KEY``.

Not all relations are allowed in a query. For instance, non-equal relations (where ``IN`` is considered as an equal
relation) on a partition key are not supported (see the use of the ``TOKEN`` method below to do non-equal queries on
the partition key). Moreover, for a given partition key, the clustering columns induce an ordering of rows and relations
on them restricted to the relations that let you select a **contiguous** (for the ordering) set of rows. For
instance, given::

    CREATE TABLE posts (
        userid text,
        blog_title text,
        posted_at timestamp,
        entry_title text,
        content text,
        category int,
        PRIMARY KEY (userid, blog_title, posted_at)
    )

The following query is allowed::

    SELECT entry_title, content FROM posts
     WHERE userid = 'john doe'
       AND blog_title='John''s Blog'
       AND posted_at >= '2012-01-01' AND posted_at < '2012-01-31'

But the following query is not, as it does not select a contiguous set of rows (and we suppose no secondary indexes are
set)::

    // Needs a blog_title to be set to select ranges of posted_at
    SELECT entry_title, content FROM posts
     WHERE userid = 'john doe'
       AND posted_at >= '2012-01-01' AND posted_at < '2012-01-31'

When specifying relations, the ``TOKEN`` function can be used on the ``PARTITION KEY`` column to query. In that case,
rows will be selected based on the token of their ``PARTITION_KEY`` rather than on the value. Note that the token of a
key depends on the partitioner in use and that, in particular, the RandomPartitioner won't yield a meaningful order. Also
note that ordering partitioners always order token values by bytes (so even if the partition key is of type int,
``token(-1) > token(0)`` in particular). For example::

    SELECT * FROM posts
     WHERE token(userid) > token('tom') AND token(userid) < token('bob')

Moreover, the ``IN`` relation is only allowed on the last column of the partition key and on the last column of the full
primary key.

It is also possible to “group” ``CLUSTERING COLUMNS`` together in a relation using the tuple notation. For instance::

    SELECT * FROM posts
     WHERE userid = 'john doe'
       AND (blog_title, posted_at) > ('John''s Blog', '2012-01-01')

will request all rows that sort after the one having “John's Blog” as ``blog_title`` and '2012-01-01' for ``posted_at``
in the clustering order. In particular, rows having a ``posted_at <= '2012-01-01'`` will be returned as long as their
``blog_title > 'John''s Blog'``.

The tuple notation may also be used for ``IN`` clauses on clustering columns::

    SELECT * FROM posts
     WHERE userid = 'john doe'
       AND (blog_title, posted_at) IN (('John''s Blog', '2012-01-01'), ('Extreme Chess', '2014-06-01'))

The ``CONTAINS`` operator may only be used on collection columns (lists, sets, and maps). In the case of maps,
``CONTAINS`` applies to the map values. The ``CONTAINS KEY`` operator may only be used on map columns and applies to the
map keys.

.. _group-by-clause:

Grouping results
~~~~~~~~~~~~~~~~~

The ``GROUP BY`` option lets you condense into a single row all selected rows that share the same values for a set of columns. 
Using the ``GROUP BY`` option, it is only possible to group rows at the partition key level or at a clustering column level. 
The ``GROUP BY`` arguments must form a prefix of the primary key. 

For example, if the primary key is ``(p1, p2, c1, c2)``, then the following queries are valid::

    GROUP BY p1
    GROUP BY p1, p2
    GROUP BY p1, p2, c1
    GROUP BY p1, p2, c1, c2

The following should be considered when using the ``GROUP BY`` option:

* If a primary key column is restricted by an equality restriction, it is not required to be present in the ``GROUP BY`` clause. 

* Aggregate functions will produce a separate value for each group. 

* If no ``GROUP BY`` clause is specified, aggregate functions will produce a single value for all the rows.

* If a column is selected without an aggregate function, in a statement with a ``GROUP BY``, the first value encounter in each group will be returned.


.. _ordering-clause:

Ordering results
~~~~~~~~~~~~~~~~

The default order for a SELECT statement depends on the default clustering order of a table, which is defined when 
the table is created - it is ``ASC`` (ascendant) by default, but can be changed using the ``WITH CLUSTERING ORDER BY``
option. See :ref:`CREATE TABLE <create-table-statement>`.

The ``ORDER BY`` clause allows you to configure a non-default order of the returned result. It takes a list of column names
along with the order for the column as an argument  (``ASC`` for ascendant and ``DESC`` for descendant, omitting the default order). 

Currently, the possible orderings are limited by the :ref:`clustering order <clustering-order>` defined on the table:

- If the table has been defined without any specific ``CLUSTERING ORDER``, then allowed orderings are the order
  induced by the clustering columns and the reverse of that one.
- Otherwise, the orderings allowed are the order of the ``CLUSTERING ORDER`` option and the reversed one.

.. _limit-clause:

Limiting results
~~~~~~~~~~~~~~~~

The ``LIMIT`` option to a ``SELECT`` statement limits the number of rows returned by a query, while the ``PER PARTITION
LIMIT``  option (introduced in ScyllaDB 3.1) limits the number of rows returned for a given **partition** by the query. Note that both types of limit can be
used in the same statement.

Examples:

The Partition Key in the following table is ``client_id``, and the clustering key is ``when``.
The table has seven rows, split between four clients (partition keys)

.. code-block:: cql

   cqlsh:ks1> SELECT client_id, when FROM test;

   client_id | when
   -----------+---------------------------------
          1 | 2019-12-31 22:00:00.000000+0000
          1 | 2020-01-01 22:00:00.000000+0000
          2 | 2020-02-10 22:00:00.000000+0000
          2 | 2020-02-11 22:00:00.000000+0000
          2 | 2020-02-12 22:00:00.000000+0000
          4 | 2020-02-10 22:00:00.000000+0000
          3 | 2020-02-10 22:00:00.000000+0000

   (7 rows)


You can ask the query to limit the number of rows returned from **all partition** with LIMIT, for example:

.. code-block:: cql

   cqlsh:ks1> SELECT client_id, when FROM ks1.test LIMIT 3;

   client_id | when
   -----------+---------------------------------
          1 | 2019-12-31 22:00:00.000000+0000
          1 | 2020-01-01 22:00:00.000000+0000
          2 | 2020-02-10 22:00:00.000000+0000

   (3 rows)

You can ask the query to limit the number of rows returned for **each** ``client_id``. For example, with limit of *1* :

.. code-block:: cql

   cqlsh:ks1> SELECT client_id, when FROM ks1.test PER PARTITION LIMIT 1;

   client_id | when
   -----------+---------------------------------
          1 | 2019-12-31 22:00:00.000000+0000
          2 | 2020-02-10 22:00:00.000000+0000
          4 | 2020-02-10 22:00:00.000000+0000
          3 | 2020-02-10 22:00:00.000000+0000

   (4 rows)

Increasing limit to *2*, would yield:

.. code-block:: cql

   cqlsh:ks1> SELECT client_id, when FROM ks1.test PER PARTITION LIMIT 2;

   client_id | when
   -----------+---------------------------------
          1 | 2019-12-31 22:00:00.000000+0000
          1 | 2020-01-01 22:00:00.000000+0000
          2 | 2020-02-10 22:00:00.000000+0000
          2 | 2020-02-11 22:00:00.000000+0000
          4 | 2020-02-10 22:00:00.000000+0000
          3 | 2020-02-10 22:00:00.000000+0000

   (6 rows)

You can also mix the two limits types:

.. code-block:: cql

   cqlsh> SELECT client_id, when FROM ks1.test PER PARTITION LIMIT 1 LIMIT 3;

   client_id | when
   -----------+---------------------------------
          1 | 2019-12-31 22:00:00.000000+0000
          2 | 2020-02-10 22:00:00.000000+0000
          4 | 2020-02-10 22:00:00.000000+0000

   (3 rows)


.. _allow-filtering:

Allowing filtering
~~~~~~~~~~~~~~~~~~

By default, CQL only allows select queries that don't involve “filtering” server-side, i.e. queries where we know that
all (live) record read will be returned (maybe partly) in the result set. The reasoning is that those “non filtering”
queries have predictable performance in the sense that they will execute in a time that is proportional to the amount of
data **returned** by the query (which can be controlled through ``LIMIT``).

The ``ALLOW FILTERING`` option lets you explicitly allow (some) queries that require filtering. Please note that a
query using ``ALLOW FILTERING`` may thus have unpredictable performance (for the definition above), i.e. even a query
that selects a handful of records **may** exhibit performance that depends on the total amount of data stored in the
cluster.

For instance, consider the following table holding user profiles with their year of birth (with a secondary index on
it) and country of residence::

    CREATE TABLE users (
        username text PRIMARY KEY,
        firstname text,
        lastname text,
        birth_year int,
        country text
    )

    CREATE INDEX ON users(birth_year);

Then the following queries are valid::

    SELECT * FROM users;
    SELECT * FROM users WHERE birth_year = 1981;

because in both cases, ScyllaDB guarantees that these queries' performance will be proportional to the amount of data
returned. In particular, if no users were born in 1981, then the second query performance will not depend on the number
of user profiles stored in the database (not directly at least: due to secondary index implementation consideration, this
query may still depend on the number of nodes in the cluster, which indirectly depends on the amount of data stored.
Nevertheless, the number of nodes will always be multiple orders of magnitude lower than the number of user profiles
stored). Of course, both queries may return very large result sets in practice, but the amount of data returned can always
be controlled by adding a ``LIMIT``.

However, the following query will be rejected::

    SELECT * FROM users WHERE birth_year = 1981 AND country = 'FR';

because ScyllaDB cannot guarantee that it won't have to scan a large amount of data even if the result of those queries is
small. Typically, it will scan all the index entries for users born in 1981 even if only a handful are actually from
France. However, if you “know what you are doing”, you can force the execution of this query by using ``ALLOW
FILTERING`` and so the following query is valid::

    SELECT * FROM users WHERE birth_year = 1981 AND country = 'FR' ALLOW FILTERING;

.. _eval-order:

Evaluation order of SELECT statement clauses
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This section explains the relative priority among the various clauses
of the SELECT statement.

 - All rows of the table named in the FROM clause are considered as candidates.
 - Rows are ordered in token order first, then partition key order, then clustering key order.
 - If ORDER BY is specified, then the clustering key order can be reversed.
 - The WHERE clause predicate is applied.
 - GROUP BY is then applied to create groups.
 - Aggregate functions in the SELECT clause are applied to groups, or to the entire query if GROUP BY was not specified.
 - If there are selectors that are not aggregate functions, then the first value in the group is selected.
 - If specified, PER PARTITION LIMIT is applied to each partition.
 - If specified, LIMIT is applied to the entire query result.

.. note:: The server may use a different execution plan, as long as it arrives at the same result. For
  example, conditions in the WHERE clause will limit the candidate row set first by looking up the
  primary index or a secondary index.


.. _bypass-cache:

Bypass Cache
~~~~~~~~~~~~~~~~~~

The ``BYPASS CACHE`` clause on SELECT statements informs the database that the data being read is unlikely to be read again in the near future, and also was unlikely to have been read in the near past; therefore, no attempt should be made to read it from the cache or to populate the cache with the data. This is mostly useful for range scans; these typically process large amounts of data with no temporal locality and do not benefit from the cache.
The clause is placed immediately after the optional ALLOW FILTERING clause.

``BYPASS CACHE`` is a ScyllaDB CQL extension and not part of Apache Cassandra CQL.

For example::
  
  SELECT * FROM users BYPASS CACHE;
  SELECT name, occupation FROM users WHERE userid IN (199, 200, 207) BYPASS CACHE;
  SELECT * FROM users WHERE birth_year = 1981 AND country = 'FR' ALLOW FILTERING BYPASS CACHE;

.. _using-timeout:

Using Timeout
~~~~~~~~~~~~~~~~~~

The ``USING TIMEOUT`` clause allows specifying a timeout for a specific request.

For example::

  SELECT * FROM users USING TIMEOUT 5s;
  SELECT name, occupation FROM users WHERE userid IN (199, 200, 207) BYPASS CACHE USING TIMEOUT 200ms;

``USING TIMEOUT`` is a ScyllaDB CQL extension and not part of Apache Cassandra CQL.

.. _like-operator:

LIKE Operator
~~~~~~~~~~~~~

The ``LIKE`` operation on ``SELECT`` statements informs ScyllaDB that you are looking for a pattern match. The expression ‘column LIKE pattern’ yields true only if the entire column value matches the pattern.   
 
The search pattern is a string of characters with two wildcards, as shown:

* ``_`` matches any single character
* ``%`` matches any substring (including an empty string)
* ``\`` escapes the next pattern character, so it matches verbatim
* any other pattern character matches itself
* an empty pattern matches empty text fields
 
 
.. note:: Only string types (ascii, text, and varchar) are valid for matching

Currently, the match is **case sensitive**. The entire column value must match the pattern. 
For example, consider the search pattern 'M%n' - this will match ``Martin``, but will not match ``Moonbeam`` because the ``m`` at the end isn't matched. In addition, ``moon`` is not matched because ``M`` is not the same as ``m``. Both the pattern and the column value are assumed to be UTF-8 encoded.
 
A query can find all values containing some text fragment by matching to an appropriate ``LIKE`` pattern.

**Differences Between ScyllaDB and Cassandra LIKE Operators**

* In Apache Cassandra, you must create a SASI index to use LIKE. ScyllaDB supports LIKE as a regular filter.
* Consequently, ScyllaDB LIKE will be less performant than Apache Cassandra LIKE for some workloads.
* ScyllaDB treats underscore (_) as a wildcard; Cassandra doesn't.
* ScyllaDB treats percent (%) as a wildcard anywhere in the pattern; Cassandra only at the beginning/end
* ScyllaDB interprets backslash (\\) as an escape character; Cassandra doesn't.
* Cassandra allows case-insensitive LIKE; ScyllaDB doesn't (see `#4911 <https://github.com/scylladb/scylla/issues/4911>`_).
* ScyllaDB allows empty LIKE pattern; Cassandra doesn't.

**Example A**
 
In this example, ``LIKE`` specifies that the match is looking for a word that starts with the letter ``S``. The ``%`` after the letter ``S`` matches any text to the end of the field. 

.. code-block:: none

   SELECT * FROM pet_owners WHERE firstname LIKE ‘S%’ ALLOW FILTERING;
   ╭──────────┬─────────────────────┬────────────────╮
   │ID        │LastName             │FirstName       │
   ├──────────┼─────────────────────┼────────────────┤
   │1         │Adams                │Steven          │
   ├──────────┼─────────────────────┼────────────────┤
   │15        │Erg                  │Sylvia          │
   ├──────────┼─────────────────────┼────────────────┤
   │20        │Goldberg             │Stephanie       │
   ├──────────┼─────────────────────┼────────────────┤
   │25        │Harris               │Stephanie       │
   ├──────────┼─────────────────────┼────────────────┤
   │88        │Rosenberg            │Samuel          │
   ├──────────┼─────────────────────┼────────────────┤
   │98        │Smith                │Sara            │
   ├──────────┼─────────────────────┼────────────────┤
   │115       │Williams             │Susan           │
   ├──────────┼─────────────────────┼────────────────┤
   │130       │Young                │Stuart          │
   ╰──────────┴─────────────────────┴────────────────╯




**Example B**

In this example, you are searching for all pet owners whose last name contains the characters 'erg'.

.. code-block:: none

   SELECT * FROM pet_owners WHERE lastname LIKE ‘%erg%’ ALLOW FILTERING;

   ╭──────────┬─────────────────────┬────────────────╮
   │ID        │LastName             │FirstName       │
   ├──────────┼─────────────────────┼────────────────┤
   │11        │Berger               │David           │
   ├──────────┼─────────────────────┼────────────────┤
   │18        │Gerg                 │Lawrence        │
   ├──────────┼─────────────────────┼────────────────┤
   │20        │Goldberg             │Stephanie       │
   ├──────────┼─────────────────────┼────────────────┤
   │88        │Rosenberg            │Samuel          │
   ├──────────┼─────────────────────┼────────────────┤
   │91        │Schulberg            │Barry           │
   ├──────────┼─────────────────────┼────────────────┤
   │110       │Weinberg             │Stuart          │
   ╰──────────┴─────────────────────┴────────────────╯

Note that this query does not return: 

.. code-block:: none

   ╭──────────┬─────────────────────┬────────────────╮
   │ID        │LastName             │FirstName       │
   ├──────────┼─────────────────────┼────────────────┤
   │15        │Erg                  │Sylvia          │
   ╰──────────┴─────────────────────┴────────────────╯

As it is case sensitive. 


**Example C**

This table contains some commonly used ``LIKE`` filters and the matches you can expect the filter to return.

.. list-table::
   :widths: 50 50
   :header-rows: 1

   * - Filter
     - Matches
   * - %abe%
     - Babel, aberration, cabernet, scarabees
   * - _0\%
     - 10%, 20%, 50%
   * - a%t
     - asphalt, adapt, at



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
