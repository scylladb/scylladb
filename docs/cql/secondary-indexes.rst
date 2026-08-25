
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
   
   create_index_statement: CREATE [ CUSTOM ] INDEX [ IF NOT EXISTS ] [ `index_name` ]
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


.. _create-vector-index-statement:

Vector Index :label-note:`ScyllaDB Cloud`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

   Vector indexes are supported in ScyllaDB Cloud only in clusters that have the Vector Search feature enabled.
   Vector indexes do not support all ScyllaDB features (e.g., tracing, paging, and grouping). More information
   about Vector Search is available in the
   `ScyllaDB Cloud documentation <https://cloud.docs.scylladb.com/stable/vector-search/>`_.

ScyllaDB supports creating vector indexes on tables, allowing queries on the table to use those indexes for efficient
similarity search on vector data. Vector indexes can be a global index for indexing vectors per table or a local
index for indexing vectors per partition.

The vector index is one of the custom indexes supported in ScyllaDB. It is
created using the ``CUSTOM`` keyword and specifying the index type as
``vector_index``. It is also possible to add additional columns to the index
for filtering the search results. The first column specified in the global
vector index definition must be the vector column, and any subsequent columns
are treated as filtering columns. The local vector index requires that the
partition key of the index is of a type allowed for filtering columns and the
vector column is the first one after the partition key definition, and any
subsequent columns are filtering columns.

ScyllaDB allows creating multiple **named** vector indexes on the same vector column.
This can be used to create a replacement index before dropping an older one.
Unnamed duplicate vector index definitions are still rejected, and index names
must remain unique within a keyspace.

Example of a simple index:

.. code-block:: cql

      CREATE CUSTOM INDEX vectorIndex ON ImageEmbeddings (embedding)
      USING 'vector_index' 
      WITH OPTIONS = {'similarity_function': 'COSINE', 'maximum_node_connections': '16'};

The vector column (``embedding``) is indexed to enable similarity search using
a global vector index. Additional filtering can be performed on the primary key
columns of the base table.

Example of a global vector index with additional filtering:

.. code-block:: cql

      CREATE CUSTOM INDEX vectorIndex ON ImageEmbeddings (embedding, category, info)
      USING 'vector_index' 
      WITH OPTIONS = {'similarity_function': 'COSINE', 'maximum_node_connections': '16'};

The vector column (``embedding``) is indexed to enable similarity search using
a global index. Additional columns are added for filtering the search results.
The filtering is possible on ``category``, ``info`` and all primary key columns
of the base table.

Example of a local vector index:

.. code-block:: cql

      CREATE CUSTOM INDEX vectorIndex ON ImageEmbeddings ((id, created_at), embedding, category, info)
      USING 'vector_index' 
      WITH OPTIONS = {'similarity_function': 'COSINE', 'maximum_node_connections': '16'};

The vector column (``embedding``) is indexed for similarity search (a local
index) and additional columns are added for filtering the search results. The
filtering is possible on ``category``, ``info`` and all primary key columns of
the base table. It is allowed to create a local vector index using
primary key columns of the base table or non-primary-key columns.

Vector indexes support a local index partition key or additional filtering
columns of native data types (excluding counter and duration). The first column
after the partition key definition must be a vector column, while the extra
columns can be used to filter search results.

The supported types are:

* ``ascii``
* ``bigint``
* ``blob``
* ``boolean``
* ``date``
* ``decimal``
* ``double``
* ``float``
* ``inet``
* ``int``
* ``smallint``
* ``text``
* ``varchar``
* ``time``
* ``timestamp``
* ``timeuuid``
* ``tinyint``
* ``uuid``
* ``varint``


The following options are supported for vector indexes. All of them are optional.

+------------------------------+----------------------------------------------------------------------------------------------------------+---------------+
| Option Name                  | Description                                                                                              | Default Value |
+==============================+==========================================================================================================+===============+
| ``similarity_function``      | The similarity function to use for vector comparisons. Supported values are:                             | ``COSINE``    |
|                              | ``COSINE``, ``EUCLIDEAN``, and ``DOT_PRODUCT``. ``DOT_PRODUCT`` requires vectors to be                   |               |
|                              | normalized, meaning each vector should have unit length (L2 norm = 1). For more information, see         |               |
|                              | `Vector normalization <https://en.wikipedia.org/wiki/Normalization_(statistics)#Vector_normalization>`_. |               |
+------------------------------+----------------------------------------------------------------------------------------------------------+---------------+
| ``maximum_node_connections`` | The maximum number of connections per node in the HNSW graph. In other HNSW implementations              | ``16``        |
|                              | it is often denoted as ``m``. Higher values lead to better recall (i.e., more relevant                   |               |
|                              | results are found) but increase memory usage and index size. Supported values are integers               |               |
|                              | between 1 and 512.                                                                                       |               |
+------------------------------+----------------------------------------------------------------------------------------------------------+---------------+
| ``construction_beam_width``  | The beam width to use during index **construction**. In other HNSW implementations it is often           | ``128``       |
|                              | denoted as ``efConstruction``. Higher values lead to better recall (i.e., more relevant                  |               |
|                              | results are found) but increase index creation time and memory usage. Supported values are               |               |
|                              | integers between 1 and 4096.                                                                             |               |
+------------------------------+----------------------------------------------------------------------------------------------------------+---------------+
| ``search_beam_width``        | The beam width to use during index **search**. In other HNSW implementations it is often denoted         | ``64``        |
|                              | as ``efSearch``. Higher values lead to better recall (i.e., more relevant results are found)             |               |
|                              | but increase query latency. Supported values are integers between 1 and 4096.                            |               |
+------------------------------+----------------------------------------------------------------------------------------------------------+---------------+
| ``quantization``             | The quantization method to use for compressing vectors in Vector Index. Vectors in base table            | ``f32``       |
|                              | are never compressed. Supported values (case-insensitive) are:                                           |               |
|                              |                                                                                                          |               |
|                              | * ``f32``: 32-bit single-precision IEEE 754 floating-point.                                              |               |
|                              | * ``f16``: 16-bit standard half-precision floating-point (IEEE 754).                                     |               |
|                              | * ``bf16``: 16-bit "Brain" floating-point (optimized for ML workloads).                                  |               |
|                              | * ``i8``: 8-bit signed integer.                                                                          |               |
|                              | * ``b1``: 1-bit binary value (packed 8 per byte).                                                        |               |
+------------------------------+----------------------------------------------------------------------------------------------------------+---------------+
| ``oversampling``             | A multiplier for the candidate set size during the search phase. For example, if a query asks for 10     | ``1.0``       |
|                              | similar vectors (``LIMIT 10``) and ``oversampling`` is 2.0, the search will initially retrieve 20        |               |
|                              | candidates. This can improve accuracy at the cost of latency. Supported values are                       |               |
|                              | floating-point numbers between 1.0 (no oversampling) and 100.0.                                          |               |
+------------------------------+----------------------------------------------------------------------------------------------------------+---------------+
| ``rescoring``                | Flag enabling recalculation of similarity scores with full precision and re-ranking of the candidate set.| ``false``     |
|                              | Valid only for quantization below ``f32``. Supported values are:                                         |               |
|                              |                                                                                                          |               |
|                              | * ``true``: Enable rescoring.                                                                            |               |
|                              | * ``false``: Disable rescoring.                                                                          |               |
+------------------------------+----------------------------------------------------------------------------------------------------------+---------------+
| ``source_model``             | The name of the embedding model that produced the vectors (e.g., ``"ada002"``). Cassandra client         | *(none)*      |
|                              | libraries such as CassIO send this option to tag the index with the model. Cassandra SAI rejects it as   |               |
|                              | an unrecognized property; ScyllaDB accepts and preserves it in ``DESCRIBE`` output for compatibility     |               |
|                              | with those libraries, but does not act on it.                                                            |               |
+------------------------------+----------------------------------------------------------------------------------------------------------+---------------+


.. _cassandra-sai-compatibility:

Cassandra SAI Compatibility for Vector Search
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

ScyllaDB accepts the Cassandra ``StorageAttachedIndex`` (SAI) class name in ``CREATE CUSTOM INDEX``
statements **for vector columns**. Cassandra libraries such as
`CassIO <https://github.com/CassioML>`_ and `LangChain <https://www.langchain.com/>`_ use SAI to create
vector indexes; ScyllaDB recognizes these statements for compatibility.

When ScyllaDB encounters an SAI class name on a **vector column**, the index is automatically
created as a native ``vector_index``. The following class names are recognized:

* ``org.apache.cassandra.index.sai.StorageAttachedIndex`` (exact case required)
* ``StorageAttachedIndex`` (case-insensitive)
* ``SAI`` (case-insensitive)

Example::

   -- Cassandra SAI statement accepted by ScyllaDB:
   CREATE CUSTOM INDEX ON my_table (embedding)
   USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'
   WITH OPTIONS = {'similarity_function': 'COSINE'};

   -- Equivalent to:
   CREATE CUSTOM INDEX ON my_table (embedding)
   USING 'vector_index'
   WITH OPTIONS = {'similarity_function': 'COSINE'};

The ``similarity_function`` option is supported by both Cassandra SAI and ScyllaDB.

.. note::

   SAI class names are supported on **vector columns** and on **ENTRIES of non-frozen map
   columns** (the CassIO metadata-map pattern).

   * For vector columns, the index is rewritten to a native ``vector_index``.
   * For ``ENTRIES(map_column)``, the SAI class is stripped and a standard secondary index
     is created instead. A CQL warning is emitted noting possible behavioral differences
     with Cassandra SAI metadata filtering. This rewrite requires the
     ``enable_cassio_compatibility`` configuration option to be set to ``true``.

   Using an SAI class name on any other non-vector column (e.g., ``text`` or ``int``) will
   result in an error. General SAI indexing is not supported by ScyllaDB; use a
   :doc:`secondary index </cql/secondary-indexes>` instead.

   Example of the metadata-map rewrite::

      -- CassIO issues this during schema setup:
      CREATE CUSTOM INDEX ON my_table (ENTRIES(metadata_s))
      USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';

      -- ScyllaDB creates the equivalent of:
      CREATE INDEX ON my_table (ENTRIES(metadata_s));

.. _create-fulltext-index-statement:

Full-text Index :label-note:`ScyllaDB Cloud`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

   Full-text indexes are supported in ScyllaDB Cloud only in clusters that have the Vector and Text Search feature enabled.
   For the full list of unsupported ScyllaDB features, see the
   :doc:`Full-Text Search documentation </features/fulltext-search>`.

ScyllaDB supports creating full-text indexes on text columns, enabling full-text search queries that rank results
by relevance using the BM25 scoring algorithm. A full-text index is a custom index created using the ``CUSTOM``
keyword and specifying the index type as ``fulltext_index``.

CDC is enabled automatically on the base table when a full-text index is created.

**Column restrictions:**

* The indexed column must be of type ``text``, ``varchar``, or ``ascii``. Other types are rejected.
* The indexed column must be a regular or clustering-key column. Partition-key columns cannot be indexed.
* The table must use tablets (not vnodes).

Example::

   CREATE CUSTOM INDEX ON articles (body) USING 'fulltext_index';

You can specify an analyzer to control how text is tokenized::

   CREATE CUSTOM INDEX ON articles (body) USING 'fulltext_index'
       WITH OPTIONS = {'analyzer': 'english'};

The following options are supported for full-text indexes:

+----------------+-------------------------------------------------------------------------------------------+-------------------+
| Option         | Description                                                                               | Default Value     |
+================+===========================================================================================+===================+
| ``analyzer``   | Text analyzer for tokenization. Determines how text is split into terms.                  | ``standard``      |
|                | Supported values (case-insensitive): ``standard``, ``english``, ``german``,               |                   |
|                | ``french``, ``spanish``, ``italian``, ``portuguese``, ``russian``, ``simple``,            |                   |
|                | ``whitespace``. No other values are supported.                                            |                   |
+----------------+-------------------------------------------------------------------------------------------+-------------------+
| ``positions``  | Whether token positions are stored. Required for phrase queries.                          | ``true``          |
|                | Supported values: ``true``, ``false`` (case-insensitive).                                 |                   |
+----------------+-------------------------------------------------------------------------------------------+-------------------+

The analyzers differ in how they tokenize and normalize text:

* ``standard`` splits text into terms on whitespace and punctuation, lowercases
  them, and applies a generic set of English stop words. It does not apply any
  language-specific stemming.
* The language analyzers (``english``, ``german``, ``french``, ``spanish``,
  ``italian``, ``portuguese``, ``russian``) tokenize and lowercase like
  ``standard``, but additionally apply stemming and stop-word removal specific
  to that language, so that related word forms (e.g., ``running`` and ``run``)
  match the same term.
* ``simple`` lowercases text and splits on any non-alphanumeric character
  (including punctuation); digits and letters adjacent to each other stay in
  the same term (e.g., ``abc123`` is one term), while punctuation is dropped
  rather than kept as part of a term or as a term of its own (e.g., ``don't``
  becomes the terms ``don`` and ``t``). It does not apply stemming or
  stop-word removal.
* ``whitespace`` splits only on whitespace characters; punctuation attached to
  a word is kept as part of that term (e.g., ``Hello, World!`` becomes the terms
  ``Hello,`` and ``World!``). It does not apply lowercasing, stemming,
  or stop-word removal.

.. _drop-index-statement:

DROP INDEX
^^^^^^^^^^

Dropping a secondary index uses the ``DROP INDEX`` statement:

.. code-block::
   
   drop_index_statement: DROP INDEX [ IF EXISTS ] `index_name`

The ``DROP INDEX`` statement is used to drop an existing secondary index. The argument of the statement is the index
name, which may optionally specify the keyspace of the index.

If the index is currently being built, the ``DROP INDEX`` can still be executed. Once the ``DROP INDEX`` command is issued,
the system stops the build process and cleans up any partially built data associated with the index.

.. If the index does not exists, the statement will return an error, unless ``IF EXISTS`` is used in which case the
.. operation is a no-op.

Changing Indexes During a Paged Read
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A paged read keeps using the query plan it started with. The paging state
returned to the client records whether the read scans the base table or uses a
particular secondary index, and every subsequent page is served by that same
plan, so creating an index that could serve the query affects only new queries.
Because a new index takes a moment to become known cluster-wide, a read that
already uses one may be resumed by a node that does not know it yet, and then
fails as described below.

Dropping the index a paged read uses is different: the position saved in the
paging state belongs to that index's scan, and no other plan can interpret it.
Requesting the next page fails with an ``InvalidRequest`` error stating that the
index is no longer available, and the query has to be retried from the
beginning. Creating another index under the same name does not help, because the
read is tied to the specific index it started with. A prepared statement fails
one step earlier: the ``DROP`` invalidates it, the driver re-prepares it before
resuming, and a query that is not valid without the index - one lacking
``ALLOW FILTERING``, for example - fails that re-preparation. A read scanning the
base table, in contrast, is not tied to that table's identity: dropping and
re-creating the base table does not change the plan, and the next page is served
from the new table - resuming where the old table's scan stopped, so whatever
the new table holds before that position is skipped without an error.

Additional Information
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* :doc:`Global Secondary Indexes </features/secondary-indexes/>`
* :doc:`Local Secondary Indexes </features/local-secondary-indexes/>`

The following courses are available from ScyllaDB University:

* `Materialized Views and Secondary Indexes <https://university.scylladb.com/courses/data-modeling/lessons/materialized-views-secondary-indexes-and-filtering/>`_
* `Global Secondary Indexes <https://university.scylladb.com/courses/data-modeling/lessons/materialized-views-secondary-indexes-and-filtering/topic/global-secondary-indexes/>`_
* `Local Secondary Indexes <https://university.scylladb.com/courses/data-modeling/lessons/materialized-views-secondary-indexes-and-filtering/topic/local-secondary-indexes-and-combining-both-types-of-indexes/>`_

.. include:: /rst_include/apache-copyrights.rst
