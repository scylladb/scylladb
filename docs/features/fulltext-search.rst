=================================
Full-Text Search in ScyllaDB
=================================

What Is Full-Text Search
-------------------------

Full-Text Search (FTS) lets you find rows that contain specific words or
phrases inside text columns. Unlike exact-match queries or ``LIKE`` filters,
FTS uses an inverted index to tokenize text and rank results by relevance
using the `BM25 <https://en.wikipedia.org/wiki/Okapi_BM25>`_ scoring
algorithm.

Typical use cases include:

* Searching for keywords in product descriptions, articles, or log messages
* Ranking results by how well they match a search query
* Filtering rows that contain at least one matching term

Creating a Full-Text Index
-----------------------------

Before you can run FTS queries, you must create a ``fulltext_index`` on the
target column::

    CREATE CUSTOM INDEX ON ks.t (v) USING 'fulltext_index';

You can specify an analyzer to control how text is tokenized::

    CREATE CUSTOM INDEX ON ks.t (v) USING 'fulltext_index'
        WITH OPTIONS = {'analyzer': 'english'};

**Requirements:**

* The indexed column must be of type ``text``, ``varchar``, or ``ascii``.
  Other types are rejected.
* The indexed column must be a regular or clustering-key column. Partition-key
  columns cannot be indexed.
* The table must use tablets (not vnodes).
* CDC must be enabled on the table with a TTL of at least 86400 seconds
  (24 hours) and either ``delta = 'full'`` or postimage enabled.
  CDC is enabled automatically when creating a fulltext index, so you do not
  need to configure it manually.

Querying with BM25
-------------------

FTS queries use the ``BM25()`` function to score rows against a search term.
``BM25()`` takes two arguments: a column name and a query string.

A full-text search query must use ``BM25()`` in **both** of these clauses, on
the **same** column:

* A ``WHERE`` clause of the exact form ``BM25(column, 'term') > 0`` to **filter**
  the rows that match the search term.
* An ``ORDER BY BM25(column, 'term')`` clause to **rank** the matching rows by
  their BM25 relevance score (highest first).

Neither clause works on its own - a query with only ``WHERE BM25()`` or only
``ORDER BY BM25()`` is rejected, and both must reference the **same column** and
the **same search term**. Every FTS query also requires a ``LIMIT``.

For the full syntax reference, see :ref:`Full-Text Search queries (BM25) <fulltext-queries>`.

Basic query
~~~~~~~~~~~

Filter the rows that contain the search term and rank them by relevance::

    SELECT * FROM ks.t
        WHERE BM25(v, 'search term') > 0
        ORDER BY BM25(v, 'search term')
        LIMIT 10;

In the ``WHERE`` clause, ``>`` is the only supported operator and the right-hand
side must be the literal ``0``. Operators such as ``>=``, ``=``, ``<``, ``<=``,
and ``!=`` are rejected, as is any non-zero threshold.

Filtering support
~~~~~~~~~~~~~~~~~

Additional ``WHERE`` restrictions (such as partition key equality) are not
currently supported alongside ``BM25()`` and will be rejected. Support for
combined filtering is planned for a future release.

Using bind markers
~~~~~~~~~~~~~~~~~~

The query term can be supplied with a bind marker in prepared statements::

    SELECT * FROM ks.t
        WHERE BM25(v, ?) > 0
        ORDER BY BM25(v, ?)
        LIMIT 10;

Disambiguating from a user-defined function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``BM25`` is not a reserved word. If a keyspace defines a user-defined function
named ``bm25``, an unqualified ``BM25()`` call becomes ambiguous; qualify the
built-in operator as ``system.bm25(...)`` to select it explicitly.

Query Constraints
------------------

FTS queries enforce the following rules:

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Constraint
     - Details
   * - Both clauses required
     - A query must include both a ``WHERE BM25() > 0`` filter and an
       ``ORDER BY BM25()`` ranking. Both must reference the same column and
       the same search term. Neither clause is accepted on its own.
   * - ``>`` and literal ``0`` only
     - In ``WHERE``, the only accepted form is ``BM25(column, 'term') > 0``.
       Other operators (``>=``, ``=``, ``<``, ``<=``, ``!=``) and non-zero
       thresholds are rejected.
   * - Filtering support
     - Only ``BM25(column, 'term') > 0`` is accepted in ``WHERE``. Any additional
       restriction (e.g., a partition key equality) is rejected. Combined filtering
       is planned for a future release.
   * - ``LIMIT`` is required
     - Every FTS query must include a ``LIMIT`` clause. Queries without
       ``LIMIT`` are rejected.
   * - ``PER PARTITION LIMIT`` not supported
     - ``PER PARTITION LIMIT`` cannot be used with FTS queries.
   * - Aggregation not supported
     - FTS queries cannot include aggregate functions (e.g., ``COUNT(*)``,
       ``SUM()``).
   * - Fulltext index required
     - The queried column must have a ``fulltext_index``. A regular secondary
       index does not satisfy this requirement.
   * - ``BM25()`` cannot appear in ``SELECT``
     - ``BM25()`` is only valid in ``WHERE`` and ``ORDER BY`` clauses, not
       as a selector.
   * - Partition key columns excluded
     - A ``fulltext_index`` cannot be created on a partition key column, so
       ``BM25()`` cannot target one. Regular and clustering-key text columns
       are supported.
   * - Single ordering only
     - ``ORDER BY BM25()`` cannot be combined with other ``ORDER BY`` columns,
       a second ``BM25()`` ordering, or with ``ANN`` ordering.
