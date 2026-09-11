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

You can specify an analyzer to control how text is tokenized. The default is
``standard``; for a description of each supported analyzer, see
:ref:`Full-text Index <create-fulltext-index-statement>`.

::

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

Cell-level TTL, set with the standard ``USING TTL`` syntax on ``INSERT`` or
``UPDATE``, makes the value unreadable at its expiration deadline but does
not generate a CDC event, so the fulltext index is not updated and keeps a
stale entry for that value. If the index must reflect expiration, use the
:ref:`Per-row TTL <cql-per-row-ttl>` feature instead: it deletes expired rows
explicitly and does generate a CDC event.

Querying with BM25
-------------------

FTS queries use the ``BM25()`` function to score rows against a search term.
``BM25()`` takes two arguments: a column name and a query string.

A full-text search query must use ``BM25()`` in **both** of these clauses, on
the **same** column and with the **same** search term:

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

``BM25()`` may also be selected, to return the relevance score of each row::

    SELECT id, BM25(v, 'search term') AS score FROM ks.t
        WHERE BM25(v, 'search term') > 0
        ORDER BY BM25(v, 'search term')
        LIMIT 10;

It is the score the rows are ranked by, so it needs the two clauses above and has to reference the
same column and the same search term they do.

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

All the search-term bind markers are checked at execution time and must be
given the same value; binding different values for the ``WHERE`` and
``ORDER BY`` clauses or for a selected ``BM25()`` causes the query to be
rejected.

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
     - Every FTS query must include a ``LIMIT`` clause of at most 1000.
       Queries without ``LIMIT``, or with a ``LIMIT`` greater than 1000,
       are rejected.
   * - ``PER PARTITION LIMIT`` not supported
     - ``PER PARTITION LIMIT`` cannot be used with FTS queries.
   * - Aggregation not supported
     - FTS queries cannot include aggregate functions (e.g., ``COUNT(*)``,
       ``SUM()``).
   * - Fulltext index required
     - The queried column must have a ``fulltext_index``. A regular secondary
       index does not satisfy this requirement.
   * - ``BM25()`` in ``SELECT`` needs the other two clauses
     - ``BM25()`` may be used as a selector, to return each row's relevance
       score, but only in a query that already has the required ``WHERE`` and
       ``ORDER BY`` clauses. Every occurrence must reference the same column
       and the same search term.
   * - Single ordering only
     - ``ORDER BY BM25()`` cannot be combined with other ``ORDER BY`` columns,
       a second ``BM25()`` ordering, or with ``ANN`` ordering.
   * - Paging not supported
     - FTS queries do not support paging; all matching rows up to ``LIMIT``
       are returned in a single page.
   * - Grouping not supported
     - FTS queries cannot include a ``GROUP BY`` clause.
