==========================
Pattern Search in ScyllaDB
==========================

What Is Pattern Search
----------------------

Pattern Search finds the rows whose text column matches a ``LIKE``
pattern, such as ``'%abc%'``, ``'abc%'`` or ``'a%b'``, but served by an
index rather than by a scan of the table. The index node does not match
approximately: a value matches when the whole of it matches the pattern. The
pattern has the same syntax as in a filtered ``LIKE``, with the
same ``%``, ``_`` and ``\`` rules, but the query does not behave like a filtered
one in other respects; see
:ref:`Indexed vs. Filtered LIKE <pattern-search-indexed-vs-filtered>`.

It suits values that are searched by any part of them rather than by whole
words, such as product codes, identifiers and tags. For example, with the
table below::

    code
    ------------
    AB-1024-XL
    AB-2048-S
    ab-4096-M
    XAB-1024
    CD-1024-AB

* ``code LIKE 'AB-%'`` returns AB-1024-XL and AB-2048-S, and also ab-4096-M on
  an index created with ``'case_sensitive': 'false'``. It does not return
  XAB-1024 or CD-1024-AB, which contain ``AB`` but do not start with ``AB-``.
* ``code LIKE '%024%'`` returns AB-1024-XL, XAB-1024 and CD-1024-AB. It matches
  inside ``1024``, which a word-based search would not find.
* ``code LIKE '%-_'`` returns AB-2048-S and ab-4096-M, the codes that end with
  a one-character suffix.

A row either matches the pattern or it does not, and no row is ranked above
another: CD-1024-AB, with ``AB`` at its end, is as much a result of
``code LIKE '%AB%'`` as AB-1024-XL. When more rows match than the ``LIMIT``,
which of them are returned is not specified. A query can also return fewer rows
than its ``LIMIT``, even when more rows match, if a row was deleted after the
index found it.

Pattern Search does not tokenize, stem or rank. For word-based search ranked
by relevance, use :doc:`Full-Text Search </features/fulltext-search>`.

Creating a Pattern Index
------------------------

A ``pattern_index`` on a column serves the ``LIKE`` restrictions on that
column. Such a query does not need ``ALLOW FILTERING``; if it is given, it is
ignored and the index is still used. To create the index::

    CREATE CUSTOM INDEX ON ks.parts (code) USING 'pattern_index';

or, to match regardless of letter case::

    CREATE CUSTOM INDEX ON ks.parts (code) USING 'pattern_index'
        WITH OPTIONS = {'case_sensitive': 'false'};

For the option and its default, see
:ref:`Pattern Index <create-pattern-index-statement>`.

.. warning::

   Creating a pattern index changes the meaning of every ``LIKE`` on its
   column, for every application that queries the column. Once the index
   exists, a ``LIKE`` on the column is served by the index, even with
   ``ALLOW FILTERING``:

   * Queries that worked by filtering start failing. The ``LIKE`` must be the
     only restriction in the ``WHERE`` clause, and the query needs a ``LIMIT``
     of at most 1000, so for example a ``LIKE`` together with a restriction on
     the partition key, or a ``LIKE`` without a ``LIMIT``, is rejected.
   * Queries that still succeed can return different rows, without any error.
     The index is eventually consistent, so a recent write may be missed, and
     the rows are neither paged nor ordered. With
     ``'case_sensitive': 'false'``, matching ignores letter case, so a ``LIKE``
     returns rows that a filtered ``LIKE`` does not.
   * When the index node is unavailable, every ``LIKE`` on the column fails.

   ``CREATE INDEX`` returns a warning that summarizes these changes. Dropping the
   index restores filtered ``LIKE`` (see `Dropping a Pattern Index`_). For all
   the differences, see
   :ref:`Indexed vs. Filtered LIKE <pattern-search-indexed-vs-filtered>`.

**Requirements:**

* The indexed column must be of type ``text``, ``varchar``, or ``ascii``.
* The indexed column must be a regular column. Primary-key and static columns
  cannot be indexed.
* The table must use tablets (not vnodes).
* CDC must be enabled on the table with a TTL of at least 86400 seconds
  (24 hours) and either ``delta = 'full'`` or postimage enabled, because the
  index is updated from the CDC log. CDC is enabled automatically when creating
  a pattern index, so you do not need to configure it manually.

Cell-level TTL, set with the standard ``USING TTL`` syntax on ``INSERT`` or
``UPDATE``, makes the value unreadable at its expiration deadline but does not
generate a CDC event, so the pattern index is not updated and keeps a stale
entry for that value. A ``LIKE`` query can then still return the row, with the
expired value read as ``null``. If the index must reflect expiration, use the
:ref:`Per-row TTL <cql-per-row-ttl>` feature instead.

Querying with LIKE
------------------

On a column with a pattern index, ``LIKE`` queries look like this::

    SELECT id, code FROM ks.parts
        WHERE code LIKE '%024%'
        LIMIT 10;

    SELECT id, code FROM ks.parts
        WHERE code LIKE 'AB-%'
        LIMIT 10;

    SELECT id, code FROM ks.parts
        WHERE code LIKE ?
        LIMIT 10;

Creating the index is a deliberate choice to serve every ``LIKE`` on the
column from the index, even with ``ALLOW FILTERING``. The full rules are in
:ref:`Indexed LIKE queries <pattern-queries>`.

.. _pattern-search-indexed-vs-filtered:

Indexed vs. Filtered LIKE
~~~~~~~~~~~~~~~~~~~~~~~~~

Without a pattern index, ``LIKE`` is a filter: ScyllaDB reads the rows and
checks each value against the pattern, as described in
:ref:`LIKE Operator <like-operator>`. On a column with a pattern index, the
same ``LIKE`` is served by the index instead, and the query differs from a
filtered one as follows:

+----------------------+------------------------------------------+------------------------------------------------------+
|                      | Filtered ``LIKE``                        | Indexed ``LIKE``                                     |
+======================+==========================================+======================================================+
| Where it works       | Every deployment, on any ``text``,       | ScyllaDB Cloud, on a regular column with a           |
|                      | ``varchar`` or ``ascii`` column.         | ``pattern_index``. Every ``LIKE`` on that column is  |
|                      |                                          | served by the index.                                 |
+----------------------+------------------------------------------+------------------------------------------------------+
| Matching             | Always case-sensitive.                   | Case-sensitive, unless the index was created with    |
|                      |                                          | ``'case_sensitive': 'false'``: then letter case is   |
|                      |                                          | ignored.                                             |
+----------------------+------------------------------------------+------------------------------------------------------+
| Patterns             | Any pattern.                             | The same syntax. Which patterns are supported        |
|                      |                                          | depends on the index node.                           |
+----------------------+------------------------------------------+------------------------------------------------------+
| ``ALLOW FILTERING``  | Required.                                | Not needed. If given, it is ignored and the index is |
|                      |                                          | still used.                                          |
+----------------------+------------------------------------------+------------------------------------------------------+
| ``LIMIT``            | Optional, any value.                     | Required, at most 1000.                              |
+----------------------+------------------------------------------+------------------------------------------------------+
| Other restrictions   | Allowed, as in any query with            | None: the ``LIKE`` must be the only ``WHERE``        |
| and clauses          | ``ALLOW FILTERING``.                     | restriction, and ``ORDER BY``, ``GROUP BY``,         |
|                      |                                          | ``PER PARTITION LIMIT`` and aggregation are          |
|                      |                                          | rejected.                                            |
+----------------------+------------------------------------------+------------------------------------------------------+
| Paging               | Paged like any other query.              | Not paged: the whole result is returned in one page, |
|                      |                                          | with a warning when the page size is smaller than    |
|                      |                                          | ``LIMIT``.                                           |
+----------------------+------------------------------------------+------------------------------------------------------+
| Order                | The order of a scan: by token, then by   | Not specified. When more rows match than the         |
|                      | clustering key.                          | ``LIMIT``, which of them are returned is not         |
|                      |                                          | specified either.                                    |
+----------------------+------------------------------------------+------------------------------------------------------+
| Consistency          | The rows are read, and matched, at the   | Eventually consistent, and the rows are not checked  |
|                      | query's consistency level.               | against the pattern again: shortly after a write, a  |
|                      |                                          | query may miss a new value, or return a row whose    |
|                      |                                          | current value no longer matches the pattern.         |
+----------------------+------------------------------------------+------------------------------------------------------+
| Failure              | Fails as any read does.                  | Also fails when the index node is unavailable or     |
|                      |                                          | does not support the pattern.                        |
+----------------------+------------------------------------------+------------------------------------------------------+

Dropping a Pattern Index
------------------------

Dropping the index restores filtered ``LIKE`` on its column::

    DROP INDEX ks.parts_code_idx;

From then on, a ``LIKE`` on the column is a filter again: it requires
``ALLOW FILTERING``, matches case-sensitively, and follows the rules of
:ref:`LIKE Operator <like-operator>`. Dropping the index is the way back when
the indexed ``LIKE`` does not suit an application that queries the column.
