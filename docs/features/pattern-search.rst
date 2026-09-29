==========================
Pattern Search in ScyllaDB
==========================

What Is Pattern Search
----------------------

Pattern Search finds the rows whose text column matches a ``LIKE``
pattern, such as ``'%keyword%'``, ``'keyword%'`` or ``'a%b'``, but served by an
index rather than by a scan of the table. The matching is exact, not
approximate: a value matches when the whole of it matches the pattern, with the
same ``%``, ``_`` and ``\`` rules as a filtered ``LIKE``.

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
which of them are returned is not specified.

Pattern Search does not tokenize, stem or rank. For word-based search ranked
by relevance, use :doc:`Full-Text Search </features/fulltext-search>`.

Creating a Pattern Index
------------------------

Before you can run ``LIKE`` queries without ``ALLOW FILTERING``, create a
``pattern_index`` on the column::

    CREATE CUSTOM INDEX ON ks.parts (code) USING 'pattern_index';

or, to match regardless of letter case::

    CREATE CUSTOM INDEX ON ks.parts (code) USING 'pattern_index'
        WITH OPTIONS = {'case_sensitive': 'false'};

For the option and its default, see
:ref:`Pattern Index <create-pattern-index-statement>`.

**Requirements:**

* The indexed column must be of type ``text``, ``varchar``, or ``ascii``.
* The indexed column must be a regular column. Primary-key and static columns
  cannot be indexed.
* The table must use tablets (not vnodes).
* CDC must be enabled on the table with a TTL of at least 86400 seconds
  (24 hours) and either ``delta = 'full'`` or postimage enabled. CDC is enabled
  automatically when creating a pattern index, so you do not need to
  configure it manually.

Cell-level TTL, set with the standard ``USING TTL`` syntax on ``INSERT`` or
``UPDATE``, makes the value unreadable at its expiration deadline but does not
generate a CDC event, so the pattern index is not updated and keeps a stale
entry for that value. If the index must reflect expiration, use the
:ref:`Per-row TTL <cql-per-row-ttl>` feature instead.

Querying with LIKE
------------------

Every ``LIKE`` on an indexed column is answered by the index, whatever its
pattern::

    SELECT id, code FROM ks.parts
        WHERE code LIKE '%024%'
        LIMIT 10;

    SELECT id, code FROM ks.parts
        WHERE code LIKE 'AB-%'
        LIMIT 10;

    SELECT id, code FROM ks.parts
        WHERE code LIKE ?
        LIMIT 10;

Creating the index is a deliberate choice to serve these queries from the
index: a ``LIKE`` on the column is never scanned anymore, even with
``ALLOW FILTERING``. The full rules are in
:ref:`Pattern search queries <pattern-queries>`.

How It Works
------------

ScyllaDB sends the pattern to the index node as written, and the index node
finds the matching rows. It may refuse a pattern it cannot serve, for instance
one with no literal characters such as ``'%'``, and the query then fails.

The index runs on the same index nodes as the vector and full-text indexes and
is updated from the table's CDC log, so it is eventually consistent. Shortly
after a write, a query may still miss a new value, or return a row by its old
value even though the new one no longer matches.

Limitations
-----------

* ``LIMIT`` is required and capped at 1000; results are not ordered and not
  paged.
* One ``LIKE`` per query, and no other ``WHERE`` restriction, ``ORDER BY``,
  ``GROUP BY`` or aggregation.
* When the index node fails or refuses the pattern, the query fails. It does
  not fall back to a scan.
