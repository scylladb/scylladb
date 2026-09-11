# Full-Text Search — Developer Notes

For user-facing documentation (index creation, query syntax, constraints), see
[Full-Text Search](../features/fulltext-search.rst) and
[Fulltext Index section](../cql/secondary-indexes.rst#create-fulltext-index-statement) in the secondary-indexes reference.

This document covers implementation details and decisions not included in the user documentation.

## Design decisions

### CJK analyzers not supported

CJK (Chinese, Japanese, Korean) analyzers are intentionally not supported and are rejected
at index creation time (VECTOR-672). Attempting to use `'analyzer': 'chinese'`, `'japanese'`,
or `'korean'` will result in an error. The exclusion reflects the current scope of the
Tantivy-based backend integration.

### Duplicate index detection

Creating a fulltext index with a name that already exists in the keyspace is rejected
(`already exists`), and creating a second **unnamed** fulltext index on a column that already
has one is rejected as a `duplicate of existing index`. `CREATE CUSTOM INDEX IF NOT EXISTS`
silently succeeds without creating a duplicate. Because the `IF NOT EXISTS` name check matches
across the whole keyspace, reusing an existing index name with `IF NOT EXISTS` on a different
table or column silently does nothing (issue VECTOR-641).

## Implementation overview

### Authorization

To support least-privilege authorization, ScyllaDB supports the `TEXT_SEARCH_INDEXING`
permission as an alternative to the `SELECT` permission for full-text-indexed reads.
It is grantable only on `ALL KEYSPACES`. A role with this permission can read fulltext-indexed base
tables, their CDC log tables, and the system tables needed by Vector Store; it cannot read
unrelated non-fulltext tables unless it also has the `SELECT` permission.

### Query routing and prepare

An FTS query is identified at prepare time by the presence of a `BM25(column, term)` call
in the `ORDER BY` clause. The index is resolved from that column, and all structural
validations are enforced at prepare time: `LIMIT` is required; `PER PARTITION LIMIT` and
aggregation are rejected; a matching `WHERE BM25(column, ...) > 0` must be present on the
**same column** as the `ORDER BY`; any additional `WHERE` restrictions are rejected. The
search-term expression is captured during prepare so that bind markers are correctly
evaluated at execute time.

`BM25()` and `BM25_HIGHLIGHT()` in `SELECT` report two values of the same search: the row's
relevance score and an excerpt of its matched text. `prepare_bm25_selectors()` handles both with
the same rules, and replaces each call with an `expr::temporary`, a slot `external_search_provider`
fills per row. One slot serves every occurrence of a value, since all of them must name the same
column and search term. The score is matched to a row by primary key, so the key columns are added
to the selection even when the query does not select them. The excerpt is matched by position, but
the highlighted column is added to the selection because its text has to be read to be sent to the
index. `BM25_HIGHLIGHT()` is accepted only in the `SELECT` clause.

Additionally, at execute time the search term values in `WHERE` and `ORDER BY` are evaluated
and compared - they must be identical. This catches mismatches that cannot be detected at
prepare time, such as two different bind markers being given different values. The same check
covers the terms of the `SELECT` occurrences, which is why a rejection there names the function
the mismatching term was written in.

### Execution

At execute time, the search term is evaluated (resolving any bind markers) and sent to the
external Vector Store (Tantivy backend) via the BM25 endpoint, which returns a ranked list
of primary keys. ScyllaDB then fetches the corresponding base-table rows and returns them
in rank order. For tables without clustering columns the fetch is batched into a single
range query; for tables with clustering columns each key is fetched individually and the
results are merged.

### Highlighting

The index stores none of the text an excerpt is generated from, and generating one needs the
analyzer and corpus statistics that only the index has. So a query selecting `BM25_HIGHLIGHT()`
makes a second request, to the `/highlight` endpoint, after the base-table rows have been read,
carrying the search term and the highlighted column's text of every row. The alternatives, storing
the text in the index or having the index read the base table, were rejected: the first duplicates
data the base table owns, the second gives the index a read path into the cluster.

That request needs all the rows in hand and has to suspend, and `external_values_provider::try_fill()`
can do neither: it is called from a synchronous walk over the serialized `query::result`. So
`execute_search()` reads the base-table rows and emits the result set in two steps, with the
request in between. `join_table_results()` walks the rows once, matching the ranked keys to them
and reading out the highlighted column; `highlights_of()` sends that text to the index and turns
the reply into one temporary's values; `external_search_provider` is built from those values and
does no I/O: `try_fill()` hands out the value computed for each row, in order, and says which rows
to leave out.

The reply is positional: entry *i* belongs to the *i*-th document sent, and carries no primary
keys. Position is exact because `join_table_results()` walks the rows with
`result_set_builder::visitor`, the same visitor the result set is built with, using a filter that
records each row and rejects it, so the join sees the rows the result set is built from, in the
same order. The score is delivered by position too, although it is matched by key.

A row the index found no fragment in gets a null and is kept. A failed or timed-out `/highlight`
call fails the whole `SELECT`. No request is made when the search returned no rows.

The fragment is generated from the row as read, not from the text that was indexed, so the two
differ for a row written after the index last caught up with the table. The second request is
load-balanced like any other, so it can be served by a node whose corpus statistics differ slightly
from the one that ranked the rows, and pick a slightly different window.
