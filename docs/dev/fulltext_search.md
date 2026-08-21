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

`BM25()` and `BM25_HIGHLIGHT()` are two readings of the one search a query is served by - the
row's relevance score and an excerpt of its matched text - so one selector pass handles both:
it decides which of the two a call names and then applies the same rules to it. Each is lowered
to an `expr::temporary`, a slot filled per row by `external_search_provider`, and one slot serves
every occurrence of that value, since all of them are required to name the same column and search
term. Because the provider identifies rows by primary key, those columns are added to the
selection even when the query does not select them. `BM25_HIGHLIGHT()` is accepted nowhere but
the `SELECT` clause.

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

An excerpt cannot come back with the ranked keys: it is generated from the row's text, and the
index stores none of it. Choosing which terms matter needs the corpus statistics and the analyzer
that only the index has, so it cannot move to the coordinator either. So the coordinator sends the
text back: a query selecting `BM25_HIGHLIGHT()` makes a second request, to the `/highlight`
endpoint, after the base-table rows have been read, carrying the search term and the highlighted
column's text for each row. This is architecture decision "Option C" (VECTOR-793); storing the
text in the index and having the index read the base table itself were both rejected.

That second request needs the whole result in hand and it needs to suspend, neither of which
`external_values_provider::try_fill()` can do - it is called from a synchronous walk over the
serialized `query::result`. Hence `prepare()`, an async phase the provider runs once before the
row loop: it walks the result to collect one document per row, makes the one batched call, and
`try_fill()` then becomes a lookup.

The reply is **positional**: entry *i* belongs to the *i*-th document sent, and carries no primary
keys. That is exact here because the two walks - the one collecting documents and the one building
the result set - are the same walk over the same merged result, so the collector need only repeat
the builder's rule for a partition holding nothing but a static row. It also collects each row's
key and `try_fill()` checks it, so a drift between the two is an internal error rather than every
excerpt silently belonging to the wrong row.

A row the index found no fragment in gets a null value and is **kept**; null means "no fragment
for this row" and never an empty string. A failed or timed-out `/highlight` call fails the whole
`SELECT` - the two must not be conflated. The call is skipped entirely when the search returned no
rows.

Two consequences worth knowing. The fragment is generated from the row as read, not from the text
that was scored, so a write landing between the last CDC cycle and the base-table read makes the
two differ; this is an accepted product-level trade. And the second call is load-balanced like any
other, so it can land on a node whose corpus statistics differ slightly from the one that served
the search and pick a slightly different window.
