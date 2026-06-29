# Full-Text Search — Developer Notes

For user-facing documentation (index creation, query syntax, constraints), see
:doc:`Full-Text Search </features/fulltext-search>` and
:ref:`Fulltext Index <create-fulltext-index-statement>` in the secondary-indexes reference.

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

### Query routing and prepare

An FTS query is identified at prepare time by the presence of a `BM25(column, term)` call
in the `ORDER BY` clause. The index is resolved from that column, and all structural
validations are enforced at prepare time: `LIMIT` is required; `PER PARTITION LIMIT` and
aggregation are rejected; a matching `WHERE BM25(column, ...) > 0` must be present on the
**same column** as the `ORDER BY`; any additional `WHERE` restrictions are rejected. The
search-term expression is captured during prepare so that bind markers are correctly
evaluated at execute time.

Additionally, at execute time the search term values in `WHERE` and `ORDER BY` are evaluated
and compared - they must be identical. This catches mismatches that cannot be detected at
prepare time, such as two different bind markers being given different values.

### Execution

At execute time, the search term is evaluated (resolving any bind markers) and sent to the
external Vector Store (Tantivy backend) via the BM25 endpoint, which returns a ranked list
of primary keys. ScyllaDB then fetches the corresponding base-table rows and returns them
in rank order. For tables without clustering columns the fetch is batched into a single
range query; for tables with clustering columns each key is fetched individually and the
results are merged.
