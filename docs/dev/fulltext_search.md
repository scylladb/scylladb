# Fulltext search (FTS) in Scylla

Scylla supports fulltext search through the `fulltext_index` custom index type.
FTS queries use the `BM25` operator to find rows matching a text query
and optionally rank them by relevance.

## Index creation

A fulltext index is a custom index registered under the class name `fulltext_index`:

```cql
CREATE CUSTOM INDEX ON ks.t (v) USING 'fulltext_index';
CREATE CUSTOM INDEX ON ks.t (v) USING 'fulltext_index' WITH OPTIONS = {'analyzer': 'english'};
```

### Column restrictions

Fulltext indexes can only be created on regular (non-primary-key) columns of type
`text`, `varchar`, or `ascii`. Attempting to index a column of any other type or
a primary key column is rejected at creation time.

### Index options

| Option      | Values                                                                                              | Description                                                                 |
|-------------|------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------|
| `analyzer`  | `standard`, `english`, `german`, `french`, `spanish`, `italian`, `portuguese`, `russian`, `simple`, `whitespace` | Text analyzer for tokenization. Determines how text is split into terms.    |
| `positions` | `true`, `false`                                                                                      | Whether token positions are stored. Required for phrase queries.            |

Unknown options are rejected with an error.

### CDC requirement

Fulltext indexes require CDC (Change Data Capture) to be enabled on the base table
with specific settings:

- **TTL** must be at least 86400 seconds (24 hours).
- **Delta mode** must be `full`, or **postimage** must be enabled.

These constraints exist because the external search backend (Tantivy) rebuilds
and maintains the fulltext index asynchronously from CDC log entries. The 24-hour
TTL ensures CDC entries are retained throughout the index build window.

Dropping or weakening CDC on a table with an active fulltext index is rejected.

### Tablets requirement

Fulltext indexes require the table to use tablets (not vnodes).

### Duplicate detection

Like vector indexes, multiple **named** fulltext indexes on the same column are
allowed (useful for zero-downtime recreation). Unnamed duplicate fulltext indexes
on the same target are rejected.

## Querying

### BM25 operator

`BM25(column, 'query_term')` scores rows using the BM25 ranking algorithm.
It can appear in `WHERE` (as a filter) and/or `ORDER BY` (for relevance ranking):

```cql
-- Filter by BM25 score
SELECT * FROM ks.t WHERE BM25(v, 'search term') > 0 LIMIT 10;

-- Order by relevance
SELECT * FROM ks.t ORDER BY BM25(v, 'search term') LIMIT 10;

-- Both: filter and order
SELECT * FROM ks.t WHERE BM25(v, 'term') > 0 ORDER BY BM25(v, 'term') LIMIT 10;

-- With partition key restriction
SELECT * FROM ks.t WHERE p = 1 AND BM25(v, 'term') > 0 LIMIT 10;

-- Bind markers
SELECT * FROM ks.t WHERE BM25(v, ?) > 0 LIMIT ?;
```

`BM25` is a reserved keyword — it cannot be used as a column, table, or keyspace name.

### Query constraints

All FTS queries enforce the following rules:

- **LIMIT is required.** FTS queries without `LIMIT` are rejected.
- **Per-partition LIMIT is not supported.** `PER PARTITION LIMIT` is rejected.
- **Aggregation is not supported.** Queries with aggregate functions are rejected.
- **A fulltext index must exist** on the queried column. Queries on columns
  without a `fulltext_index` are rejected. A regular secondary index does not
  satisfy this requirement.

For `ORDER BY BM25`:

- **BM25 must be the only ordering.** It cannot be combined with other
  `ORDER BY` columns or with ANN ordering.

## Implementation overview

### Query routing

A SELECT statement is classified as an FTS query when its `WHERE` clause
contains a `BM25(...)` restriction, its `ORDER BY` clause contains a
`BM25(...)` call, or both. FTS queries are handled by
`fulltext_indexed_table_select_statement`.

### Index resolution

During statement preparation the fulltext index is resolved as follows:

1. If `ORDER BY BM25` is present, the index is resolved from the ordering
   clause — all indexes are scanned for one that covers the target column and
   is of type `fulltext_index`. This follows the same pattern as ANN ordering.
2. If only `WHERE BM25` is present, the index is resolved from the
   restriction's index manager.
3. When both are present, the ORDER BY index takes priority.

### Execution

Query execution against the Tantivy search backend is not yet implemented.
Currently FTS queries return an empty result set with the correct metadata.
