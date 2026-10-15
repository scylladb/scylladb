# Pattern Search — Developer Notes

For user-facing documentation see [Pattern Search](../features/pattern-search.rst),
the [Pattern Index section](../cql/secondary-indexes.rst#create-pattern-index-statement)
and [Indexed LIKE queries](../cql/dml/select.rst#pattern-queries).

This document covers the implementation and the decisions behind it.

## What it is

`pattern_index` is the third external (Vector Store backed) index kind, next to `vector_index`
and `fulltext_index`. The index node answers `POST /api/v1/indexes/{ks}/{index}/like` with the
primary keys of the rows whose value matches a `LIKE` pattern, and ScyllaDB reads those rows from
the base table. Nothing is scored, so the reply carries primary keys only and the rows come back
in no particular order.

## Why a separate index kind

The full-text index tokenizes into words, stores frequencies and positions, and ranks by BM25.
None of that is wanted for pattern matching, and the CQL entry point is `LIKE`, not
`BM25()`. Sharing the class would mean every option and code path had two modes. The two share
everything around the engine instead: CDC ingestion, the index-node protocol and query execution.

## Query routing and prepare

Unlike BM25 and ANN, which are function calls introduced by ORDER BY, a `LIKE` is an ordinary
column restriction:

- A pattern index supports `LIKE` on its target column, whatever the pattern, and no other
  operator, so the secondary index selection accepts a `LIKE` alone without `ALLOW FILTERING`. A
  query on the column without a `LIKE`, such as an equality, is filtered as if there were no index.
- A `LIKE` on that column becomes a pattern search, whichever index the selection picked, and runs
  like an ANN or BM25 search rather than through the view-indexed path. It is found in the WHERE
  clause before the restrictions are prepared, which are then prepared as if `ALLOW FILTERING`
  were given, as for ANN and BM25. A query the index cannot serve, such as one with a second
  restriction on the column, is therefore rejected with the pattern search's error, rather than
  asked for an `ALLOW FILTERING` that would not help. Next to the `LIKE`, any other restriction
  is rejected, an equality included.
  No post-filtering happens, so the filtering checks are skipped, as for BM25 and ANN.
- A pattern search is not combined with other searches: a `LIKE` on a pattern-indexed column next
  to an ANN or BM25 search is rejected.

ScyllaDB does not look into the pattern. Which patterns the index node serves, and how, is up to
it, so widening that set needs no change here. It must treat `%`, `_` and `\` exactly as
a filtered `LIKE` does, so that a case-sensitive index matches a value exactly when a filtered
`LIKE` would. The index is updated from CDC and is eventually consistent, and the rows read from
the base table are not checked against the pattern again, so a stale entry can return a row that
no longer matches.

Creating the index is the opt-in: a `LIKE` on the column is routed even with `ALLOW FILTERING`, and
a failed or refused request fails the query. A `LIKE` the index cannot serve, such as one without a
`LIMIT` or with another restriction, is rejected. The index defines `LIKE` on its column for as
long as it exists, and `DROP INDEX` restores filtered `LIKE`. Since the index changes the results
of existing `LIKE` queries on its column, creating a pattern index returns a CQL warning naming the
column and stating these rules.

## Execution

The pattern is evaluated, a null one rejected, and posted with the `LIMIT` to `/like`. At most
`LIMIT` of the returned keys are kept and read from the base table. Nothing is scored, so on a table
with clustering keys the rows come back in the order the base-table reads deliver them, not in the
index's order.

## Options

The only option is `case_sensitive` (default true, matching `LIKE`). With `false`, the index node
lowercases both the values and the patterns. ScyllaDB sends the pattern as written and does no case
folding itself. How the index node matches patterns is its own concern, so none of it is exposed
as an option.

Only regular columns can be targets. ScyllaDB keeps every WHERE condition on a partition or
clustering key column, `LIKE` included, as a key restriction, and a pattern search rejects key
restrictions, so a `LIKE` on a key column could never reach the index. The Vector Store fetches
each row by its full primary key, which ScyllaDB refuses when the only selected column is static.

## Testing

- `test/cqlpy/test_pattern_index.py`: schema and option validation, and prepare-time query
  validation, including the columns a `LIKE` is still filtered on.
- `test/cqlpy/test_pattern_search_with_mock.py`: routing to the mocked `/like` endpoint, patterns
  and bind markers sent as written, error mapping, paging warning.
