# Pattern Search — Developer Notes

For user-facing documentation see [Pattern Search](../features/pattern-search.rst),
the [Pattern Index section](../cql/secondary-indexes.rst#create-pattern-index-statement)
and [Pattern search queries](../cql/dml/select.rst#pattern-queries).

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
everything around the engine instead: CDC ingestion, the index-node protocol, the
`external_index` base class and the `external_index_select_statement` execution.

## Query routing and prepare

Unlike BM25 and ANN, which are function calls the WHERE analysis holds out as "scoring
restrictions", a `LIKE` is an ordinary column restriction. It is routed through the regular
"does an index support this restriction" path:

- `index::supports_expression()` (`index/secondary_index_manager.cc`) answers yes for
  `oper_t::LIKE` on the target column of a pattern index, whatever the pattern, and no for any
  other operator, so an equality on the column is still filtered.
- A `LIKE` on that column therefore makes the query "use secondary indexing" and the pattern
  index is the chosen one. `raw::select_statement::prepare` recognises the chosen index as a
  pattern index and dispatches to `pattern_indexed_table_select_statement` before the
  view-indexed statement. `check_needs_filtering` and the filtering-column retrieval are skipped,
  as for BM25 and ANN, because no post-filtering happens.

ScyllaDB does not look into the pattern. Which patterns the index node serves, and how, is up to
it, so widening that set needs no change here. It must treat `%`, `_` and `\` exactly as
`like_matcher` does, so that a case-sensitive index returns the same rows as a filtered `LIKE`.

Creating the index is the opt-in: a `LIKE` on the column is routed even with `ALLOW FILTERING`, and
a failed or refused request fails the query rather than falling back to a scan. A scan would not
return the same rows when the index was created with `'case_sensitive': 'false'`, since a filtered
`LIKE` is always case-sensitive. It would also turn an index node outage into a full table scan
for every `LIKE` on the column.

A `LIKE` on a column without a pattern index is unchanged: it still requires `ALLOW FILTERING`
and scans the table.

## Execution

`execute_search` evaluates the pattern, rejecting a null one, posts it and `LIMIT` to `/like`,
and reads the returned keys from the base table with the shared `query_base_table` and
`emit_result_set`. No values provider is installed, so on a table with clustering keys the rows
come back in the token-merged order of the read, not in the index's order. This is the same
"no particular order" a filtered `LIKE` has.

## Options

The only option is `case_sensitive` (default true, matching `LIKE`). With `false`, the index node
lowercases both the values and the patterns. ScyllaDB sends the pattern as written and does no case
folding itself. How the index node matches patterns is its own concern, so none of it is exposed
as an option.

Only regular columns can be targets. ScyllaDB keeps every WHERE condition on a partition or
clustering key column, `LIKE` included, as a key restriction, and
`pattern_indexed_table_select_statement` rejects key restrictions, so a `LIKE` on a key column
could never reach the index. The Vector Store fetches each row by its full primary key, which
ScyllaDB refuses when the only selected column is static.

## Testing

- `test/cqlpy/test_pattern_index.py`: schema and option validation, and prepare-time query
  validation, including the columns a `LIKE` is still filtered on.
- `test/cqlpy/test_pattern_search_with_mock.py`: routing to the mocked `/like` endpoint, patterns
  and bind markers sent as written, error mapping, paging warning.
