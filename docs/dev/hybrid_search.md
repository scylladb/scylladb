# Hybrid Search - Developer Notes

A hybrid query is a SELECT served by more than one external search at once: a vector search and a
full-text search over the same table, with the rows sorted by a score fused from the ranks each
search gave them.

```cql
SELECT id, BM25_HIGHLIGHT(body, 'wombat')
  FROM ks.t
  ORDER BY RRF(ANN(embedding, [0.1, 0.2]), BM25(body, 'wombat'))
  LIMIT 10;
```

The limitations are listed at the end.

## Why ANN() and BM25() return a (score, rank) tuple

Scores of different searches are not on one scale: a BM25 relevance of 3.5 and a cosine similarity
of 0.9 cannot be added, and whichever is numerically larger would decide the order on its own. Ranks
are comparable. So `ANN()` and `BM25()` return `(score, rank)`, and a fusion function takes one
argument per search: `RRF(ANN(...), BM25(...))` type-checks as written, with no special rule.

`RRF(hit, hit, ...)` is reciprocal-rank fusion: `sum(1 / (k + rank))` with `k = 60`. `ANN_SCORE()`,
`ANN_RANK()`, `BM25_SCORE()` and `BM25_RANK()` return one element of the tuple. All calls with the
same arguments describe one search, so a query using several of them makes one request.

## How a query is prepared

`external_search_plan` owns the searches a statement runs. A `search_source` is one search: a kind
(ANN or BM25), a column, a query value, and the temporaries the score and rank are delivered in.
Every call to an external function (`functions::function::is_external()`) in ORDER BY, SELECT and
WHERE is matched to a source by kind and column and replaced by a read of the temporary that
search's value is delivered in, or, for a rescoring vector index, by the similarity expression the
coordinator evaluates itself.

ORDER BY is the clause that introduces a search; SELECT and WHERE may only refer to one it
introduced. A bare call in ORDER BY means "return the rows in the order this index ranked them",
which costs no temporary and no sorting. Any other expression becomes the score the rows are sorted
by, appended as a hidden trailing selector, the same mechanism the rescoring vector index already
used.

## How a query is run

`external_search_select_statement` builds one `vector_search::search_request` per search and hands
them to `vector_search::search_all()`, which asks every index in parallel and joins the answers by
primary key into one `hybrid_candidate` per distinct key (a row is worth reading if any search
returned it), each carrying what every search said about it: a score and a rank, or nothing where
the search did not return the key. The statement reads those rows once and fills in each search's
score and rank per row from its candidate, with nulls where a search did not return the row.

`search_all()` lives beside the client, not in it, and returns the shape a `/hybrid` endpoint would;
when one exists the client gains a method and the statement need not change.

The rows are sorted after they are read, and only then cut to the LIMIT.

## Limitations

- **Each search is asked for exactly the LIMIT.** Two searches can only disagree about rows both of
  them returned, so asked for the LIMIT alone they leave the fusion little to choose between. Asking
  each for more than the LIMIT is a follow-up.
- **A rank is a position in the index's answer.** A key the index returned but the base table no
  longer has leaves a gap in the ranks. Renumbering after the read is the same problem as the
  rescoring rank below: both need the rank computed once every row is in hand.
- **A rescoring vector index cannot take part in a hybrid query.** The rows are reordered by a
  similarity the coordinator recomputes, so the index's rank does not apply, and the rank in the new
  order is not known until every row is scored, which happens after the selectors are evaluated.
  `ANN()` and `ANN_RANK()` are rejected in SELECT and in a fusion on such an index; a bare
  `ORDER BY ANN(...)` and `ANN_SCORE()` work. Returning the real rank means computing it where the
  whole sorted result is in hand.
- **A hybrid query takes no WHERE clause.** A vector index prefilters and a full-text index does not,
  and how to combine the two is not decided.
- **`ORDER BY 0.7 * ANN_SCORE(...) + 0.3 * BM25_SCORE(...)` does not parse.** The plan lowers and
  type-checks the ordering as an expression, but the grammar rule `orderByClause` accepts a function
  call, not an expression.
- **Paging** is unsupported for external-index queries generally, and unchanged here.
- **Latency** is attributed to the first search's index, and a hybrid query is named "Hybrid Search"
  in the paging warning.
