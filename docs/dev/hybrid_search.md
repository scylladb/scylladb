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

A fusion needs to know, for each search, where that search put the row, and it may need the score
as well. So each search call returns both, as one value.

The rank is there because scores of different searches are not on one scale: a BM25 relevance of
3.5 and a cosine similarity of 0.9 cannot be added, and whichever is numerically larger would decide
the order on its own. Ranks are comparable, and RRF uses only them. The score is kept because a
user-defined fusion may want to weigh by it, and because `SELECT ANN(...)` reports what the search
said about the row.

They are one value rather than two calls so that a fusion function takes exactly one argument per
search. `RRF(ANN(...), BM25(...))` is then an ordinary call that type-checks as written, a
user-defined fusion is an ordinary function of `tuple<float, int>` arguments, and a score cannot be
paired with another search's rank.

`RRF(hit, hit, ...)` is reciprocal-rank fusion: `sum(1 / (k + rank))` with `k = 60`. `ANN_SCORE()`,
`ANN_RANK()`, `BM25_SCORE()` and `BM25_RANK()` return one element of the tuple. All calls with the
same arguments describe one search, so a query using several of them makes one request.

## How a query is prepared

`external_search_plan` owns the searches a statement runs. A `search_source` is one search: a kind
(ANN or BM25), a column, a query value, and the temporaries the score and rank are delivered in.
Every call to a search function (one whose `functions::function::is_external()` is true) in ORDER BY, SELECT and
WHERE is matched to a source by kind and column and replaced by a read of the temporary that
search's value is delivered in, or, for a rescoring vector index, by the similarity expression the
coordinator evaluates itself.

ORDER BY is the clause that introduces a search; SELECT and WHERE may only refer to one it
introduced. `ANN()` or `BM25()` called directly, as in `ORDER BY ANN(embedding, [0.1, 0.2])`, means
"return the rows in the order this index ranked them", which costs no temporary and no sorting;
`ANN_SCORE()` and `BM25_SCORE()` called directly mean the same, the index's order being the score's,
highest first. Any other function call, such as `ORDER BY RRF(ANN(...), BM25(...))`, becomes the
score the rows are sorted by, highest first, appended as a hidden trailing selector, the same
mechanism the rescoring vector index already used. It must evaluate to a float, so `ANN_RANK()`,
`BM25_RANK()` or `BM25_HIGHLIGHT()` called directly is rejected, while a function of ranks is
accepted like any other: it is up to the function to give the best row the highest score, as `RRF()`
does. A null score sorts after every other, so a function of `*_SCORE()` or `*_RANK()` that
`RETURNS NULL ON NULL INPUT` puts a row only one search returned after every row both found.

## How a query is run

`external_index_select_statement` builds one `vector_search::search_request` per search and hands
them to `vector_search::search_all()`, which asks every index in parallel and joins the answers by
primary key into one `search_candidate` per distinct key (a row is worth reading if any search
returned it), each carrying what every search said about it: a score and a rank, or nothing where
the search did not return the key. The statement reads those rows once and fills in each search's
score, rank and excerpt per row from its candidate, with nulls where a search did not return the
row. The excerpt is asked for only for the rows its search returned.

`search_all()` lives beside the client, not in it, and returns the shape a `/hybrid` endpoint would;
when one exists the client gains a method and the statement need not change.

The rows are sorted after they are read, and only then cut to the LIMIT.

## Limitations

- **Each search's answer is cut to the LIMIT before the fusion.** A vector index is asked for the
  LIMIT times its `oversampling`, and only the first LIMIT keys of its answer are fused and read, as
  a Vector Store doing the oversampling itself would answer. Two searches can only disagree about
  rows both of them returned, so cut to the LIMIT they leave the fusion little to choose between.
  Asking each for more than the LIMIT is a follow-up.
- **The ORDER BY clause is a single scoring function call.** A query over searches orders by one
  function call (`ANN()`, `BM25()`, a fusion such as `RRF()`, or a user-defined function of them)
  instead of the usual list of clustering columns: it cannot be combined with a column ordering and
  takes no `ASC` or `DESC`.
- **A rank is a position in the index's answer.** A key the index returned but the base table no
  longer has leaves a gap in the ranks. Renumbering after the read is the same problem as the
  rescoring rank below: both need the rank computed once every row is in hand.
- **A rescoring vector index orders the rows only by itself.** The rows are reordered by a
  similarity the coordinator recomputes, so the index's rank does not apply, and the rank in the new
  order is not known until every row is scored, which happens after the selectors are evaluated.
  What `ANN_SCORE()` should be for a row the vector search did not return is not decided either. So
  on such an index ORDER BY supports only `ANN()` or `ANN_SCORE()` called directly, which keeps it
  out of a fusion, and in SELECT `ANN()` and `ANN_RANK()` are rejected while `ANN_SCORE()` works.
  Returning the real rank means computing it where the whole sorted result is in hand.
- **A hybrid query takes no WHERE clause.** A vector index prefilters and a full-text index does not,
  and how to combine the two is not decided.
- **Paging** is unsupported for external-index queries generally, and unchanged here. The warning
  names a hybrid query "Hybrid search".
- **Latency** is attributed to the first search's index.
