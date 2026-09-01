# Alternator Vector Search

```{admonition} Availability
:class: important

The Vector Search feature is only available in [ScyllaDB Cloud](https://cloud.docs.scylladb.com/) - a fully managed DBaaS running ScyllaDB.
```

## Introduction

Vector search enables _approximate nearest neighbor_ (ANN) search on numeric
vectors stored as item attributes. In a typical use case, each item in a
table contains a high-dimensional embedding vector (e.g., produced by a
machine-learning model), and a search asks for the _k_ items whose stored
vectors are closest to a given query vector. This kind of similarity search
is a building block for recommendation engines, semantic text search, image
retrieval, and other AI/ML workloads.

Alternator implements the same Vector Search API that Amazon DynamoDB does:
`CreateTable`/`UpdateTable`'s `VectorIndexes`/`VectorIndexUpdates`
parameters, `DescribeTable`'s reporting of vector indexes, and the
`SearchVectors` operation all work the same way in Alternator as they do in
DynamoDB, including DynamoDB's `SearchSchema` mechanism for filtering
(`HASH` and `INLINE_FILTER` attributes, described below). Applications
written against DynamoDB's Vector Search API should work against Alternator
unmodified, using an AWS SDK version that supports this API.

On top of this DynamoDB-compatible core, Alternator also has its own
`FLOAT32VECTOR` attribute type — a ScyllaDB-only, more compact way to
store and send vectors, described in
[its own section below](#the-float32vector-type-scylladb-extension). Sections
below are marked **ScyllaDB extension** whenever they describe something
beyond what DynamoDB's own Vector Search API defines.

This document describes the wire-level API in enough detail to use it
directly (e.g., with a raw HTTP client, or an SDK version that doesn't
model this operation), but application authors will normally just use
their favorite AWS SDK's generated `SearchVectors`/`CreateTable` bindings.
For a broader introduction to vector search concepts and terminology
(embeddings, ANN, distance functions, etc.) not specific to any particular
database's API, see the
[Vector Search Concepts](https://cloud.docs.scylladb.com/stable/vector-search/vector-search-concepts.html)
and
[Vector Search Glossary](https://cloud.docs.scylladb.com/stable/vector-search/vector-search-glossary.html)
sections of the ScyllaDB Cloud documentation.

## Overview

The workflow has three steps:

1. **Create** a table (or update an existing one) with one or more _vector
   indexes_, each on a numeric vector attribute. Choose a `Projection` to
   control which attributes are returned by a search without an extra
   round-trip to the base table, and optionally declare one or more other
   attributes as the index's `SearchSchema`, to allow filtering the vector
   search itself, not just examining the ANN results afterwards.
2. **Write** items that include the indexed vector attribute. The attribute
   can be stored as a standard DynamoDB list (`L`) of number (`N`) elements,
   or as the ScyllaDB-specific `FLOAT32VECTOR` type — a list of JSON
   floating-point numbers stored internally as compact 4-byte floats instead
   of JSON-encoded decimal strings, saving space and parse overhead.
3. **Search** using the `SearchVectors` operation to retrieve the _k_ nearest
   neighbors of a query vector.

## Vector indexes: CreateTable's VectorIndexes parameter

An optional `VectorIndexes` parameter can be passed to `CreateTable`. It is
a list of vector index definitions, each an object with the following
fields. All of them are mandatory, with no defaults, except `SearchSchema`
which is genuinely optional:

| Field | Type | Description |
|-------|------|-------------|
| `IndexName` | String | Unique name for this vector index (unique among all vector indexes, GSIs, and LSIs on the table). Follows the same naming rules as table names: 3–192 characters, matching the regex `[a-zA-Z0-9._-]+`. |
| `VectorAttribute` | Structure | `{"AttributeName": "<name>"}` — the item attribute that holds the vector. It must not be a key column of the base table or of any GSI/LSI. |
| `Dimensions` | Integer | The fixed size of the vector (number of elements), between 1 and 16000. |
| `DistanceFunction` | String | The distance metric used for nearest-neighbor search: `COSINE`, `EUCLIDEAN`, or `DOT_PRODUCT`. See [Similarity scores](#similarity-scores) below for what each one means for the score returned by a search. |
| `Projection` | Structure | Which attributes are stored in the vector index and returned by default. See [Projection](#projection) below. |
| `SearchSchema` | Array | Optional. Which attributes can be used to filter the search itself, and how. See [Filtering with SearchSchema](#filtering-with-searchschema-hash-and-inline_filter) below. |

Example (using boto3):
```python
table = dynamodb.create_table(
    TableName='my-table',
    KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
    AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
    BillingMode='PAY_PER_REQUEST',
    VectorIndexes=[
        {
            'IndexName': 'embedding-index',
            'VectorAttribute': {'AttributeName': 'embedding'},
            'Dimensions': 1536,
            'DistanceFunction': 'COSINE',
            'Projection': {'ProjectionType': 'KEYS_ONLY'},
        }
    ],
)
```

**Constraints:**
- Multiple vector indexes can be created on the same table in a single
  `CreateTable` call (or accumulated one at a time via `UpdateTable`, see
  below).
- Two vector indexes may target the same `VectorAttribute` as long as they
  agree on `Dimensions` — this is useful for comparing different
  `DistanceFunction`s on the same data, or for having indexes with
  different `SearchSchema` or `Projection`.
- Vector indexes require ScyllaDB to operate with tablets, not vnodes.

## Filtering with SearchSchema: HASH and INLINE_FILTER

Without a `SearchSchema`, a vector search examines every item indexed by the
vector index. `SearchSchema` lets a search instead be restricted, before the
ANN algorithm even runs, to only the items matching a condition on one or
more designated attributes — this is DynamoDB's equivalent of what other
vector databases call "pre-filtering" or a "filterable/local index".

`SearchSchema` is a list of objects, each `{"AttributeName": "<name>",
"SearchSchemaElementType": "HASH"|"INLINE_FILTER"}`. Every attribute it
names must also be declared in the request's `AttributeDefinitions`, with
one of the key types `S`, `N`, or `B` — exactly like a GSI or LSI key
attribute. Any attribute may be used, including one that is already a key
attribute of the base table or one of its GSIs or LSIs.

There are two kinds of `SearchSchema` elements, and they behave quite
differently:

- **`HASH`** (at most one per index) acts like a partition key for the
  vector index itself: items are effectively bucketed by this attribute's
  value, the way a local secondary index buckets by the base table's own
  partition key. Whenever a vector index declares a `HASH` attribute, every
  `SearchVectors` call against it **must** include a condition on that
  attribute — there is no way to search "across all partitions" at once.
- **`INLINE_FILTER`** (any number per index) is a plain equality filter,
  evaluated inside the vector store before the ANN search runs. Including a
  condition on an `INLINE_FILTER` attribute in a search is optional.

Both kinds of `SearchSchema` attribute are automatically projected into the
vector index and returned by a search by default, regardless of the index's
`Projection` setting (see below) — there's no need to separately list them
in `Projection`.

**Write-time enforcement:** once a vector index's `SearchSchema` exists,
`PutItem`/`UpdateItem` reject any write that gives a `SearchSchema`
attribute a value of the wrong type (compared to what was declared in
`AttributeDefinitions`), and additionally reject an empty string specifically
for the `HASH` attribute (empty strings remain fine for `INLINE_FILTER`
attributes, since they aren't key-like). A write that simply omits the
attribute is always allowed.

**What happens to items already in the table:** adding a vector index to
an existing (non-empty) table via `UpdateTable` triggers a backfill scan
that indexes the table's current contents (see `IndexStatus`/`Backfilling`
below); this can't rely on write-time validation, since that data was
written before the `SearchSchema` existed. So instead:
  - An item that lacks the `HASH` attribute, or whose existing value has the
    wrong type or is an empty string, is simply left out of the index
    entirely — it will never be found by any search on this index. This is
    the same as what happens to an item that's missing its `HASH` value even
    after `SearchSchema` was already in place.
  - An item that lacks an `INLINE_FILTER` attribute is still indexed
    normally; it just never matches a search that filters on that
    attribute.
  - An item whose *existing* value for an `INLINE_FILTER` attribute has the
    wrong type, however, is skipped from indexing entirely — not indexed
    with that one value simply missing. This might be surprising, since it's
    treated differently than an item simply missing the same attribute. The
    reason: if the item were indexed with a placeholder "missing" value, an
    unfiltered search could return it with an attribute that, according to
    the base table, does have a value — misrepresenting the item to the
    caller. Since ANN search is already inherently approximate (it doesn't
    promise to find every matching item), it's better to leave such an item
    out entirely than to return it with an incorrect projected attribute.

## Projection

`Projection` controls which attributes (beyond the base table's own key
columns and `SearchSchema` attributes, which are always projected) a vector
index stores, and therefore which attributes a search can return:

| `ProjectionType` | Description |
|-----------------|-------------|
| `KEYS_ONLY` | Only the base table's key attributes (plus `SearchSchema` attributes) are projected. |
| `INCLUDE` | The above, plus the additional non-key attributes listed in the required `NonKeyAttributes` field. |
| `ALL` | All of the item's attributes are available. |

`ProjectionType=ALL` is currently implemented less efficiently than
`KEYS_ONLY`/`INCLUDE`: since an item can carry arbitrary attributes, there's
no fixed list to hand to the vector store ahead of time, so a search against
such an index falls back to reading each matching item from the base table
individually, rather than serving results directly out of the vector store
(the one exception is a `ProjectionExpression` naming only key columns, which
needs nothing beyond the ANN results regardless of `ProjectionType`). For
latency-sensitive applications, prefer `KEYS_ONLY` or
`INCLUDE` with a specific attribute list.

DynamoDB's Vector Search API has no way to read an attribute that isn't
projected into the index. As a ScyllaDB extension, `SearchVectors`'s
`BaseRead` parameter (described below) can still reach into the base table
for such attributes when needed.

## UpdateTable — VectorIndexUpdates parameter

An optional `VectorIndexUpdates` parameter can be passed to `UpdateTable` to
add or remove one vector index — **at most one** `Create` or `Delete`
operation per `UpdateTable` call (the same restriction DynamoDB places on
`GlobalSecondaryIndexUpdates`).

**Create** (same fields as `CreateTable`'s `VectorIndexes` entries above):
```json
{
  "Create": {
    "IndexName": "my-vector-index",
    "VectorAttribute": {"AttributeName": "embedding"},
    "Dimensions": 1536,
    "DistanceFunction": "COSINE",
    "Projection": {"ProjectionType": "KEYS_ONLY"}
  }
}
```

**Delete:**
```json
{
  "Delete": {
    "IndexName": "my-vector-index"
  }
}
```

`VectorIndexUpdates` cannot be combined with `GlobalSecondaryIndexUpdates`
in the same `UpdateTable` request.

## DescribeTable — VectorIndexes in the response

`DescribeTable` (and `CreateTable`'s own response) returns a `VectorIndexes`
list in `TableDescription` whenever the table has at least one vector index.
Each entry echoes back `IndexName`, `IndexArn`, `VectorAttribute`,
`Dimensions`, `DistanceFunction`, `Projection`, and — if the index has one —
`SearchSchema`, plus two status fields that mirror how DynamoDB reports GSI
status:

| Field | Type | Description |
|-------|------|-------------|
| `IndexStatus` | String | `"ACTIVE"` once the vector store has finished building the index and is serving it. `"CREATING"` while the index is still being built. |
| `Backfilling` | Boolean | Present and `true` only while `IndexStatus` is `"CREATING"` *and* the vector store is actively scanning the base table's existing data (as opposed to still starting up). Absent once the scan hasn't started yet, or has already finished. |

When a vector index is added to a non-empty table, this backfill scan
populates the index from the table's current contents; once it completes,
the vector store switches to consuming ongoing changes via CDC, and
`IndexStatus` becomes `"ACTIVE"`. Applications should wait for
`IndexStatus="ACTIVE"` before relying on `SearchVectors` finding all
existing data — this applies both to a `VectorIndexUpdates` `Create` on an
existing table and to a brand new table created with `VectorIndexes`
already set (even though it starts out empty, the vector store still needs
a moment to initialize).

**Vector indexes are eventually consistent.** Even after `IndexStatus`
reaches `"ACTIVE"`, ongoing writes (`PutItem`, `UpdateItem`,
`BatchWriteItem`, etc.) reach the vector index asynchronously via the same
CDC-based mechanism, not as part of the write itself. A `SearchVectors`
call immediately following a write may not yet reflect it. Applications
that need a just-written item to be immediately searchable must poll
`SearchVectors` (or otherwise wait) until it appears, the same way they'd
wait for `IndexStatus="ACTIVE"` during backfill.

## Performing a search: the SearchVectors operation

A vector search is performed with its own distinct top-level operation,
`SearchVectors` — not a variant of `Query` — with its own request and
response shape:

| Field | Type | Description |
|-------|------|-------------|
| `TableName` | String | Required. |
| `IndexName` | String | Required. Must name a vector index on this table. |
| `SearchVector` | List (`L`) or `FLOAT32VECTOR` | Required. The query vector. Its length must equal the index's `Dimensions`. |
| `TopK` | Integer | Required. How many nearest neighbors to return — between 1 and 1000. |
| `SearchConditionExpression` | String | Optional (mandatory if the index has a `HASH` `SearchSchema` attribute). A pre-filter evaluated by the vector store itself before the ANN search, using `SearchSchema` attributes — see below. |
| `FilterExpression` | String | Optional, ScyllaDB extension. A post-filter applied to the `TopK` candidates found by the ANN search — see below. |
| `ExpressionAttributeNames` / `ExpressionAttributeValues` | | As usual, substituted into `SearchConditionExpression`/`FilterExpression`/`ProjectionExpression`. |
| `ProjectionExpression` | String | Optional. Which attributes to return — see below. |
| `BaseRead` | Boolean | Optional, ScyllaDB extension, default `false`. Whether to read matching items from the base table — see below. |

`SearchConditionExpression` only supports the `=` operator, combining
multiple conditions with `AND` (no `OR`, `NOT`, or other comparators). Every
attribute it references must be a `SearchSchema` attribute (`HASH` or
`INLINE_FILTER`) of the index being searched, and only string (`S`),
number (`N`), and binary (`B`) values are supported.

Example:
```python
client = table.meta.client

# A vector index without a SearchSchema:
response = client.search_vectors(
    TableName='my-table',
    IndexName='embedding-index',
    SearchVector=[0.1, -0.3, 0.7, ...],   # or {'FLOAT32VECTOR': [...]}
    TopK=10,
)

# A vector index whose SearchSchema declares 'category' as HASH:
response = client.search_vectors(
    TableName='my-table',
    IndexName='embedding-index',
    SearchVector=[0.1, -0.3, 0.7, ...],
    TopK=10,
    SearchConditionExpression='category = :c',
    ExpressionAttributeValues={':c': 'electronics'},
)
```

`ProjectionExpression`, by default (`BaseRead=false`, see below), can only
request attributes actually
available from the index itself — the base table's key columns,
`SearchSchema` attributes, and `Projection`'s `NonKeyAttributes` — plus, as
a special case, the vector attribute itself can always be requested
explicitly even under `ProjectionType=KEYS_ONLY`, though the value returned
is only as precise as the vector store's own storage of it (see
[FLOAT32VECTOR](#the-float32vector-type-scylladb-extension) below). Naming
an attribute that isn't actually available is not an error; it's simply
left out of the result, the same as `ProjectionExpression` naming an
attribute an item doesn't have. With `BaseRead=true`, this restriction is
lifted: `ProjectionExpression` can then name any attribute of the base
table, not just ones projected into the index.

Omitting `ProjectionExpression` entirely always means the same thing - the
entire item - but `BaseRead` decides which copy of the
item that refers to: with `BaseRead=false`, the vector index's own copy,
which only ever has the projected attributes to begin with (matching
DynamoDB's own `ALL_PROJECTED_ATTRIBUTES` default for a GSI/LSI `Query` -
not a restricted view, just "the entire item" as the index actually has
it); with `BaseRead=true`, the base table's row, with every attribute it
actually has. Either way, the vector attribute is excluded from this
default; request it explicitly to get it.

**BaseRead — ScyllaDB extension.** DynamoDB's own `SearchVectors` can only
ever return the attributes projected into the index — it has no way to read
anything else from the base table. As a ScyllaDB extension, `SearchVectors`
accepts a boolean `BaseRead` parameter to lift that restriction:

| `BaseRead` value | Behavior |
|------------------|----------|
| `false` (default) | The response is built entirely out of what's already projected into the index; no base-table reads. This is the *only* mode real DynamoDB's `SearchVectors` supports. Two things currently still force a base-table read even here, though neither is a deliberate restriction - they're missing features to fix later, not part of the design: `ProjectionType=ALL`'s default response (see [Projection](#projection)), and an explicit request for the vector attribute itself (the vector store can't yet reconstruct it on demand). |
| `true` | Unconditionally reads each matching item from the base table, returning whatever `ProjectionExpression` asked for — any base table attribute, not just projected ones, and the full item if it wasn't given (see above) — and giving `FilterExpression` access to the full item. As a minor optimization, this is skipped for an explicit `ProjectionExpression` naming only key columns (regardless of `ProjectionType`, even `ALL`), with no `FilterExpression` - keys can't differ between the index and the base table, so a base-table read couldn't tell us anything new there. |

`BaseRead=true` also gives up-to-date data, unlike `BaseRead=false`: vector
indexes are only eventually consistent with the base table (see above), so
a `BaseRead=false` search can miss a very recent write, while `BaseRead=true`
reads the base table directly for every matching item, at the cost of an
extra read per item.

Interestingly, `BaseRead=false`'s fast path — serving a search entirely out
of the vector index, without touching the base table — has no equivalent in
ScyllaDB's own CQL `ANN OF` vector search: CQL always works the way
`BaseRead=true` does here, retrieving only the matching keys from the
vector search engine and then reading the rest of each item from the base
table. `BaseRead=false`, DynamoDB's only supported mode, is thus an
Alternator-only optimization not available to CQL users of the same index.

Unlike `Query`, `SearchVectors` does not accept a `Select` parameter at
all — `BaseRead` together with `ProjectionExpression` already covers the
same ground (including `Query`'s `SPECIFIC_ATTRIBUTES` and `ALL_ATTRIBUTES`
behaviors). Nor does it accept the legacy `AttributesToGet`
(`ProjectionExpression`'s predecessor) - use `ProjectionExpression` instead.
There's also no `SearchVectors` equivalent of `Select=COUNT`: the
`SearchResults` response shape has no field for a bare count.

**FilterExpression — ScyllaDB extension.** Also as a ScyllaDB extension, a
`FilterExpression` can be applied to the `TopK` candidates found by the ANN
search, discarding any that don't match. This is *post*-filtering: it
happens only after the candidates have already been chosen — here, by the
ANN search, rather than by a key condition or table scan as in `Query` or
`Scan` — so a discarded item is never replaced by another candidate. This
is the same relationship `FilterExpression` has to `Limit` in a standard
`Query` or `Scan`, and because of it, the response can end up with fewer
than `TopK` items.

`FilterExpression` uses the exact same expression syntax, with the same
capabilities, as `Query`'s and `Scan`'s `FilterExpression` — comparison
operators (`=`, `<>`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IN`), functions
(`attribute_exists`, `attribute_not_exists`, `attribute_type`,
`begins_with`, `contains`, `size`), and conditions combined with
`AND`/`OR`/`NOT`. This is far more general than `SearchConditionExpression`
(see above), which only supports the `=` operator, `AND`-combined
conditions, and only on `SearchSchema` attributes — `FilterExpression`, in
contrast, can reference *any* attribute of the item, of any type (including
one that varies from item to item, since Alternator attributes are
otherwise untyped): a condition like `x = :want` simply treats an item
whose `x` has a different type than `:want` (or is missing `x` entirely)
as not matching, rather than an error.

The attributes available to `FilterExpression` are exactly the ones
available to `ProjectionExpression` (see `BaseRead` above) — not the
possibly-smaller subset that a given `ProjectionExpression` actually asks
to have *returned*: with `BaseRead=false`, only what's projected into the
index is available, regardless of what `ProjectionExpression` requests — an
attribute outside that set is silently treated as missing, the same as for
an item that's genuinely missing it, and referencing it never by itself
triggers a base-table read; with `BaseRead=true`, the full base-table item
is available, again regardless of what `ProjectionExpression` requests.
`ProjectionExpression` only trims the final response, after filtering has
already happened.

## Similarity scores

A `SearchVectors` response has the shape:
```json
{
  "SearchResults": [
    {"Item": {"id": {"S": "item1"}}, "Score": 0.0123},
    {"Item": {"id": {"S": "item2"}}, "Score": 0.4567}
  ]
}
```
(with up to `TopK` entries in `SearchResults`, each carrying its own `Item`
and `Score`)

`SearchResults` is already ordered — nearest match first — and every entry
carries a `Score` alongside its `Item`. What `Score` means, and whether a
*lower* or *higher* value is a better match, depends on the index's
`DistanceFunction`:

| `DistanceFunction` | What `Score` is | Range | Better match is... |
|---------------------|------------------|-------|---------------------|
| `COSINE` | Cosine distance: `1 - cosine_similarity` | `[0, 2]` | **lower** (`0` = identical direction, `2` = opposite direction) |
| `EUCLIDEAN` | Raw Euclidean (L2) distance | `[0, ∞)` | **lower** (`0` = identical vectors) |
| `DOT_PRODUCT` | Raw dot product | unbounded | **higher** (this one is a genuine similarity, not a distance — the other two are distances) |

Because `COSINE` and `EUCLIDEAN` scores are distances while `DOT_PRODUCT`'s
is a similarity, take care when comparing scores across indexes using
different `DistanceFunction`s, or when deciding a "good enough" threshold:
the direction of "better" flips for `DOT_PRODUCT`.

## The FLOAT32VECTOR type (ScyllaDB extension)

Amazon DynamoDB has no dedicated vector type: a vector is just an ordinary
list (`L`) of numbers (`N`), each of which DynamoDB stores and transmits as
a self-describing, variable-length decimal string. That's a natural fit for
DynamoDB's general-purpose number type, but it's needlessly wasteful for
what a vector actually is: a fixed-length array of IEEE-754 floats.

As a ScyllaDB-only extension, Alternator additionally accepts the vector
attribute — both when writing an item and as the `SearchVector` in a
search — using the `FLOAT32VECTOR` type: a JSON array of plain floating
point numbers (not quoted strings), stored internally as packed 4-byte
big-endian floats rather than as a list of individually-tagged, decimal-
string-encoded numbers:

```json
// Standard DynamoDB list-of-numbers format:
{"embedding": {"L": [{"N": "0.1"}, {"N": "-0.3"}, {"N": "0.7"}]}}

// FLOAT32VECTOR format (ScyllaDB extension):
{"embedding": {"FLOAT32VECTOR": [0.1, -0.3, 0.7]}}
```

For the high-dimensional vectors typical of ML embeddings — often in the
hundreds or thousands of dimensions — this adds up: each `L`/`N` element
carries per-value framing and decimal-text overhead on top of the number
itself, while `FLOAT32VECTOR` is exactly 4 bytes per dimension, both on the
wire and in storage, with no JSON-number parsing needed to reconstruct the
floats. Since a vector attribute's precision is limited to what a 32-bit
float can represent anyway (the vector store itself only ever deals in
`float`, regardless of which format was used to write it), storing it as
`L`/`N` doesn't buy any extra precision — only extra bytes and CPU time.
Both formats are accepted anywhere a vector value is expected (writing an
item, or as `SearchVector` in a search), and can be freely mixed between
different items or requests.

## Constraints and limits

| Limit | Value |
|-------|-------|
| `Dimensions` | 1 to 16000 |
| `TopK` | 1 to 1000 |
| `IndexName` length | 3 to 192 characters |
| `SearchSchema` `AttributeName` length | up to 255 characters (same as any key attribute) |
| `VectorAttribute`/`NonKeyAttributes` attribute name length | up to 65535 characters (same as any non-key attribute) |
| `VectorIndexUpdates` per `UpdateTable` call | exactly one `Create` or `Delete` |
| `SearchConditionExpression` | `=` only, `AND`-combined only, `S`/`N`/`B` values only |
| Base table replication | must use tablets, not vnodes |

## Metrics

ScyllaDB exposes the following metrics (under the `alternator` group, and
per-table under `alternator_table`) for monitoring vector search activity,
alongside the standard `SearchVectors` label on the general
`scylla_alternator_operation`/`scylla_alternator_operation_latency`
metrics:

| Metric | Description |
|--------|-------------|
| `vector_search_returned_items` | Total number of items actually returned in `SearchResults`, across all `SearchVectors` calls. |
| `vector_search_items_from_vs` | Total number of nearest-neighbor candidates returned by the vector store itself. Can exceed `vector_search_returned_items` when a `FilterExpression` (a ScyllaDB extension) discards some of them. |
| `vector_search_items_from_base_table` | Total number of items read from the base table to serve `SearchVectors` requests. `BaseRead=false` requests never need this - that's the fast path real DynamoDB always uses - with two known-gap exceptions: `ProjectionType=ALL`'s default response (see [Projection](#projection)) and an explicit request naming the vector attribute itself. `BaseRead=true` (a ScyllaDB extension) forces it, at a real latency cost - see `BaseRead` below for the one minor case it doesn't. |
