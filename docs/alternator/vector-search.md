# Alternator Vector Search

```{admonition} Availability
:class: important

The Vector Search feature is only available in [ScyllaDB Cloud](https://cloud.docs.scylladb.com/) - a fully managed DBaaS running ScyllaDB.
```

## Introduction

Alternator vector search is a ScyllaDB extension to the DynamoDB-compatible
API that enables _approximate nearest neighbor_ (ANN) search on numeric
vectors stored as item attributes.

In a typical use case, each item in a table contains a high-dimensional
embedding vector (e.g., produced by a machine-learning model), and a query
asks for the _k_ items whose stored vectors are closest to a given query
vector. This kind of similarity search is a building block for
recommendation engines, semantic text search, image retrieval, and other
AI/ML workloads.

Because this feature does not exist in Amazon DynamoDB, all applications
that use it must be written specifically for Alternator.

For a broader introduction to Vector Search concepts and terminology, see the
[Vector Search Concepts](https://cloud.docs.scylladb.com/stable/vector-search/vector-search-concepts.html)
and
[Vector Search Glossary](https://cloud.docs.scylladb.com/stable/vector-search/vector-search-glossary.html)
sections of the ScyllaDB Cloud documentation.

## Overview

The workflow has three steps:

1. **Create** a table (or update an existing one) with one or more
   _vector indexes_.
2. **Write** items that include the indexed vector attribute, just like any
   other list attribute.
3. **Query** using the `VectorSearch` parameter to retrieve the _k_ nearest
   neighbors.

## API extensions

### CreateTable — VectorIndexes parameter

A new optional parameter `VectorIndexes` can be passed to `CreateTable`.
It is a list of vector index definitions, each specifying:

| Field | Type | Description |
|-------|------|-------------|
| `IndexName` | String | Unique name for this vector index. Follows the same naming rules as table names: 3–192 characters, matching the regex `[a-zA-Z0-9._-]+`. |
| `VectorAttribute` | Structure | Describes the attribute to index (see below). |
| `Projection` | Structure | Optional. Specifies which attributes are projected into the vector index (see below). |

**VectorAttribute fields:**

| Field | Type | Description |
|-------|------|-------------|
| `AttributeName` | String | The item attribute that holds the vector. It must not be a key column. |
| `Dimensions` | Integer | The fixed size of the vector (number of elements). |

**Projection fields:**

The optional `Projection` parameter is identical to the one used for DynamoDB
GSI and LSI, and specifies which attributes are stored in the vector index and
returned when `Select=ALL_PROJECTED_ATTRIBUTES` is used in a vector search query:

| `ProjectionType` | Description |
|-----------------|-------------|
| `KEYS_ONLY` | Only the primary key attributes of the base table (hash key and range key if present) are projected into the index. This is the default when `Projection` is omitted. |
| `ALL` | All table attributes are projected into the index. *(Not yet supported.)* |
| `INCLUDE` | The primary key attributes plus the additional non-key attributes listed in `NonKeyAttributes` are projected. *(Not yet supported.)* |

> **Note:** Currently only `ProjectionType=KEYS_ONLY` is implemented. Specifying
> `ProjectionType=ALL` or `ProjectionType=INCLUDE` returns a `ValidationException`.
> Since `KEYS_ONLY` is also the default, omitting `Projection` entirely is
> equivalent to specifying `{'ProjectionType': 'KEYS_ONLY'}`.

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
            'VectorAttribute': {'AttributeName': 'embedding', 'Dimensions': 1536},
        }
    ],
)
```

**Constraints:**
- A vector index may not share a name with another vector index, a GSI, or an LSI on the same table.
- The target attribute must not be a key column or an index key column.
- `Dimensions` must be a positive integer up to the implementation maximum.
- Vector indexes require ScyllaDB to operate with tablets (not vnodes).
- Multiple vector indexes can be created on the same table in a single `CreateTable` call.

---

### UpdateTable — VectorIndexUpdates parameter

A new optional parameter `VectorIndexUpdates` can be passed to `UpdateTable`
to add or remove a vector index after the table is created. At most one
index operation (Create or Delete) may be requested per call.

Each element of the list is an object with exactly one of the following keys:

**Create:**
```json
{
  "Create": {
    "IndexName": "my-vector-index",
    "VectorAttribute": {"AttributeName": "embedding", "Dimensions": 1536},
    "Projection": {"ProjectionType": "KEYS_ONLY"}
  }
}
```

The `Projection` field in the `Create` action is optional and accepts the same
values as the `Projection` field in `CreateTable`'s `VectorIndexes` (see above).
Currently only `ProjectionType=KEYS_ONLY` is supported; it is also the default
when `Projection` is omitted.

**Delete:**
```json
{
  "Delete": {
    "IndexName": "my-vector-index"
  }
}
```

The same constraints as `CreateTable`'s `VectorIndexes` apply.

---

### DescribeTable — VectorIndexes in the response

`DescribeTable` (and `CreateTable`'s response) returns a `VectorIndexes`
field in the `TableDescription` object when the table has at least one
vector index. The structure mirrors the `CreateTable` input: a list of
objects each containing `IndexName`, `VectorAttribute`
(`AttributeName` + `Dimensions`), and `Projection` (`ProjectionType`).
Currently `Projection` always contains `{"ProjectionType": "KEYS_ONLY"}`
because that is the only supported projection type.

Each vector index entry also includes status fields that mirror the standard
behavior of `GlobalSecondaryIndexes` in DynamoDB:

| Field | Type | Description |
|-------|------|-------------|
| `IndexStatus` | String | `"ACTIVE"` when the vector store has finished building the index and it is fully operational. `"CREATING"` while the index is still being built (the vector store service is still initializing or has not yet been discovered). |
| `Backfilling` | Boolean | Present and `true` only when `IndexStatus` is `"CREATING"` and the vector store is actively performing the initial scan of the base table (bootstrapping). When the initial scan is not yet started, or has already completed, this field is absent. |

When creating a vector index on a non-empty table (via `UpdateTable`), data
already in the table is picked up by the vector store through a full-table
scan (backfill). During this period `IndexStatus` will be `"CREATING"` and
`Backfilling` will be `true`. Once the scan completes the vector store
transitions to monitoring CDC for ongoing changes, and `IndexStatus` becomes
`"ACTIVE"`.

**Type enforcement before and after index creation differs:**

- **Pre-existing items:** when the backfill scan encounters an item whose
  indexed attribute is not a list of exactly the right number of numbers
  (e.g., it is a string, a list of the wrong length, or contains
  non-numeric elements), that item is silently skipped and not indexed.
  The item remains in the base table unchanged.

- **New writes once the index exists:** any attempt to write a value to the
  indexed attribute that is not a list of exactly `Dimensions` numbers —
  where each number is representable as a 32-bit float — is rejected with a
  `ValidationException`. This applies to `PutItem`, `UpdateItem`, and
  `BatchWriteItem`. A missing value for the indexed attribute is always
  allowed; such items simply are not indexed.

> **Important:** Applications must wait until `IndexStatus` is `"ACTIVE"` before
> issuing `Query` requests against a vector index. Queries on a vector index
> whose `IndexStatus` is still `"CREATING"` may fail. This applies both when
> adding a vector index to an existing table via `UpdateTable` **and** when
> creating a new table with a `VectorIndexes` parameter in `CreateTable` — even
> though the new table starts empty, the vector store still needs a short
> initialization period before it can serve queries.

---

### Query — VectorSearch parameter

To perform a nearest-neighbor search, pass the `VectorSearch` parameter
to `Query`. When this parameter is present the request is interpreted as a
vector search rather than a standard key-condition query.

**VectorSearch fields:**

| Field | Type | Description |
|-------|------|-------------|
| `QueryVector` | AttributeValue (list `L`) | The query vector as a DynamoDB `AttributeValue` of type `L`. Every element must be of type `N` (number). |

Example:
```python
response = table.query(
    IndexName='embedding-index',
    Limit=10,
    VectorSearch={
        'QueryVector': {'L': [{'N': '0.1'}, {'N': '-0.3'}, {'N': '0.7'}, ...]},
    },
)
```

**Requirements:**

| Parameter | Details |
|-----------|---------|
| `IndexName` | Required. Must name a vector index on this table (not a GSI or LSI). |
| `VectorSearch.QueryVector` | Required. A DynamoDB `AttributeValue` of type `L`; all elements must be of type `N` (number). |
| QueryVector length | Must match the `Dimensions` configured for the named vector index. |
| `Limit` | Required. Defines _k_ — how many nearest neighbors to return. Must be a positive integer. |

**Differences from standard Query:**

Vector search reinterprets several standard `Query` parameters in a fundamentally
different way, and explicitly rejects others that have no meaningful interpretation:

- **`Limit` means top-k, not page size.** In a standard Query, `Limit` caps
  the number of items examined per page, and you page through results with
  `ExclusiveStartKey`. In vector search, `Limit` defines _k_: the ANN
  algorithm runs once and returns exactly the _k_ nearest neighbors. There
  is no natural "next page" — each page would require a full re-run of the
  search — so **`ExclusiveStartKey` is rejected**.

- **Results are ordered by vector distance, not by sort key.** A standard
  Query returns rows in sort-key order; `ScanIndexForward=false` reverses
  that order. Vector search always returns results ordered by their distance
  to `QueryVector` (nearest first). Having `ScanIndexForward` specify sort-key
  direction has no meaning here, so **`ScanIndexForward` is rejected**.

- **Eventual consistency only.** The vector store is an external service fed
  asynchronously from ScyllaDB via CDC. Like GSIs, vector indexes can never
  reflect writes instantly, so strongly-consistent reads are impossible.
  **`ConsistentRead=true` is rejected.**

- **No key condition.** A standard Query requires a `KeyConditionExpression`
  to select which partition to read. Vector search queries the vector store
  globally across all partitions of the table. `KeyConditions` and
  `KeyConditionExpression` are therefore not applicable and are silently
  ignored. (Local vector indexes, which would scope the search to a single
  partition and use `KeyConditionExpression`, are not yet supported.)

**Select parameter:**

The standard DynamoDB `Select` parameter is supported for vector search queries
and controls which attributes are returned for each matching item:

| `Select` value | Behavior |
|----------------|----------|
| `ALL_PROJECTED_ATTRIBUTES` (default) | Return only the attributes projected to the vector index. Currently, only the primary key attributes (hash key, and range key if present) are projected; support for configuring additional projected attributes is not yet implemented. Note that the vector attribute itself is **not** included: the vector store may not retain the original floating-point values (e.g., it may quantize them), so the authoritative copy lives only in the base table. This is the most efficient option because Scylla can return the results directly from the vector store without an additional fetch from the base table. |
| `ALL_ATTRIBUTES` | Return all attributes of each matching item, fetched from the base table. |
| `SPECIFIC_ATTRIBUTES` | Return only the attributes named in `ProjectionExpression` or `AttributesToGet`. |
| `COUNT` | Return only the count of matching items; no `Items` list is included in the response. |

When neither `Select` nor `ProjectionExpression`/`AttributesToGet` is specified,
`Select` defaults to `ALL_PROJECTED_ATTRIBUTES`.  When `ProjectionExpression` or
`AttributesToGet` is present without an explicit `Select`, it implies
`SPECIFIC_ATTRIBUTES`.  Using `ProjectionExpression` or `AttributesToGet`
together with an explicit `Select` other than `SPECIFIC_ATTRIBUTES` is an error.

**Note on performance:** Unlike a DynamoDB LSI, a vector index allows you to
read non-projected attributes (e.g., with `ALL_ATTRIBUTES` or
`SPECIFIC_ATTRIBUTES` requesting a non-key column).  However, doing so requires
an additional read from the base table for each result — similar to reading
through a secondary index (rather than a materialized view) in CQL — and is
therefore significantly slower than returning only projected attributes with
`ALL_PROJECTED_ATTRIBUTES`.  For latency-sensitive applications, prefer
`ALL_PROJECTED_ATTRIBUTES` or limiting `SPECIFIC_ATTRIBUTES` to key columns.

**FilterExpression:**

Vector search supports `FilterExpression` for post-filtering results. This
works the same way as `FilterExpression` on a standard DynamoDB `Query`: after
the ANN search, the filter is applied to each candidate item and only matching
items are returned.

**Important:** filtering happens _after_ the `Limit` nearest neighbors have
already been selected by the vector index. If the filter discards some of
those candidates, the response may contain **fewer than `Limit` items**. The
server does not automatically fetch additional neighbors to replace filtered-out
items. This is identical to how `FilterExpression` interacts with `Limit` in a
standard DynamoDB `Query`.

The response always includes two count fields:

| Field | Description |
|-------|-------------|
| `ScannedCount` | The number of candidates returned by the vector index (always equal to `Limit`, unless the table contains fewer than `Limit` items). |
| `Count` | The number of items that passed the `FilterExpression` (or equal to `ScannedCount` when no filter is present). |

**Interaction with `Select`:**

- `Select=ALL_ATTRIBUTES`: Each candidate item is fetched from the
  base table, the filter is evaluated against all its attributes, and only
  matching items are returned. `Count` reflects the number of items that passed
  the filter.

- `Select=SPECIFIC_ATTRIBUTES`: Each candidate item is fetched from the base
  table — including any attributes needed by the filter expression, even if
  those attributes are not listed in `ProjectionExpression` — and the filter is
  applied. Only the projected attributes are returned in the response; filter
  attributes that were not requested are not included in the returned items.

- `Select=COUNT`: The candidate items are still fetched from the base table and
  the filter is evaluated for each one, but no `Items` list is returned. `Count`
  reflects the number of items that passed the filter; `ScannedCount` is the
  total number of candidates examined. This is useful for counting matches
  without transferring item data to the client.

- `Select=ALL_PROJECTED_ATTRIBUTES` (default): When no filter is present this is the most
  efficient mode — results are returned directly from the vector store without
  any base-table reads. When a `FilterExpression` is present, however, the full
  item must be fetched from the base table to evaluate the filter, and only the
  projected (key) attributes are returned for items that pass.

> **Note:** `QueryFilter` (the legacy non-expression filter API) is **not**
> supported for vector search queries and will be rejected with a
> `ValidationException`. Use `FilterExpression` instead.
