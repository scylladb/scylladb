# Vector Search Architecture

This document describes the component-level architecture behind CQL `ORDER BY <vector_column> ANN OF <query_vector>` queries. It covers the read path, write path, CDC-based index maintenance, index initialization and full scan, authentication and authorization, workload isolation, consistency semantics, and shard awareness.

**Ownership boundary.** ScyllaDB owns CQL processing, base-table reads and writes, CDC log generation, and read/write consistency. The Vector Store is an external service that owns approximate-nearest-neighbor indexing and search, and connects to ScyllaDB as a regular CQL client.

**Scope.** Vector search for Alternator is out of scope for this document; the Alternator read path, write path, and index creation work differently.

**Terminology.** This document uses "vectors" and "embeddings" interchangeably.

---

## Components

| Component | Role |
|-----------|------|
| **CQL client and coordinator** | The client sends CQL over the native protocol. The coordinator parses, prepares, and executes the statement. |
| **Query processor** | Detects `ANN OF`, validates the vector column, builds filters, calls the Vector Store client, fetches authoritative rows, and optionally rescores results. |
| **Vector index metadata** | The custom index (`USING 'vector_index'`) stores only vector index metadata in ScyllaDB. No materialized view is created. |
| **Vector Store client** | A per-shard HTTP/HTTPS client configured via `vector_store_primary_uri` and `vector_store_secondary_uri`. Load balancing uses uniform-random selection across DNS-resolved endpoints. Each request retries up to 3 rounds; within a round the client tries every resolved endpoint in random order, moving to the next endpoint immediately on failure (no timeout between endpoints). DNS is refreshed between rounds. Falls back to the secondary URI when all primary rounds fail. |
| **Storage proxy and replicas** | Executes reads and writes against the base table and CDC log tables, enforcing the requested consistency level. |
| **CDC service** | Provides the CDC feed that the Vector Store consumes for vector-indexed tables. |
| **External Vector Store indexing pipeline** | Lives outside ScyllaDB. Discovers vector indexes, performs the initial full-table scan while CDC readers capture concurrent changes, and then continues consuming CDC to keep the index up to date. |

---

## Read Path (SELECT … ANN OF)

Example query:

```sql
SELECT pk, payload
FROM ks.tbl
WHERE category = 'x'
ORDER BY embedding ANN OF [0.1, 0.2, 0.3]
LIMIT 10;
```

### Step-by-step

1. **Parse and prepare.** The CQL grammar accepts `ANN OF` only as a single `ORDER BY` item. Descending order and combined orderings are rejected. ScyllaDB validates that the ordered column is `vector<float, N>` with an existing `vector_index`. Requirements: `LIMIT` is mandatory; `PER PARTITION LIMIT` and aggregation are forbidden; `LIMIT` above 1000 is rejected. The mandatory limit exists because HNSW graph traversal cannot be meaningfully paged — the search must complete in a single pass to produce correctly ranked results — and in practice vector similarity search use cases rarely need more than a 1000 top-k result set.

2. **Build the filter.** `WHERE` restrictions are serialized to Vector Store filter JSON. Single-column and multi-column restrictions become structured filter objects. The `ALLOW FILTERING` flag is passed through. The Vector Store interprets the filter against its own index metadata. This is pre-filtering: the Vector Store applies the filter during ANN traversal so that only matching rows count toward the LIMIT. The search scope depends on the index type:
   - **Global index:** searches across all vectors in the table. Requires `ALLOW FILTERING` because the query is not partition-scoped.
   - **Local index:** searches only within a single partition (the query must include a partition-key equality restriction). Faster because the search space is smaller, and does not require `ALLOW FILTERING`.

3. **Call the Vector Store client.** The coordinator evaluates the query vector, computes `fetch_count = ceil(LIMIT × oversampling)`, and asks the local Vector Store client to send:
   ```
   POST /api/v1/indexes/{keyspace}/{index}/ann
   {"vector": [...], "limit": fetch_count, "filter": {...}}
   ```
   ANN requests for a non-serving index return `503 Service Unavailable` with scan progress when available. Only `SERVING` indexes are eligible for ANN routing.
   If rescoring is disabled, the response is trimmed to the CQL limit immediately. If rescoring is enabled, all oversampled candidates proceed to the base-table fetch (see step 5).

4. **Receive candidate keys.** The Vector Store responds with `primary_keys` and `similarity_scores`. ScyllaDB deserializes the keys into decorated partition keys plus clustering keys. The Vector Store's similarity scores are not exposed to the client. When rescoring is disabled, ScyllaDB preserves the Vector Store's ordering. When rescoring is enabled, ScyllaDB recomputes exact similarity from base-table vectors and reorders based on its own scores.

5. **Fetch from the base table.** The coordinator reads candidate rows at the user's read CL.
   - *Without clustering columns:* a single multi-range read over partition ranges.
   - *With clustering columns:* parallel per-key reads (partition range + clustering range), merged before returning results.

6. **Exclude stale candidates.** Because the Vector Store is eventually consistent, a returned key may reference a deleted or not-yet-visible row. The base-table read is authoritative: rows not found are simply absent from the result.

7. **Rescore (optional).** Rescoring is enabled when `quantization != f32` **and** `rescoring = "true"`. During prepare, ScyllaDB injects a hidden similarity selector (`cosine`, `euclidean`, or `dot_product`). After fetching oversampled rows, it computes exact similarity from the stored full-precision vectors, sorts by score, trims to the CQL limit, and hides the score column from the client.

8. **Return results.** Paging is unsupported. If the requested page size is smaller than the limit, the coordinator returns the full result set and emits a warning.

### Communication summary

- Client → coordinator: CQL native protocol.
- Coordinator → Vector Store client: local sharded client.
- Vector Store client → Vector Store: HTTP/HTTPS.
- Coordinator → replicas: internal storage-proxy RPCs.

---

## Write Path (INSERT / UPDATE / DELETE)

The write path is deliberately **decoupled** from Vector Store updates.

1. **Client writes to ScyllaDB.** Mutations arrive through the normal CQL write path at the user's write CL.

2. **Automatic CDC enablement.** A table is CDC-enabled if CDC is explicitly enabled **or** if the table has a vector index. The CDC log table is created automatically for vector-indexed tables.

3. **Vector index validates CDC requirements.** Preconditions:
   - Tablet-based keyspace.
   - Target column is `vector<float, N>`.
   - Filtering columns use supported types.
   - CDC is not explicitly disabled.
   - If CDC options are set: TTL >= 24 hours, and either full delta mode or postimage enabled.

4. **Standard CDC records the change.** Apart from vector-index validation and automatic CDC use, vector search relies on standard ScyllaDB CDC behavior to record base-table changes; it does not add a synchronous write-side index update.

5. **External indexing pipeline advances the Vector Store.** The pipeline consumes CDC streams, extracts primary keys, vectors, filtering columns, and operation types, and applies upserts/deletes. Initial index creation on a non-empty table requires a full-table scan before the index can serve queries.

**Implication:** A successful CQL write confirms the ScyllaDB-side write, including standard CDC log work, at the requested CL. It does **not** guarantee the Vector Store has applied the change.

---

## Index Initialization and Full Scan

The initial scan is executed by the external Vector Store process, not by ScyllaDB's CQL coordinator. ScyllaDB provides the base table and CDC log data through regular CQL access; the Vector Store builds its serving ANN index from them.

### Discovery and initialization

1. **Schema monitoring.** The Vector Store monitors ScyllaDB schema changes, discovers custom `vector_index` entries, reads index version/options/dimensions, and validates that the CDC log table exists. The vector index schema format (target encoding, `index_version`, duplicate detection rules) is documented in [docs/dev/vector_index.md](vector_index.md).

2. **Index actor creation.** The index actor applies vector additions and removals and serves ANN searches by mapping internal vector IDs back to ScyllaDB primary keys and similarity scores.

3. **CDC readers start before the full scan.** The Vector Store creates CDC readers before launching the full scan. Full-scan rows and CDC rows flow through the same internal channel, so writes that arrive during the scan follow the same path as scanned rows.

### Full-scan query shape

Before scanning, the Vector Store waits for an active ScyllaDB session and schema agreement, then prepares a range-scan query over the base table.

The full-scan query shape is:

```sql
SELECT <primary-key-columns>, <vector-column>, writetime(<vector-column>)
FROM <keyspace>.<table>
WHERE token(<partition-key-columns>) >= ?
   AND token(<partition-key-columns>) <= ?
BYPASS CACHE;
```

### Token-range execution

The Vector Store takes the token ring from the ScyllaDB Rust driver and builds inclusive token ranges from adjacent ring tokens, adding the minimum and maximum token boundaries to cover the full ring. It scans these ranges concurrently.

Concurrency is based on the total shard count reported by the driver, falling back to 1 if shard information is unavailable: `parallel_queries = total_cluster_shards × 3`. Each token range is streamed with the prepared query. If a page fetch fails because the connection is lost, the range scan is retried with exponential backoff starting at 100 ms and capped at 16 seconds. Non-retryable errors are logged and the affected range is skipped.

Each successfully parsed full-scan row becomes an embedding event containing the primary key, vector value, and vector-column write timestamp. Rows with missing or unparsable vectors are skipped. The timestamp enables deduplication against CDC rows that arrive for the same key during the scan.

### Applying scanned rows

The indexing pipeline consumes embeddings from both the full scan and CDC and applies them to the table cache and ANN index:

- older events are ignored when a newer timestamp is already known for the row;
- vector updates remove the old internal vector ID before adding the replacement;
- vector deletes remove the existing vector and, for local indexes, can remove an empty partition;
- full-scan progress advances by token-range coverage, but only after all embeddings from that range have been applied to the index pipeline.

---

## Authentication, Authorization, and Workload Isolation

These controls apply to the Vector Store's regular CQL connection back to ScyllaDB. They are separate from the coordinator-to-Vector-Store HTTP/HTTPS call used for ANN lookup.

The Vector Store can connect without authentication, or authenticate using configured username/password credentials with the password read from a file. For TLS, it validates the ScyllaDB server certificate against a configured CA certificate file (PEM). TLS and username/password authentication are independent and can be combined.

To support least-privilege authorization, ScyllaDB supports the `VECTOR_SEARCH_INDEXING` permission as an alternative to the `SELECT` permission for vector indexing reads. It is grantable only on `ALL KEYSPACES`. A role with this permission can read vector-indexed base tables, their CDC log tables, and the system tables needed by Vector Store; it cannot read unrelated non-vector tables unless it also has the `SELECT` permission.

Workload isolation uses standard ScyllaDB service levels. Because the Vector Store is a normal CQL client, full scans and CDC reads run under the authenticated role's effective service level. Operators can attach a dedicated service level to the Vector Store role to control the scheduling share of its ScyllaDB-side CQL work; this does not limit CPU or memory used inside the external Vector Store process.

---

## Consistency Model

### Two domains

| Domain | Guarantee |
|--------|-----------|
| **ScyllaDB base data and CDC logs** | Standard consistency levels. Writes use the user's write CL; base-table reads after candidate lookup use the user's read CL. |
| **Vector Store index** | Eventually consistent. Updated asynchronously via CDC. May temporarily miss new rows or return stale keys. |

ScyllaDB can **remove** stale candidates (base-table read returns nothing) but cannot **discover** missing candidates not yet indexed.

Vector search has no separate consistency option.

### Practical meaning

A `SELECT … ANN OF … LIMIT 10` at `QUORUM` means: candidates come from the Vector Store's current view, and base-table rows for those candidates are read at `QUORUM`. It is **not** a linearizable nearest-neighbor query over the latest acknowledged writes.

---

## Shard and Tablet Awareness

### Read path

The ANN lookup is global: the coordinator calls its local shard's Vector Store client regardless of token ownership. Once candidate keys are known, normal token/tablet routing resumes — decorated partition keys identify tokens, the replication map selects replicas, and requests reach the correct shards.

### Write path

Writes are token- and tablet-aware from the start. The mutation token selects natural and pending replicas, local writes run on the owning shard, and tablet migration fences protect writes during topology changes.

### CDC stream consumption

Because vector indexes require tablets, the external Vector Store consumes the standard tablet-aware CDC stream for the indexed table.

---

## Configuration

| Setting | Location | Purpose |
|---------|----------|---------|
| `vector_store_primary_uri` | scylla.yaml | Primary Vector Store endpoint(s) |
| `vector_store_secondary_uri` | scylla.yaml | Failover Vector Store endpoint(s) |
| `vector_store_unreachable_node_detection_time_in_ms` | scylla.yaml | Timeout for detecting an unreachable Vector Store node |
| `vector_store_encryption_options` | scylla.yaml | TLS options for HTTPS Vector Store endpoints |
| `similarity_function` | `CREATE INDEX` options | `cosine`, `euclidean`, or `dot_product` |
| `quantization` | `CREATE INDEX` options | `f32`, `f16`, `bf16`, `i8`, `b1` |
| `oversampling` | `CREATE INDEX` options | 1.0–100.0 candidate multiplier |
| `rescoring` | `CREATE INDEX` options | `"true"` / `"false"` |
| `maximum_node_connections` | `CREATE INDEX` options | HNSW graph connectivity (1–512) |
| `construction_beam_width` | `CREATE INDEX` options | HNSW construction width (1–4096) |
| `search_beam_width` | `CREATE INDEX` options | HNSW search width (1–4096) |

