# Vector index in Scylla

Vector indexes are custom indexes (USING 'vector\_index'). Their `target` option in `system_schema.indexes` uses following format:

- Simple single-column vector index `(v)`: just the (escaped) column name, e.g. `v`
- Vector index with filtering columns `(v, f1, f2)`: JSON with `tc` (target column) and `fc` (filtering columns): `{"tc":"v","fc":["f1","f2"]}`
- Local vector index `((p1, p2), v)`: JSON with `tc` and `pk` (partition key columns): `{"tc":"v","pk":["p1","p2"]}`
- Local vector index with filtering columns `((p1, p2), v, f1, f2)`: JSON with `tc`, `pk`, and `fc`: `{"tc":"v","pk":["p1","p2"],"fc":["f1","f2"]}`

The `target` option acts as the interface for the vector-store service, providing the metadata necessary to determine which columns are indexed and how they are structured.

## Metadata semantics

Vector indexes also store an `index_version` option in `system_schema.indexes`.
It is an auto-generated timeuuid created by `CREATE INDEX`, and it identifies a specific index creation time.

In particular:

- dropping and recreating the same vector index definition generates a new `index_version`;
- altering the base table does not change an existing vector index's `index_version`;
- when multiple named vector indexes exist on the same target column, the Vector Store routes queries to the most recent, serving one (with the highest `index_version`).

## Duplicate detection

Scylla allows multiple **named** vector indexes on the same target column.
This is useful for zero-downtime recreation: create a replacement index, let it build, and then drop the older one.

Unnamed duplicate vector indexes are still rejected. For identity detection, `index_version` is ignored,
because it changes on every `CREATE INDEX` even when the logical index definition stays the same.
