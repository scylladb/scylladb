# Tablets routing v2 for batches

This document extends [tablets routing v2](tablets-routing-v2.md) to batches. It uses the terminology and the definitions of that document.

`BATCH` requests are covered for all tables. Prepared batches are covered only for strongly consistent tables: eventually consistent prepared batches never return routing information (cf. section [“Future work: eventually consistent prepared batches”](#future-work-eventually-consistent-prepared-batches)).

## Summary

* A prepared `BEGIN BATCH … APPLY BATCH` is sent as an `EXECUTE` request, which already carries a `tablet_version_block`. The format of the request doesn’t change. If the batch is strongly consistent, the server checks the block like for any other prepared statement.
* A `BATCH` request carries a routing hint at the end of the frame: the index of the statement the driver routed the batch by, followed by a `tablet_version_block` for the tablet of that statement. Upon a mismatch, the server returns routing information for that tablet, whatever the consistency of the batch.
* The name of the protocol extension changes so that the server and the driver never disagree about the layout of `BATCH` requests.

The response is the same as in the base document: routing information is carried by the `tablets-routing-v2` custom payload in successful responses only.

## Background

### How a batch reaches the server

To send a meaningful `tablet_version_block` and to use the routing information that comes back, the driver must know:

1. **The target table.** The `tablets-routing-v2` payload doesn’t carry the name of the table. The driver files it under the table it believes it targeted.
2. **The target token.** The driver computes it from the partition key, so it must know which bound values form that key.

A prepared statement provides both through its `PREPARED` metadata: the table spec and the partition key bind indices.

| Request | Example | Does the driver know the token? |
|---------|---------|---------------------------------|
| `QUERY` | `session.execute("BEGIN BATCH ... APPLY BATCH")` | No. |
| `EXECUTE` | `session.execute(session.prepare("BEGIN BATCH ... APPLY BATCH"), values)` | Yes, if the batch has partition key bind indices. |
| `BATCH` | `Batch` (Rust driver), `BatchStatement` (Python driver) | Only through prepared statements in the batch. |

`QUERY` requests are out of scope. As with any unprepared statement, there is nothing to route by.

A `BATCH` frame is a list of statements, each of one of the following kinds:

* kind = 0: query text,
* kind = 1: the ID of a prepared statement.

The two kinds can be mixed freely.

### Partition key bind indices of a prepared batch

The server computes the partition key bind indices of a prepared batch only when every statement in it uses the same table. For each partition key column, it takes the bind index of the last marker that targets that column. If any partition key column isn’t targeted by a bind marker, there are no indices.

```sql
-- ks.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))
-- ks.tbl_composite (pk1 int, pk2 int, v int, PRIMARY KEY ((pk1, pk2)))

-- Partition key bind indices: [0].
BEGIN BATCH
  INSERT INTO ks.tbl (pk, ck, v) VALUES (?, 1, ?);
  UPDATE ks.tbl SET v = ? WHERE pk = 1 AND ck = 2;
APPLY BATCH

-- The partition key is a literal. No partition key bind indices.
BEGIN BATCH
  INSERT INTO ks.tbl (pk, ck, v) VALUES (1, 1, ?);
  INSERT INTO ks.tbl (pk, ck, v) VALUES (1, 2, ?);
APPLY BATCH

-- Partition key bind indices: [0, 2].
BEGIN BATCH
  INSERT INTO ks.tbl_composite (pk1, pk2, v) VALUES (?, 1, ?);
  INSERT INTO ks.tbl_composite (pk1, pk2, v) VALUES (2, ?, 3);
APPLY BATCH

-- pk2 isn't targeted by a bind marker. No partition key bind indices.
BEGIN BATCH
  INSERT INTO ks.tbl_composite (pk1, pk2, v) VALUES (?, 1, ?);
  INSERT INTO ks.tbl_composite (pk1, pk2, v) VALUES (?, 2, 3);
APPLY BATCH
```

### Strongly and eventually consistent batches

A strongly consistent batch must target a single partition of a single table. It can’t be mixed with eventually consistent statements and it can’t contain conditional statements.

An eventually consistent batch can span multiple partitions of multiple tables. If it contains conditional statements, it must target a single partition of a single table, and it runs through Paxos.

## Protocol format and negotiation

### Negotiation

The protocol extension is negotiated as described in the base document, but under the name `TABLETS_ROUTING_V2_EXPERIMENTAL_V2`. The name changes because the layout of `BATCH` requests changes: a driver that only knows `TABLETS_ROUTING_V2_EXPERIMENTAL` would send `BATCH` requests that the server rejects.

The name occupies the same value of the `cql_protocol_extension` type as before. That value is sent between nodes when a request is forwarded. Forwarding only happens for strongly consistent tables, which are hidden behind a cluster feature, so there are no upgrades to support.

### EXECUTE

The format doesn’t change:

```
<id><query_parameters><tablet_version_block>
```

For a prepared batch, the driver computes the token from the partition key bind indices in the `PREPARED` metadata, chooses a `tablet_version_block` for that token, and caches the returned payload under the table from the metadata. That’s what it does for any prepared statement. Only strongly consistent batches return routing information.

### BATCH

The routing hint is appended after the query parameters of the batch:

```
BATCH (pre-TABLETS_ROUTING_V2_EXPERIMENTAL_V2):
  <type><n><query_1>...<query_n><consistency><flags>[<serial_consistency>][<timestamp>]

BATCH (post-TABLETS_ROUTING_V2_EXPERIMENTAL_V2):
  <type><n><query_1>...<query_n><consistency><flags>[<serial_consistency>][<timestamp>]
  <routing_statement_index><tablet_version_block>

where
  <routing_statement_index> is a [short]: the 0-based position of the routing statement,
                            or 0xFFFF for "no routing statement".
  <tablet_version_block>    is a [byte] as in the base document. It's always sent, but the
                            server ignores it when <routing_statement_index> is 0xFFFF.
```

The routing statement is the statement the driver computed the token from, so the driver must be able to compute the token from it: it must be a prepared statement (kind = 1) with partition key bind indices in its `PREPARED` metadata. Other than that, the driver chooses it according to its own policy: the server doesn’t make any assumptions about it. The driver sends `0xFFFF` when it can’t compute a token or doesn’t want to receive routing information for any other reason. Because `n` is a `[short]`, `0xFFFF` never corresponds to a statement. For an empty batch, `0xFFFF` is the only valid index.

The hint is mandatory. When the protocol extension is negotiated, every `BATCH` request carries it, whatever the consistency of the batch. The server rejects the request with a protocol error if:

* the hint is missing,
* `<routing_statement_index>` is neither smaller than `n` nor equal to `0xFFFF`,
* `<routing_statement_index>` points to a statement the token can't be computed from: query text (kind = 0) or a prepared statement without partition key bind indices.

The driver caches the returned payload under the table of the routing statement.

## Server-side changes

**Returning routing information.** The server returns routing information for a batch if all of the following hold:

* The batch is strongly consistent, or it comes from a `BATCH` request.
* The driver could compute the token of the batch (see below).
* The batch executes successfully.
* The `tablet_version_block` doesn’t match the `tablet_version` of the tablet, as described in the base document.

**Whether the driver could compute the token.**

* `EXECUTE`: the prepared batch has partition key bind indices.
* `BATCH`: `<routing_statement_index>` isn’t `0xFFFF`. Any other index names a statement the token can be computed from, or the request is rejected.

That’s the same rule as for single statements: a statement whose partition key isn’t fully bound by markers never gets routing information.

**The routing token.** The server checks the `tablet_version_block` against the tablet that owns the partition of the token the driver computed.

* **Strongly consistent batches:** a batch that executes successfully targets exactly one partition, so every statement in it uses the same value for every partition key column. Whatever combination of bind markers the driver computed the token from, it’s the token of that partition. The server never needs to compute the token from the bound values.
* **Eventually consistent batches:** the server computes the token from the partition key the routing statement writes, using the values of that statement, exactly as the driver does. A batch spanning multiple partitions only gets routing information for the partition of the routing statement. If the routing statement writes more than one partition (e.g. `WHERE pk IN (?, ?)`), no routing information is returned.

**Eventually consistent prepared batches.** The server ignores the `tablet_version_block` of an `EXECUTE` request.

### Pseudocode (on the processing shard; simplified)

```cpp
if (!connection.uses_tablets_routing_v2()) {
    // No routing information for batches.
    return;
}

if (request.is_batch_frame()) {
    // Done for every batch, before it's executed.
    if (request.routing_statement_index != 0xFFFF) {
        if (request.routing_statement_index >= request.statements.size()) {
            throw protocol_error();
        }
        routing_statement = request.statements[request.routing_statement_index];
        if (!routing_statement.is_prepared() || !routing_statement.has_partition_key_bind_indices()) {
            throw protocol_error();
        }
    }
    may_use_token_aware_routing = request.routing_statement_index != 0xFFFF;
} else {
    may_use_token_aware_routing = prepared_batch.has_partition_key_bind_indices();
}

execute(batch);

if (!may_use_token_aware_routing) {
    return;
}

// As for a single statement, see the base document.
if (batch.is_strongly_consistent()) {
    maybe_set_tablets_routing_v2(response, batch.token(), request.tablet_version_block);
} else if (request.is_batch_frame()) {
    if (routing_statement.partition_keys().size() == 1) {
        maybe_set_tablets_routing_v2(response, routing_statement.partition_keys()[0].token(), request.tablet_version_block);
    }
}
```

## Future work: eventually consistent prepared batches

Eventually consistent prepared batches, including multi-partition ones, could get routing information too. The format of `EXECUTE` requests already supports it.

All statements of a prepared batch take their values from one shared list, so the driver and the server would compute the same token from the same partition key bind indices. Unlike for strongly consistent batches, the server would have to compute it from the bound values, exactly as the driver does.

**The routing key may belong to none of the partitions of the batch.** For a single-column partition key, the last marker that targets it belongs to the last statement that binds it. For a composite partition key, the components can come from different statements:

```sql
-- ks.tbl (a int, b int, c int, PRIMARY KEY ((a, b)))
BEGIN BATCH
  INSERT INTO ks.tbl (a, b, c) VALUES (?, 5, ?); -- Markers: 0, 1.
  UPDATE ks.tbl SET c = ? WHERE a = 1 AND b = ?; -- Markers: 2, 3.
APPLY BATCH;
-- Partition key bind indices:
--   a -> 0
--   b -> 3
-- With values (7, 5, 1, 9), the batch writes (7, 5) and (1, 9),
-- but the routing key is (7, 9).
```

Drivers already route such batches by this token. Routing information returned for it would be valid, but useless: the token doesn’t say anything about where the data of the batch lives. That’s not a problem for strongly consistent batches, which target a single partition, nor for `BATCH` requests, where the token always belongs to the partition of the routing statement.
