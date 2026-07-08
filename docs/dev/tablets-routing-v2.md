# Tablets routing v2

Although the document covers both `global` and `local` consistency for strongly consistent
tables, note that `local` is **not** supported yet.

## Terminology

For convenience, we introduce a few new terms that we’ll use for the remainder of this document:

* Routing information comprises of a list of replicas and their distinguished leader. For more details, see section [“Tablet version”](#tablet-version) below.
* A leader hint is part of routing information and corresponds to the identity of the leader.
* The processing node is the node that executes a request. Nodes that forward it are not processing. (Note: In the case of eventual consistency, the processing node is always the coordinator.)
* The processing shard is the shard that executes a request. Shards that bounce it are not processing.

In some places, to avoid overly complex and wordy sentences, we might use the term “processing node” to mean “processing node or processing shard” or simply “processing shard”. Rely on context when interpreting it.

## Summary

The driver includes its cached tablet_version in each `EXECUTE` request. Upon a mismatch with the server’s tablet version, the server returns routing information corresponding to the tablet. The driver then caches this information and routes subsequent queries directly to the leader.

Request processing paths
* The tablet version matches. The response doesn’t contain any additional payload.
* The tablet version is mismatched. The server includes information about the current replica set and also information about the current Raft leader. The driver updates its cache.

Note that it’s possible that a request doesn’t arrive at the leader and yet the tablet_version matches. In that case, no additional payload is returned, and this is an acceptable situation.

## Tablet version

A tablet_version is a 64-bit hash produced based on an ordered list of replicas. The order of the replicas is chosen by a deterministic sorting algorithm.

1. Eventually consistent tablets: the replica list consists of all replicas of the tablet:
```cpp
// Fetch ALL of the replicas of the tablet and sort them.
replica_list = sort(tablet.replica_list)

tablet_version = hash(replica_list)
```
2. Strongly consistent tablets:
    - Global consistency: the replica list consists of all replicas of the tablet. The list is shifted so that the leader of the Raft group is the first replica in the list:
    ```cpp
    // Fetch ALL of the replicas of the tablet and sort them.
    replica_list = sort(tablet.replica_list)

    // Shift the replica list so that the leader is the first in list.
    replica_list = shift_for_leader(replica_list, tablet.leader)

    tablet_version = hash(replica_list)
    ```
    - Local consistency: the replica list consists of the replicas in the local DC only. The list is shifted so that the leader of the Raft group is the first replica in the list:
    ```cpp
    // Fetch the list of replicas in the local DC ONLY.
    dc_replica_list = tablet.dcs[local_dc].replica_list

    // Sort the replicas.
    dc_replica_list = sort(dc_replica_list)

    // Shift the replica list so that the leader is the first in list.
    dc_replica_list = shift_for_leader(dc_replica_list, tablet.dcs[local_dc].leader)

    tablet_version = hash(dc_replica_list)
    ```

**Note:** In the case of `local` consistency, a single tablet can have multiple tablet_versions, depending on which DC it’s computed in (because `tablet_version` depends on the identity of the leader).

**Note:** Because a locally-consistent tablet may have multiple tablet_versions, one for each DC, it makes sense to reduce the number of arguments that `tablet_version` depends on to only the replicas in the local DC. This will reduce the number of tablet version mismatches: changes in one DC don’t affect any other DC.

### Tablet version block

We divide a 64-bit tablet_version into 16 blocks, each consisting of 4 bits. We then encode a given block with 8 bits in the following way:
* The index of the block in the hash (0-15): 4 bits.
* The contents of the block: 4 bits.

Blocks are indexed from the least significant bits to the most significant ones, so block `i` occupies bits `[i*4, i*4 + 4)` of the hash. The bit positions refer to the tablet_version as a 64-bit integer (bit 0 is the least significant bit); they are independent of endianness.

**Example:** If the 13-th block is equal to 0xC, then the block is encoded by the byte 0xDC (because the hex representation of 13 is 0xD).

We refer to this representation with the term: **tablet version block**. The driver uses this construct instead of the full tablet_version when sending requests to the server to reduce network usage (cf. section [“Server-side changes”](#server-side-changes) below).

## Protocol format and negotiation

During connection setup, the driver and server negotiate `TABLETS_ROUTING_V2` support:
1. The server advertises `TABLETS_ROUTING_V2` in the `SUPPORTED` response (as a key in the options multimap with an empty value), alongside the existing `TABLETS_ROUTING_V1`.
2. During `STARTUP`, the driver echoes back `TABLETS_ROUTING_V2` in its options map to opt in.
3. The server records the extension on the connection’s client state via `set_protocol_extensions()`.

`TABLETS_ROUTING_V2` subsumes `TABLETS_ROUTING_V1`. A driver that negotiates v2 **does not** negotiate v1. If a driver only negotiates v1, the server falls back to the existing v1 behavior.

The extension is gated behind the experimental feature `strongly-consistent-tables`.

**Note:** We temporarily use a different name for the protocol extension: `TABLETS_ROUTING_V2_EXPERIMENTAL`.
The name explicitly conveys that the feature is still experimental and may be modified.

### EXECUTE request extension

The `tablet_version` is not transmitted directly. Instead, an arbitrarily chosen tablet version block (cf. section [“Tablet version”](#tablet-version)) is carried as part of the `EXECUTE` request body, right after the `query_parameters`:
```
EXECUTE (pre-TABLETS_ROUTING_V2):
  <id><query_parameters>

EXECUTE (post-TABLETS_ROUTING_V2):
  <id><query_parameters><tablet_version_block>

where <tablet_version_block> is a byte.
```

The `tablet_version_block` is associated with the target table of the prepared statement and the specific token being accessed. The driver fetches the `tablet_version` it has cached for the tablet covering the request’s partition token, chooses an arbitrary tablet version block, and sends it as part of the `EXECUTE` request.

For requests that are not single-partition (e.g. range scans), the driver sends an arbitrary `tablet_version_block`. It will be ignored and no routing information will be returned (cf. section [“Server-side changes”](#server-side-changes) below).

### Response payload

The routing information is carried as a custom payload in the `RESULT` frame under the key `"tablets-routing-v2"`. The frame’s custom_payload flag (bit 2, 0x04) is set in the frame header.

The payload value is serialized using CQL type serialization as a tuple:
```
TupleType(
  u64,                    // first_token (exclusive)
  u64,                    // last_token (inclusive)
  List<Tuple<UUID, u32>>, // replicas [(host_id, shard_id), ...]
  u64,                    // tablet_version, big-endian
)
```

The replica list corresponds to the same order that was used for computing the tablet_version (cf. section [“Tablet version”](#tablet-version) above). In particular, for strongly consistent tables, the first replica in the list is always the leader of the Raft group.

For tables using the `local` consistency mode, the returned replica list only consists of the replicas in the local DC (as specified in the section [“Tablet version”](#tablet-version)). For all other tables, the replica list consists of all replicas of the tablet.

### Interaction with TABLETS_ROUTING_V1

If the driver negotiated `TABLETS_ROUTING_V2`, the server only uses the v2 key (`"tablets-routing-v2"`) in responses. The v1 key (`"tablets-routing-v1"`) is never sent alongside v2 for the same response. If the driver only negotiated v1, the behavior remains as in v1.

## Server-side changes

**Computing tablet version.** The `tablet_version` hash is computed according to the algorithm specified in section [“Tablet version”](#tablet-version).

Note that for strongly consistent tablets computing the `tablet_version` is only possible for the members of the Raft group because only they know the identity of the leader. Moreover, a single tablet can have multiple `tablet_version`s associated with it if the table uses the `local` consistency mode. Due to that, for strongly-consistent tablets, this information can only be computed on the members of the Raft group and corresponds to the tablet_version of that specific Raft group.

**Example:** Consider nodes A, B, C in two DCs: DC1: {A, B}, DC2: {C}. Consider a tablet T whose replicas are A and C.
* T is eventually consistent:
    - A: tablet_version = 0x11111111
    - B: tablet_version = 0x11111111
    - C: tablet_version = 0x11111111
* T is strongly consistent and uses global consistency mode:
    - A: tablet_version = 0x11111111
    - B: tablet_version = \<unknown\>
    - C: tablet_version = 0x11111111
* T is strongly consistent and uses local consistency mode:
    - A: tablet_version = 0x11111111
    - B: tablet_version = \<unknown\>
    - C: tablet_version = 0x23232323 (different Raft group -> different tablet version)

**Returning routing information.** When a request arrives and the attached `tablet_version_block` doesn’t match the actual `tablet_version`, the server includes the full routing information in a **successful** response. Errors don’t carry any additional payload as we usually cannot predict what happened. For instance, if the driver gets a server timeout error, it’s not clear if it’s because the node is overloaded, partitioned, or if other replicas don’t respond to it. Any routing information returned in such circumstances would ultimately be unreliable.

If the target table is vnode-based, the passed `tablet_version_block` is ignored and no routing information is returned in the response.

**Raft group member status.** The driver may send a read to a strongly-consistent table using consistency level `ONE` or `LOCAL_ONE`. In that case, the processing node can be a non-leader replica. If the Raft status of the processing node is a candidate, that means it’s observing an ongoing election. In that case, the provided `tablet_version_block` is ignored and no routing information is returned. Because there’s no leader, computing the `tablet_version` is not possible. Moreover, that could be a sign of a network partition.

### Who provides routing information

The node attaching routing information is always the processing node. It’s attached to the result by the statement execution code.
1. **Eventual consistency.** The coordinator uses `storage_proxy::mutate_*()` and `storage_proxy::query()`, which internally contact replicas via dedicated RPC verbs. These RPCs return raw data, not CQL results. The coordinator assembles the final CQL result and is the one that attaches `tablets-routing-v1` payload today. This means that for eventually-consistent tables, the coordinator is the processing node.

    The tablet version comparison and payload attachment happen locally on the coordinator. No changes to the existing forwarding infrastructure are needed. The coordinator can compute the tablet_version because every node has access to the list of replicas of every tablet.
2. **Strong consistency.** The statement code may produce a bounce that will redirect the statement to a replica via CQL forwarding. This forwards the raw CQL request bytes to the target node, which runs the full statement execution and produces a complete CQL result (including any custom payloads). Finally, it returns the raw serialised response body and flags to the coordinator. The coordinator reconstructs a response from the raw bytes and sends it to the client without inspecting or modifying the response body. Custom payloads attached by the processing node are passed to the driver transparently. No forwarding infrastructure changes are needed.

**Note:** It may happen that there are two bounces: the coordinator passes a request to a replica but not the leader of the tablet. The forwarding code returns redirect information containing the new target. The coordinator follows it. No result is produced at the redirect step, and no routing information is attached. Only the successful execution on the processing node attaches routing information.

### Pseudocode (on the processing shard; simplified)
```cpp
// The cluster has not enabled the cluster feature yet.
// Fall back to TABLETS_ROUTING_V1.
if (!connection.uses_tablets_routing_v2()) {
    return handle_v1();
}

// The driver thought the table was tablet-based, but it uses vnodes.
// No routing information returned.
if (!table.uses_tablets()) {
    return handle_vnodes();
}

tablet_version = compute_tablet_version(tablet)
tablet_version_block = extract_tablet_version_block(tablet_version)

// Version mismatch. Provide the driver with the updated routing information.
if (status() != raft::candidate
        && request.is_single_partition()
        && request.tablet_version_block != tablet_version_block
) {
    replica_list = tablet.get_replica_list()
    // Shift the replica list so that the leader
    // is the first in the list.
    shifted_replica_list = shift_replica_list(replica_list, tablet.leader)

    response->set_tablets_routing_v2(
        tablet.token_begin,
        tablet.token_end,
        shifted_replica_list,
        tablet_version
    );
}

// No additional payload attached.
```
