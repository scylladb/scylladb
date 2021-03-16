# CDC

### Stream IDs
A __stream ID__ is a 128-bit number, represented as a pair of `int64`s:
```
class stream_id final {
    int64_t _first;
    int64_t _second;
public:
    ... methods ...
};
```
When a write is performed to a CDC-enabled user-created table (the "base table"), a corresponding write, or a set of writes, is synchronously performed to the CDC table associated with the base table (the "log table"); the partition key for these log writes is chosen from some set of stream IDs. Where this set comes from and how those stream IDs are chosen is described below.

The 128 bits are composed of:

```
128           64            27            4             0
  | <token:64> | <random:38> | <index:22> | <version:4> |

```

With `version` making up the lowest 4 bits. The id is stored as bytes, and sorted on string ordering, i.e. the high qword (token) is msb in ordering. 

The `index` bits indicate the vnode index the id belongs to, i.e. the vnode owning the end of the token range in which the id sits.

The random bits exist to help ensure ids are sufficiently unique across
generations.

### Generations
A __CDC generation__ is a structure consisting of:
1. a __generation timestamp__, describing the time point from which this generation "starts operating" (more on that later),
2. a set of stream IDs,
3. a mapping from the set of tokens (in the entire token ring) to the set of stream IDs in this generation.

The mapping from point 3 has a simple structure, allowing us to compactly store a CDC generation. This is the purpose of the `cdc::topology_description` class:
```
namespace cdc {
    struct token_range_description {
        dht::token token_range_end;
        std::vector<stream_id> streams;
        uint8_t sharding_ignore_msb;
    };
    class topology_description {
        std::vector<token_range_description> _entries;
public:
        ... methods ...
    };
}
```
From a `cdc::topology_description` we can read the set of stream IDs of this generation and the mapping. How `cdc::topology_description` represents the mapping is explained later.

Each node in a Scylla cluster stores a set of known CDC generations using the `cdc::metadata` class. Simplified definition:
```
namespace cdc {
    class metadata final {
        using container_t = std::map<api::timestamp_type, topology_description>;
        container_t _gens;
    };
}
```
The `_gens` map's key is the generation's timestamp.

Let `container_t::iterator it` point to some generation in this set. We say that the generation given by `*it` __operates__ in a time interval `[T, T')` (where T, T' are timestamps) if and only if:
1. `T` is the generation's timestamp (`it->first`),
2. `T'` is the following generation's timestamp (`std::next(it)->first`).

This set changes while the node runs. If there is no following generation (`std::next(it) == std::end(_gens)`), it simply means that this node doesn't yet know what that following generation will be. It might happen that `T' = ∞` (i.e. there really won't ever be a following generation) but that's unlikely.

When a write is performed to a base table, the write is translated to a mutation, which holds a timestamp. This timestamp together with the token of the write's partition key is used to retrieve the stream ID which will then be used to create a corresponding log table write. The stream ID is taken from the generation operating at this timestamp using:
```
    cdc::stream_id cdc::metadata::get_stream(api::timestamp_type ts, dht::token tok, const dht::i_partitioner&);
```

Here's a simplified snippet of code illustrating how the log write's mutation is created (from cdc/log.cc; `m` is the base table mutation):
```
    mutation transform(const mutation& m) const {
        auto ts = find_timestamp(m);
        auto stream_id = _ctx._cdc_metadata.get_stream(ts, m.token(), _ctx._partitioner);
        mutation res(_log_schema, stream_id.to_partition_key(*_log_schema));

        ... fill "res" with the log write's column data ...

    }
```

##### The generation's mapping

The `cdc::topology_description` class contains a vector of `token_range_description` entries, sorted by `token_range_end`. These entries split the token ring into ranges: for each `i` in `0, ..., _entries.size() - 1` we get the range (`_entries[i].token_range_end`, `_entries[(i+1) % _entries.size()]`] (the range is left-opened, right-closed).

The `i`th entry defines how tokens in the `i`th range (the one ending with `_entries[i].token_range_end`) are mapped to the vector of streams given by `_entries[i].streams` as follows. Suppose that the used partitioner is given by `p` (of type `i_partitioner&`). Suppose `tok` (of type `dht::token`) falls into the `i`th range.
Then `tok` is mapped into
```
_entries[i][p.shard_of(tok, _entries[i].streams.size(), _entries[i].sharding_ignore_msb)]
```
The motivation for this is the following: the token ranges defined by `topology_description` using `token_range_end`s are a refinement of vnodes in the token ring at the time when this generation operates (i.e. each range defined by this generation is wholly contained in a single vnode). The streams in vector `_entries[i].streams` have their tokens in the `i`th range. Therefore we map each token `tok` to a stream whose token falls into the the same vnode as `tok`. Hence, when we perform a base table write, the corresponding CDC log write will fall into the same vnode, thus it will have the same set of replicas as the base write. We call this property __colocation__ of base and log writes.

To achieve the above it would be enough if `_entries[i].streams` was a single stream, not a vector of streams. But we went further and aim to achieve not only colocation of replicas, but also colocation of shards (not necessarily at all replicas, but a subset of them at least).

Suppose that some node `A` is a replica for the vnode containing the `i`th range. Suppose that the number of shards of `A` is `_entries[i].streams.size()`, and the value of `sharding_ignore_msb` parameter used by the configured partitioner is `_entries[i].sharding_ignore_msb`. Suppose that we chose `_entries[i].streams[j]` so that its token is owned by the `j`th shard on node `A`.
Then base table writes with tokens that fall into the `i`th range and into the `j`th shard on node `A` will have their corresponding log table entries also written to the `j`th shard, at least on node `A`. If all nodes use the same number of shards (which is pretty common), we'll get shard-colocation on every replica.

Vnode-colocation is important for consistency: when the base write goes to the same set of replicas as the log write, it is possible to make sure that each replica either receives both writes or none of them. Thus the CDC log will be able to truly reflect what happened in the base table.
Shard-colocation is an optimization.

### Generation switching

Having different generations operating at different points in time is necessary to maintain colocation in presence of topology changes. When a new node joins the cluster it modifies the token ring by refining existing vnodes into smaller vnodes. But before it does it, it will introduce a new CDC generation whose token ranges refine those new (smaller) vnodes (which means they also refine the old vnodes; that way writes will be colocated on both old and new replicas).

The joining node learns about the current vnodes, chooses tokens which will split them into smaller vnodes and creates a new `cdc::topology_description` which refines those smaller vnodes. This is done in the `cdc::topology_description_generator` class. It then inserts the generation description into an internal distributed table `cdc_generation_descriptions_v2` in the `system_distributed_everywhere` keyspace. The table is defined as follows:
```
CREATE TABLE system_distributed_everywhere.cdc_generation_descriptions_v2 (
    id uuid,
    range_end bigint,
    ignore_msb tinyint,
    num_ranges int static,
    streams frozen<set<blob>>,
    PRIMARY KEY (id, range_end)
) ...
```
The table's partition key is the `id uuid` column. The UUID used to insert a new generation into this table is randomly generated by the joining node.

Note that constructing the `cdc::topology_description` (which describes the generation's mapping and contains its stream IDs) and inserting it into this table does not require knowing the generation's timestamp.

The table lies in the `system_distributed_everywhere` keyspace which is replicated using the `Everywhere` strategy, meaning that the generation data is replicated by every node. The insert is performed using `CL=ALL`, allowing nodes to read the data locally later using `CL=ONE`.

The timestamp for the new generation is chosen after the data is inserted to the table. To choose the timestamp, the node takes its local time and adds a minute or two so that other nodes have a chance to learn about this generation before it starts operating. Thus, the node makes the following assumptions:
1. its clock is not too desynchronized with other nodes' clocks,
2. the cluster is not partitioned.

Future patches will make the solution safe by using a two-phase-commit approach.

The timestamp and the randomly generated UUID together form a "generation ID" which uniquely identifies this generation and can be used to retrieve its data from the table and to learn when it starts operating.

Next, the node starts gossiping the ID of the new generation together with its set of chosen tokens and status:
```
        _gossiper.add_local_application_state({
            { gms::application_state::TOKENS, versioned_value::tokens(_bootstrap_tokens) },
            { gms::application_state::CDC_GENERATION_ID, versioned_value::cdc_generation_id(_cdc_gen_id) },
            { gms::application_state::STATUS, versioned_value::bootstrapping(_bootstrap_tokens) },
        }).get();
```

The node persists the currently gossiped generation ID in order to recover it on restart in the `system.cdc_local` table. This is the schema:
```
CREATE TABLE system.cdc_local (
    key text PRIMARY KEY,
    streams_timestamp timestamp,
    uuid uuid
) ...
```
The timestamp and UUID forming the generation ID are kept under the `"cdc_local"` key in the `streams_timestamp` and `uuid` columns, respectively.

When other nodes learn about the generation, they'll extract it from the `cdc_generation_descriptions_v2` table and insert it into their set of known CDC generations using `cdc::metadata::insert(db_clock::time_point, topology_description&&)`.
Notice that nodes learn about the generation together with the new node's tokens. When they learn about its tokens they'll immediately start sending writes to the new node (in the case of bootstrapping, it will become a pending replica). But the old generation will still be operating for a minute or two. Thus colocation will be lost for a while. This problem will be fixed when the two-phase-commit approach is implemented.

We're not able to prevent a node learning about a new generation too late due to a network partition: if gossip doesn't reach the node in time, some writes might be sent to the wrong (old) generation.
However, it could happen that a node learns about the generation from gossip in time, but then won't be able to extract it from `cdc_generation_descriptions_v2`. In that case we can still maintain consistency: the node will remember that there is a new generation even though it doesn't yet know what it is (it knows only the ID, in particular it knows the timestamp) using the `cdc::metadata::prepare(db_clock::time_point)` method, and then _reject_ writes for CDC-enabled tables that are supposed to use this new generation. The node will keep trying to read the generation's data in background until it succeeds or sees that it's not necessary anymore (e.g. because the generation was already superseded by a new generation).
Thus we give up availability for safety. This likely won't happen if the administrator ensures that the cluster is not partitioned before bootstrapping a new node. This problem will also be mitigated with a future patch.

Due to the need of maintaining colocation we don't allow the client to send writes with arbitrary timestamps.
Suppose that a write is requested and the write coordinator's local clock has time `C` and the generation operating at time `C` has timestamp `T` (`T <= C`). Then we only allow the write if its timestamp is in the interval [`T`, `C + generation_leeway`), where `generation_leeway` is a small time-inteval constant (e.g. 5 seconds).
Reason: we cannot allow writes before `T`, because they belong to the old generation whose token ranges might no longer refine the current vnodes, so the corresponding log write would not necessarily be colocated with the base write. We also cannot allow writes too far "into the future" because we don't know what generation will be operating at that time (the node which will introduce this generation might not have joined yet). But, as mentioned before, we assume that we'll learn about the next generation in time. Again --- the need for this assumption will be gone in a future patch.

### Streams description tables

The `cdc_streams_descriptions_v2` table in the `system_distributed` keyspace allows CDC clients to learn about available sets of streams and the time intervals they are operating at. It's definition is as follows (db/system_distributed_keyspace.cc):
```
        return schema_builder(system_distributed_keyspace::NAME, system_distributed_keyspace::CDC_DESC_V2, {id})
                /* The timestamp of this CDC generation. */
                .with_column("time", timestamp_type, column_kind::partition_key)
                /* For convenience, the list of stream IDs in this generation is split into token ranges
                 * which the stream IDs were mapped to (by the partitioner) when the generation was created.  */
                .with_column("range_end", long_type, column_kind::clustering_key)
                /* The set of stream identifiers used in this CDC generation for the token range
                 * ending on `range_end`. */
                .with_column("streams", cdc_streams_set_type)
                .with_version(system_keyspace::generate_schema_version(id))
                .build();
```
where
```
thread_local data_type cdc_stream_tuple_type = tuple_type_impl::get_instance({long_type, long_type});
thread_local data_type cdc_streams_set_type = set_type_impl::get_instance(cdc_stream_tuple_type, false);
```
This table contains each generation's timestamp (as partition key) and the set of stream IDs used by this generation grouped by token ranges that the stream IDs are mapped to. It is meant to be user-facing, in contrast to `cdc_generation_descriptions_v2` which is used internally.

There is a second table that contains just the generations' timestamps, `cdc_generation_timestamps`:
```
        return schema_builder(system_distributed_keyspace::NAME, system_distributed_keyspace::CDC_TIMESTAMPS, {id})
                /* This is a single-partition table. The partition key is always "timestamps". */
                .with_column("key", utf8_type, column_kind::partition_key)
                /* The timestamp of this CDC generation. */
                .with_column("time", timestamp_type, column_kind::clustering_key)
                /* Expiration time of this CDC generation (or null if not expired). */
                .with_column("expired", timestamp_type)
                .with_version(system_keyspace::generate_schema_version(id))
                .build();
```
It is a single-partition table, containing the timestamps of generations found in `cdc_streams_descriptions_v2` in separate clustered rows. It allows clients to efficiently query if there are any new generations, e.g.:
```
SELECT time FROM system_distributed.cdc_generation_timestamps` WHERE time > X
```
where `X` is the last timestamp known by that particular client.

When nodes learn about a CDC generation through gossip, they race to update these description tables by first inserting the set of rows containing this generation's stream IDs into `cdc_streams_descriptions_v2` and then, if the node succeeds, by inserting its timestamp into `cdc_generation_timestamps` (see `cdc::update_streams_description`). This operation is idempotent so it doesn't matter if multiple nodes do it at the same time.

Note that the first phase of inserting stream IDs may fail in the middle; in that case, the partition for that generation may contain partial information. Thus a client can only safely read a partition from `cdc_streams_descriptions_v2` (i.e. without the risk of observing only a part of the stream IDs) if they first observe its timestamp in `cdc_generation_timestamps`.

### Internal generation descriptions table V1 and upgrade procedure

As the name suggests, `cdc_generation_descriptions_v2` is the second version of the generation description table. The previous schema was:
```
CREATE TABLE system_distributed.cdc_generation_descriptions (
    time timestamp PRIMARY KEY,
    description frozen<list<frozen<tuple<bigint, frozen<list<blob>>, tinyint>>>>,
    expired timestamp
) ...
```
The entire set of stream IDs (for all token ranges) was stored as a single collection. With large clusters the collection could grow quite big: for example, with 100 nodes 64 shards each and 256 vnodes per node, a new generation would contain 1,6M stream IDs, resulting in a ~32MB collection. For reasons described in issue #7961 this would disqualify the previous schema.

The new schema stores each generation in multiple rows, each of manageable size. The size of each row, which describes a single token range, is proportional to the number of shards on the owner of that token range at the moment the generation was created; for example, for 64 shards, the size is ~1KB.

Currently generations are identified using a (timestamp, UUID) pair (let's call these identifiers "V2 IDs"). Previously they were identified using only a timestamp ("V1 IDs") - the generation's timestamp - and the timestamp was the partition key for the generation's description in the above table. For this reason the generation's timestamp had to be chosen before the description was inserted into the table, so the duration of the insertion operation increased the chance that nodes learn about the generation too late (after their clocks go past the generation's timestamp).

A cluster may be upgraded from a version of Scylla that uses the old table with the V1 ID format. Upgraded nodes still understand the V1 ID format and the old table, and until the cluster keeps gossiping a generation ID that was created before the upgrade - thus, still in the V1 format - they will keep using the old table to retrieve the generation's data (they do it on restart, for example). However, as soon as a new node bootstraps into a fully upgraded cluster, it will introduce a new generation using the new table and the V2 ID format. All nodes in the upgraded cluster understand the V2 format and know that the new generation must be retrieved from the new table.

To prevent situations where a node introduces a generation using the V2 format into a cluster that contains nodes which don't understand the V2 format (because they didn't upgrade yet), a cluster feature called "CDC_GENERATIONS_V2" was added. When a modern node tries to join a cluster it checks if the cluster understands this feature. If it doesn't then the node introduces the new generation using the old format.

### Streams description table V1 and rewriting

As the name suggests, `cdc_streams_descriptions_v2` is the second version of the streams description table. The previous schema was:
```
        return schema_builder(system_distributed_keyspace::NAME, system_distributed_keyspace::CDC_DESC, {id})
                /* The timestamp of this CDC generation. */
                .with_column("time", timestamp_type, column_kind::partition_key)
                /* The set of stream identifiers used in this CDC generation. */
                .with_column("streams", cdc_streams_set_type)
                /* Expiration time of this CDC generation (or null if not expired). */
                .with_column("expired", timestamp_type)
                .with_version(system_keyspace::generate_schema_version(id))
                .build();
```

This schema had the same problem as `cdc_generation_descriptions` described in the previous section.

That was the schema used in the Scylla 4.3 release. For clusters that used CDC with this schema we need to ensure that stream descriptions residing in the old table appear in the new table as well (if necessary, i.e. if these streams may still contain some data) so CDC clients can learn them.

To do that, we perform a rewrite procedure. Each node does the following on restart:
1. Check if the `system_distributed.cdc_streams_descriptions` table exists. If it doesn't, there's nothing to rewrite, so stop.
2. Check if the `system.cdc_local` table contains a row with `key = "rewritten"`. If it does then rewrite was already performed, so stop.
3. Check if there is a table with CDC enabled. If not, add a row with `key = "rewritten"` to `system.cdc_local` and stop; no rewriting is necessary (and won't be) since old generations - even if they exists - are not needed.
4. Retrieve all generation timestamps from the old streams description table by performing a full range scan: `select time from system_distributed.cdc_streams_descriptions`. This may be a long/expensive operation, hence it's performed in a background task (the procedure is moved to background in this step).
5. Filter out timestamps that are "too old". A generation timestamp is "too old" if there is a greater timestamp `T` such that for every table with CDC enabled, `now - ttl > T`, where `now` is the current time and `ttl` is the table's TTL setting. This means that the table cannot contain data that belongs to the "too old" generation. Thus, if each table passes this check for a given generation, that generation doesn't need to be rewritten.
6. For each timestamp that's left:
6.1 if it's already present in the new table, skip it (we check this by querying `cdc_generation_timestamps`
6.2 fetch the generation (by querying `cdc_generation_descriptions`)
6.3 insert the generation's streams into the new table
7. Insert a row with `key = "rewritten"` into `system.cdc_local`.

Note that every node will perform this procedure on upgrade, but there's a high chance that only one of them actually proceeds all the way to step 6.2 if upgrade is performed correctly, i.e. in a rolling fashion (nodes are restarted one-by-one).

In order to prevent new nodes to do the rewriting (we only want upgrading nodes to do it), we insert the `key = "rewritten"` row on bootstrap as well, before we start this procedure (so the node won't pass the second check).

#### TODO: expired generations
The `expired` column in `cdc_generation_timestamps` means that this generation was superseded by some new generation and will soon be removed (its table entry will be gone). This functionality is yet to be implemented.
