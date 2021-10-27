# Virtual Tables

Virtual tables are tables that are not backed by physical storage (sstables), instead they generate their content on-the-fly when queried, by a specific reader instance.
This reader is created by a `mutation_source` object stored in the `table` instance, set previously by `table::set_virtual_reader()`.
So on a very low level, a table is virtual, if one calls `table::set_virtual_reader()` in its `table` instance.

Virtual tables allow for exposing information already available in memory to the user in the form of a CQL table.
They are much more lightweight than their regular counterparts and they completely lack all the burden and overhead of updating a persistent storage and keeping it consistent with the already existing in-memory structures.
Instead virtual table readers can just translate these in-memory structures into CQL results at the time they are queried.
These tables completely look and act like regular tables from a user's point of view, which is the entire point: the user can use an interface they are already familiar with to query and manipulate data.


## What is a good candidate for a virtual table?

In general a good candidate for a virtual table has to match the following conditions:
1) It exposes data readily available in in-memory structures of the node. The data doesn't have to be related to the node, it can be about the entire cluster too.
2) The amount of data is small to moderate (partitions/rows in the order of low thousands).
3) The data is not available from other tables, like regular system tables.


## Information retrieval nodetool commands as virtual tables

Some time ago we started adding virtual tables to replace information retrieval nodetool commands.
These nodetool commands are currently served by one or more REST API endpoints.
CQL tables are a natural fit to replace these nodetool commands:
* One can retrieve the information with just a CQL client, instead of having to have both jmx and nodetool setup;
* Remote access: REST and therefore nodetool is only accessible on the same machine the node lives on for security considerations (see below);
* Security: CQL has authentication and authorization already, with fine-grained RBAC support;
* Filtering and aggregation: CQL allows for filtering and selecting rows/columns as well as aggregating them;

Even though not widely known, CQL also has built-in JSON support (`select json...`) so if somebody prefers JSON (used by REST) they can still use JSON with CQL too.

## How to add a new virtual table?

The process of adding a new virtual table is as follows:
* Choose the appropriate class to inherit from: `db::memtable_filling_virtual_table` or `db::streaming_virtual_table` (located in `db/virtual_table.hh`);
* Implement the interface generating the data, mind shard awareness and query restrictions if they apply;
* Instantiate and register your virtual table in `register_virtual_tables()` in `db/system_keyspace.cc`;

### Choosing the right class for you virtual table

#### memtable_filling_virtual_table

If your table generates a constant (that is known in advance) and small amount of data you should use the `db::memtable_filling_virtual_table`.
This works by first inserting all data into a memtable, then querying said memtable. The memtable takes care of all aspects of the query: read range restrictions, slicing, etc.
Your implementation's only job is to generate the data and feed it to the `mutation_sink` parameter of `execute()`.
Partitions and rows can be generated in any order, the memtable takes care of ordering it.
Shard awareness still applies, see the [shard awareness](#shard-awareness) section.

#### streaming_virtual_table

If your table generates either a lot of data (say 100+ lines) or the amount of data generated is not fixed but dynamic, you should use `db::streaming_virtual_table`.
This works by generating one partition at a time, fragment-by-fragment, yielding when the data fills a reader buffer.
Your implementation has to make sure partitions are generated in the right order (very important)!!! The same goes for rows.
Although the `streaming_virtual_table` will take care of dropping any partitions/rows that are outside of the read-range, this is wasteful, so you also should avoid emitting any partitions that are not in the read-range -- obtainable from `query_restrictions` parameter of `execute()`.
Shard awareness also applies, see the [shard awareness](#shard-awareness) section.

A typical algorithm works like this:
```c++

future<> execute(reader_permit permit, result_collector& result, const query_restrictions& qr) override {
    // First produce all possible partition keys for the to-be-emitted data
    std::vector<dht::decorated_key> keys;
    for (const auto& my_key : generate_all_keys()) {
        auto dk = make_decorated_key(my_key);
        // Drop those that either don't belong to this shard (see shard awareness) or are outside the read range (not a must but nice-to-do)
        if (this_shard_owns(dk) && contains_key(qr.partition_range(), dk)) {
            keys.push_back(std::move(dk));
        }
    }

    // Sort keys in token order
    boost::sort(keys, dht::ring_position_less_comparator(*_s));

    // Iterate over the keys and generate content for them
    for (auto& dk : keys) {
        co_await result.emit_partition_start(dk.key);
        // generate rows, in clustering order!!!
        co_await result.emit_partition_end();
    }
}
```

#### Shard awareness

Virtual tables have to take care to not emit partitions that don't belong to the shard the read runs on.
When querying a virtual table, the normal read algorithms are used.
These expect that the table exists on all shards and on each shard the table will only emit data, whose token belongs to that shard according to the table's (schema's) sharder.
For convenience the virtual table infrastructure (`db::virtual_table`) takes care of dropping all partitions from your virtual table output that doesn't belong to the current shard.
This is inefficient however because the data is produced just to be dropped and filtering is not free either.
So virtual table implementations can instead do this filtering themselves and promise to the `db::virtual_table` (their ancestor class) that they are shard aware and will take care of this.
They can do this by setting `_shard_aware = true` in their constructor.
If your table generates very little data and generating partitions is not much more expensive then generating just the keys, you can opt for `db::virtual_table` to take care of shard awareness for you.
You can do this by setting `_shard_aware = false` in the constructor of your virtual table.

Whichever you choose to do, just make sure to match your implementation with what you set `_shard_aware` to, doing otherwise can cause all sorts of unpredictable errors when your table is queried.
