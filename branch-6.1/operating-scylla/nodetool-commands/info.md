# Nodetool info

**info** - Provides detailed statistics for a specific node in the
cluster, such as uptime, load, key cache hit rate, the total count of all
exceptions, and more. If needed, you can specify which node to view by
using the –host argument with the host IP address.

For example:

```default
nodetool info
```

Example output:

```default
ID                     : 2110829b-47f2-4a6b-b87e-a81bc3b5cb31
Gossip active          : true
Thrift active          : false
Native Transport active: true
Load                   : 294.44 MB
Generation No          : 1474434958
Uptime (seconds)       : 1868
Heap Memory (MB)       : 39.21 / 247.50
Off Heap Memory (MB)   : 7.74
Data Center            : us-east
Rack                   : 1b
Exceptions             : 0
Key Cache              : entries 0, size 0 bytes, capacity 0 MB, 0 hits, 0 requests, 0 recent hit rate, 0 save period in seconds
Row Cache              : entries 1064771, size 1.02 MB, capacity 450.8 MB, 96 hits, 120 requests, 0.800 recent hit rate, 0 save period in seconds
Counter Cache          : entries 0, size 0 bytes, capacity 0 bytes, 0 hits, 0 requests, 0.000 recent hit rate, 0 save period in seconds
Token                  : (invoke with -T/--tokens to see all 256 tokens)
```

| Parameter                        | Description                                                                                                                                      |
|----------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| ID                               | Node ID                                                                                                                                          |
| Gossip<br/>active                | Gossip status                                                                                                                                    |
| Thrift<br/>active                | Thrift status                                                                                                                                    |
| Native<br/>Transport<br/>active  | Native Transport status (CQL)                                                                                                                    |
| Load                             | How much hard drive space is<br/>used by SSTable<br/>(updates every 60 seconds)                                                                  |
| Generation<br/>No                | Generation No - When a major<br/>change occurs on a node, such<br/>as a restart or a changing<br/>tokens, the Generation number<br/>is increased |
| Uptime<br/>(seconds)             | Node Uptime since last restart                                                                                                                   |
| Heap<br/>Memory<br/>(MB)         | Not applicable with ScyllaDB                                                                                                                     |
| Off<br/>Heap<br/>Memory<br/>(MB) | Shows how much memory is used,<br/>by all tables, for Memtables ,<br/>Bloom filters, Indexes, and<br/>Compression Metadata                       |
| Data<br/>Center                  | Within which Data Center the<br/>node is located                                                                                                 |
| Rack                             | The Rack that the node is<br/>located on                                                                                                         |
| Exceptions                       | Not applicable with ScyllaDB                                                                                                                     |
| Key<br/>Cache                    | Not applicable with ScyllaDB                                                                                                                     |
| Row<br/>Cache                    | Row Cache usage                                                                                                                                  |
| Counter<br/>Cache                | Not applicable with ScyllaDB                                                                                                                     |
| Token                            | List of the node tokens                                                                                                                          |

[Nodetool Reference](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/nodetool.md)
