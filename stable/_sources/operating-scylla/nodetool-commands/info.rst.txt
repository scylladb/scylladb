Nodetool info
=============
**info** - Provides detailed statistics for a specific node in the
cluster, such as uptime, load, key cache hit rate, the total count of all
exceptions, and more. If needed, you can specify which node to view by
using the --host argument with the host IP address.

For example:

::

    nodetool info

Example output:

::

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

+-----------+------------------------------+
| Parameter |Description                   |
|           |                              |
|           |                              |
|           |                              |
|           |                              |
+===========+==============================+
| ID        |Node ID                       |
+-----------+------------------------------+
| Gossip    |Gossip status                 |
| active    |                              |
+-----------+------------------------------+
| Thrift    |Thrift status                 |
| active    |                              |
+-----------+------------------------------+
| Native    |Native Transport status (CQL) |
| Transport |                              |
| active    |                              |
|           |                              |
+-----------+------------------------------+
| Load      |How much hard drive space is  |
|           |used by SSTable               |
|           |(updates every 60 seconds)    |
|           |                              |
|           |                              |
|           |                              |
+-----------+------------------------------+
| Generation|Generation No - When a major  |
| No        |change occurs on a node, such |
|           |as a restart or a changing    |
|           |tokens, the Generation number |
|           |is increased                  |
|           |                              |
|           |                              |
|           |                              |
|           |                              |
|           |                              |
|           |                              |
|           |                              |
|           |                              |
|           |                              |
|           |                              |
|           |                              |
|           |                              |
+-----------+------------------------------+
| Uptime    |Node Uptime since last restart|
| (seconds) |                              |
|           |                              |
|           |                              |
|           |                              |
+-----------+------------------------------+
| Heap      |Not applicable with ScyllaDB  |
| Memory    |                              |
| (MB)      |                              |
|           |                              |
+-----------+------------------------------+
| Off       |Shows how much memory is used,|
| Heap      |by all tables, for Memtables ,|
| Memory    |Bloom filters, Indexes, and   |
| (MB)      |Compression Metadata          |
|           |                              |
|           |                              |
|           |                              |
|           |                              |
|           |                              |
|           |                              |
|           |                              |
|           |                              |
|           |                              |
|           |                              |
|           |                              |
+-----------+------------------------------+
| Data      |Within which Data Center the  |
| Center    |node is located               |
|           |                              |
|           |                              |
|           |                              |
+-----------+------------------------------+
| Rack      |The Rack that the node is     |
|           |located on                    |
|           |                              |
|           |                              |
|           |                              |
+-----------+------------------------------+
| Exceptions|Not applicable with ScyllaDB  |
|           |                              |
|           |                              |
|           |                              |
+-----------+------------------------------+
| Key       |Not applicable with ScyllaDB  |
| Cache     |                              |
|           |                              |
|           |                              |
+-----------+------------------------------+
| Row       |Row Cache usage               |
| Cache     |                              |
+-----------+------------------------------+
| Counter   |Not applicable with ScyllaDB  |
| Cache     |                              |
|           |                              |
|           |                              |
+-----------+------------------------------+
| Token     |List of the node tokens       |
|           |                              |
|           |                              |
+-----------+------------------------------+

.. include:: nodetool-index.rst
