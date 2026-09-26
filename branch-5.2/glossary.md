<a id="glossary"></a>

# Glossary

<a id="term-Anti-entropy"></a>

Anti-entropy
: A state where data is in order and organized. Scylla has processes in place to make sure that data is antientropic where all replicas contain the most recent data and that data is consistent between replicas. See [Scylla Anti-Entropy](https://opensource.docs.scylladb.com/branch-5.2/architecture/anti-entropy/index.md).

<a id="term-Bootstrap"></a>

Bootstrap
: When a new node is added to a cluster, the bootstrap process ensures that the data in the cluster is automatically redistributed to the new node. A new node in this case is an empty node without system tables or data. See [bootstrap](https://opensource.docs.scylladb.com/branch-5.2/kb/compaction.md#temporary-fallback-to-stcs).

<a id="term-CAP-Theorem"></a>

CAP Theorem
: The CAP Theorem is the notion that **C** (Consistency), **A** (Availability) and **P** (Partition Tolerance) of data are mutually dependent in a distributed system. Increasing any 2 of these factors will reduce the third. Scylla chooses availability and partition tolerance over consistency. See [Fault Tolerance](https://opensource.docs.scylladb.com/branch-5.2/architecture/architecture-fault-tolerance.md).

<a id="term-Cluster"></a>

Cluster
: One or multiple Scylla nodes, acting in concert, which own a single contiguous token range. State is communicated between nodes in the cluster via the Gossip protocol. See [Ring Architecture](https://opensource.docs.scylladb.com/branch-5.2/architecture/ringarchitecture/index.md).

<a id="term-Clustering-Key"></a>

Clustering Key
: A single or multi-column clustering key determines a row’s uniqueness and sort order on disk within a partition. See [Ring Architecture](https://opensource.docs.scylladb.com/branch-5.2/architecture/ringarchitecture/index.md).

<a id="term-Column-Family"></a>

Column Family
: See [table](#term-Table).

<a id="term-Compaction"></a>

Compaction
: The process of reading several SSTables, comparing the data and time stamps and then writing one SSTable containing the merged, most recent, information. See [Compaction Strategies](https://opensource.docs.scylladb.com/branch-5.2/architecture/compaction/compaction-strategies.md).

<a id="term-Compaction-Strategy"></a>

Compaction Strategy
: Determines which of the SSTables will be compacted, and when. See [Compaction Strategies](https://opensource.docs.scylladb.com/branch-5.2/architecture/compaction/compaction-strategies.md).

<a id="term-Consistency-Level-CL"></a>

Consistency Level (CL)
: A dynamic value which dictates the number of replicas (in a cluster) that must acknowledge a read or write operation. This value is set by the client on a per operation basis. For the CQL Shell, the consistency level defaults to ONE for read and write operations. See [Consistency Levels](https://opensource.docs.scylladb.com/branch-5.2/cql/consistency.md).

<a id="term-Date-tiered-compaction-strategy-DTCS"></a>

Date-tiered compaction strategy (DTCS)
:  is designed for time series data, but should not be used. Use [Time-Window Compaction Strategy](#term-Time-window-compaction-strategy). See [Compaction Strategies](https://opensource.docs.scylladb.com/branch-5.2/architecture/compaction/compaction-strategies.md).

<a id="term-Dummy-Rows"></a>

Dummy Rows
: Cache dummy rows are entries in the row set, which have a clustering position, although they do not represent CQL rows written by users.  Scylla cache uses them to mark boundaries of population ranges, to represent the information that the whole range is complete, and there is no need to go to sstables to read the gaps between existing row entries when scanning.

<a id="term-Entropy"></a>

Entropy
: A state where data is not consistent. This is the result when replicas are not synced and data is random. Scylla has measures in place to be antientropic. See [Scylla Anti-Entropy](https://opensource.docs.scylladb.com/branch-5.2/architecture/anti-entropy/index.md).

<a id="term-Eventual-Consistency"></a>

Eventual Consistency
: In Scylla, when considering the [CAP Theorem](#term-CAP-Theorem), availability and partition tolerance are considered a higher priority than consistency.

<a id="term-Hint"></a>

Hint
: A short record of a write request that is held by the co-ordinator until the unresponsive node becomes responsive again, at which point the write request data in the hint is written to the replica node. See [Hinted Handoff](https://opensource.docs.scylladb.com/branch-5.2/architecture/anti-entropy/hinted-handoff.md).

<a id="term-Hinted-Handoff"></a>

Hinted Handoff
: Reduces data inconsistency which can occur when a node is down or there is network congestion. In Scylla, when data is written and there is an unresponsive replica, the coordinator writes itself a hint. When the node recovers, the coordinator sends the node the pending hints to ensure that it has the data it should have received. See [Hinted Handoff](https://opensource.docs.scylladb.com/branch-5.2/architecture/anti-entropy/hinted-handoff.md).

<a id="term-Idempotent"></a>

Idempotent
: Denoting an element of a set which is unchanged in value when multiplied or otherwise operated on by itself. [Scylla Counters](https://opensource.docs.scylladb.com/branch-5.2/using-scylla/counters.md) are not indepotent because in the case of a write failure, the client cannot safely retry the request.

<a id="term-JBOD"></a>

JBOD
: JBOD or Just another Bunch Of Disks is a non-raid storage system using a server with multiple disks in order to instantiate a separate file system per disk. The benefit is that if a single disk fails, only it needs to be replaced and not the whole disk array. The disadvantage is that free space and load may not be evenly distributed. See the [FAQ](https://opensource.docs.scylladb.com/branch-5.2/faq.md#faq-raid0-required).

<a id="term-Key-Management-Interoperability-Protocol-KMIP"></a>

Key Management Interoperability Protocol (KMIP)
:  is a communication protocol that defines message formats for storing keys on a key management server (KMIP server). You can use a KMIP server to protect your keys when using Encryption at Rest. See [Encryption at Rest](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/security/encryption-at-rest.md).

<a id="term-Keyspace"></a>

Keyspace
: A collection of tables with attributes which define how data is replicated on nodes. See [Ring Architecture](https://opensource.docs.scylladb.com/branch-5.2/architecture/ringarchitecture/index.md).

<a id="term-Leveled-compaction-strategy-LCS"></a>

Leveled compaction strategy (LCS)
:  uses small, fixed-size (by default 160 MB) SSTables divided into different levels. See [Compaction Strategies](https://opensource.docs.scylladb.com/branch-5.2/architecture/compaction/compaction-strategies.md).

<a id="term-Log-structured-merge-LSM"></a>

Log-structured-merge (LSM)
: A technique of keeping sorted files and merging them. LSM is a data structure that maintains key-value pairs. See [Compaction](https://opensource.docs.scylladb.com/branch-5.2/kb/compaction.md)

<a id="term-Logical-Core-lcore"></a>

Logical Core (lcore)
: A hyperthreaded core on a hyperthreaded system, or a physical core on a system without hyperthreading.

<a id="term-MemTable"></a>

MemTable
: An in-memory data structure servicing both reads and writes. Once full, the Memtable flushes to an [SSTable](#term-SSTable). See [Compaction Strategies](https://opensource.docs.scylladb.com/branch-5.2/architecture/compaction/compaction-strategies.md).

<a id="term-Mutation"></a>

Mutation
: A change to data such as column or columns to insert, or a deletion. See [Hinted Handoff](https://opensource.docs.scylladb.com/branch-5.2/architecture/anti-entropy/hinted-handoff.md).

<a id="term-Node"></a>

Node
: A single installed instance of Scylla. See [Ring Architecture](https://opensource.docs.scylladb.com/branch-5.2/architecture/ringarchitecture/index.md).

<a id="term-Nodetool"></a>

Nodetool
: A simple command-line interface for administering a Scylla node. A nodetool command can display a given node’s exposed operations and attributes. Scylla’s nodetool contains a subset of these operations. See [Ring Architecture](https://opensource.docs.scylladb.com/branch-5.2/architecture/ringarchitecture/index.md).

<a id="term-Partition"></a>

Partition
: A subset of data that is stored on a node and replicated across nodes. There are two ways to consider a partition. In CQL, a partition appears as a group of sorted rows, and is the unit of access for queried data, given that most queries access a single partition. On the physical layer, a partition is a unit of data stored on a node and is identified by a partition key. See [Ring Architecture](https://opensource.docs.scylladb.com/branch-5.2/architecture/ringarchitecture/index.md).

<a id="term-Partition-Key"></a>

Partition Key
: The unique identifier for a partition, a partition key may be hashed from the first column in the primary key. A partition key may also be hashed from a set of columns, often referred to as a compound primary key. A partition key determines which virtual node gets the first partition replica. See [Ring Architecture](https://opensource.docs.scylladb.com/branch-5.2/architecture/ringarchitecture/index.md).

<a id="term-Partitioner"></a>

Partitioner
: A hash function for computing which data is stored on which node in the cluster. The partitioner takes a partition key as an input, and returns a ring token as an output. By default Scylla uses the 64 bit Murmurhash3 function and this hash range is numerically represented as a signed 64bit integer, see [Ring Architecture](https://opensource.docs.scylladb.com/branch-5.2/architecture/ringarchitecture/index.md).

<a id="term-Primary-Key"></a>

Primary Key
: In a CQL table definition, the primary key clause specifies the partition key and optional clustering key. These keys uniquely identify each partition and row within a partition. See [Ring Architecture](https://opensource.docs.scylladb.com/branch-5.2/architecture/ringarchitecture/index.md).

<a id="term-Quorum"></a>

Quorum
: Quorum is a *global* consistency level setting across the entire cluster including all data centers. See [Consistency Levels](https://opensource.docs.scylladb.com/branch-5.2/cql/consistency.md).

<a id="term-Read-Amplification"></a>

Read Amplification
: Excessive read requests which require many SSTables. RA is calculated by the number of disk reads per query. High RA occurs when there are many pages to read in order to answer a query.  See [Compaction Strategies](https://opensource.docs.scylladb.com/branch-5.2/architecture/compaction/compaction-strategies.md).

<a id="term-Read-Operation"></a>

Read Operation
: A  read operation occurs when an application gets information from an SSTable and does not change that information in any way. See [Fault Tolerance](https://opensource.docs.scylladb.com/branch-5.2/architecture/architecture-fault-tolerance.md).

<a id="term-Read-Repair"></a>

Read Repair
: An anti-entropy mechanism for read operations ensuring that replicas are updated with most recently updated data. These repairs run automatically, asynchronously, and in the background. See [Scylla Read Repair](https://opensource.docs.scylladb.com/branch-5.2/architecture/anti-entropy/read-repair.md).

<a id="term-Reconciliation"></a>

Reconciliation
: A verification phase during a data migration where the target data is compared against original source data to ensure that the migration architecture has transferred the data correctly. See [Scylla Read Repair](https://opensource.docs.scylladb.com/branch-5.2/architecture/anti-entropy/read-repair.md).

<a id="term-Repair"></a>

Repair
: A process which runs in the background and synchronizes the data between nodes, so that eventually, all the replicas hold the same data. See [Scylla Repair](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/procedures/maintenance/repair.md).

<a id="term-Replication"></a>

Replication
: The process of replicating data across nodes in a cluster. See [Fault Tolerance](https://opensource.docs.scylladb.com/branch-5.2/architecture/architecture-fault-tolerance.md).

<a id="term-Replication-Factor-RF"></a>

Replication Factor (RF)
: The total number of replica nodes across a given cluster. An  of 1 means that the data will only exist on a single node in the cluster and will not have any fault tolerance. This number is a setting defined for each keyspace. All replicas share equal priority; there are no primary or master replicas. An RF for any table, can be defined for each . See [Fault Tolerance](https://opensource.docs.scylladb.com/branch-5.2/architecture/architecture-fault-tolerance.md).

<a id="term-Reshape"></a>

Reshape
: Rewrite a set of SSTables to satisfy a compaction strategy’s criteria. For example, restoring data from an old backup or before the strategy update.

<a id="term-Reshard"></a>

Reshard
: Splitting an SSTable, that is owned by more than one shard (core), into SSTables that are owned by a single shard. For example: when restoring data from a different server, importing SSTables from Apache Cassandra, or changing the number of cores in a machine (upscale).

<a id="term-Shard"></a>

Shard
: Each Scylla node is internally split into *shards*, an independent thread bound to a dedicated core.
  Each shard of data is allotted CPU, RAM, persistent storage, and networking resources which it uses as efficiently as possible.
  See [Scylla Shard per Core Architecture](https://www.scylladb.com/product/technology/shard-per-core-architecture/) for more information.

<a id="term-Shedding"></a>

Shedding
: Dropping requests to protect the system. This will occur if the request is too large or exceeds the max number of concurrent requests per shard.

<a id="term-Size-tiered-compaction-strategy"></a>

Size-tiered compaction strategy
: Triggers when the system has enough (four by default) similarly sized SSTables.  See [Compaction Strategies](https://opensource.docs.scylladb.com/branch-5.2/architecture/compaction/compaction-strategies.md).

<a id="term-Snapshot"></a>

Snapshot
: Snapshots in Scylla are an essential part of the backup and restore mechanism. Whereas in other databases a backup starts with creating a copy of a data file (cold backup, hot backup, shadow copy backup), in Scylla the process starts with creating a table or keyspace snapshot.  See [Scylla Snapshots](https://opensource.docs.scylladb.com/branch-5.2/kb/snapshots.md).

<a id="term-Snitch"></a>

Snitch
: The mapping from the IP addresses of nodes to physical and virtual locations, such as racks and data centers. There are several types of snitches. The type of snitch affects the request routing mechanism. See [Scylla Snitches](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/system-configuration/snitch.md).

<a id="term-Space-amplification"></a>

Space amplification
: Excessive disk space usage which requires that the disk be larger than a perfectly-compacted representation of the data (i.e., all the data in one single SSTable). SA is calculated as the ratio of the size of database files on a disk to the actual data size. High SA occurs when there is more disk space being used than the size of the data.  See [Compaction Strategies](https://opensource.docs.scylladb.com/branch-5.2/architecture/compaction/compaction-strategies.md).

<a id="term-SSTable"></a>

SSTable
: A concept borrowed from Google Big Table, SSTables or Sorted String Tables store a series of immutable rows where each row is identified by its row key.  See [Compaction Strategies](https://opensource.docs.scylladb.com/branch-5.2/architecture/compaction/compaction-strategies.md). The SSTable format is a persistent file format. See [Scylla SSTable Format](https://opensource.docs.scylladb.com/branch-5.2/architecture/sstable/index.md).

<a id="term-Table"></a>

Table
: A collection of columns fetched by row. Columns are ordered by Clustering Key. See [Ring Architecture](https://opensource.docs.scylladb.com/branch-5.2/architecture/ringarchitecture/index.md).

<a id="term-Time-window-compaction-strategy"></a>

Time-window compaction strategy
: TWCS is designed for time series data and replaced Date-tiered compaction. See [Compaction Strategies](https://opensource.docs.scylladb.com/branch-5.2/architecture/compaction/compaction-strategies.md).

<a id="term-Token"></a>

Token
: A value in a range, used to identify both nodes and partitions. Each node in a Scylla cluster is given an (initial) token, which defines the end of the range a node handles. See [Ring Architecture](https://opensource.docs.scylladb.com/branch-5.2/architecture/ringarchitecture/index.md).

<a id="term-Token-Range"></a>

Token Range
: The total range of potential unique identifiers supported by the partitioner. By default, each Scylla node in the cluster handles 256 token ranges. Each token range corresponds to a Vnode. Each range of hashes in turn is a segment of the total range of a given hash function. See [Ring Architecture](https://opensource.docs.scylladb.com/branch-5.2/architecture/ringarchitecture/index.md).

<a id="term-Tombstone"></a>

Tombstone
: A marker that indicates that data has been deleted. A large number of tombstones may impact read performance and disk usage, so an efficient tombstone garbage collection strategy should be employed. See [Tombstones GC options](https://opensource.docs.scylladb.com/branch-5.2/cql/ddl.md#ddl-tombstones-gc).

<a id="term-Tunable-Consistency"></a>

Tunable Consistency
: The possibility for unique, per-query, Consistency Level settings. These are incremental and override fixed database settings intended to enforce data consistency. Such settings may be set directly from a CQL statement when response speed for a given query or operation is more important. See [Fault Tolerance](https://opensource.docs.scylladb.com/branch-5.2/architecture/architecture-fault-tolerance.md).

<a id="term-Virtual-node"></a>

Virtual node
: A range of tokens owned by a single Scylla node. Scylla nodes are configurable and support a set of . In legacy token selection, a node owns one token (or token range) per node. With Vnodes, a node can own many tokens or token ranges; within a cluster, these may be selected randomly from a non-contiguous set. In a Vnode configuration, each token falls within a specific token range which in turn is represented as a Vnode. Each Vnode is then allocated to a physical node in the cluster. See [Ring Architecture](https://opensource.docs.scylladb.com/branch-5.2/architecture/ringarchitecture/index.md).

<a id="term-Workload"></a>

Workload
: A database category that allows you to manage different sources of database activities, such as requests or administrative activities. By defining workloads, you can specify how ScyllaDB will process those activities. For example, you can prioritize one workload over another (e.g., user requests over administrative activities). See [Workload Prioritization](https://opensource.docs.scylladb.com/branch-5.2/using-scylla/workload-prioritization.md).

<a id="term-Write-Amplification"></a>

Write Amplification
: Excessive compaction of the same data.  is calculated by the ratio of bytes written to storage versus bytes written to the database. High WA occurs when there are more bytes/second written to storage than are actually written to the database. See [Compaction Strategies](https://opensource.docs.scylladb.com/branch-5.2/architecture/compaction/compaction-strategies.md).

<a id="term-Write-Operation"></a>

Write Operation
: A write operation occurs when information is added or removed from an SSTable. See [Fault Tolerance](https://opensource.docs.scylladb.com/branch-5.2/architecture/architecture-fault-tolerance.md).
