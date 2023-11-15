.. _glossary:

=====================
Glossary
=====================

.. glossary::
    :sorted:

    Bootstrap 
      When a new node is added to a cluster, the bootstrap process ensures that the data in the cluster is automatically redistributed to the new node. A new node in this case is an empty node without system tables or data. See :ref:`bootstrap <temporary-fallback-to-stcs>`.

    Anti-entropy
      A state where data is in order and organized. Scylla has processes in place to make sure that data is antientropic where all replicas contain the most recent data and that data is consistent between replicas. See :doc:`Scylla Anti-Entropy </architecture/anti-entropy/index>`.
    
    CAP Theorem
      The CAP Theorem is the notion that **C** (Consistency), **A** (Availability) and **P** (Partition Tolerance) of data are mutually dependent in a distributed system. Increasing any 2 of these factors will reduce the third. Scylla chooses availability and partition tolerance over consistency. See :doc:`Fault Tolerance </architecture/architecture-fault-tolerance>`.
 
    Cluster 
      One or multiple Scylla nodes, acting in concert, which own a single contiguous token range. State is communicated between nodes in the cluster via the Gossip protocol. See :doc:`Ring Architecture </architecture/ringarchitecture/index>`.

    Clustering Key
      A single or multi-column clustering key determines a row’s uniqueness and sort order on disk within a partition. See :doc:`Ring Architecture </architecture/ringarchitecture/index>`.

    Column Family
      See :term:`table<Table>`.

    Compaction
      The process of reading several SSTables, comparing the data and time stamps and then writing one SSTable containing the merged, most recent, information. See :doc:`Compaction Strategies</architecture/compaction/compaction-strategies/>`.

    Compaction Strategy
      Determines which of the SSTables will be compacted, and when. See :doc:`Compaction Strategies</architecture/compaction/compaction-strategies/>`.

    Consistency Level (CL)
      A dynamic value which dictates the number of replicas (in a cluster) that must acknowledge a read or write operation. This value is set by the client on a per operation basis. For the CQL Shell, the consistency level defaults to ONE for read and write operations. See :doc:`Consistency Levels </cql/consistency>`.

    Quorum
      Quorum is a *global* consistency level setting across the entire cluster including all data centers. See :doc:`Consistency Levels </cql/consistency>`.

    Entropy
      A state where data is not consistent. This is the result when replicas are not synced and data is random. Scylla has measures in place to be antientropic. See :doc:`Scylla Anti-Entropy </architecture/anti-entropy/index>`.

    Eventual Consistency
      In Scylla, when considering the :term:`CAP Theorem <CAP Theorem>`, availability and partition tolerance are considered a higher priority than consistency.

    Hint
      A short record of a write request that is held by the co-ordinator until the unresponsive node becomes responsive again, at which point the write request data in the hint is written to the replica node. See :doc:`Hinted Handoff </architecture/anti-entropy/hinted-handoff>`. 

    Hinted Handoff
      Reduces data inconsistency which can occur when a node is down or there is network congestion. In Scylla, when data is written and there is an unresponsive replica, the coordinator writes itself a hint. When the node recovers, the coordinator sends the node the pending hints to ensure that it has the data it should have received. See :doc:`Hinted Handoff </architecture/anti-entropy/hinted-handoff>`. 
    
    Idempotent   
      Denoting an element of a set which is unchanged in value when multiplied or otherwise operated on by itself. :doc:`Scylla Counters </using-scylla/counters>` are not indepotent because in the case of a write failure, the client cannot safely retry the request.
    
    JBOD
      JBOD or Just another Bunch Of Disks is a non-raid storage system using a server with multiple disks in order to instantiate a separate file system per disk. The benefit is that if a single disk fails, only it needs to be replaced and not the whole disk array. The disadvantage is that free space and load may not be evenly distributed. See the :ref:`FAQ <faq-raid0-required>`.

    Keyspace
      A collection of tables with attributes which define how data is replicated on nodes. See :doc:`Ring Architecture </architecture/ringarchitecture/index>`.
    
    Leveled compaction strategy (LCS)
      :abbr:`LCS (Leveled compaction strategy)` uses small, fixed-size (by default 160 MB) SSTables divided into different levels. See :doc:`Compaction Strategies</architecture/compaction/compaction-strategies/>`.

    Liveness
      The ability to update a configuration property without restarting the node. Properties that support live updates can be updated via the ``system.config`` virtual table or the REST API.
      The change will take effect without a node restart, changing the value in the config file, then sending ``SIGHUP`` to the scylla-process, triggering it to re-read its configuration.
    
    Log-structured-merge (LSM)
      A technique of keeping sorted files and merging them. LSM is a data structure that maintains key-value pairs. See :doc:`Compaction </kb/compaction>`

    Logical Core (lcore)
      A hyperthreaded core on a hyperthreaded system, or a physical core on a system without hyperthreading.

    MemTable
     An in-memory data structure servicing both reads and writes. Once full, the Memtable flushes to an :term:`SSTable<SSTable>`. See :doc:`Compaction Strategies</architecture/compaction/compaction-strategies/>`.

    Mutation
     A change to data such as column or columns to insert, or a deletion. See :doc:`Hinted Handoff </architecture/anti-entropy/hinted-handoff>`. 

    Node
     A single installed instance of Scylla. See :doc:`Ring Architecture </architecture/ringarchitecture/index>`.

    Nodetool
     A simple command-line interface for administering a Scylla node. A nodetool command can display a given node’s exposed operations and attributes. Scylla’s nodetool contains a subset of these operations. See :doc:`Ring Architecture </architecture/ringarchitecture/index>`.

    Primary Key
     In a CQL table definition, the primary key clause specifies the partition key and optional clustering key. These keys uniquely identify each partition and row within a partition. See :doc:`Ring Architecture </architecture/ringarchitecture/index>`.

    Partition
     A subset of data that is stored on a node and replicated across nodes. There are two ways to consider a partition. In CQL, a partition appears as a group of sorted rows, and is the unit of access for queried data, given that most queries access a single partition. On the physical layer, a partition is a unit of data stored on a node and is identified by a partition key. See :doc:`Ring Architecture </architecture/ringarchitecture/index>`.

    Partition Key
     The unique identifier for a partition, a partition key may be hashed from the first column in the primary key. A partition key may also be hashed from a set of columns, often referred to as a compound primary key. A partition key determines which virtual node gets the first partition replica. See :doc:`Ring Architecture </architecture/ringarchitecture/index>`.

    Partitioner
     A hash function for computing which data is stored on which node in the cluster. The partitioner takes a partition key as an input, and returns a ring token as an output. By default Scylla uses the 64 bit :term:`MurmurHash3` function and this hash range is numerically represented as a signed 64bit integer, see :doc:`Ring Architecture </architecture/ringarchitecture/index>`.

    Read Amplification
     Excessive read requests which require many SSTables. RA is calculated by the number of disk reads per query. High RA occurs when there are many pages to read in order to answer a query.  See :doc:`Compaction Strategies</architecture/compaction/compaction-strategies/>`.

    Read Operation
      A  read operation occurs when an application gets information from an SSTable and does not change that information in any way. See :doc:`Fault Tolerance </architecture/architecture-fault-tolerance>`.

    Read Repair
      An anti-entropy mechanism for read operations ensuring that replicas are updated with most recently updated data. These repairs run automatically, asynchronously, and in the background. See :doc:`Scylla Read Repair </architecture/anti-entropy/read-repair>`.

    Reconciliation
      A verification phase during a data migration where the target data is compared against original source data to ensure that the migration architecture has transferred the data correctly. See :doc:`Scylla Read Repair </architecture/anti-entropy/read-repair>`.

    Repair
      A process which runs in the background and synchronizes the data between nodes, so that eventually, all the replicas hold the same data. See :doc:`Scylla Repair </operating-scylla/procedures/maintenance/repair>`.
    
    Replication
      The process of replicating data across nodes in a cluster. See :doc:`Fault Tolerance </architecture/architecture-fault-tolerance>`.

    Replication Factor (RF)
      The total number of replica nodes across a given cluster. An :abbr:`RF (Replication Factor)` of 1 means that the data will only exist on a single node in the cluster and will not have any fault tolerance. This number is a setting defined for each keyspace. All replicas share equal priority; there are no primary or master replicas. An RF for any table, can be defined for each :abbr:`DC (Data Center)`. See :doc:`Fault Tolerance </architecture/architecture-fault-tolerance>`.

    Repair Based Node Operations (RBNO)
      :abbr:`RBNO (Repair Based Node Operations)` is an internal ScyllaDB mechanism that uses repair to 
      synchronize data between the nodes in a cluster instead of using streaming. RBNO significantly 
      improve database performance and data consistency.

      RBNO is enabled by default for a subset node operations. 
      See :doc:`Repair Based Node Operations </operating-scylla/procedures/cluster-management/repair-based-node-operation>` for details.

    Shard
      Each Scylla node is internally split into *shards*, an independent thread bound to a dedicated core.
      Each shard of data is allotted CPU, RAM, persistent storage, and networking resources which it uses as efficiently as possible.
      See `Scylla Shard per Core Architecture <https://www.scylladb.com/product/technology/shard-per-core-architecture/>`_ for more information.

    Size-tiered compaction strategy
      Triggers when the system has enough (four by default) similarly sized SSTables.  See :doc:`Compaction Strategies</architecture/compaction/compaction-strategies/>`.

    Snapshot
      Snapshots in Scylla are an essential part of the backup and restore mechanism. Whereas in other databases a backup starts with creating a copy of a data file (cold backup, hot backup, shadow copy backup), in Scylla the process starts with creating a table or keyspace snapshot.  See :doc:`Scylla Snapshots </kb/snapshots>`.

    Snitch
      The mapping from the IP addresses of nodes to physical and virtual locations, such as racks and data centers. There are several types of snitches. The type of snitch affects the request routing mechanism. See :doc:`Scylla Snitches </operating-scylla/system-configuration/snitch/>`.

    Space amplification
      Excessive disk space usage which requires that the disk be larger than a perfectly-compacted representation of the data (i.e., all the data in one single SSTable). SA is calculated as the ratio of the size of database files on a disk to the actual data size. High SA occurs when there is more disk space being used than the size of the data.  See :doc:`Compaction Strategies</architecture/compaction/compaction-strategies/>`.

    SSTable
      A concept borrowed from Google Big Table, SSTables or Sorted String Tables store a series of immutable rows where each row is identified by its row key.  See :doc:`Compaction Strategies</architecture/compaction/compaction-strategies/>`. The SSTable format is a persistent file format. See :doc:`Scylla SSTable Format</architecture/sstable/index>`.

    Table
      A collection of columns fetched by row. Columns are ordered by Clustering Key. See :doc:`Ring Architecture </architecture/ringarchitecture/index>`.

    Time-window compaction strategy
      TWCS is designed for time series data. See :doc:`Compaction Strategies</architecture/compaction/compaction-strategies/>`.

    Token
      A value in a range, used to identify both nodes and partitions. Each node in a Scylla cluster is given an (initial) token, which defines the end of the range a node handles. See :doc:`Ring Architecture </architecture/ringarchitecture/index>`.

    Token Range
      The total range of potential unique identifiers supported by the partitioner. By default, each Scylla node in the cluster handles 256 token ranges. Each token range corresponds to a Vnode. Each range of hashes in turn is a segment of the total range of a given hash function. See :doc:`Ring Architecture </architecture/ringarchitecture/index>`.

    Tombstone
      A marker that indicates that data has been deleted. A large number of tombstones may impact read performance and disk usage, so an efficient tombstone garbage collection strategy should be employed. See :ref:`Tombstones GC options <ddl-tombstones-gc>`.
    
    Tunable Consistency
      The possibility for unique, per-query, Consistency Level settings. These are incremental and override fixed database settings intended to enforce data consistency. Such settings may be set directly from a CQL statement when response speed for a given query or operation is more important. See :doc:`Fault Tolerance </architecture/architecture-fault-tolerance>`.

    Virtual node
      A range of tokens owned by a single Scylla node. Scylla nodes are configurable and support a set of :abbr:`Vnodes (virtual nodes)`. In legacy token selection, a node owns one token (or token range) per node. With Vnodes, a node can own many tokens or token ranges; within a cluster, these may be selected randomly from a non-contiguous set. In a Vnode configuration, each token falls within a specific token range which in turn is represented as a Vnode. Each Vnode is then allocated to a physical node in the cluster. See :doc:`Ring Architecture </architecture/ringarchitecture/index>`.

    Write Amplification
      Excessive compaction of the same data. :abbr:`WA (Write amplification)` is calculated by the ratio of bytes written to storage versus bytes written to the database. High WA occurs when there are more bytes/second written to storage than are actually written to the database. See :doc:`Compaction Strategies</architecture/compaction/compaction-strategies/>`.

    Write Operation
      A write operation occurs when information is added or removed from an SSTable. See :doc:`Fault Tolerance </architecture/architecture-fault-tolerance>`.

    Reshard
       Splitting an SSTable, that is owned by more than one shard (core), into SSTables that are owned by a single shard. For example: when restoring data from a different server, importing SSTables from Apache Cassandra, or changing the number of cores in a machine (upscale).

    Reshape
       Rewrite a set of SSTables to satisfy a compaction strategy’s criteria. For example, restoring data from an old backup or before the strategy update.

    Shedding
       Dropping requests to protect the system. This will occur if the request is too large or exceeds the max number of concurrent requests per shard.

    Dummy Rows
       Cache dummy rows are entries in the row set, which have a clustering position, although they do not represent CQL rows written by users.  Scylla cache uses them to mark boundaries of population ranges, to represent the information that the whole range is complete, and there is no need to go to sstables to read the gaps between existing row entries when scanning.
      
    Workload
      A database category that allows you to manage different sources of database activities, such as requests or administrative activities. By defining workloads, you can specify how ScyllaDB will process those activities. For example, `ScyllaDB Enterprise <https://enterprise.docs.scylladb.com/>`_
      ships with a feature that allows you to prioritize one workload over another (e.g., user requests over administrative activities). See `Workload Prioritization <https://enterprise.docs.scylladb.com/stable/using-scylla/workload-prioritization.html>`_.

    MurmurHash3
       A hash function `created by Austin Appleby <https://en.wikipedia.org/wiki/MurmurHash>`_, and used by the :term:`Partitioner` to distribute the partitions between nodes.
       The name comes from two basic operations, multiply (MU) and rotate (R), used in its inner loop.
       The MurmurHash3 version used in ScyllaDB originated from `Apache Cassandra <https://commons.apache.org/proper/commons-codec/apidocs/org/apache/commons/codec/digest/MurmurHash3.html>`_, and is **not** identical to the `official MurmurHash3 calculation <https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/utils/MurmurHash.java#L31-L33>`_. More `here <https://github.com/russss/murmur3-cassandra>`_.

