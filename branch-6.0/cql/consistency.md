# Consistency Levels

A [Consistency Level (CL)](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Consistency-Level-CL) is a dynamic value which dictates the number of replicas (in a cluster) that must acknowledge a read or write operation in order for the coordinator node to determine the operation was successful.
CLs can be used with any transaction including LWTs.

This value is set by the client on a per operation basis. For the CQL Shell, the consistency level defaults to ONE for read and write operations.
If there is a conflict in settings, the CQLSH setting supersedes a consistency level global setting.

## Syntax

```cql
CONSISTENCY <consistency level>
```

### Examples

Set CONSISTENCY to force the majority of the nodes to respond:

```cql
CONSISTENCY QUORUM
```

Set level to serial for LWT read requests:

```cql
CONSISTENCY SERIAL
```

Consistency level set to SERIAL.

<a id="consistency-levels-reference"></a>

### Consistency Levels Reference

The following table describes the different levels you can set.

| Consistency Level        | Which Replicas Must Respond                                                                                                                                                                                                                                          | Consistency                   | Availability                          |
|--------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------|---------------------------------------|
| ANY (Write Only)         | Closest replica, as determined by the snitch. If all replica nodes are down, write succeeds after a [hinted handoff](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Hinted-Handoff). Provides low latency, guarantees writes never fail. | Lowest (WRITE)                | Highest (WRITE)                       |
| ONE                      | The closest replica as determined by the [Snitch](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Snitch). Consistency requirements are not too strict.                                                                                   | Lowest (READ)                 | Highest (READ)                        |
| TWO                      | The closest two replicas as determined by the Snitch.                                                                                                                                                                                                                |                               |                                       |
| THREE                    | The closest three replicas as determined by the Snitch.                                                                                                                                                                                                              |                               |                                       |
| QUORUM                   | A simple majority of all replicas across all datacenters. This CL allows for some level of failure                                                                                                                                                                   |                               |                                       |
| LOCAL_QUORUM             | Same as QUORUM, but confined to the same datacenter as the coordinator.                                                                                                                                                                                              | Low in multi-data centers     |                                       |
| ALL                      | *All* replicas in the cluster                                                                                                                                                                                                                                        | Highest                       | Lowest (may cause performance issues) |
| EACH_QUORUM (Write Only) | A simple majority in each datacenter.                                                                                                                                                                                                                                | Same across the datacenters.  |                                       |
| LOCAL_ONE                | Same as ONE, but confined to the local datacenter.                                                                                                                                                                                                                   |                               |                                       |
| SERIAL                   | Returns results with the most recent data. Including uncommitted in-flight LWTs. Writes are not supported, but read transactions are supported.                                                                                                                      | Linearizable                  |                                       |
| LOCAL_SERIAL             | Same as SERIAL, but confined to a local datacenter. Writes are not supported, but read transactions are supported.                                                                                                                                                   | Linearizable for the local DC |                                       |

## Display the Current CL in CQLSh

To display your current CL in your CQLsh session, use the CONSISTENCY Command with no options.

```cql
CONSISTENCY
```

returns

```cql
Current consistency level is ALL.
```

## Additional Information

* [Consistency Level Calculator](https://opensource.docs.scylladb.com/branch-6.0/cql/consistency-calculator.md)
* [Fault Tolerance](https://opensource.docs.scylladb.com/branch-6.0/architecture/architecture-fault-tolerance.md)
* [Consistency Level Compatibility](https://opensource.docs.scylladb.com/branch-6.0/using-scylla/cassandra-compatibility.md#consistency-level-read-and-write)
* [Consistency Quiz](https://opensource.docs.scylladb.com/branch-6.0/kb/quiz-administrators.md)
* Take a course on [Consistency Levels at Scylla University](https://university.scylladb.com/courses/scylla-essentials-overview/lessons/high-availability/topic/consistency-level/)
