# How to Switch Snitches

<!-- REMOVE IN FUTURE VERSIONS - when the limitation in the note is no longer valid -->

#### NOTE
Switching from one type of snitch to another is NOT supported for clusters
where one or more keyspaces have tablets enabled.

NOTE: If you [create a new keyspace](https://opensource.docs.scylladb.com/branch-6.1/cql/ddl.md#create-keyspace-statement),
it has tablets enabled by default.

This procedure describes the steps that need to be done when switching from one type of snitch to another.
Such a scenario can be when increasing the cluster and adding more data-centers in different locations.
Snitches are responsible for specifying how ScyllaDB distributes the replicas. The procedure is dependent on any changes in the cluster topology.

**Note** - Switching a snitch requires a full cluster shutdown, so It is highly recommended to choose the [right snitch](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/system-configuration/snitch.md) for your needs at the cluster setup phase.

| Cluster Status                | Needed Procedure                  |
|-------------------------------|-----------------------------------|
| No change in network topology | Set the new snitch                |
| Network topology was changed  | Set the new snitch and run repair |

Changes in network topology mean that there are changes in the racks or data-centers where the nodes are located.

For example:

**No topology changes**

Original cluster: three nodes cluster on a single data-center with [Simplesnitch](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/system-configuration/snitch.md#snitch-simple-snitch) or [Ec2snitch](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/system-configuration/snitch.md#snitch-ec2-snitch).

Change to: three nodes in one data-center and one rack using a [GossipingPropertyFileSnitch](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/system-configuration/snitch.md#snitch-gossiping-property-file-snitch) or [Ec2multiregionsnitch](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/system-configuration/snitch.md#snitch-ec2-multi-region-snitch).

**Topology changes**

Original cluster: three nodes using the [Simplesnitch](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/system-configuration/snitch.md#snitch-simple-snitch) or [Ec2snitch](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/system-configuration/snitch.md#snitch-ec2-snitch) in a single data-center.

Change to: nine nodes in two data-centers using the [GossipingPropertyFileSnitch](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/system-configuration/snitch.md#snitch-gossiping-property-file-snitch) or [Ec2multiregionsnitch](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/system-configuration/snitch.md#snitch-ec2-multi-region-snitch) (add a new data-center).

## Procedure

1. Stop all the nodes in the cluster.

Supported OS

```shell
sudo systemctl stop scylla-server
```

Docker

```shell
docker exec -it some-scylla supervisorctl stop scylla
```

(without stopping *some-scylla* container)

1. In the `scylla.yaml` file edit the endpoint_snitch. The file can be found under `/etc/scylla/`. Change the endpoint_snitch to all the nodes in the cluster.

For example:

`endpoint_snitch: GossipingPropertyFileSnitch`

1. In the `cassandra-rackdc.properties` file edit the rack and data-center information.

For example, `Ec2MultiRegionSnitch`:

A node in the `us-east-1` region, us-east is the data center name, and 1 is the rack location.

1. Start all the nodes in the cluster in parallel.

Supported OS

```shell
sudo systemctl start scylla-server
```

Docker

```shell
docker exec -it some-scylla supervisorctl start scylla
```

(with *some-scylla* container already running)

1. Run full repair (consult with the table above if this action is needed).
