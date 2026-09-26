# Create a ScyllaDB Cluster - Single Data Center (DC)

## Prerequisites

* Make sure that all the [ports](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/admin.md#cqlsh-networking) are open.
* Obtain the IP addresses of all nodes which have been created for the cluster.
* Select a unique name as `cluster_name` for the cluster (identical for all the nodes in the cluster).
* Choose which [snitch](https://opensource.docs.scylladb.com/branch-5.2/faq.md#faq-snitch-strategy) to use (identical for all the nodes in the cluster). For a production system, it is recommended to use a DC-aware snitch, which can support a `NetworkTopologyStrategy` [replication-strategy](https://opensource.docs.scylladb.com/branch-5.2/cql/ddl.md#create-keyspace-statement) for your Keyspaces.

## Procedure

These steps need to be done for each of the nodes in the new cluster.

1. Install Scylla on a node. See [Getting Started](https://opensource.docs.scylladb.com/branch-5.2/getting-started/index.md) for further instructions.
Follow the Scylla install procedure up to `scylla.yaml` configuration phase.

In case that the node starts during the process follow [these instructions](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/procedures/cluster-management/clear-data.md)

2. In the `scylla.yaml` file, edit the parameters listed below.
The file can be found under `/etc/scylla/`

- **cluster_name** - Set the selected cluster_name
- **seeds** - Specify the IP of the first node and **only the first node**. New nodes will use the IP of this seed node to connect to the cluster and learn the cluster topology and state.
- **listen_address** - IP address that Scylla used to connect to other Scylla nodes in the cluster
- **endpoint_snitch** - Set the selected snitch
- **rpc_address** - Address for client connection (Thrift, CQL)
- **consistent_cluster_management** - `true` by default, can be set to `false` if you don’t want to use Raft for consistent schema management in this cluster (will be mandatory in later versions). Check the [Raft in ScyllaDB document](https://opensource.docs.scylladb.com/branch-5.2/architecture/raft.md) to learn more.

3. This step needs to be done **only** if you are using the **GossipingPropertyFileSnitch**. If not, skip this step.
In the `cassandra-rackdc.properties` file, edit the parameters listed below.
The file can be found under `/etc/scylla/`

- **dc** - Set the datacenter name
- **rack** - Set the rack name

For example:

```shell
# cassandra-rackdc.properties
#
# The lines may include white spaces at the beginning and the end.
# The rack and data center names may also include white spaces.
# All trailing and leading white spaces will be trimmed.
#
dc=thedatacentername
rack=therackname
# prefer_local=<false | true>
# dc_suffix=<Data Center name suffix, used by EC2SnitchXXX snitches>
```

1. After Scylla has been installed and configured, edit `scylla.yaml` file on all the nodes, using the first node as the seed node. Start the seed node, and once it is in **UN** state, repeat for all the other nodes, each after the previous is in **UN** state.

Supported OS

```shell
sudo systemctl start scylla-server
```

Docker

```shell
docker exec -it some-scylla supervisorctl start scylla
```

(with *some-scylla* container already running)

5. Verify that the node has been added to the cluster using
`nodetool status`

1. If you are running a Scylla version earlier than Scylla Open Source 4.3 or Scylla Enterprise 2021.1, you need to update the `seeds` parameter in each node’s `scylla.yaml` file to include the IP of at least one more seed node. See [Older Version Of Scylla](https://opensource.docs.scylladb.com/branch-5.2/kb/seed-nodes.md#seeds-older-versions) for details.

## Example

This example shows how to install and configure a three nodes cluster using GossipingPropertyFileSnitch as the endpoint_snitch, each node on a different rack.

1. Installing Three Scylla nodes, the IP’s are:

```shell
192.168.1.201 (seed)
192.168.1.202
192.168.1.203
```

1. In each Scylla node, edit the `scylla.yaml` file

**192.168.1.201**

```shell
cluster_name: 'Scylla_cluster_demo'
seeds: "192.168.1.201"
endpoint_snitch: GossipingPropertyFileSnitch
rpc_address: "192.168.1.201"
listen_address: "192.168.1.201"
```

**192.168.1.202**

```shell
cluster_name: 'Scylla_cluster_demo'
seeds: "192.168.1.201"
endpoint_snitch: GossipingPropertyFileSnitch
rpc_address: "192.168.1.202"
listen_address: "192.168.1.202"
```

**192.168.1.203**

```shell
cluster_name: 'Scylla_cluster_demo'
seeds: "192.168.1.201"
endpoint_snitch: GossipingPropertyFileSnitch
rpc_address: "192.168.1.203"
listen_address: "192.168.1.203"
```

3. This step needs to be done only if using **GossipingPropertyFileSnitch**.
In each Scylla node, edit the `cassandra-rackdc.properties` file

**192.168.1.201**

```shell
# cassandra-rackdc.properties
#
# The lines may include white spaces at the beginning and the end.
# The rack and data center names may also include white spaces.
# All trailing and leading white spaces will be trimmed.
#
dc=datacenter1
rack=rack43
# prefer_local=<false | true>
# dc_suffix=<Data Center name suffix, used by EC2SnitchXXX snitches>
```

**192.168.1.202**

```shell
# cassandra-rackdc.properties
#
# The lines may include white spaces at the beginning and the end.
# The rack and data center names may also include white spaces.
# All trailing and leading white spaces will be trimmed.
#
dc=datacenter1
rack=rack44
# prefer_local=<false | true>
# dc_suffix=<Data Center name suffix, used by EC2SnitchXXX snitches>
```

**192.168.1.203**

```shell
# cassandra-rackdc.properties
#
# The lines may include white spaces at the beginning and the end.
# The rack and data center names may also include white spaces.
# All trailing and leading white spaces will be trimmed.
#
dc=datacenter1
rack=rack45
# prefer_local=<false | true>
# dc_suffix=<Data Center name suffix, used by EC2SnitchXXX snitches>
```

1. Starting Scylla nodes, since our seed node is `192.168.1.201` we will start it first, wait until it is in a **UN** state, and repeat for the other nodes.

Supported OS

```shell
sudo systemctl start scylla-server
```

Docker

```shell
docker exec -it some-scylla supervisorctl start scylla
```

(with *some-scylla* container already running)

1. Verify that the node has been added to the cluster by using the `nodetool status` command

```shell
Datacenter: datacenter1
Status=Up/Down
State=Normal/Leaving/Joining/Moving
--  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   43
UN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-mac8-43fddce9123e   44
UN  192.168.1.203  124.42 KB  256     32.6%             675ed9f4-6564-6dbd-can8-43fddce952gy   45
```
