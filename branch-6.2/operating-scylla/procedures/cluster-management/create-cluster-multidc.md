# Create a ScyllaDB Cluster - Multi Data Centers (DC)

<a id="create-cluster-multi-config-table"></a>

Consult with the table below if each node in the cluster has internal IP for internal DC communication and external IP for cross DC communication.

## Single Multi Data Centers Configuration Table

| Parameter             | Multi DC                    |
|-----------------------|-----------------------------|
| seeds                 | External IP address         |
| listen_address        | Internal IP address         |
| rpc_address           | Internal IP address         |
| broadcast_address     | External IP address         |
| broadcast_rpc_address | External IP address         |
| endpoint_snitch       | GossipingPropertyFileSnitch |

**Important**

If the node has two physical network interfaces in a multi-datacenter installation:

* Set `listen_address` to this node’s private IP or hostname.
* Set `broadcast_address` to the second IP or hostname (for communication between data centers).
* Set `listen_on_broadcast_address` to true.
* Open the storage_port or ssl_storage_port on the public IP firewall.

### Prerequisites

* Make sure that all the [ports](https://opensource.docs.scylladb.com/stable/operating-scylla/admin.md#cqlsh-networking) are open.
* Obtain the IP addresses of all nodes that have been created for the cluster.
* Choose one of the nodes to be a seed node. You’ll need to provide the IP
  of that node using the seeds parameter in the scylla.yaml configuration file on each node.
* Select a unique name as `cluster_name` for the cluster (identical for all the nodes in the cluster).
* Choose which [snitch](https://opensource.docs.scylladb.com/stable/faq.md#faq-snitch-strategy) to use (identical for all the nodes in the cluster). For a production system, it is recommended to use a DC-aware snitch, which can support the `NetworkTopologyStrategy` [replication strategy](https://opensource.docs.scylladb.com/stable/cql/ddl.md#create-keyspace-statement) for your keyspaces.

* Decide the name of the rack, for example,  RACK1, RACK2 or RC1, RC2.
* Decide the name of the data center, for example, DC1, DC2 or US-DC, ASIA-DC.

**Choose the data center name carefully. It is not possible to rename a data center later**

When working with production environments, you must choose one of the snitches below:

* [Ec2multiregionsnitch](https://opensource.docs.scylladb.com/stable/operating-scylla/system-configuration/snitch.md#snitch-ec2-multi-region-snitch) - for AWS cloud-based, multi-data-center deployments.
* [GossipingPropertyFileSnitch](https://opensource.docs.scylladb.com/stable/operating-scylla/system-configuration/snitch.md#snitch-gossiping-property-file-snitch) - for bare metal and cloud (other than AWS) deployments.

### Procedure

1. Install ScyllaDB on the nodes you want to add to the cluster. See [Getting Started](https://opensource.docs.scylladb.com/stable/getting-started/index.md) for further instructions, create as many nodes that you need.
Follow the ScyllaDB install procedure up to scylla.yaml configuration phase.

In case that your node starts during the process follow [these instructions](https://opensource.docs.scylladb.com/stable/operating-scylla/procedures/cluster-management/clear-data.md)

2. On each node, edit the `scylla.yaml` file to configure the parameters listed below.
The file can be found under `/etc/scylla/`.

- **cluster_name** - Set the selected cluster_name
- **seeds** - Specify the IP of the node you chose to be a seed node. New nodes will use the IP of this seed node to connect to the cluster and learn the cluster topology and state.
- **listen_address** - IP address that the ScyllaDB use to connect to other ScyllaDB nodes in the cluster
- **endpoint_snitch** - Set the selected snitch
- **rpc_address** - Address for CQL client connection

3. In the `cassandra-rackdc.properties` file, edit the rack and data center information.
The file can be found under `/etc/scylla/`.

To save bandwidth, add the `prefer_local=true` parameter. ScyllaDB will use the node private (local) IP address when the nodes are in the same data center.

1. Start the nodes.

Supported OS

```shell
sudo systemctl start scylla-server
```

Docker

```shell
docker exec -it some-scylla supervisorctl start scylla
```

(with *some-scylla* container already running)

5. Verify that the nodes have been added to the cluster using
`nodetool status`.

**For Example:**

In this example, we will show how to install a nine nodes cluster.

1. Install nine ScyllaDB nodes, three nodes in each data center (U.S, ASIA, EUROPE). The IP’s are:

```shell
U.S Data-center
Node# Private IP    Public IP
Node1 192.168.1.201 54.187.36.59 (seed)
Node2 192.168.1.202 54.187.142.201
Node3 192.168.1.203 54.187.168.20

ASIA Data-center
Node# Private IP    Public IP
Node4 192.168.1.204 54.191.72.56
Node5 192.168.1.205 54.187.25.99
Node6 192.168.1.206 54.191.2.121

EUROPE Data-center
Node# Private IP    Public IP
Node7 192.168.1.207 54.160.174.243
Node8 192.168.1.208 54.235.9.159
Node9 192.168.1.209 54.146.228.25
```

1. In each ScyllaDB node, edit the `scylla.yaml` file. See [Single Multi Data Centers Configuration Table](#create-cluster-multi-config-table) for reference.

**U.S Data-center - 192.168.1.201**

```shell
cluster_name: 'multi_dc_demo'
seeds: "54.187.36.59"
endpoint_snitch: GossipingPropertyFileSnitch
rpc_address: "192.168.1.201"
listen_address: "192.168.1.201"
broadcast_address: "54.187.36.59"
broadcast_rpc_address: "54.187.36.59"
listen_on_broadcast_address: true (optional)
```

**ASIA Data-center - 192.168.1.204**

```shell
cluster_name: 'multi_dc_demo'
seeds: "54.187.36.59"
endpoint_snitch: GossipingPropertyFileSnitch
rpc_address: "192.168.1.204"
listen_address: "192.168.1.204"
broadcast_address: "54.191.72.56"
broadcast_rpc_address: "54.191.72.56"
listen_on_broadcast_address: true (optional)
```

**EUROPE Data-center - 192.168.1.207**

```shell
cluster_name: 'multi_dc_demo'
seeds: "54.187.36.59"
endpoint_snitch: GossipingPropertyFileSnitch
rpc_address: "192.168.1.207"
listen_address: "192.168.1.207"
broadcast_address: "54.160.174.243"
broadcast_rpc_address: "54.160.174.243"
listen_on_broadcast_address: true (optional)
```

1. In each ScyllaDB node, edit the `cassandra-rackdc.properties` file with the relevant rack and data center information

**Nodes 1-3**

```shell
dc=US-DC
rack=RACK1
```

**Nodes 4-6**

```shell
dc=ASIA-DC
rack=RACK1
```

**Nodes 7-9**

```shell
dc=EUROPE-DC
rack=RACK1
```

1. Start the nodes.

Supported OS

```shell
sudo systemctl start scylla-server
```

Docker

```shell
docker exec -it some-scylla supervisorctl start scylla
```

(with *some-scylla* container already running)

1. Verify that the nodes have been added to the cluster by using the `nodetool status` command.

```shell
nodetool status

Datacenter: US-DC
=========================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--   Address         Load            Tokens  Owns            Host ID                                 Rack
UN   54.191.2.121    120.97 KB       256     ?               c84b80ea-cb60-422b-bc72-fa86ede4ac2e    RACK1
UN   54.191.72.56    109.54 KB       256     ?               129087eb-9aea-4af6-92c6-99fdadb39c33    RACK1
UN   54.187.25.99    104.94 KB       256     ?               0540c7d7-2622-4f1f-a3f0-acb39282e0fc    RACK1

Datacenter: ASIA-DC
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--   Address         Load            Tokens  Owns            Host ID                                 Rack
UN   54.160.174.243  109.54 KB       256     ?               c7686ffd-7a5b-4124-858e-df2e61130aaa    RACK1
UN   54.235.9.159    109.75 KB       256     ?               39798227-9f6f-4868-8193-08570856c09a    RACK1
UN   54.146.228.25   128.33 KB       256     ?               7a4957a1-9590-4434-9746-9c8a6f796a0c    RACK1

Datacenter: EUROPE-DC
=========================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--   Address         Load            Tokens  Owns            Host ID                                 Rack
UN   54.187.36.59    114.35 KB       256     ?               4c3e1533-1b78-45bf-8bd4-818090f019ab    RACK1
UN   54.187.142.201  109.54 KB       256     ?               d99967d6-987c-4a54-829d-86d1b921470f    RACK1
UN   54.187.168.20   109.54 KB       256     ?               2329c2e0-64e1-41dc-8202-74403a40f851    RACK1
```

### Preventing Quorum Loss

If your cluster is symmetrical, i.e., it has  an even number of datacenters
with the same number of nodes, consider adding an arbiter DC to mitigate
the risk of losing a quorum at a minimum cost.
See [Preventing Quorum Loss in Symmetrical Multi-DC Clusters](https://opensource.docs.scylladb.com/stable/operating-scylla/procedures/cluster-management/arbiter-dc.md)
for details.

### See also:

[Create a ScyllaDB Cluster - Single Data Center (DC)](https://opensource.docs.scylladb.com/stable/operating-scylla/procedures/cluster-management/create-cluster.md)
