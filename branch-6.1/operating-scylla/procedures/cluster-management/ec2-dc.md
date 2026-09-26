# Create a ScyllaDB Cluster on EC2 (Single or Multi Data Center)

The easiest way to run a ScyllaDB cluster on EC2 is by using [ScyllaDB AMI](https://www.scylladb.com/download/?platform=aws), which is Ubuntu-based.
To use a different OS or your own [AMI](https://en.wikipedia.org/wiki/Amazon_Machine_Image) (Amazon Machine Image) or set up a multi DC ScyllaDB cluster,
you need to configure the ScyllaDB cluster on your own. This page guides you through this process.

A ScyllaDB cluster on EC2 can be deployed as a single-DC cluster or a multi-DC cluster. The table below describes how to configure parameters in the `scylla.yaml` file for each node in your cluster for both cluster types.

For more information on ScyllaDB AMI and the configuration of parameters in `scylla.yaml` from the EC2 user data, see [ScyllaDB Machine Image](https://github.com/scylladb/scylla-machine-image).

The best practice is to use each EC2 region as a ScyllaDB DC. In such a case, nodes communicate using Internal (Private) IPs inside the region and using External (Public) IPs between regions (Data Centers).

For further information, see [AWS instance addressing](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html).

<a id="table"></a>

## EC2 Configuration Table

| Parameter             | Single DC           | Multi DC             |
|-----------------------|---------------------|----------------------|
| seeds                 | Internal IP address | External IP address  |
| listen_address        | Internal IP address | Internal IP address  |
| rpc_address           | Internal IP address | Internal IP address  |
| broadcast_address     | Internal IP address | External IP address  |
| broadcast_rpc_address | Internal IP address | External IP address  |
| endpoint_snitch       | Ec2Snitch           | Ec2MultiRegionSnitch |

## Prerequisites

* EC2 instance with SSH access.
* Ensure that all the relevant [ports](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/admin.md#cqlsh-networking) are open in your EC2 Security Group.
* Select a unique name as `cluster_name` for the cluster (identical for all the nodes in the cluster).
* Choose one of the nodes to be a seed node. You’ll need to provide the IP of that node using
  the `seeds` parameter in the `scylla.yaml` configuration file on each node.

## Procedure

1. Install ScyllaDB on the nodes you want to add to the cluster. See [Getting Started](https://opensource.docs.scylladb.com/branch-6.1/getting-started/index.md) for installation instructions and
   follow the procedure up to  the `scylla.yaml` configuration phase.

   If the ScyllaDB service is already running (for example, if you are using [ScyllaDB AMI](https://www.scylladb.com/download/?platform=aws)), stop it before moving to the next step by using [these instructions](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/procedures/cluster-management/clear-data.md).
2. On each node, edit the `scylla.yaml` file in `/etc/scylla/` to configure the parameters listed below. See the [EC2 Configuration Table](#table) above on how to configure your cluster.
   > * **cluster_name** - Set the selected cluster_name.
   > * **seeds** - Specify the IP of the node you chose to be a seed node. See [ScyllaDB Seed Nodes](https://opensource.docs.scylladb.com/branch-6.1/kb/seed-nodes.md) for details.
   > * **listen_address** - IP address that ScyllaDB used to connect to other ScyllaDB nodes in the cluster.
   > * **endpoint_snitch** - Set the selected snitch.
   > * **rpc_address** - Address for CQL client connection.
   > * **broadcast_address** - The IP address a node tells other nodes in the cluster to contact it by.
   > * **broadcast_rpc_address** - Default: unset. The RPC address to broadcast to drivers and other ScyllaDB nodes. It cannot be set to 0.0.0.0. If left blank, it will be set to the value of `rpc_address`. If `rpc_address` is set to 0.0.0.0, `broadcast_rpc_address` must be explicitly configured.
3. Start the nodes.

   Supported OS
   ```shell
   sudo systemctl start scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl start scylla
   ```

   (with *some-scylla* container already running)
4. Verify that the node has been added to the cluster using
   `nodetool status`.

### EC2snitch’s Default DC and Rack Names, and how to Override DC Names

EC2snitch and Ec2MultiRegionSnitch give each DC and rack default names. The region name is defined as the datacenter name, and [availability zones](https://opensource.docs.scylladb.com/branch-6.1/faq.md#faq-best-scenario-node-multi-availability-zone) are defined as racks within a datacenter. The rack names cannot be changed.

#### Example

For a node in the `us-east-1` region, `us-east` is the datacenter name and `1` is the rack.

To change the name of the datacenter, open the `cassandra-rackdc.properties` file located in `/etc/scylla/` and edit the DC name.

The `dc_suffix` defines a suffix added to the datacenter name. For example:

* for region us-east and suffix `dc_suffix=_1_scylla`, it will be `us-east_1_scylla`.
* for region us-west and suffix `dc_suffix=_1_scylla`, it will be `us-west_1_scylla`.
