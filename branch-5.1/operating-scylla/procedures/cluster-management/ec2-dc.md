# Create a Scylla Cluster on EC2 (Single or Multi Data Center)

The easiest way to run a Scylla cluster on EC2 is by using [Scylla AMI](https://www.scylladb.com/download/?platform=aws), which is Ubuntu-based since ScyllaDB Enterprise 2021.1.0 and
ScyllaDB Open Source 4.5 (prior versions are CentOS-based). To use a different OS or your own [AMI](https://en.wikipedia.org/wiki/Amazon_Machine_Image) (Amazon Machine Image) or set up a multi DC Scylla cluster,
you need to configure the Scylla cluster on your own. This page guides you through this process.

A Scylla cluster on EC2 can be deployed as a single-DC cluster or a multi-DC cluster. The table below describes how to configure parameters in the `scylla.yaml` file for each node in your cluster for both cluster types.

For more information on Scylla AMI and the configuration of parameters in `scylla.yaml` from the EC2 user data, see [Scylla Machine Image](https://github.com/scylladb/scylla-machine-image).

The best practice is to use each EC2 region as a Scylla DC. In such a case, nodes communicate using Internal (Private) IPs inside the region and using External (Public) IPs between regions (Data Centers).

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
* Ensure that all the relevant [ports](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/admin.md#cqlsh-networking) are open in your EC2 Security Group.
* Select a unique name as `cluster_name` for the cluster (identical for all the nodes in the cluster).

## Procedure

Perform the following steps for each node in the new cluster:

1. Install Scylla on the node. See [Getting Started](https://opensource.docs.scylladb.com/branch-5.1/getting-started/index.md) for installation instructions and
   follow the procedure up to  the `scylla.yaml` configuration phase.

   If the Scylla service is already running (for example, if you are using [Scylla AMI](https://www.scylladb.com/download/?platform=aws)), stop it before moving to the next step by using [these instructions](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/procedures/cluster-management/clear-data.md).
2. Edit the parameters listed below in the `scylla.yaml` file located in `/etc/scylla/`. See the [EC2 Configuration Table](#table) above on how to configure your cluster.
   > * **cluster_name** - Set the selected cluster_name.
   > * **seeds** - IP address of the first node in the cluster. See [Scylla Seed Nodes](https://opensource.docs.scylladb.com/branch-5.1/kb/seed-nodes.md) for details.
   > * **listen_address** - IP address that Scylla used to connect to other Scylla nodes in the cluster.
   > * **auto_bootstrap** - By default, this parameter is set to **true**. It allows new nodes to migrate data to themselves automatically. If using Scylla AMI add `--bootstrap` to the user settings when creating a node.
   > * **endpoint_snitch** - Set the selected snitch.
   > * **rpc_address** - Address for client connection (Thrift, CQL).
   > * **broadcast_address** - The IP address a node tells other nodes in the cluster to contact it by.
   > * **broadcast_rpc_address** - Default: unset. The RPC address to broadcast to drivers and other Scylla nodes. It cannot be set to 0.0.0.0. If left blank, it will be set to the value of `rpc_address`. If `rpc_address` is set to 0.0.0.0, `broadcast_rpc_address` must be explicitly configured.
3. After you have installed and configured Scylla and edited `scylla.yaml` file on all the nodes, start the node specified with the `seeds` parameter. Then start the rest of the nodes in your cluster, one at a time, using
   `sudo systemctl start scylla-server`.
4. Verify that the node has been added to the cluster using
   `nodetool status`.

### EC2snitch’s Default DC and Rack Names, and how to Override DC Names

EC2snitch and Ec2MultiRegionSnitch give each DC and rack default names. The region name is defined as the datacenter name, and [availability zones](https://opensource.docs.scylladb.com/branch-5.1/faq.md#faq-best-scenario-node-multi-availability-zone) are defined as racks within a datacenter. The rack names cannot be changed.

#### Example

For a node in the `us-east-1` region, `us-east` is the datacenter name and `1` is the rack.

To change the name of the datacenter, open the `cassandra-rackdc.properties` file located in `/etc/scylla/` and edit the DC name.

The `dc_suffix` defines a suffix added to the datacenter name. For example:

* for region us-east and suffix `dc_suffix=_1_scylla`, it will be `us-east_1_scylla`.
* for region us-west and suffix `dc_suffix=_1_scylla`, it will be `us-west_1_scylla`.
