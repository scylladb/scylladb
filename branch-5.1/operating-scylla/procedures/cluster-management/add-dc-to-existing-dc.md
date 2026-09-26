# Adding a New Data Center Into an Existing Scylla Cluster

The following procedure specifies how to add a Data Center (DC) to a live Scylla Cluster, in a single data center, [multi-availability zone](https://opensource.docs.scylladb.com/branch-5.1/faq.md#faq-best-scenario-node-multi-availability-zone), or multi-datacenter. Adding a DC out-scales the cluster and provides higher availability (HA).

The procedure includes:

* Install nodes on the new DC.
* Add the new nodes one by one to the cluster.
* Update the replication strategy of the selected keyspace/keyspaces to use with the new DC.
* Rebuild new nodes
* Run full cluster repair
* Update the Monitoring stack

#### NOTE
Make sure to complete the full procedure **before** starting to read from the new datacenter. You should also be aware that the new DC by default will be used for reads.

#### WARNING
The node/nodes you add **must** be clean (no data). Otherwise, you risk data loss. See [Clean Data from Nodes]() below.

## Prerequisites

1. Log in to one of the nodes in the cluster, collect the following info from the node:
   * cluster_name - `cat /etc/scylla/scylla.yaml | grep cluster_name`
   * seeds - `cat /etc/scylla/scylla.yaml | grep seeds:`
   * endpoint_snitch - `cat /etc/scylla/scylla.yaml | grep endpoint_snitch`
   * Scylla version - `scylla --version`
   * Authentication status - `cat /etc/scylla/scylla.yaml | grep authenticator`

   #### NOTE
   If `authenticator` is set to `PasswordAuthenticator` - increase the `system_auth` table replication factor.

   For example

   `ALTER KEYSPACE system_auth WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'dc1' : <new_replication_factor>};`

   It is recommended to set `system_auth` replication factor to the number of nodes in each DC.
2. On all client applications, switch the consistency level to `LOCAL_*` (LOCAL_ONE, LOCAL_QUORUM,etc.) to prevent the coordinators from accessing the data center you’re adding.
3. Install the new **clean** Scylla nodes (See [Clean Data from Nodes]() below) on the new datacenter, see [Getting Started](https://opensource.docs.scylladb.com/branch-5.1/getting-started/index.md) for further instructions, create as many nodes that you need.
   Follow the Scylla install procedure up to `scylla.yaml` configuration phase.
   In the case that the node starts during the installation process follow [these instructions](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/procedures/cluster-management/clear-data.md).

## Clean Data from Nodes

```sh
sudo rm -rf /var/lib/scylla/data
sudo find /var/lib/scylla/commitlog -type f -delete
sudo find /var/lib/scylla/hints -type f -delete
sudo find /var/lib/scylla/view_hints -type f -delete
```

## Add New DC

**Procedure**

#### WARNING
Make sure **all** your keyspaces are using `NetworkTopologyStrategy`. If this is not the case, follow the [these instructions](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/procedures/cluster-management/update-topology-strategy-from-simple-to-network.md) to fix that.

1. For each node in the **existing data-center(s)** edit the `scylla.yaml` file to use either
   * `Ec2MultiRegionSnitch` - for AWS cloud-based, multi-data-center deployments.
   * `GossipingPropertyFileSnitch` - for bare metal and cloud (other than AWS) deployments.
2. Set the DC and Rack Names.
   In the `cassandra-rackdc.properties` file edit the parameters listed below.
   The file can be found under `/etc/scylla/`.

   `Ec2MultiRegionSnitch`

   Ec2MultiRegionSnitch gives each DC and rack default names, the region name defined as datacenter name, and [availability zones](https://opensource.docs.scylladb.com/branch-5.1/faq.md#faq-best-scenario-node-multi-availability-zone) are defined as racks within a datacenter.

   For example:

   A node in the `us-east-1` region,
   us-east is the data center name, and 1 is the rack location.

   To change the DC names, do the following:

   Edit the `cassandra-rackdc.properties` file with the preferred datacenter name.
   The file can be found under `/etc/scylla/`

   The dc_suffix defines a suffix added to the datacenter name as described below.

   For example:

   Region us-east

   `dc_suffix=_1_scylla` will be `us-east_1_scylla`

   Or

   Region us-west

   `dc_suffix=_1_scylla` will be `us-west_1_scylla`

   `GossipingPropertyFileSnitch`
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
3. In the **existing datacenter(s)** restart Scylla nodes one by one.

   Supported OS
   ```shell
   sudo systemctl restart scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl restart scylla
   ```

   (without restarting *some-scylla* container)
4. For each node in the **new datacenter** edit the `scylla.yaml` file parameters listed below,
   the file can be found under `/etc/scylla/`.
   * **cluster_name** - Set the selected cluster_name.
   * **seeds** - IP address of an existing node (or nodes).
   * **listen_address** - IP address that Scylla used to connect to the other Scylla nodes in the cluster.
   * **auto_bootstrap** - By default, this parameter is set to true, it allows new nodes to migrate data to themselves automatically.
   * **endpoint_snitch** - Set the selected snitch.
   * **rpc_address** - Address for client connections (Thrift, CQL).

   The parameters `seeds`, `cluster_name` and `endpoint_snitch` need to match the existing cluster.
5. In the **new datacenter**, set the DC and Rack Names (see step number **three** for more details).
6. In the **new datacenter**, start Scylla nodes one by one using.

   Supported OS
   ```shell
   sudo systemctl start scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl start scylla
   ```

   (with *some-scylla* container already running)
7. Verify that the nodes were added to the cluster using `nodetool status`.

   For example:
   ```shell
   $ nodetool status

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
   ```
8. When all nodes are up and running `ALTER` the following Keyspaces in the new nodes:
   * Keyspace created by the user (which needed to replicate to the new DC).
   * System: `system_auth`, `system_distributed`, `system_traces` For example, replicate the data to three nodes in the new DC.

   For example:

   Before
   ```cql
   DESCRIBE KEYSPACE mykeyspace;

   CREATE KEYSPACE mykeyspace WITH replication = { 'class' : 'NetworkTopologyStrategy', '<exiting_dc>' : 3};
   ```

   ALTER Command
   ```cql
   ALTER KEYSPACE mykeyspace WITH replication = { 'class' : 'NetworkTopologyStrategy', '<exiting_dc>' : 3, <new_dc> : 3};
   ALTER KEYSPACE system_auth WITH replication = { 'class' : 'NetworkTopologyStrategy', '<exiting_dc>' : 3, <new_dc> : 3};
   ALTER KEYSPACE system_distributed WITH replication = { 'class' : 'NetworkTopologyStrategy', '<exiting_dc>' : 3, <new_dc> : 3};
   ALTER KEYSPACE system_traces WITH replication = { 'class' : 'NetworkTopologyStrategy', '<exiting_dc>' : 3, <new_dc> : 3};
   ```

   After
   ```cql
   DESCRIBE KEYSPACE mykeyspace;
   CREATE KEYSPACE mykeyspace WITH REPLICATION = {'class’: 'NetworkTopologyStrategy', <exiting_dc>:3, <new_dc>: 3};
   CREATE KEYSPACE system_auth WITH replication = { 'class' : 'NetworkTopologyStrategy', '<exiting_dc>' : 3, <new_dc> : 3};
   CREATE KEYSPACE system_distributed WITH replication = { 'class' : 'NetworkTopologyStrategy', '<exiting_dc>' : 3, <new_dc> : 3};
   CREATE KEYSPACE system_traces WITH replication = { 'class' : 'NetworkTopologyStrategy', '<exiting_dc>' : 3, <new_dc> : 3};
   ```
9. Run `nodetool rebuild` on each node in the new datacenter, specify the existing datacenter name in the rebuild command.

   For example:

   `nodetool rebuild -- <existing_data_center_name>`

   The rebuild ensures that the new nodes that were just added to the cluster will recognize the existing datacenters in the cluster.
10. Run a full cluster repair, using [nodetool repair -pr](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/nodetool-commands/repair.md) on each node, or using [Scylla Manager ad-hoc repair](https://manager.docs.scylladb.com/stable/repair)
11. If you are using Scylla Monitoring, update the [monitoring stack](https://monitoring.docs.scylladb.com/stable/install/monitoring_stack.html#configure-scylla-nodes-from-files) to monitor it. If you are using Scylla Manager, make sure you install the [Manager Agent](https://manager.docs.scylladb.com/stable/install-scylla-manager-agent.html) and Manager can access the new DC.

## Configure the Client not to Connect to the New DC

This procedure will help your clients not to connect to the DC you just added.

The example below is for clients using the Java driver. Modify this example to suit your needs.

1. One way to prevent clients from connecting to the new DC is to temporarily restrict them to only use the nodes which are in the same DC as the clients.
   This can be done by ensuring the operations are performed with CL=LOCAL_\* (for example LOCAL_QUORUM) and using
   `DCAwareRoundRobinPolicy` created by calling `withLocalDc("dcLocalToTheClient)` and `withUsedHostsPerRemoteDc(0)` on `DCAwareRoundRobinPolicy.Builder`.

   Example of DCAwareRoundRobinPolicy creation:
   ```java
   variable = DCAwareRoundRobinPolicy.builder()
       .withLocalDc("<name of DC local to the client being configured>")
       .withUsedHostsPerRemoteDc(0)
       .build();
   ```
2. Once the new DC is operational you can remove “withUsedHostsPerRemoteDc(0)” from the configuration and change CL to the previous value.

### Additional Resources for Java Clients

* [DCAwareRoundRobinPolicy.Builder](https://java-driver.docs.scylladb.com/scylla-3.10.2.x/api/com/datastax/driver/core/policies/DCAwareRoundRobinPolicy.Builder.html)
* [DCAwareRoundRobinPolicy](https://java-driver.docs.scylladb.com/scylla-3.10.2.x/api/com/datastax/driver/core/policies/DCAwareRoundRobinPolicy.html)
