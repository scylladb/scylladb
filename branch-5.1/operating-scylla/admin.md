# Administration Guide

For training material, also check out the [Admin Procedures lesson](https://university.scylladb.com/courses/scylla-operations/lessons/admin-procedures-and-basic-monitoring/) on Scylla University.

## System requirements

Make sure you have met the [System Requirements](https://opensource.docs.scylladb.com/branch-5.1/getting-started/system-requirements.md)  before you install and configure Scylla.

## Download and Install

See the [getting started page](https://opensource.docs.scylladb.com/branch-5.1/getting-started/index.md) for info on installing Scylla on your platform.

## System configuration

See [System Configuration Guide](https://opensource.docs.scylladb.com/branch-5.1/getting-started/system-configuration.md#system-configuration-files-and-scripts) for details on optimum OS settings for Scylla. (These settings are performed automatically in the Scylla packages, Docker containers, and Amazon AMIs.)

<a id="admin-scylla-configuration"></a>

## Scylla Configuration

Scylla configuration files are:

| Installed location                                                                       | Description                    |
|------------------------------------------------------------------------------------------|--------------------------------|
| `/etc/default/scylla-server` (Ubuntu/Debian)<br/>`/etc/sysconfig/scylla-server` (others) | Server startup options         |
| `/etc/scylla/scylla.yaml`                                                                | Main Scylla configuration file |
| `/etc/scylla/cassandra-rackdc.properties`                                                | Rack & dc configuration file   |

<a id="check-your-current-version-of-scylla"></a>

### Check your current version of Scylla

This command allows you to check your current version of Scylla. Note that this command is not the [nodetool version](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/nodetool-commands/version.md) command which reports the CQL version.
If you are looking for the CQL or Cassandra version, refer to the CQLSH reference for [SHOW VERSION](https://opensource.docs.scylladb.com/branch-5.1/cql/cqlsh.md#cqlsh-show-version).

```shell
scylla --version
```

Output displays the Scylla version. Your results may differ.

```shell
4.4.0-0.20210331.05c6a40f0
```

<a id="admin-address-configuration-in-scylla"></a>

### Address Configuration in Scylla

The following addresses can be configured in scylla.yaml:

| Address Type               | Description                                                                                                                                                                                                                                             |
|----------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| listen_address             | Address Scylla listens for connections from other nodes. See storage_port and ssl_storage_ports.                                                                                                                                                        |
| rpc_address                | Address on which Scylla is going to expect Thrift and CQL client connections. See rpc_port, native_transport_port and native_transport_port_ssl in the [Networking](#cqlsh-networking) parameters.                                                      |
| broadcast_address          | Address that is broadcasted to tell other Scylla nodes to connect to. Related to listen_address above.                                                                                                                                                  |
| broadcast_rpc_address      | Address that is broadcasted to tell the clients to connect to. Related to rpc_address.                                                                                                                                                                  |
| seeds                      | The broadcast_addresses of the existing nodes. You must specify the address of at least one existing node.                                                                                                                                              |
| endpoint_snitch            | Node’s address resolution helper.                                                                                                                                                                                                                       |
| api_address                | Address for REST API requests. See api_port in the [Networking](#cqlsh-networking) parameters.                                                                                                                                                          |
| prometheus_address         | Address for Prometheus queries. See prometheus_port in the [Networking](#cqlsh-networking) parameters and [ScyllaDB Monitoring Stack](https://monitoring.docs.scylladb.com/stable/) for more details.                                                   |
| replace_address_first_boot | Address of the node this Scylla instance is meant to replace. Refer to [Replace a Dead Node in a Scylla Cluster](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/procedures/cluster-management/replace-dead-node.md) for more details. |

#### NOTE
When the listen_address, rpc_address, broadcast_address, and broadcast_rpc_address parameters are not set correctly, Scylla does not work as expected.

### scylla-server

The `scylla-server` file contains configuration related to starting up the Scylla server.

* **orphan:**

## scylla.yaml

scylla.yaml is equivalent to the Apache Cassandra cassandra.yaml configuration file and it is compatible with relevant parameters. Below is a **subset** of scylla.yaml with parameters you are likely to update. For a full list of parameters, look at the file itself.

```yaml
# The name of the cluster. This is mainly used to prevent machines in
# one logical cluster from joining another.
cluster_name: 'Test Cluster'

# This defines the number of tokens randomly assigned to this node on the ring
# The more tokens, relative to other nodes, the larger the proportion of data
# that this node will store. You probably want all nodes to have the same number
# of tokens assuming they have equal hardware capability.
num_tokens: 256

# Directory where Scylla should store data on disk.
data_file_directories:
    - /var/lib/scylla/data

# commit log.  when running on magnetic HDD, this should be a
# separate spindle than the data directories.
commitlog_directory: /var/lib/scylla/commitlog

# seed_provider class_name is saved for future use.
# A seed address is mandatory.
seed_provider:
    # The addresses of hosts that will serve as contact points for the joining node.
    # It allows the node to discover the cluster ring topology on startup (when
    # joining the cluster).
    # Once the node has joined the cluster, the seed list has no function.
    - class_name: org.apache.cassandra.locator.SimpleSeedProvider
      parameters:
          # In a new cluster, provide the address of the first node.
          # In an existing cluster, specify the address of at least one existing node.
          # If you specify addresses of more than one node, use a comma to separate them.
          # For example: "<IP1>,<IP2>,<IP3>"
          - seeds: "127.0.0.1"

# Address or interface to bind to and tell other Scylla nodes to connect to.
# You _must_ change this if you want multiple nodes to be able to communicate!
#
# Setting listen_address to 0.0.0.0 is always wrong.
listen_address: localhost

# Address to broadcast to other Scylla nodes
# Leaving this blank will set it to the same value as listen_address
# broadcast_address: 1.2.3.4

# port for the CQL native transport to listen for clients on
# For security reasons, you should not expose this port to the internet.  Firewall it if needed.
native_transport_port: 9042

# Uncomment to enable experimental features
# experimental: true
```

By default scylla.yaml is located at `/etc/scylla/scylla.yaml`. Note that the file will open as read-only unless you edit it as the root user or by using sudo.

### scylla.yaml Required Settings

The following configuration items must be set

| Item           | Content                                                                                                                             |
|----------------|-------------------------------------------------------------------------------------------------------------------------------------|
| cluster_name   | Name of the cluster, all the nodes in the cluster must have the same name                                                           |
| seeds          | IP address of an existing node in the cluster. It allows a new node to discover the cluster ring topology when joining the cluster. |
| listen_address | IP address that the Scylla use to connect to other Scylla nodes in the cluster                                                      |
| rpc_address    | IP address of the interface for client connections (Thrift, CQL)                                                                    |

<a id="yaml-enabling-experimental-features"></a>

### Enabling Experimental Features

There are two ways to enable experimental features:

* Enable all experimental features (which may be risky)
* Enable only the features you want to try (available in Scylla Open Source from version 3.2)

#### Enable All Experimental Features

To enable all experimental features to add to the scylla.yaml:

```yaml
experimental: true
```

#### Enable Specific Experimental Features

#### Versionadded
Added in version 3.2: Scylla Open Source

To enable specific experimental features, add to the scylla.yaml the list of experimental features you want to enable, by setting the experimental_features array.
The list of valid elements for this array can be obtained from `scylla --help`.
Note that this list is version specific. Your results may be different.
For example:

```yaml
experimental_features:
 - cdc
 - lwt
```

<a id="ipv6-addresses"></a>

### IPv6 Addresses

Starting with Scylla Open Source 3.2, Scylla Enterprise 2019.1.4, Manager 2.0, and Monitoring 3.0, you can use IPv6 addresses wherever an IPv4 address is used, including client-to-node and node-to-node communication, Scylla Manager to Scylla nodes (Scylla Manager Agent), and Monitoring to nodes.

For example:

```yaml
- seeds: "2a05:d018:223:f00:971d:14af:6418:fe2d"
- listen_address: 2a05:d018:223:f00:971d:14af:6418:fe2d
- broadcast_rpc_address: 2a05:d018:223:f00:971d:14af:6418:fe2d
```

To enable IPv6 addressing, set the following paramerter in scylla.yaml:

```yaml
enable_ipv6_dns_lookup: true
```

To read the rest of the [Administration Guide](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/admin.md) (from the top).

To go to the System Configuration documentation, click [here](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/system-configuration/index.md).

### Compression

In Scylla, you can configure compression at rest and compression in transit.
For compression in transit, you can configure compression between nodes or between the client and the node.

#### Client - Node Compression

Compression between the client and the node is set by the driver that the application is using to access Scylla.

For example:

* [Scylla Python Driver](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.html#cassandra.cluster.Cluster.compression)
* [Scylla Java Driver](https://github.com/scylladb/java-driver/tree/3.7.1-scylla/manual/compression)
* [Go Driver](https://godoc.org/github.com/gocql/gocql#Compressor)

Refer to the [Drivers Page](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/drivers/index.md) for more drivers.

<a id="internode-compression"></a>

#### Internode Compression

Internode compression is configured in the scylla.yaml

internode_compression controls whether traffic between nodes is compressed.

* all  - all traffic is compressed.
* dc   - traffic between different datacenters is compressed.
* none - nothing is compressed (default).

### Configuring TLS/SSL in scylla.yaml

Scylla versions 1.1 and greater support encryption between nodes and between client and node. See the Scylla [Scylla TLS/SSL guide:](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/index.md) for configuration settings.

<a id="cqlsh-networking"></a>

### Networking

Scylla uses the following ports:

|   Port | Description                              | Protocol   |
|--------|------------------------------------------|------------|
|   9042 | CQL (native_transport_port)              | TCP        |
|   9142 | SSL CQL (secure client to node)          | TCP        |
|   7000 | Inter-node communication (RPC)           | TCP        |
|   7001 | SSL inter-node communication (RPC)       | TCP        |
|   7199 | JMX management                           | TCP        |
|  10000 | Scylla REST API                          | TCP        |
|   9180 | Prometheus API                           | TCP        |
|   9100 | node_exporter (Optionally)               | TCP        |
|   9160 | Scylla client port (Thrift)              | TCP        |
|  19042 | Native shard-aware transport port        | TCP        |
|  19142 | Native shard-aware transport port  (ssl) | TCP        |

#### NOTE
For Scylla Manager ports, see Scylla Manager <https://manager.docs.scylladb.com/>.

![image](operating-scylla/security/Scylla-Ports2.png)

The Scylla ports are detailed in the table below. For Scylla Manager ports, see the [Scylla Manager Documentation](https://manager.docs.scylladb.com/).

Scylla uses the following ports:

|   Port | Description                              | Protocol   |
|--------|------------------------------------------|------------|
|   9042 | CQL (native_transport_port)              | TCP        |
|   9142 | SSL CQL (secure client to node)          | TCP        |
|   7000 | Inter-node communication (RPC)           | TCP        |
|   7001 | SSL inter-node communication (RPC)       | TCP        |
|   7199 | JMX management                           | TCP        |
|  10000 | Scylla REST API                          | TCP        |
|   9180 | Prometheus API                           | TCP        |
|   9100 | node_exporter (Optionally)               | TCP        |
|   9160 | Scylla client port (Thrift)              | TCP        |
|  19042 | Native shard-aware transport port        | TCP        |
|  19142 | Native shard-aware transport port  (ssl) | TCP        |

#### NOTE
For Scylla Manager ports, see Scylla Manager <https://manager.docs.scylladb.com/>.

All ports above need to be open to external clients (CQL), external admin systems (JMX), and other nodes (RPC). REST API port can be kept closed for incoming external connections.

The JMX service, `scylla-jmx`, runs on port 7199. It is required in order to manage Scylla using `nodetool` and other Apache Cassandra-compatible utilities. The `scylla-jmx` process must be able to connect to port 10000 on localhost. The JMX service listens for incoming JMX connections on all network interfaces on the system.

### Advanced networking

It is possible that a client, or another node, may need to use a different IP address to connect to a Scylla node from the address that the node is listening on. This is the case when a node is behind port forwarding. Scylla allows for setting alternate IP addresses.

Do not set any IP address to `0.0.0.0`.

| Address Type              | Description                                                                                                                                                                                         | Default        |
|---------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|
| listen_address (required) | Address Scylla listens for connections from other nodes. See storage_port and ssl_storage_ports.                                                                                                    | No default     |
| rpc_address (required)    | Address on which Scylla is going to expect Thrift and CQL clients connections. See rpc_port, native_transport_port and native_transport_port_ssl in the [Networking](#cqlsh-networking) parameters. | No default     |
| broadcast_address         | Address that is broadcasted to tell other Scylla nodes to connect to. Related to listen_address above.                                                                                              | listen_address |
| broadcast_rpc_address     | Address that is broadcasted to tell the clients to connect to. Related to rpc_address.                                                                                                              | rpc_address    |

If other nodes can connect directly to `listen_address`, then `broadcast_address` does not need to be set.

If clients can connect directly to `rpc_address`, then `broadcast_rpc_address` does not need to be set.

#### NOTE
For tips and examples on how to configure these addresses, refer to [How to Properly Set Address Values in scylla.yaml](https://opensource.docs.scylladb.com/branch-5.1/kb/yaml-address.md)

<a id="admin-core-dumps"></a>

### Core dumps

On RHEL and CentOS, the Automatic Bug Reporting Tool ([ABRT](https://docs.fedoraproject.org/en-US/Fedora_Draft_Documentation/0.1/html/System_Administrators_Guide/ch-abrt.html)) conflicts with Scylla coredump configuration. Remove it before installing Scylla: `sudo yum remove -y abrt`

Scylla places any core dumps in `var/lib/scylla/coredump`. They are not visible with the `coredumpctl` command. See the [System Configuration Guide](https://opensource.docs.scylladb.com/branch-5.1/getting-started/system-configuration.md) for details on core dump configuration scripts. Check with Scylla support before sharing any core dump, as they may contain sensitive data.

## Manual bootstrapping

It’s possible to skip the bootstrapping process entirely and join the ring straight away by setting the hidden parameter `auto_bootstrap: false`.
A manual bootstrap node will skip the data stream phase and immediately join the cluster in a *UN* (Up Normal) state and start serving queries, even though it has no data.
**It is firmly advised not to use Manual bootstrapping.**

Note that starting a node as a seed, by including itself in the seeds list, has a similar effect of and it is similarly dangerous.

See [Node Joined With No Data](https://opensource.docs.scylladb.com/branch-5.1/troubleshooting/node-joined-without-any-data.md) for troubleshooting a manually bootstrap node.

## Schedule fstrim

Scylla sets up daily fstrim on the filesystem(s),
containing your Scylla commitlog and data directory. This utility will
discard, or trim, any blocks no longer in use by the filesystem.

## Experimental Features

Scylla Open Source uses experimental flags to expose non-production-ready features safely. These features are not stable enough to be used in production, and their API will likely change, breaking backward or forward compatibility.

In recent Scylla versions, these features are controlled by the `experimental_features` list in scylla.yaml, allowing one to choose which experimental to enable.
For example, some of the experimental features in Scylla Open Source 4.5 are: `udf`, `alternator-streams` and `raft`.
Use `scylla --help` to get the list of experimental features.

Scylla Enterprise and Scylla Cloud do not officially support experimental Features.

## Monitoring

Scylla exposes interfaces for online monitoring, as described below.

### Monitoring Interfaces

[Scylla Monitoring Interfaces](https://monitoring.docs.scylladb.com/stable/reference/monitoring_apis.html)

### Monitoring Stack

[Scylla Monitoring Stack](https://monitoring.docs.scylladb.com)

### JMX

Scylla JMX is compatible with Apache Cassandra, exposing the relevant subset of MBeans.

<!-- REST -->

### REST

For each JMX operation, attribute get and set, Scylla exposes a matching REST API. You can interact with the REST API using `curl` or using the Swagger UI available at `your-ip:10000/ui`

### Un-contents

Scylla is designed for high performance before tuning, for fewer layers that interact in unpredictable ways, and to use better algorithms that do not require manual tuning. The following items are found in the manuals for other data stores but do not need to appear here.

#### Configuration un-contents

* Generating tokens
* Configuring virtual nodes

#### Operations un-contents

* Tuning Bloom filters
* Data caching
* Configuring memtable throughput
* Configuring compaction
* Compression

#### Testing compaction and compression

* Tuning Java resources
* Purging gossip state on a node

## Help with Scylla

Contact [Support](https://www.scylladb.com/product/support/), or visit the Scylla [Community](https://www.scylladb.com/open-source-community/) page for peer support.

© 2016, The Apache Software Foundation.

Apache®, Apache Cassandra®, Cassandra®, the Apache feather logo and the Apache Cassandra® Eye logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. No endorsement by The Apache Software Foundation is implied by the use of these marks.
