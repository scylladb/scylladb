# ScyllaDB Web Installer for Linux

ScyllaDB Web Installer is a platform-agnostic installation script you can run with `curl` to install ScyllaDB on Linux.

See [ScyllaDB Download Center](https://www.scylladb.com/download/#core) for information on manually installing ScyllaDB with platform-specific installation packages.

## Prerequisites

Ensure that your platform is supported by the ScyllaDB version you want to install.
See [OS Support by Platform and Version](https://opensource.docs.scylladb.com/branch-6.1/getting-started/os-support.md).

## Install ScyllaDB with Web Installer

To install ScyllaDB with Web Installer, run:

```console
curl -sSf get.scylladb.com/server | sudo bash
```

By default, running the script installs the latest official version of ScyllaDB Open Source. You can use the following
options to install a different version or ScyllaDB Enterprise:

| Option             | Acceptable values              | Description                                                                                                                                                                                                          |
|--------------------|--------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--scylla-product` | `scylla` | `scylla-enterprise` | Specifies the ScyllaDB product to install: Open Source (`scylla`) or Enterprise (`scylla-enterprise`)  The default is `scylla`.                                                                                      |
| `--scylla-version` | `<version number>`             | Specifies the ScyllaDB version to install. You can specify the major release (`x.y`) to install the latest patch for that version or a specific patch release (`x.y.x`). The default is the latest official version. |

You can run the command with the `-h` or `--help` flag to print information about the script.

### Examples

Installing ScyllaDB Open Source 6.0.1:

```console
curl -sSf get.scylladb.com/server | sudo bash -s -- --scylla-version 6.0.1
```

Installing the latest patch release for ScyllaDB Open Source 6.0:

```console
curl -sSf get.scylladb.com/server | sudo bash -s -- --scylla-version 6.0
```

Installing ScyllaDB Enterprise 2024.1:

```console
curl -sSf get.scylladb.com/server | sudo bash -s -- --scylla-product scylla-enterprise --scylla-version 2024.1
```

## Configure and Run ScyllaDB

1. Configure the following parameters in the `/etc/scylla/scylla.yaml` configuration file.
   * `cluster_name` - The name of the cluster. All the nodes in the cluster must have the same
     cluster name configured.
   * `seeds` - The IP address of the first node. Other nodes will use it as the first contact
     point to discover the cluster topology when joining the cluster.
   * `listen_address` - The IP address that ScyllaDB uses to connect to other nodes in the cluster.
   * `rpc_address` - The IP address of the interface for CQL client connections.
2. Run the `scylla_setup` script to tune the system settings and determine the optimal configuration.
   ```console
   sudo scylla_setup
   ```

   * The script invokes a set of [scripts](https://opensource.docs.scylladb.com/branch-6.1/getting-started/system-configuration.md#system-configuration-scripts) to configure several operating system settings; for example, it sets
     RAID0 and XFS filesystem.
   * The script runs a short (up to a few minutes) benchmark on your storage and generates the `/etc/scylla.d/io.conf`
     configuration file. When the file is ready, you can start ScyllaDB. ScyllaDB will not run without XFS
     or `io.conf` file.
   * You can bypass this check by running ScyllaDB in [developer mode](https://opensource.docs.scylladb.com/branch-6.1/getting-started/installation-common/dev-mod.md).
     We recommend against enabling developer mode in production environments to ensure ScyllaDB’s maximum performance.
3. Run ScyllaDB as a service (if not already running).
   ```console
   sudo systemctl start scylla-server
   ```

Now you can start using ScyllaDB. Here are some tools you may find useful.

Run nodetool:

```console
nodetool status
```

Run cqlsh:

```console
cqlsh
```

Run cassandra-stress:

```console
cassandra-stress write -mode cql3 native
```
