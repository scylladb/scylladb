<!-- The |RHEL_EPEL| variable needs to be adjuster per release, depending on support for RHEL. -->
<!-- 5.2 supports Rocky/RHEL 8 only -->
<!-- 5.4 supports Rocky/RHEL 8 and 9 -->

# Install ScyllaDB Linux Packages

We recommend installing ScyllaDB using [ScyllaDB Web Installer for Linux](https://opensource.docs.scylladb.com/branch-6.1/getting-started/installation-common/scylla-web-installer.md),
a platform-agnostic installation script, to install ScyllaDB on any supported Linux platform.
Alternatively, you can install ScyllaDB using Linux packages.

This article will help you install ScyllaDB on Linux using platform-specific packages.

## Prerequisites

* Ubuntu, Debian, CentOS, or RHEL (see [OS Support by Platform and Version](https://opensource.docs.scylladb.com/branch-6.1/getting-started/os-support.md)
  for details about supported versions and architecture)
* Root or `sudo` access to the system
* Open [ports used by ScyllaDB](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/security/security-checklist.md#networking-ports)
* (CentOS and RHEL only) Removing Automatic Bug Reporting Tool (ABRT) if installed before installing ScyllaDB,
  as it may conflict with ScyllaDB coredump configuration:
  ```console
  sudo yum remove -y abrt
  ```

<!-- The last requirement may need to be removed. See https://github.com/scylladb/scylladb/issues/14488. -->

## Install ScyllaDB

Debian/Ubuntu

1. Install a repo file and add the ScyllaDB APT repository to your system.
   > ```console
   > sudo mkdir -p /etc/apt/keyrings
   > ```

   > ```console
   > sudo gpg --homedir /tmp --no-default-keyring --keyring /etc/apt/keyrings/scylladb.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 491c93b9de7496a7
   > ```

   > ```console
   > sudo wget -O /etc/apt/sources.list.d/scylla.list http://downloads.scylladb.com/deb/debian/scylla-6.1.list
   > ```
2. Install ScyllaDB packages.
   > ```console
   > sudo apt-get update
   > sudo apt-get install -y scylla
   > ```

   > Running the command installs the latest official version of ScyllaDB Open Source.
   > To install a specific patch version, list all the available patch versions:
   > ```console
   > apt-cache madison scylla
   > ```

   > Then install the selected patch version:
   > ```console
   > apt-get install scylla{,-server,-jmx,-tools,-tools-core,-kernel-conf,-node-exporter,-conf,-python3}=<your patch version>
   > ```

   > The following example shows installing ScyllaDB 5.2.3.
   > ```console
   > apt-cache madison scylla
   > scylla | 5.2.3-0.20230608.ea08d409f155-1 | https://downloads.scylladb.com/downloads/scylla/deb/debian-ubuntu/scylladb-5.2 stable/main amd64 Packages
   > scylla | 5.2.2-0.20230521.9dd70a58c3f9-1 | https://downloads.scylladb.com/downloads/scylla/deb/debian-ubuntu/scylladb-5.2 stable/main amd64 Packages
   > scylla | 5.2.1-0.20230508.f1c45553bc29-1 | https://downloads.scylladb.com/downloads/scylla/deb/debian-ubuntu/scylladb-5.2 stable/main amd64 Packages
   > scylla | 5.2.0-0.20230427.429b696bbc1b-1 | https://downloads.scylladb.com/downloads/scylla/deb/debian-ubuntu/scylladb-5.2 stable/main amd64 Packages
   > ```

   > ```console
   > apt-get install scylla{,-server,-jmx,-tools,-tools-core,-kernel-conf,-node-exporter,-conf,-python3}=5.2.3-0.20230608.ea08d409f155-1
   > ```
3. (Ubuntu only) Set Java 11.
   > ```console
   > sudo apt-get update
   > sudo apt-get install -y openjdk-11-jre-headless
   > sudo update-java-alternatives --jre-headless -s java-1.11.0-openjdk-amd64
   > ```

Centos/RHEL

1. Install the EPEL repository.

   CentOS:
   > ```console
   > sudo yum install epel-release
   > ```

   Rocky/RHEL 8
   > ```console
   > sudo yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm
   > ```

   Rocky/RHEL 9
   > ```console
   > sudo yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm
   > ```
2. Add the ScyllaDB RPM repository to your system.
   > ```console
   > sudo curl -o /etc/yum.repos.d/scylla.repo -L http://downloads.scylladb.com/rpm/centos/scylla-6.1.repo
   > ```
3. Install ScyllaDB packages.
   > ```console
   > sudo yum install scylla
   > ```

   > Running the command installs the latest official version of ScyllaDB Open Source.
   > Alternatively, you can to install a specific patch version:
   > ```console
   > sudo yum install scylla-<your patch version>
   > ```

   > Example: The following example shows the command to install ScyllaDB 5.2.3.
   > ```console
   > sudo yum install scylla-5.2.3
   > ```

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

## Next Steps

* [Configure ScyllaDB](https://opensource.docs.scylladb.com/branch-6.1/getting-started/system-configuration.md)
* Manage your clusters with [ScyllaDB Manager](https://manager.docs.scylladb.com/)
* Monitor your cluster and data with [ScyllaDB Monitoring](https://monitoring.docs.scylladb.com/)
* Get familiar with ScyllaDB’s [command line reference guide](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/nodetool.md).
* Learn about ScyllaDB at [ScyllaDB University](https://university.scylladb.com/)
