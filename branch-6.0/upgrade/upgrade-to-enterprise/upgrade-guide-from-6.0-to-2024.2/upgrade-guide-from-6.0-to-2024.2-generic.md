# Upgrade Guide - ScyllaDB 6.0 to 2024.2

This document is a step-by-step procedure for upgrading from ScyllaDB 6.0
to ScyllaDB Enterpise 2024.2, and rollback to version 6.0 if required.

This guide covers upgrading ScyllaDB on Red Hat Enterprise Linux (RHEL) CentOS, Debian,
and Ubuntu. See [OS Support by Platform and Version](https://opensource.docs.scylladb.com/branch-6.0/getting-started/os-support.md)
for information about supported versions.

This guide also applies when you’re upgrading ScyllaDB Enterprise official image on EC2,
GCP, or Azure.

## Before You Upgrade ScyllaDB

**Upgrade Your Driver**

If you’re using a [ScyllaDB driver](https://opensource.docs.scylladb.com/branch-6.0/using-scylla/drivers/cql-drivers/index.md),
upgrade the driver before you upgrade ScyllaDB. The latest two versions of each driver
are supported.

**Upgrade ScyllaDB Monitoring Stack**

If you’re using the ScyllaDB Monitoring Stack, verify that your Monitoring Stack
version supports the ScyllaDB version to which you want to upgrade. See
[ScyllaDB Monitoring Stack Support Matrix](https://monitoring.docs.scylladb.com/stable/reference/matrix.html).

We recommend upgrading the Monitoring Stack to the latest version.

**Check Feature Updates**

See the ScyllaDB Release Notes for the latest updates. The Release Notes are published
at the [ScyllaDB Community Forum](https://forum.scylladb.com/).

#### NOTE
Unlike ScyllaDB 6.0, ScyllaDB Enterprise 2024.2 has **tablets disabled by
default**. This means that after you upgrade to 2024.2:

* Keyspaces that had tablets enabled in 6.0 will continue to work with tablets.
* Keyspaces created with default settings after upgrading to 2024.2 will have
  tablets disabled.

  To use tablets, create a new keyspace with the `tablets = { 'enabled': true }`
  option. For example:
  > ```default
  > CREATE KEYSPACE my_keyspace
  > WITH replication = {
  >     'class': 'NetworkTopologyStrategy',
  >     'replication_factor': 3
  > } AND tablets = {
  >     'enabled': true
  > };
  > ```

  > All tables created in this keyspace will use tablets.
  > Note that `NetworkTopologyStrategy` is required when tablets are enabled.

See [Data Distribution with Tablets](https://opensource.docs.scylladb.com/branch-6.0/architecture/tablets.md) for more information
about tablets.

## Upgrade Procedure

A ScyllaDB upgrade is a rolling procedure that does **not** require full cluster shutdown.
For each of the nodes in the cluster, you will:

* Check that the cluster’s schema is synchronized
* Drain the node and backup the data
* Backup the configuration file
* Stop ScyllaDB
* Download and install new ScyllaDB packages
* Start ScyllaDB
* Validate that the upgrade was successful

**During** the rolling upgrade, it is highly recommended:

* Not to use the new 2024.2 features.
* Not to run administration functions, like repairs, refresh, rebuild, or add or remove
  nodes. See [sctool](https://manager.docs.scylladb.com/stable/sctool/) for suspending
  ScyllaDB Manager’s scheduled or running repairs.
* Not to apply schema changes.

## Upgrade Steps

### Check the cluster schema

Make sure that all nodes have the schema synchronized before upgrade. The upgrade
procedure will fail if there is a schema disagreement between nodes.

```sh
nodetool describecluster
```

### Drain the nodes and backup the data

Before any major procedure, like an upgrade, it is recommended to backup all
the data to an external device. In ScyllaDB, you can backup the data using
the `nodetool snapshot` command. For **each** node in the cluster, run
the following command:

```sh
nodetool drain
nodetool snapshot
```

Take note of the directory name that nodetool gives you, and copy all the directories
having that name under `/var/lib/scylla` to a backup device.

When the upgrade is completed on all nodes, remove the snapshot with the
`nodetool clearsnapshot -t <snapshot>` command to prevent running out of space.

### Backup the configuration file

```sh
sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-src
```

### Gracefully stop the node

```sh
sudo service scylla-server stop
```

### Download and install the new release

Before upgrading, check what version you are running now using `scylla --version`.
You should use the same version as this version in case you want to [rollback](./#rollback-procedure)
the upgrade.

Debian/Ubuntu

1. Update the ScyllaDB deb repo ([Debian](https://www.scylladb.com/customer-portal/?product=ent&platform=debian-11&version=stable-release-2024.2), [Ubuntu](https://www.scylladb.com/customer-portal/?product=ent&platform=ubuntu-22.04&version=stable-release-2024.2)) to 2024.2.
2. Configure Java 1.8:
   > ```console
   > sudo apt-get update
   > sudo apt-get install -y openjdk-8-jre-headless
   > sudo update-java-alternatives -s java-1.8.0-openjdk-amd64
   > ```
3. Install the new ScyllaDB version:
   > ```console
   > sudo apt-get clean all
   > sudo apt-get update
   > sudo apt-get remove scylla\*
   > sudo apt-get install scylla-enterprise
   > sudo systemctl daemon-reload
   > ```

Answer ‘y’ to the first two questions.

RHEL/CentOS

1. Update the [ScyllaDB rpm repo](https://www.scylladb.com/customer-portal/?product=ent&platform=centos7&version=stable-release-2024.2)  to 2024.2.
2. Install the new ScyllaDB version:
   > ```sh
   > sudo yum clean all
   > sudo rm -rf /var/cache/yum
   > sudo yum remove scylla\*
   > sudo yum install scylla-enterprise
   > ```

EC2/GCP/Azure Ubuntu Image

If you’re using the ScyllaDB official image (recommended), see
the **Debian/Ubuntu** tab for upgrade instructions. If you’re using your
own image and have installed ScyllaDB packages for Ubuntu or Debian,
you need to apply an extended upgrade procedure:

1. Update the ScyllaDB deb repo (see above).
2. Configure Java 1.8 (see above).
3. Install the new ScyllaDB version with the additional
   `scylla-enterprise-machine-image` package:

> ```default
> sudo apt-get clean all
> sudo apt-get update
> sudo apt-get dist-upgrade scylla-enterprise
> sudo apt-get dist-upgrade scylla-enterprise-machine-image
> ```
1. Run `scylla_setup` without running `io_setup`.
2. Run `sudo /opt/scylladb/scylla-machine-image/scylla_cloud_io_setup`.

### Start the node

```sh
sudo service scylla-server start
```

### Validate

1. Check cluster status with `nodetool status` and make sure **all** nodes, including
   the one you just upgraded, are in `UN` status.
2. Use `curl -X GET "http://localhost:10000/storage_service/scylla_release_version"`
   to check the ScyllaDB version. Validate that the version matches the one you upgraded to.
3. Check scylla-server log (using `journalctl _COMM=scylla`) and `/var/log/syslog`
   to validate there are no new errors in the log.
4. Check again after two minutes to validate that no new issues are introduced.

Once you are sure the node upgrade was successful, move to the next node in the cluster.

## Rollback Procedure

#### WARNING
The rollback procedure can only be applied if some nodes have **not** been upgraded
to 2024.2 yet. As soon as the last node in the rolling upgrade procedure is
started with 2024.2, rollback becomes impossible. At that point, the only way
to restore a cluster to 6.0 is by restoring it from backup.

The following procedure describes a rollback from ScyllaDB 2024.2.x to
6.0.y. Apply this procedure if an upgrade from 6.0 to 2024.2
failed before completing on all nodes.

* Use this procedure only for nodes you upgraded to 2024.2.
* Execute the commands one node at a time, moving to the next node
  only after the rollback procedure is completed successfully.

ScyllaDB rollback is a rolling procedure that does **not** require a full cluster shutdown.
For each of the nodes you rollback to 6.0, you will:

* Drain the node and stop ScyllaDB
* Retrieve the old ScyllaDB packages
* Restore the configuration file
* Restore system tables
* Reload systemd configuration
* Restart ScyllaDB
* Validate the rollback success

Apply the procedure **serially** on each node. Do not move to the next node
before validating that the rollback was successful and the node is up and
running the old version.

## Rollback Steps

### Drain and gracefully stop the node

```sh
nodetool drain
sudo service scylla-server stop
```

### Download and install the old release

Debian/Ubuntu

1. Remove the old repo file.
   > ```sh
   > sudo rm -rf /etc/apt/sources.list.d/scylla.list
   > ```
2. Update the ScyllaDB deb repo ([Debian](https://www.scylladb.com/download/?platform=debian-11&version=scylla-6.0), [Ubuntu](https://www.scylladb.com/download/?platform=ubuntu-22.04&version=scylla-6.0)) to 6.0.
3. Install:
   > ```default
   > sudo apt-get update
   > sudo apt-get remove scylla\* -y
   > sudo apt-get install scylla
   > ```

Answer ‘y’ to the first two questions.

RHEL/CentOS

1. Remove the old repo file.
   > ```sh
   > sudo rm -rf /etc/yum.repos.d/scylla.repo
   > ```
2. Update the [ScyllaDB rpm repo](https://www.scylladb.com/download/?platform=centos&version=scylla-6.0)  to 6.0.
3. Install:
   > ```console
   > sudo yum clean all
   > sudo yum remove scylla\*
   > sudo yum install scylla
   > ```

#### NOTE
If you are running a ScyllaDB Enterprise official image (for EC2 AMI, GCP, or Azure), follow the instructions for Ubuntu.

### Restore the configuration file

```sh
sudo rm -rf /etc/scylla/scylla.yaml
sudo cp -a /etc/scylla/scylla.yaml.backup-src | /etc/scylla/scylla.yaml
```

### Restore system tables

Restore all tables of **system** and **system_schema** from the previous snapshot because
2024.2 uses a different set of system tables.
See [Restore from a Backup and Incremental Backup](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/procedures/backup-restore/restore.md)
for reference.

```console
cd /var/lib/scylla/data/keyspace_name/table_name-UUID/
sudo find . -maxdepth 1 -type f  -exec sudo rm -f "{}" +
cd /var/lib/scylla/data/keyspace_name/table_name-UUID/snapshots/<snapshot_name>/
sudo cp -r * /var/lib/scylla/data/keyspace_name/table_name-UUID/
sudo chown -R scylla:scylla /var/lib/scylla/data/keyspace_name/table_name-UUID/
```

### Reload systemd configuration

You must reload the unit file if the systemd unit file is changed.

```sh
sudo systemctl daemon-reload
```

### Start the node

```sh
sudo service scylla-server start
```

### Validate

Check the upgrade instructions above for validation. Once you are sure the node rollback
is successful, move to the next node in the cluster.
