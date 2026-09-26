# Upgrade Guide - ScyllaDB 5.0 to 5.1

This document is a step by step procedure for upgrading from ScyllaDB 5.0 to ScyllaDB 5.1, and rollback to version 5.0 if required.

This guide covers upgrading Scylla on Red Hat Enterprise Linux (RHEL) 7/8, CentOS 7/8, Debian 10 and Ubuntu 20.04. It also applies when using ScyllaDB official image on EC2, GCP, or Azure; the image is based on Ubuntu 20.04.

See [OS Support by Platform and Version](https://opensource.docs.scylladb.com/branch-5.4/getting-started/os-support.md) for information about supported versions.

## Upgrade Procedure

A ScyllaDB upgrade is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes in the cluster, serially (i.e. one node at a time), you will:

* Check that the cluster’s schema is synchronized
* Drain the node and backup the data
* Backup the configuration file
* Stop ScyllaDB
* Download and install new ScyllaDB packages
* Start ScyllaDB
* Validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to the next node before validating that the node you upgraded is up and running the new version.

**During** the rolling upgrade, it is highly recommended:

* Not to use the new 5.1 features
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes. See [sctool](https://manager.docs.scylladb.com/stable/sctool/) for suspending ScyllaDB Manager (only available for ScyllaDB Enterprise) scheduled or running repairs.
* Not to apply schema changes

#### NOTE
Before upgrading, make sure to use the latest [ScyllaDB Monitoring](https://monitoring.docs.scylladb.com/) stack.

## Upgrade Steps

### Check the cluster schema

Make sure that all nodes have the schema synchronized before upgrade. The upgrade procedure will fail if there is a schema disagreement between nodes.

```sh
nodetool describecluster
```

### Drain the nodes and backup the data

Before any major procedure, like an upgrade, it is recommended to backup all the data to an external device. In Scylla, backup is done using the `nodetool snapshot` command. For **each** node in the cluster, run the following command:

```sh
nodetool drain
nodetool snapshot
```

Take note of the directory name that nodetool gives you, and copy all the directories having that name under `/var/lib/scylla` to a backup device.

When the upgrade is completed on all nodes, remove the snapshot with the `nodetool clearsnapshot -t <snapshot>` command to prevent running out of space.

### Backup the configuration file

Back up the `scylla.yaml` configuration file and the ScyllaDB packages
in case you need to rollback the upgrade.

Debian/Ubuntu

```sh
sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup
sudo cp /etc/apt/sources.list.d/scylla.list ~/scylla.list-backup
```

RHEL/CentOS

```sh
sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup
sudo cp /etc/yum.repos.d/scylla.repo ~/scylla.repo-backup
```

### Gracefully stop the node

```sh
sudo service scylla-server stop
```

### Download and install the new release

Debian/Ubuntu

Before upgrading, check what version you are running now using `dpkg -s scylla-server`. You should use the same version as this version in case you want to [rollback](./#rollback-procedure) the upgrade. If you are not running a 5.0.x version, stop right here! This guide only covers 5.0.x to 5.1.y upgrades.

**To upgrade ScyllaDB:**

1. Update the ScyllaDB deb repo ([Debian](https://www.scylladb.com/download/?platform=debian-10&version=scylla-5.1), [Ubuntu](https://www.scylladb.com/download/?platform=ubuntu-20.04&version=scylla-5.1)) to 5.1.
2. Install the new ScyllaDB version:
   > ```console
   > sudo apt-get clean all
   > sudo apt-get update
   > sudo apt-get dist-upgrade scylla
   > ```

Answer ‘y’ to the first two questions.

RHEL/CentOS

Before upgrading, check what version you are running now using `rpm -qa | grep scylla-server`. You should use the same version as this version in case you want to [rollback](./#rollback-procedure) the upgrade. If you are not running a 5.0.x version, stop right here! This guide only covers 5.0.x to 5.1.y upgrades.

**To upgrade ScyllaDB:**

1. Update the [ScyllaDB rpm repo](https://www.scylladb.com/download/?platform=centos&version=scylla-5.1)  to 5.1.
2. Install the new ScyllaDB version:
   > ```sh
   > sudo yum clean all
   > sudo yum update scylla\* -y
   > ```

#### NOTE
If you are running a ScyllaDB official image (for EC2 AMI, GCP, or Azure),
you need to:

> * Download and install the new ScyllaDB release for Ubuntu; see
>   the Debian/Ubuntu tab above for instructions.
> * Update underlying OS packages.
>   > See [Upgrade ScyllaDB Image](https://opensource.docs.scylladb.com/branch-5.4/upgrade/ami-upgrade.md) for details.

### Start the node

```sh
sudo service scylla-server start
```

### Validate

1. Check cluster status with `nodetool status` and make sure **all** nodes, including the one you just upgraded, are in `UN` status.
2. Use `curl -X GET "http://localhost:10000/storage_service/scylla_release_version"` to check the ScyllaDB version. Validate that the version matches the one you upgraded to.
3. Check scylla-server log (by `journalctl _COMM=scylla`) and `/var/log/syslog` to validate there are no new errors in the log.
4. Check again after two minutes, to validate no new issues are introduced.

Once you are sure the node upgrade was successful, move to the next node in the cluster.

See [ScyllaDB Metrics Update - ScyllaDB 5.0 to 5.1](/upgrade/upgrade-opensource/upgrade-guide-from-5.0-to-5.1/metric-update-5.0-to-5.1) for more information.

### Update the Mode in perftune.yaml

Due to performance improvements in version 5.1, your cluster’s existing nodes may use a different mode than
the nodes created after the upgrade. Using different modes across one cluster is not recommended, so you
should ensure that the same mode is used on all nodes. See
[Updating the Mode in perftune.yaml After a ScyllaDB Upgrade](https://opensource.docs.scylladb.com/branch-5.4/kb/perftune-modes-sync.md) for instructions.

## Rollback Procedure

#### NOTE
Execute the following commands one node at a time, moving to the next node only **after** the rollback procedure is completed successfully.

The following procedure describes a rollback from ScyllaDB 5.1.x to 5.0.y. Apply this procedure if an upgrade from 5.0 to 5.1 failed before completing on all nodes. Use this procedure only for nodes you upgraded to 5.1.

ScyllaDB rollback is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes you rollback to 5.0, serially (i.e. one node at a time), you will:

* Drain the node and stop Scylla
* Retrieve the old ScyllaDB packages
* Restore the configuration file
* Reload systemd configuration
* Restart ScyllaDB
* Validate the rollback success

Apply the following procedure **serially** on each node. Do not move to the next node before validating that the rollback was successful and the node is up and running the old version.

## Rollback Steps

### Drain and gracefully stop the node

```sh
nodetool drain
nodetool snapshot
sudo service scylla-server stop
```

### Restore and install the old release

Debian/Ubuntu

1. Restore the 5.0 packages backed up during the upgrade.
   > ```sh
   > sudo cp ~/scylla.list-backup /etc/apt/sources.list.d/scylla.list
   > sudo chown root.root /etc/apt/sources.list.d/scylla.list
   > sudo chmod 644 /etc/apt/sources.list.d/scylla.list
   > ```
2. Install:
   > ```default
   > sudo apt-get update
   > sudo apt-get remove scylla\* -y
   > sudo apt-get install scylla
   > ```

Answer ‘y’ to the first two questions.

RHEL/CentOS

1. Restore the 5.0 packages backed up during the upgrade.
   > ```sh
   > sudo cp ~/scylla.repo-backup /etc/yum.repos.d/scylla.repo
   > sudo chown root.root /etc/yum.repos.d/scylla.repo
   > sudo chmod 644 /etc/yum.repos.d/scylla.repo
   > ```
2. Install:
   > ```console
   > sudo yum clean all
   > sudo rm -rf /var/cache/yum
   > sudo yum downgrade scylla-\*cqlsh -y
   > sudo yum remove scylla-\*cqlsh -y
   > sudo yum downgrade scylla\* -y
   > sudo yum install scylla -y
   > ```

### Restore the configuration file

```sh
sudo rm -rf /etc/scylla/scylla.yaml
sudo cp /etc/scylla/scylla.yaml-backup /etc/scylla/scylla.yaml
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

Check the upgrade instructions above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
