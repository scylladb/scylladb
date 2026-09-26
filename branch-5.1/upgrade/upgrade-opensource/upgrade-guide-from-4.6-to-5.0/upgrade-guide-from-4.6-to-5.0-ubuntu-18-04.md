# Upgrade Guide - Scylla 4.6 to 5.0 for Ubuntu 18.04

This document is a step by step procedure for upgrading from Scylla 4.6 to Scylla 5.0, and rollback to version 4.6 if required.

## Applicable Versions

This guide covers upgrading Scylla 4.6 to Scylla 5.0 on Ubuntu 18.04.
See [OS Support by Platform and Version](https://opensource.docs.scylladb.com/branch-5.1/getting-started/os-support.md) for information about supported versions.

## Upgrade Procedure

A ScyllaDB upgrade is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes in the cluster, you will:

* Check the cluster’s schema
* Drain the node and backup the data
* Backup the configuration file
* Stop ScyllaDB
* Download and install new ScyllaDB packages
* Start ScyllaDB
* Validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to the next node before validating that the node you upgraded is up and running the new version.

**During** the rolling upgrade, it is highly recommended:

* Not to use the new 5.0 features
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes. See [sctool](https://manager.docs.scylladb.com/stable/sctool/) for suspending ScyllaDB Manager (only available for ScyllaDB Enterprise) scheduled or running repairs.
* Not to apply schema changes

#### NOTE
Before upgrading, make sure to use the latest [ScyllaDB Montioring](https://monitoring.docs.scylladb.com/) stack.

## Upgrade Steps

### Check the cluster schema

Make sure that all nodes have the schema synced before the upgrade. The upgrade will fail if there is a schema disagreement between nodes.

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

```sh
sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-src
```

### Gracefully stop the node

```sh
sudo service scylla-server stop
```

### Download and install the new release

Before upgrading, check what version you are running now using `dpkg -s scylla-server`. You should use the same version in case you want to [rollback](./#rollback-procedure) the upgrade. If you are not running a 4.6.x version, stop right here! This guide only covers 4.6.x to 5.0.y upgrades.

**To upgrade ScyllaDB:**

1. Update the [Scylla deb repo](https://www.scylladb.com/download/?platform=ubuntu-18.04&version=scylla-5.0) to 5.0.
2. Install:
   > ```console
   > sudo apt-get clean all
   > sudo apt-get update
   > sudo apt-get dist-upgrade scylla
   > ```

Answer ‘y’ to the first two questions.

### Start the node

```sh
sudo service scylla-server start
```

### Validate

1. Check cluster status with `nodetool status` and make sure **all** nodes, including the one you just upgraded, are in UN status.
2. Use `curl -X GET "http://localhost:10000/storage_service/scylla_release_version"` to check the Scylla version.
3. Check scylla-server log (by `journalctl _COMM=scylla`) and `/var/log/syslog` to validate there are no errors.
4. Check again after two minutes to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

See [Scylla Metrics Update - Scylla 4.6 to 5.0](../metric-update-4.6-to-5.0) for more information.

## Rollback Procedure

#### NOTE
Execute the following commands one node at the time, moving to the next node only **after** the rollback procedure completed successfully.

The following procedure describes a rollback from Scylla 5.0.x to 4.6.y. Apply this procedure if an upgrade from 4.6 to 5.0 failed before completing on all nodes. Use this procedure only for nodes you upgraded to 5.0.

ScyllaDB rollback is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes you rollback to 4.6, you will:

* Drain the node and stop Scylla
* Retrieve the old ScyllaDB packages
* Restore the configuration file
* Restore system tables
* Reload systemd configuration
* Restart ScyllaDB
* Validate the rollback success

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running the old version.

## Rollback Steps

### Gracefully shutdown ScyllaDB

```sh
nodetool drain
sudo service scylla-server stop
```

### Download and install the old release

1. Remove the old repo file.
   > ```sh
   > sudo rm -rf /etc/apt/sources.list.d/scylla.list
   > ```
2. Update the [Scylla deb repo](https://www.scylladb.com/download/?platform=ubuntu-18.04&version=scylla-5.0) to 4.6.
3. Install:
   > ```default
   > sudo apt-get update
   > sudo apt-get remove scylla\* -y
   > sudo apt-get install scylla
   > ```

Answer ‘y’ to the first two questions.

### Restore the configuration file

```sh
sudo rm -rf /etc/scylla/scylla.yaml
sudo cp -a /etc/scylla/scylla.yaml.backup-src | /etc/scylla/scylla.yaml
```

### Restore system tables

Restore all tables of **system** and **system_schema** from the previous snapshot because 5.0 uses a different set of system tables. See [Restore from a Backup and Incremental Backup](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/procedures/backup-restore/restore.md) for reference.

```sh
cd /var/lib/scylla/data/keyspace_name/table_name-UUID/snapshots/<snapshot_name>/
sudo cp -r * /var/lib/scylla/data/keyspace_name/table_name-UUID/
sudo chown -R scylla:scylla /var/lib/scylla/data/keyspace_name/table_name-UUID/
```

### Reload systemd configuration

It is required to reload the unit file if the systemd unit file is changed.

```sh
sudo systemctl daemon-reload
```

### Start the node

```sh
sudo service scylla-server start
```

### Validate

Check the upgrade instructions above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
