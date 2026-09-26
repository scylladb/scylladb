# Upgrade Guide - Scylla 1.7 to 2.0 for Ubuntu 14.04 or 16.04

This document is a step by step procedure for upgrading from Scylla 1.7 to Scylla 2.0, and rollback to 1.7 if required.

## Applicable versions

This guide covers upgrading Scylla from the following versions: 1.7.x (x >= 4) to Scylla version 2.0.y on the following platform:

* Ubuntu 14.04 or 16.04

## Upgrade Procedure

#### NOTE
Execute the following commands one node at the time, moving to the next node only **after** the upgrade procedure completed successfully.

#### WARNING
If you are using CDC and upgrading Scylla 4.3 to 4.4, please review the API updates in [querying CDC streams](https://opensource.docs.scylladb.com/branch-5.2/using-scylla/cdc/cdc-querying-streams.md) and [CDC stream generations](https://opensource.docs.scylladb.com/branch-5.2/using-scylla/cdc/cdc-stream-generations.md).
In particular, you should update applications that use CDC according to [CDC Upgrade notes](https://opensource.docs.scylladb.com/branch-5.2/using-scylla/cdc/cdc-querying-streams.md#scylla-4-3-to-4-4-upgrade) **before** upgrading the cluster to 4.4.

If you are using CDC and upgrading from pre 4.3 version to 4.3, note the [upgrading from experimental CDC](https://opensource.docs.scylladb.com/branch-5.2/kb/cdc-experimental-upgrade.md).

A Scylla upgrade is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes in the cluster, you will:

* check cluster schema
* drain node and backup the data
* backup configuration file
* stop Scylla
* download and install new Scylla packages
* start Scylla
* validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running with the new version.

**During** the rolling upgrade it is highly recommended:

* Not to use new 2.0 features
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes
* Not to apply schema changes

## Upgrade steps

### Check cluster schema

Make sure that all nodes have the schema synched prior to upgrade, we won’t survive an upgrade that has schema disagreement between nodes.

```sh
nodetool describecluster
```

### Drain node and backup the data

Before any major procedure, like an upgrade, it is recommended to backup all the data to an external device. In Scylla, backup is done using the `nodetool snapshot` command. For **each** node in the cluster, run the following command:

```sh
nodetool drain
nodetool snapshot
```

Take note of the directory name that nodetool gives you, and copy all the directories having this name under `/var/lib/scylla` to a backup device.

When the upgrade is complete (all nodes), the snapshot should be removed by `nodetool clearsnapshot -t <snapshot>`, or you risk running out of space.

### Backup configuration file

```sh
sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-1.7
```

### Gracefully stop the node

```sh
sudo service scylla-server stop
```

### Download and install the new release

Before upgrading, check what version you are running now using `dpkg -s scylla-server`. You should use the same version in case you want to [rollback](/upgrade/upgrade-opensource/upgrade-guide-from-1.7-to-2.0/upgrade-guide-from-1.7-to-2.0-ubuntu/#rollback-procedure) the upgrade. If you are not running a 1.7.x version, stop right here! This guide only covers 1.7.x to 2.0.y upgrades.

To upgrade:

1. Update the [Scylla deb repo](http://www.scylladb.com/download/) to **2.0**
2. Upgrade java to 1.8 on Ubuntu 14.04 and Debian 8, which is requested by Scylla 2.0

* sudo add-apt-repository -y ppa:openjdk-r/ppa
* sudo apt-get update
* sudo apt-get install -y openjdk-8-jre-headless
* sudo update-java-alternatives -s java-1.8.0-openjdk-amd64

1. Install

```sh
sudo apt-get update
sudo apt-get dist-upgrade scylla
```

Answer ‘y’ to the first two questions.

### Start the node

```sh
sudo service scylla-server start
```

### Validate

1. Check cluster status with `nodetool status` and make sure **all** nodes, including the one you just upgraded, are in UN status.
2. Use `curl -X GET "http://localhost:10000/storage_service/scylla_release_version"` to check scylla version.
3. Check scylla-server log (check `/var/log/upstart/scylla-server.log` for Ubuntu 14.04, execute `journalctl _COMM=scylla` for Ubuntu 16.04 and Debian 8) and `/var/log/syslog` to validate there are no errors.
4. Check again after 2 minutes, to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

* More on [Scylla Metrics Update - Scylla 1.7 to 2.0](https://opensource.docs.scylladb.com/branch-5.2/upgrade/upgrade-opensource/upgrade-guide-from-1.7-to-2.0/metric-update-1.7-to-2.0.md)

## Rollback Procedure

#### NOTE
Execute the following commands one node at the time, moving to the next node only **after** the rollback procedure completed successfully.

The following procedure describes a rollback from Scylla release 2.0.x to 1.7.y (y >= 4). Apply this procedure if an upgrade from 1.7 to 2.0 failed before completing on all nodes. Use this procedure only for nodes you upgraded to 2.0

Scylla rollback is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes rollback to 1.7, you will:

* drain the node and stop Scylla
* retrieve the old Scylla packages
* restore the configuration file
* restart Scylla
* validate the rollback success

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running with the new version.

## Rollback steps

### Gracefully shutdown Scylla

```sh
nodetool drain
sudo service scylla-server stop
```

### download and install the old release

1. Remove the old repo file.

```sh
sudo rm -rf /etc/apt/sources.list.d/scylla.list
```

1. Update the [Scylla deb repo](http://www.scylladb.com/download/) to **1.7**
2. install

```sh
sudo apt-get update
sudo apt-get remove scylla\* -y
sudo apt-get install scylla
```

Answer ‘y’ to the first two questions.

### Restore the configuration file

```sh
sudo rm -rf /etc/scylla/scylla.yaml
sudo cp -a /etc/scylla/scylla.yaml.backup-1.7 /etc/scylla/scylla.yaml
```

### Restore system tables

Restore all tables of **system** and **system_schema** from previous snapshot, 2.0 uses a different set of system tables. Reference doc: [Restore from a Backup and Incremental Backup](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/procedures/backup-restore/restore.md)

```sh
cd /var/lib/scylla/data/keyspace_name/table_name-UUID/snapshots/<snapshot_name>/
sudo cp -r * /var/lib/scylla/data/keyspace_name/table_name-UUID/
```

### Start the node

```sh
sudo service scylla-server start
```

### Validate

Check upgrade instruction above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
