# Upgrade Guide - Scylla Enterprise 2017.1 to 2018.1 for Ubuntu 14.04 or 16.04

This document is a step by step procedure for upgrading from Scylla Enterprise 2017.1 to Scylla Enterprise 2018.1, and rollback to 2017.1 if required.

## Applicable versions

This guide covers upgrading Scylla from the following versions: 2017.1.x to Scylla Enterprise version 2018.1.y on the following platform:

* Ubuntu 14.04 or 16.04

## Upgrade Procedure

#### NOTE
Execute the following commands one node at the time, moving to the next node only **after** the upgrade procedure completed successfully.

#### WARNING
If you are using CDC and upgrading Scylla 4.3 to 4.4, please review the API updates in [querying CDC streams](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/cdc/cdc-querying-streams.md) and [CDC stream generations](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/cdc/cdc-stream-generations.md).
In particular, you should update applications that use CDC according to [CDC Upgrade notes](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/cdc/cdc-querying-streams.md#scylla-4-3-to-4-4-upgrade) **before** upgrading the cluster to 4.4.

If you are using CDC and upgrading from pre 4.3 version to 4.3, note the [upgrading from experimental CDC](https://opensource.docs.scylladb.com/branch-5.1/kb/cdc-experimental-upgrade.md).

A Scylla upgrade is a rolling procedure that does **not** require a full cluster shutdown.
For each of the nodes in the cluster, you will:

* Check cluster schema
* Drain node and backup the data
* Backup configuration file
* Stop Scylla
* Download and install new Scylla packages
* Start Scylla
* Validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running with the new version.

Once all the nodes have been upgraded to the new version 2018.1, run a **serial** restart of all the nodes.

**During** the rolling upgrade, it is highly recommended:

* Not to use new 2018.1 features
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes
* Not to apply schema changes

## Upgrade steps

### Check cluster schema

Make sure that all nodes have the schema synched prior to the upgrade. The upgrade will not succeed if there is a schema disagreement between nodes.

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
sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-2017.1
```

### Gracefully stop the node

```sh
sudo service scylla-server stop
```

### Download and install the new release

Before upgrading, check what version you are running now using `dpkg -s scylla-server`. You should use the same version in case you want to [rollback](/upgrade/upgrade-enterprise/upgrade-guide-from-2017.1-to-2018.1/upgrade-guide-from-2017.1-to-2018.1-ubuntu/#rollback-procedure) the upgrade. If you are not running a 2017.1.x version, stop right here! This guide only covers 2017.1.x to 2018.1.y upgrades.

To upgrade:

1. Update the [Scylla Enterprise Deb repo](http://www.scylladb.com/enterprise-download/) to **2018.1**, and enable scylla/ppa repo

```sh
Debian 8:
sudo apt-get install gnupg-curl -y
sudo apt-key adv --fetch-keys https://download.opensuse.org/repositories/home:/scylladb:/scylla-3rdparty-jessie/Debian_8.0/Release.key
sudo sh -c "echo 'deb http://download.opensuse.org/repositories/home:/scylladb:/scylla-3rdparty-jessie/Debian_8.0/ /' > /etc/apt/sources.list.d/scylla-3rdparty.list"

Ubuntu 14/16:
sudo add-apt-repository -y ppa:scylladb/ppa
```

1. Upgrade java to 1.8 on Ubuntu 14.04 and Debian 8, which is requested by Scylla Enterprise 2018.1

* sudo add-apt-repository -y ppa:openjdk-r/ppa
* sudo apt-get update
* sudo apt-get install -y openjdk-8-jre-headless
* sudo update-java-alternatives -s java-1.8.0-openjdk-amd64

1. Install

```sh
sudo apt-get update
sudo apt-get dist-upgrade scylla-enterprise
```

Answer ‘y’ to the first two questions.

### Start the node

```sh
sudo service scylla-server start
```

### Validate

1. Check cluster status with `nodetool status` and make sure **all** nodes, including the one you just upgraded, are in UN status.
2. Use `curl -X GET "http://localhost:10000/storage_service/scylla_release_version"` to check the Scylla version.
3. Check scylla-server log (check `/var/log/upstart/scylla-server.log` for Ubuntu 14.04, execute `journalctl _COMM=scylla` for Ubuntu 16.04 and Debian 8) and `/var/log/syslog` to validate there are no errors.
4. Check again after two minutes to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

* More on [Scylla Metrics Update - Scylla Enterprise 2017.1 to 2018.1](https://opensource.docs.scylladb.com/branch-5.1/upgrade/upgrade-enterprise/upgrade-guide-from-2017.1-to-2018.1/metric-update-2017.1-to-2018.1.md)

## Rollback Procedure

#### NOTE
Execute the following commands one node at the time, moving to the next node only **after** the rollback procedure completed successfully.

The following procedure describes a rollback from Scylla Enterprise release 2018.1.x to 2017.1.y. Apply this procedure if an upgrade from 2017.1 to 2018.1 failed before completing on all nodes. Use this procedure only for nodes you upgraded to 2018.1

Scylla rollback is a rolling procedure that does **not** require a full cluster shutdown.
For each of the nodes rollback to 2017.1, you will:

* Drain the node and stop Scylla
* Retrieve the old Scylla packages
* Restore the configuration file
* Restart Scylla
* Validate the rollback success

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

1. Update the [Scylla Enterprise Deb repo](http://www.scylladb.com/enterprise-download/) to **2017.1**
2. install

```sh
sudo apt-get update
sudo apt-get remove scylla\* -y
sudo apt-get install scylla-enterprise
```

Answer ‘y’ to the first two questions.

### Restore the configuration file

```sh
sudo rm -rf /etc/scylla/scylla.yaml
sudo cp -a /etc/scylla/scylla.yaml.backup-2017.1 /etc/scylla/scylla.yaml
```

### Restore system tables

Restore all tables of **system** and **system_schema** from previous snapshot, 2018.1 uses a different set of system tables. Reference doc: [Restore from a Backup and Incremental Backup](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/procedures/backup-restore/restore.md)

```sh
cd /var/lib/scylla/data/keyspace_name/table_name-UUID/snapshots/<snapshot_name>/
sudo cp -r * /var/lib/scylla/data/keyspace_name/table_name-UUID/
```

### Start the node

```sh
sudo service scylla-server start
```

### Validate

Check the upgrade instruction above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
