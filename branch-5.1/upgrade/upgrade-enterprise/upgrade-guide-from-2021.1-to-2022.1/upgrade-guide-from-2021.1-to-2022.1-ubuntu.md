# Upgrade Guide - ScyllaDB Enterprise 2021.1 to 2022.1 for Ubuntu

This document is a step-by-step procedure for upgrading from ScyllaDB Enterprise 2021.1 to ScyllaDB Enterprise 2022.1, and rollback to 2021.1 if required.

## Applicable Versions

This guide covers upgrading ScyllaDB Enterprise from version **2021.1.8** or later to ScyllaDB Enterprise version 2022.1.y on Ubuntu. See [OS Support by Platform and Version](https://opensource.docs.scylladb.com/branch-5.1/getting-started/os-support.md) for information about supported versions.

## Upgrade Procedure

#### NOTE
The note is only useful when CDC is GA supported in the target ScyllaDB. Execute the following commands one node at a time, moving to the next node only **after** the upgrade procedure completed successfully.

#### WARNING
If you are using CDC and upgrading ScyllaDB 2021.1 to 2022.1, please review the API updates in [querying CDC streams](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/cdc/cdc-querying-streams.md) and [CDC stream generations](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/cdc/cdc-stream-generations.md).
In particular, you should update applications that use CDC according to [CDC Upgrade notes](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/cdc/cdc-querying-streams.md#scylla-4-3-to-4-4-upgrade) **before** upgrading the cluster to 2022.1.

A ScyllaDB upgrade is a rolling procedure that does **not** require a full cluster shutdown.
For each of the nodes in the cluster, you will:

* Check the cluster schema
* Drain the node and backup the data
* Backup the configuration file
* Stop ScyllaDB
* Download and install the new ScyllaDB packages
* Start ScyllaDB
* Validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node that you upgraded is up and running the new version.

**During** the rolling upgrade, it is highly recommended:

* Not to use new 2022.1 features.
* Not to run administration functions, like repairs, refresh, rebuild, or add or remove nodes. See [sctool](https://manager.docs.scylladb.com/stable/sctool/index.html) for suspending ScyllaDB Manager’s scheduled or running repairs.
* Not to apply schema changes.

#### NOTE
Before upgrading to 2022.1, make sure to use [Scylla Monitoring 4.0](https://github.com/scylladb/scylla-monitoring/releases/tag/scylla-monitoring-4.0.0) or newer for the 2022.1 dashboards.

## Upgrade Steps

### Check the cluster schema

Make sure that all nodes have the schema synched before the upgrade. The upgrade will fail if there is a schema disagreement between nodes.

```sh
nodetool describecluster
```

### Drain the nodes and backup the data

Before any major procedure, like an upgrade, it is recommended to backup all the data to an external device. In ScyllaDB, backup is done using the `nodetool snapshot` command. For **each** node in the cluster, run the following command:

```sh
nodetool drain
nodetool snapshot
```

Take note of the directory name that nodetool gives you, and copy all the directories having this name under `/var/lib/scylla` to a backup device.

When the upgrade is completed on all nodes, the snapshot should be removed with the `nodetool clearsnapshot -t <snapshot>` command to prevent running out of space.

### Backup the configuration file

```sh
sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-2021.1
```

### Gracefully stop the node

```sh
sudo service scylla-server stop
```

### Download and install the new release

Before upgrading, check what version you are running now using `dpkg -l scylla\*server`. You should use the same version in case you want to [rollback](/upgrade/upgrade-enterprise/upgrade-guide-from-2021.1-to-2022.1/upgrade-guide-from-2021.1-to-2022.1-ubuntu/#rollback-procedure) the upgrade. If you are not running a 2021.1.x version, stop right here! This guide only covers 2021.1.x to 2022.1.y upgrades.

**To upgrade ScyllaDB:**

1. Update the [ScyllaDB Enterprise Deb repo](https://www.scylladb.com/customer-portal/?product=ent&platform=ubuntu-20.04&version=stable-release-2022.1) to **2022.1** and enable scylla/ppa repo.
   ```sh
   Ubuntu 16:
   sudo add-apt-repository -y ppa:scylladb/ppa
   ```
2. Configure Java 1.8, which is requested by ScyllaDB Enterprise 2022.1.
   ```sh
   sudo apt-get update
   sudo apt-get install -y openjdk-8-jre-headless
   sudo update-java-alternatives -s java-1.8.0-openjdk-amd64
   ```
3. Install:
   ```sh
   sudo apt-get clean all
   sudo apt-get update
   sudo apt-get dist-upgrade scylla-enterprise
   ```

Answer ‘y’ to the first two questions.

### Start the node

A new io.conf format was introduced in Scylla 2.3 and 2019.1. If your io.conf doesn’t contain –io-properties-file option, then it’s still the old format. You need to re-run the io setup to generate new io.conf.

```sh
sudo scylla_io_setup
```

```sh
sudo service scylla-server start
```

### Validate

1. Check cluster status with `nodetool status` and make sure **all** nodes, including the one you just upgraded, are in UN status.
2. Use `curl -X GET "http://localhost:10000/storage_service/scylla_release_version"` to check the ScyllaDB version.
3. Check scylla-server log (by `journalctl _COMM=scylla`) and `/var/log/syslog` to validate there are no errors.
4. Check again after two minutes to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

See [Scylla Metrics Update - Scylla Enterprise 2021.1 to 2022.1](https://opensource.docs.scylladb.com/branch-5.1/upgrade/upgrade-enterprise/upgrade-guide-from-2021.1-to-2022.1/metric-update-2021.1-to-2022.1.md) for more information.

## Rollback Procedure

#### NOTE
Execute the following commands one node at the time, moving to the next node only **after** the rollback procedure completed successfully.

The following procedure describes a rollback from ScyllaDB Enterprise release 2022.1.x to 2022.1.y. Apply this procedure if an upgrade from 2021.1 to 2022.1 failed before completing on all nodes. Use this procedure only for nodes you upgraded to 2022.1

ScyllaDB rollback is a rolling procedure that does **not** require a full cluster shutdown.
For each of the nodes you rollback to 2021.1, you will:

* Drain the node and stop ScyllaDB
* Retrieve the old Scylla packages
* Restore the configuration file
* Restart ScyllaDB
* Validate the rollback success

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running with the new version.

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
2. Update the [ScyllaDB Enterprise Deb repo](https://www.scylladb.com/customer-portal/?product=ent&platform=ubuntu-20.04&version=stable-release-2022.1) to **2021.1**.
3. Install:
   > ```sh
   > sudo apt-get clean all
   > sudo apt-get update
   > sudo apt-get remove scylla\* -y
   > sudo apt-get install scylla-enterprise
   > ```

Answer ‘y’ to the first two questions.

### Restore the configuration file

```sh
sudo rm -rf /etc/scylla/scylla.yaml
sudo cp -a /etc/scylla/scylla.yaml.backup-2021.1 /etc/scylla/scylla.yaml
```

### Restore system tables

Restore all tables of **system** and **system_schema** from the previous snapshot - 2022.1 uses a different set of system tables. Refer to [Restore from a Backup and Incremental Backup](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/procedures/backup-restore/restore.md).

```sh
cd /var/lib/scylla/data/keyspace_name/table_name-UUID/snapshots/<snapshot_name>/
sudo cp -r * /var/lib/scylla/data/keyspace_name/table_name-UUID/
sudo chown -R scylla:scylla /var/lib/scylla/data/keyspace_name/table_name-UUID/
```

### Start the node

```sh
sudo service scylla-server start
```

### Validate

Check the upgrade instructions above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
