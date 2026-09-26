# Upgrade Guide - Scylla Enterprise 2018.1 to 2019.1 for 16.04

This document is a step by step procedure for upgrading from Scylla Enterprise 2018.1 to Scylla Enterprise 2019.1, and rollback to 2018.1 if required.

## Applicable versions

This guide covers upgrading Scylla from the following versions: 2018.1.7 or later to Scylla Enterprise version 2019.1.y on the following platform:

* 16.04

#### NOTE
This upgrade procedure only works from **2018.1.7** or later. If you have an older Scylla Enterprise 2018.1.x version, please contact the Scylla Support team for advice.

## Upgrade Procedure

#### NOTE
Execute the following commands one node at the time, moving to the next node only **after** the upgrade procedure completed successfully.

#### WARNING
If you are using CDC and upgrading Scylla 4.3 to 4.4, please review the API updates in [querying CDC streams](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/cdc/cdc-querying-streams.md) and [CDC stream generations](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/cdc/cdc-stream-generations.md).
In particular, you should update applications that use CDC according to [CDC Upgrade notes](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/cdc/cdc-querying-streams.md#scylla-4-3-to-4-4-upgrade) **before** upgrading the cluster to 4.4.

If you are using CDC and upgrading from pre 4.3 version to 4.3, note the [upgrading from experimental CDC](https://opensource.docs.scylladb.com/branch-5.1/kb/cdc-experimental-upgrade.md).

#### NOTE
If **any** of your instances are running Scylla Enterprise 2019.1.6 or earlier, **and** one of your Scylla nodes is up for more than a year, you might have been exposed to issue [#6063](https://github.com/scylladb/scylla/pull/6083).
One way to check this is by comparing Generation No  (from nodetool gossipinfo output) with the current time in Epoch format (date +%s), and check if the difference is higher than one year (31536000 seconds).
See [scylla-check-gossiper-generation](https://github.com/scylladb/scylla-code-samples/tree/master/scylla-check-gossiper-generation) for a script to do just that.

If this is the case, do **not** initiate the upgrade process before consulting with Scylla Support for further instructions.

#### NOTE
Scylla Enterprise 2019.1.6 added a new configuration to restrict the memory usage cartesian product IN queries.
If you are using IN in SELECT operations and hitting a  *“cartesian product size … is greater than maximum”* error, you can either update the query (recommended) or bypass the warning temporarily by adding the following parameters to *scylla.yaml*:

* *max_clustering_key_restrictions_per_query: 1000*
* *max_partition_key_restrictions_per_query: 1000*

The higher the values, the more likely you will hit an out of memory issue.

#### NOTE
Scylla Enterprise 2019.1.8 added a new configuration to restrict the memory usage of reverse queries.
If you are using reverse queries and hitting an error  *“Aborting reverse partition read because partition … is larger than the maximum safe size of … for reversible partitions”* see the [reverse queries FAQ section](https://opensource.docs.scylladb.com/branch-5.1/troubleshooting/reverse-queries.md).

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

**During** the rolling upgrade it is highly recommended:

* Not to use new 2019.1 features
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes. See [sctool](https://manager.docs.scylladb.com/stable/sctool/index.html) for suspending Scylla Manager scheduled or running repairs.
* Not to apply schema changes

#### NOTE
Before upgrading to 2019.1, make sure to use [Scylla Monitoring 2.3](https://github.com/scylladb/scylla-monitoring/releases/tag/scylla-monitoring-2.3) or newer, for the 2019.1 Dashboards.

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
sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-2018.1
```

### Gracefully stop the node

```sh
sudo service scylla-server stop
```

### Download and install the new release

Before upgrading, check what version you are running now using `dpkg -s scylla-server`. You should use the same version in case you want to [rollback](/upgrade/upgrade-enterprise/upgrade-guide-from-2018.1-to-2019.1/upgrade-guide-from-2018.1-to-2019.1-ubuntu-16-04/#rollback-procedure) the upgrade. If you are not running a 2018.1.x version, stop right here! This guide only covers 2018.1.x to 2019.1.y upgrades.

To upgrade:

1. Update the [Scylla Enterprise Deb repo](https://www.scylladb.com/download/enterprise/scylla-ubuntu-16-04/) to **2019.1**, and enable scylla/ppa repo

```sh
Ubuntu 16:
sudo add-apt-repository -y ppa:scylladb/ppa
```

1. Config java to 1.8, which is requested by Scylla Enterprise 2019.1

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
3. Check scylla-server log (by `journalctl _COMM=scylla`) and `/var/log/syslog` to validate there are no errors.
4. Check again after two minutes, to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

* More on [Scylla Metrics Update - Scylla Enterprise 2018.1 to 2019.1](https://opensource.docs.scylladb.com/branch-5.1/upgrade/upgrade-enterprise/upgrade-guide-from-2018.1-to-2019.1/metric-update-2018.1-to-2019.1.md)

## Rollback Procedure

#### NOTE
Execute the following commands one node at the time, moving to the next node only **after** the rollback procedure completed successfully.

The following procedure describes a rollback from Scylla Enterprise release 2019.1.x to 2018.1.y. Apply this procedure if an upgrade from 2018.1 to 2019.1 failed before completing on all nodes. Use this procedure only for nodes you upgraded to 2019.1

Scylla rollback is a rolling procedure that does **not** require a full cluster shutdown.
For each of the nodes rollback to 2018.1, you will:

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

1. Update the [Scylla Enterprise Deb repo](https://www.scylladb.com/download/enterprise/scylla-ubuntu-16-04/) to **2018.1**
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
sudo cp -a /etc/scylla/scylla.yaml.backup-2018.1 /etc/scylla/scylla.yaml
```

### Restore system tables

Restore all tables of **system** and **system_schema** from previous snapshot, 2019.1 uses a different set of system tables. Reference doc: [Restore from a Backup and Incremental Backup](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/procedures/backup-restore/restore.md)

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

Check the upgrade instruction above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
