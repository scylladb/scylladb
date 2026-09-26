# Upgrade Guide - Scylla 3.x.y to 3.x.z for Ubuntu 14.04, 16.04 or 18.04

This document is a step by step procedure for upgrading from Scylla 3.x.y to Scylla 3.x.z.

## Applicable versions

This guide covers upgrading Scylla from the following versions: 3.x.y to Scylla version 3.x.z on the following platform:

* Ubuntu 14.04, 16.04 or 18.04

#### NOTE
Ubuntu 14 is not supported in the following versions of Scylla Open Source: 3.1.x and 3.0.11 and above

#### WARNING
If and only if you installed a fresh Scylla 3.1.0, you must add the following line to `scylla.yaml` of each node before upgrading to a later Scylla version:

`enable_3_1_0_compatibility_mode: true`

This is not relevant if your cluster was upgraded to 3.1.0 from an older version, or you are upgrading from or to any other Scylla release.

## Upgrade Procedure

#### NOTE
Execute the following commands one node at the time, moving to the next node only **after** the upgrade procedure completed successfully.

#### WARNING
If you are using CDC and upgrading Scylla 4.3 to 4.4, please review the API updates in [querying CDC streams](https://opensource.docs.scylladb.com/branch-5.2/using-scylla/cdc/cdc-querying-streams.md) and [CDC stream generations](https://opensource.docs.scylladb.com/branch-5.2/using-scylla/cdc/cdc-stream-generations.md).
In particular, you should update applications that use CDC according to [CDC Upgrade notes](https://opensource.docs.scylladb.com/branch-5.2/using-scylla/cdc/cdc-querying-streams.md#scylla-4-3-to-4-4-upgrade) **before** upgrading the cluster to 4.4.

If you are using CDC and upgrading from pre 4.3 version to 4.3, note the [upgrading from experimental CDC](https://opensource.docs.scylladb.com/branch-5.2/kb/cdc-experimental-upgrade.md).

A Scylla upgrade is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes in the cluster, you will:

* Drain node and backup the data
* Check your current release
* Backup configuration file
* Stop Scylla
* Download and install new Scylla packages
* Start Scylla
* Validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running with the new version.

**During** the rolling upgrade it is highly recommended:

* Not to use new 3.x.z features
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes
* Not to apply schema changes

## Upgrade steps

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
sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-3.x.z
```

### Gracefully stop the node

```sh
sudo service scylla-server stop
```

### Download and install the new release

Before upgrading, check what version you are running now using `dpkg -s scylla-server`. You should use the same version in case you want to [rollback](/upgrade/upgrade-opensource/upgrade-guide-from-3.x.y-to-3.x.z/upgrade-guide-from-3.x.y-to-3.x.z-ubuntu/#rollback-procedure) the upgrade. If you are not running a 3.x.y version, stop right here! This guide only covers 3.x.y to 3.x.z upgrades.

To upgrade:

1. Update the [Scylla deb repo](http://www.scylladb.com/download/) to **3.x**
2. Install

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
3. Check scylla-server log (check `/var/log/upstart/scylla-server.log` for Ubuntu 14.04, execute `journalctl _COMM=scylla` for Ubuntu 16.04 and Ubuntu 18.04) and `/var/log/syslog` to validate there are no errors.
4. Check again after 2 minutes, to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

## Rollback Procedure

#### NOTE
Execute the following commands one node at the time, moving to the next node only **after** the rollback procedure completed successfully.

The following procedure describes a rollback from Scylla release 3.x.z to 3.x.y. Apply this procedure if an upgrade from 3.x.y to 3.x.z failed before completing on all nodes. Use this procedure only for nodes you upgraded to 3.x.z

Scylla rollback is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes rollback to 3.x.y, you will:

* Drain the node and stop Scylla
* Downgrade to previous release
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

### Downgrade to previous release

1. install

```sh
sudo apt-get install scylla=3.x.y\* scylla-server=3.x.y\* scylla-jmx=3.x.y\* scylla-tools=3.x.y\* scylla-tools-core=3.x.y\* scylla-kernel-conf=3.x.y\* scylla-conf=3.x.y\*
```

Answer ‘y’ to the first two questions.

### Restore the configuration file

```sh
sudo rm -rf /etc/scylla/scylla.yaml
sudo cp -a /etc/scylla/scylla.yaml.backup-3.x.z /etc/scylla/scylla.yaml
```

### Start the node

```sh
sudo service scylla-server start
```

### Validate

Check upgrade instruction above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
