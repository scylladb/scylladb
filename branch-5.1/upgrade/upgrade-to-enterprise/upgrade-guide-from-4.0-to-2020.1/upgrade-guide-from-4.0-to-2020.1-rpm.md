# Upgrade Guide - Scylla 4.0 to 2020.1 for Red Hat Enterprise Linux 7 or CentOS 7

This document is a step by step procedure for upgrading from Scylla 4.0 to Scylla Enterprise 2020.1, and rollback to 4.0 if required.

## Applicable versions

This guide covers upgrading Scylla from the following versions: 4.0.x to Scylla Enterprise version 2020.1.y, on the following platforms:

* Red Hat Enterprise Linux, version 7 and later
* CentOS, version 7 and later
* No longer provide packages for Fedora

#### NOTE
Before upgrading to 2020.1, make sure to use [Scylla Monitoring 3.3](https://github.com/scylladb/scylla-monitoring/releases/tag/scylla-monitoring-3.3) or newer for the 2020.1 Dashboards.

## Upgrade Procedure

#### NOTE
Execute the following commands one node at the time, moving to the next node only **after** the upgrade procedure completed successfully.

#### WARNING
If you are using CDC and upgrading Scylla 4.3 to 4.4, please review the API updates in [querying CDC streams](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/cdc/cdc-querying-streams.md) and [CDC stream generations](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/cdc/cdc-stream-generations.md).
In particular, you should update applications that use CDC according to [CDC Upgrade notes](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/cdc/cdc-querying-streams.md#scylla-4-3-to-4-4-upgrade) **before** upgrading the cluster to 4.4.

If you are using CDC and upgrading from pre 4.3 version to 4.3, note the [upgrading from experimental CDC](https://opensource.docs.scylladb.com/branch-5.1/kb/cdc-experimental-upgrade.md).

A Scylla upgrade is a rolling procedure which does not require full cluster shutdown. For each of the nodes in the cluster, serially (i.e. one at a time), you will:

* Check cluster schema
* Drain node and backup the data
* Backup configuration file
* Stop Scylla
* Download and install new Scylla packages
* Start Scylla
* Validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running with the new version.

**During** the rolling upgrade it is highly recommended:

* Not to use new 2020.1 features
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes. See [sctool](https://manager.docs.scylladb.com/stable/sctool/index.html) for suspending Scylla Manager scheduled or running repairs.
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

### Backup configuration files

```sh
for conf in $( rpm -qc $(rpm -qa | grep scylla) | grep -v contains ); do sudo cp -v $conf $conf.backup-4.0; done
```

### Stop Scylla

```sh
sudo systemctl stop scylla-server
```

### Download and install the new release

Before upgrading, check what version you are running now using `rpm -qa | grep scylla-server`. You should use the same version in case you want to [rollback](#upgrade-4-0-2020-1-rpm-rollback-procedure) the upgrade. If you are not running a 4.0.x version, stop right here! This guide only covers 4.0.x to 2020.1.y upgrades.

To upgrade:

1. Update the [Scylla RPM Enterprise repo](http://www.scylladb.com/enterprise-download/centos_rpm/)  to **2020.1**
2. install

```sh
sudo yum clean all
sudo rm -rf /var/cache/yum
sudo yum remove scylla\*
sudo yum install scylla-enterprise
for conf in $( rpm -qc $(rpm -qa | grep scylla) | grep -v contains ); do sudo cp -v $conf.backup-4.0 $conf; done
```

### Start the node

```sh
sudo systemctl start scylla-server
```

### Validate

1. Check cluster status with `nodetool status` and make sure **all** nodes, including the one you just upgraded, are in UN status.
2. Use `curl -X GET "http://localhost:10000/storage_service/scylla_release_version"` to check scylla version.
3. Use `journalctl _COMM=scylla` to check there are no new errors in the log.
4. Check again after 2 minutes, to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

* More on [Scylla Metrics Update - Scylla 4.0 to 2020.1](https://opensource.docs.scylladb.com/branch-5.1/upgrade/upgrade-to-enterprise/upgrade-guide-from-4.0-to-2020.1/metric-update-4.0-to-2020.1.md)

<a id="upgrade-4-0-2020-1-rpm-rollback-procedure"></a>

## Rollback Procedure

#### NOTE
Execute the following commands one node at the time, moving to the next node only **after** the rollback procedure completed successfully.

The following procedure describes a rollback from Scylla Enterprise release 2020.1.x to 4.0.y. Apply this procedure if an upgrade from 4.0 to 2020.1 failed before completing on all nodes. Use this procedure only for nodes you upgraded to 2020.1

Scylla rollback is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes rollback to 4.0, you will:

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
sudo systemctl stop scylla-server
```

### Download and install the new release

1. Remove the old repo file.

```sh
sudo rm -rf /etc/yum.repos.d/scylla.repo
```

1. Update the [Scylla RPM repo](http://www.scylladb.com/download/?platform=centos) to **4.0**
2. Install

```sh
sudo yum clean all
sudo yum remove scylla\*
sudo yum install scylla
```

### Restore the configuration file

```sh
for conf in $( rpm -qc $(rpm -qa | grep scylla) | grep -v contains ); do sudo cp -v $conf.backup-4.0 $conf; done
```

### Start the node

```sh
sudo systemctl start scylla-server
```

### Validate

Check upgrade instruction above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
