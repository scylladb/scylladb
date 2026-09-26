# Upgrade Guide - Scylla 4.2 to 4.3 for Red Hat Enterprise Linux 7/8 or CentOS 7/8

This document is a step by step procedure for upgrading from Scylla 4.2 to Scylla 4.3, and rollback to 4.2 if required.

## Applicable versions

This guide covers upgrading Scylla from the following versions: 4.2.x or later to Scylla version 4.3.y, on the following platforms:

* Red Hat Enterprise Linux 7/8 or CentOS 7/8

## Upgrade Procedure

#### NOTE
Execute the following commands one node at the time, moving to the next node only **after** the upgrade procedure completed successfully.

#### WARNING
If you are using CDC and upgrading Scylla 4.3 to 4.4, please review the API updates in [querying CDC streams](https://opensource.docs.scylladb.com/branch-5.2/using-scylla/cdc/cdc-querying-streams.md) and [CDC stream generations](https://opensource.docs.scylladb.com/branch-5.2/using-scylla/cdc/cdc-stream-generations.md).
In particular, you should update applications that use CDC according to [CDC Upgrade notes](https://opensource.docs.scylladb.com/branch-5.2/using-scylla/cdc/cdc-querying-streams.md#scylla-4-3-to-4-4-upgrade) **before** upgrading the cluster to 4.4.

If you are using CDC and upgrading from pre 4.3 version to 4.3, note the [upgrading from experimental CDC](https://opensource.docs.scylladb.com/branch-5.2/kb/cdc-experimental-upgrade.md).

Upgrading your Scylla version is a rolling procedure that does not require a full cluster shutdown. For each of the nodes in the cluster, serially (i.e. one at a time), you will:

* Check the cluster’s schema
* Drain the node and backup the data
* Backup the configuration file
* Stop the Scylla service
* Download and install new Scylla packages
* Start the Scylla service
* Validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running with the new version.

**During** the rolling upgrade, it is highly recommended:

* Not to use new 4.3 features
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes. See [sctool](https://manager.docs.scylladb.com/stable/sctool/index.html) for suspending Scylla Manager (only available Scylla Enterprise) scheduled or running repairs.
* Not to apply schema changes

#### NOTE
Before upgrading, make sure to use [Scylla Monitoring 3.4](/operating-scylla/monitoring/) or newer, for the Dashboards.

## Upgrade steps

### Check the cluster schema

Make sure that all nodes have the schema synched prior to upgrade as any schema disagreement between the nodes causes the upgrade to fail.

```sh
nodetool describecluster
```

### Drain node and backup the data

Before any major procedure, like an upgrade, it is **highly recommended** to backup all the data to an external device. In Scylla, backup is done using the `nodetool snapshot` command. For **each** node in the cluster, run the following command:

```sh
nodetool drain
nodetool snapshot
```

Take note of the directory name that nodetool gives you, and copy all the directories having this name under `/var/lib/scylla` to an external backup device.

When the upgrade is complete (for all nodes), remove the snapshot by running `nodetool clearsnapshot -t <snapshot>`, or you risk running out of disk space.

### Backup configuration file

```sh
sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-src
```

### Stop Scylla

Supported OS

```shell
sudo systemctl stop scylla-server
```

Docker

```shell
docker exec -it some-scylla supervisorctl stop scylla
```

(without stopping *some-scylla* container)

### Download and install the new release

Before upgrading, check what Scylla version you are currently running with `rpm -qa | grep scylla-server`. You should use the same version as this version in case you want to [rollback](https://opensource.docs.scylladb.com/branch-5.2/upgrade/upgrade-opensource/upgrade-guide-from-3.0-to-3.1/upgrade-guide-from-3.0-to-3.1-rpm.md#rollback-procedure) the upgrade. If you are not running a 4.2.x version, stop right here! This guide only covers 4.2.x to 4.3.y upgrades.

To upgrade:

1. Update the [Scylla rpm repo](https://www.scylladb.com/download/?platform=centos&version=scylla-4.3)  to 4.3
2. Install the new Scylla version

```sh
sudo yum clean all
sudo yum update scylla\* -y
```

#### NOTE
Alternator users upgrading from Scylla 4.0 to 4.1, need to set [default isolation level](https://opensource.docs.scylladb.com/branch-5.2/upgrade/upgrade-opensource/upgrade-guide-from-4.0-to-4.1/alternator.md)

### Start the node

> Supported OS

> ```shell
> sudo systemctl start scylla-server
> ```

> Docker

> ```shell
> docker exec -it some-scylla supervisorctl start scylla
> ```

> (with *some-scylla* container already running)

### Validate

1. Check cluster status with `nodetool status` and make sure **all** nodes, including the one you just upgraded, are in UN status.
2. Use `curl -X GET "http://localhost:10000/storage_service/scylla_release_version"` to check the Scylla version. Validate that the version matches the one you upgraded to.
3. Use `journalctl _COMM=scylla` to check there are no new errors in the log.
4. Check again after two minutes, to validate no new issues are introduced.

Once you are sure the node upgrade was successful, move to the next node in the cluster.

* More on [Scylla Metrics Update - Scylla 4.2 to 4.3](../metric-update-4.2-to-4.3)

## Rollback Procedure

#### NOTE
Execute the following commands one node at the time, moving to the next node only **after** the rollback procedure completed successfully.

The following procedure describes a rollback from Scylla release 4.3.x to 4.2.y. Apply this procedure if an upgrade from 4.2 to 4.3 failed before completing on all nodes. Use this procedure only for the nodes that you upgraded to 4.3

Scylla rollback is a rolling procedure that does **not** require a full cluster shutdown.
For each of the nodes rollback to 4.2, you will:

* Drain the node and stop Scylla
* Retrieve the old Scylla packages
* Restore the configuration file
* Reload the systemd configuration
* Restart the Scylla service
* Validate the rollback success

Apply the following procedure **serially** on each node. Do not move to the next node before validating the rollback was successful and that the node is up and running with the old version.

## Rollback steps

### Gracefully shutdown Scylla

```sh
  nodetool drain
.. include:: /rst_include/scylla-commands-stop-index.rst
```

### Download and install the new release

1. Remove the old repo file.

```sh
sudo rm -rf /etc/yum.repos.d/scylla.repo
```

1. Update the [Scylla rpm repo](https://www.scylladb.com/download/?platform=centos&version=scylla-4.3)  to 4.2
2. Install

```default
   sudo yum clean all
   sudo rm -rf /var/cache/yum
   sudo yum remove scylla\*tools-core
   sudo yum downgrade scylla\* -y
   sudo yum install scylla
```

### Restore the configuration file

```sh
sudo rm -rf /etc/scylla/scylla.yaml
sudo cp -a /etc/scylla/scylla.yaml.backup-src| /etc/scylla/scylla.yaml
```

### Restore system tables

Restore all tables of **system** and **system_schema** from previous snapshot, 4.3 uses a different set of system tables. Reference doc: [Restore from a Backup and Incremental Backup](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/procedures/backup-restore/restore.md)

```sh
cd /var/lib/scylla/data/keyspace_name/table_name-UUID/snapshots/<snapshot_name>/
sudo cp -r * /var/lib/scylla/data/keyspace_name/table_name-UUID/
sudo chown -R scylla:scylla /var/lib/scylla/data/keyspace_name/table_name-UUID/
```

### Reload systemd configuration

Require to reload the unit file if the systemd unit file is changed.

```sh
sudo systemctl daemon-reload
```

### Start the node

> Supported OS

> ```shell
> sudo systemctl start scylla-server
> ```

> Docker

> ```shell
> docker exec -it some-scylla supervisorctl start scylla
> ```

> (with *some-scylla* container already running)

### Validate

Check the upgrade instruction above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster. Keep in mind that the version you want to see on your node is the old version, which you noted at the beginning of the procedure.
