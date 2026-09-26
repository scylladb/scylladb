# Upgrade Guide - Scylla Enterprise 2019.x.y to 2019.x.z for Red Hat Enterprise 7 or CentOS 7

This document is a step by step procedure for upgrading from Scylla Enterprise 2019.x.y to 2019.x.z.

## Applicable versions

This guide covers upgrading Scylla Enterprise from the following versions: 2019.x.y to 2019.x.z, on the following platforms:

* Red Hat Enterprise Linux, version 7 and later
* CentOS, version 7 and later
* No longer provide packages for Fedora

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

A Scylla upgrade is a rolling procedure that does not require a full cluster shutdown. For each of the nodes in the cluster, serially (i.e. one at a time), you will:

* Drain node and backup the data
* Check your current release
* Backup configuration file
* Stop Scylla
* Download and install new Scylla packages
* Start Scylla
* Validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running with the new version.

**During** the rolling upgrade, it is highly recommended:

* Not to use new 2019.x.z features
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
sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-2019.x.z
```

### Stop Scylla

```sh
sudo systemctl stop scylla-server
```

### Download and install the new release

Before upgrading, check what version you are running now using `rpm -qa | grep scylla-server`. You should use the same version in case you want to [rollback](#upgrade-2019-x-y-to-2019-x-z-rpm-rollback-procedure) the upgrade. If you are not running a 2019.x.y version, stop right here! This guide only covers 2019.x.y to 2019.x.z upgrades.

To upgrade:

1. Update the [Scylla Enterprise RPM repo](http://www.scylladb.com/enterprise-download/centos_rpm) to **2019.x**
2. install

```sh
sudo yum update scylla\* -y
```

### Start the node

```sh
sudo systemctl start scylla-server
```

### Validate

1. Check cluster status with `nodetool status` and make sure **all** nodes, including the one you just upgraded, are in UN status.
2. Use `curl -X GET "http://localhost:10000/storage_service/scylla_release_version"` to check scylla version.
3. Use `journalctl _COMM=scylla` to check there are no new errors in the log.
4. Check again after two minutes, to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

<a id="upgrade-2019-x-y-to-2019-x-z-rpm-rollback-procedure"></a>

## Rollback Procedure

#### NOTE
Execute the following commands one node at the time, moving to the next node only **after** the rollback procedure completed successfully.

The following procedure describes a rollback from Scylla Enterprise release 2019.x.z to 2019.x.y. Apply this procedure if an upgrade from 2019.x.y to 2019.x.z failed before completing on all nodes. Use this procedure only for nodes you upgraded to 2019.x.z

Scylla rollback is a rolling procedure that does **not** require a full cluster shutdown.
For each of the nodes rollback to 2019.x.y, you will:

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
sudo systemctl stop scylla-server
```

### Downgrade to previous release

1. Install

```sh
sudo yum downgrade scylla\*-2019.x.y -y
```

### Restore the configuration file

```sh
sudo rm -rf /etc/scylla/scylla.yaml
sudo cp -a /etc/scylla/scylla.yaml.backup-2019.x.z /etc/scylla/scylla.yaml
```

### Start the node

```sh
sudo systemctl start scylla-server
```

### Validate

Check the upgrade instruction above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
