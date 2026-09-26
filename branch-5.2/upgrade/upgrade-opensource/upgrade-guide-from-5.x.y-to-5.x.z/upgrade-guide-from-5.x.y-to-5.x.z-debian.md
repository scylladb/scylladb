# Upgrade Guide - ScyllaDB 5.x.y to 5.x.z for Debian 10 and 11

This document is a step by step procedure for upgrading ScyllaDB from version 5.x.y to version 5.x.z.

## Applicable Versions

This guide covers upgrading ScyllaDB from version 5.x.y to version 5.x.z on the following platform:

* Debian 10 and 11

## Upgrade Procedure

#### NOTE
Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running the new version.

A ScyllaDB upgrade is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes in the cluster, you will:

* Drain node and backup the data.
* Check your current release.
* Backup configuration file.
* Stop ScyllaDB.
* Download and install new ScyllaDB packages.
* Start ScyllaDB.
* Validate that the upgrade was successful.

**During** the rolling upgrade it is highly recommended:

* Not to use new 5.x.z features.
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes.
* Not to apply schema changes.

## Upgrade steps

### Drain node and backup the data

Before any major procedure, like an upgrade, it is recommended to backup all the data to an external device. In ScyllaDB, backup is done using the `nodetool snapshot` command. For **each** node in the cluster, run the following command:

```sh
nodetool drain
nodetool snapshot
```

Take note of the directory name that nodetool gives you, and copy all the directories having this name under `/var/lib/scylla` to a backup device.

When the upgrade is complete (all nodes), the snapshot should be removed by `nodetool clearsnapshot -t <snapshot>`, or you risk running out of space.

### Backup configuration file

```sh
sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-5.x.z
```

### Gracefully stop the node

```sh
sudo service scylla-server stop
```

### Download and install the new release

Before upgrading, check what version you are running now using `dpkg -s scylla-server`. You should use the same version in case you want to [rollback](/upgrade/upgrade-opensource/upgrade-guide-from-5.x.y-to-5.x.z/upgrade-guide-from-5.x.y-to-5.x.z-debian/#rollback-procedure) the upgrade. If you are not running a 5.x.y version, stop right here! This guide only covers 5.x.y to 5.x.z upgrades.

To upgrade:

1. Update the [ScyllaDB deb repo](http://www.scylladb.com/download/) to **5.x**.
2. Install:

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
2. Use `curl -X GET "http://localhost:10000/storage_service/scylla_release_version"` to check the ScyllaDB version.
3. Check the scylla-server log (execute `journalctl _COMM=scylla`) and `/var/log/syslog` to validate there are no errors.
4. Check again after 2 minutes, to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

## Rollback Procedure

#### NOTE
Execute the following commands one node at the time, moving to the next node only **after** the rollback procedure completed successfully.

The following procedure describes a rollback from ScyllaDB release 5.x.z to 5.x.y. Apply this procedure if an upgrade from 5.x.y to 5.x.z failed before completing on all nodes. Use this procedure only for nodes you upgraded to 5.x.z.

ScyllaDB rollback is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes rollback to 5.x.y, you will:

* Drain the node and stop ScyllaDB.
* Downgrade to previous release.
* Restore the configuration file.
* Restart ScyllaDB.
* Validate the rollback success.

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running with the new version.

## Rollback steps

### Gracefully shutdown ScyllaDB

```sh
nodetool drain
sudo service scylla-server stop
```

### Downgrade to previous release

1. Install:

```sh
sudo apt-get install scylla=5.x.y\* scylla-server=5.x.y\* scylla-jmx=5.x.y\* scylla-tools=5.x.y\* scylla-tools-core=5.x.y\* scylla-kernel-conf=5.x.y\* scylla-conf=5.x.y\*
```

Answer ‘y’ to the first two questions.

### Restore the configuration file

```sh
sudo rm -rf /etc/scylla/scylla.yaml
sudo cp -a /etc/scylla/scylla.yaml.backup-5.x.z /etc/scylla/scylla.yaml
```

### Start the node

```sh
sudo service scylla-server start
```

### Validate

Check upgrade instruction above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
