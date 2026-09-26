# Upgrade Guide - Scylla 4.5 to 4.6 for Red Hat Enterprise Linux 7/8 or CentOS 7/8

This document is a step by step procedure for upgrading from Scylla 4.5 to Scylla 4.6, and rollback to version 4.5 if required.

## Applicable Versions

This guide covers upgrading Scylla 4.5 to Scylla 4.6 on Red Hat Enterprise Linux 7/8 or CentOS 7/8.
See [OS Support by Platform and Version](https://opensource.docs.scylladb.com/branch-5.2/getting-started/os-support.md) for information about supported versions.

## Upgrade Procedure

Upgrading your ScyllaDB version is a rolling procedure that does not require a full cluster shutdown. For each of the nodes in the cluster, serially (i.e. one at a time), you will:

* Check the cluster’s schema
* Drain the node and backup the data
* Backup the configuration file
* Stop ScyllaDB
* Download and install new ScyllaDB packages
* Start ScyllaDB
* Validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running the new version.

**During** the rolling upgrade, it is highly recommended:

* Not to use the new 4.6 features
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes. See [sctool](https://manager.docs.scylladb.com/stable/sctool/index.html) for suspending ScyllaDB Manager (only available for ScyllaDB Enterprise) scheduled or running repairs.
* Not to apply schema changes

#### NOTE
Before upgrading, make sure to use the latest [ScyllaDB Monitoring](https://monitoring.docs.scylladb.com/) stack.

## Upgrade Steps

### Check the cluster schema

Make sure that all nodes have the schema synced before the upgrade. The upgrade will fail if there is a schema disagreement between nodes.

```sh
nodetool describecluster
```

### Drain the nodes and backup the data

Before any major procedure, like an upgrade, it is recommended to backup all the data to an external device. In ScyllaDB, backup is done using the `nodetool snapshot` command. For **each** node in the cluster, run the following command:

```sh
nodetool drain
nodetool snapshot
```

Take note of the directory name that nodetool gives you, and copy all the directories having that name under `/var/lib/scylla` to an external backup device.

When the upgrade is completed on all nodes, remove the snapshot with the `nodetool clearsnapshot -t <snapshot>` command to prevent running out of space.

### Backup the configuration file

Back up the `scylla.yaml` configuration file and the ScyllaDB packages
in case you need to rollback the upgrade.

```sh
sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup
sudo cp /etc/yum.repos.d/scylla.repo ~/scylla.repo-backup
```

### Stop ScyllaDB

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

Before upgrading, check what version you are running now using `rpm -qa | grep scylla-server`. You should use the same version as this version in case you want to [rollback](https://opensource.docs.scylladb.com/branch-5.2/upgrade/upgrade-opensource/upgrade-guide-from-3.0-to-3.1/upgrade-guide-from-3.0-to-3.1-rpm.md#rollback-procedure) the upgrade. If you are not running a 4.5.x version, stop right here! This guide only covers 4.5.x to 4.6.y upgrades.

To upgrade:

1. Update the [Scylla rpm repo](https://www.scylladb.com/download/?platform=centos&version=scylla-4.6)  to 4.6.
2. Install the new ScyllaDB version:
   > ```sh
   > sudo yum clean all
   > sudo yum update scylla\* -y
   > ```

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
2. Use `curl -X GET "http://localhost:10000/storage_service/scylla_release_version"` to check the ScyllaDB version. Validate that the version matches the one you upgraded to.
3. Use `journalctl _COMM=scylla` to check there are no new errors in the log.
4. Check again after two minutes, to validate no new issues are introduced.

Once you are sure the node upgrade was successful, move to the next node in the cluster.

See [Scylla Metrics Update - Scylla 4.5 to 4.6](../metric-update-4.5-to-4.6) for more information..

## Rollback Procedure

#### NOTE
Execute the following commands one node at the time, moving to the next node only **after** the rollback procedure completed successfully.

The following procedure describes a rollback from Scylla release 4.6.x to 4.5.y. Apply this procedure if an upgrade from 4.5 to 4.6 failed before completing on all nodes. Use this procedure only for the nodes that you upgraded to 4.6.

ScyllaDB rollback is a rolling procedure that does **not** require a full cluster shutdown.
For each of the nodes you rollback to 4.5, you will:

* Drain the node and stop ScyllaDB
* Retrieve the old ScyllaDB packages
* Restore the configuration file
* Reload the systemd configuration
* Restart ScyllaDB
* Validate the rollback success

Apply the following procedure **serially** on each node. Do not move to the next node before validating the rollback was successful and that the node is up and running the old version.

## Rollback Steps

### Gracefully shutdown ScyllaDB

```sh
nodetool drain
nodetool snapshot
sudo service scylla-server stop
```

### Restore and install the old release

1. Restore the 4.5 packages backed up during the upgrade.
   > ```sh
   > sudo cp ~/scylla.repo-backup /etc/yum.repos.d/scylla.repo
   > sudo chown root.root /etc/yum.repos.d/scylla.repo
   > sudo chmod 644 /etc/yum.repos.d/scylla.repo
   > ```
2. Install:
   > ```console
   > sudo yum clean all
   > sudo rm -rf /var/cache/yum
   > sudo yum downgrade scylla-\*cqlsh -y
   > sudo yum remove scylla-\*cqlsh -y
   > sudo yum downgrade scylla\* -y
   > sudo yum install scylla -y
   > ```

### Restore the configuration file

```sh
sudo rm -rf /etc/scylla/scylla.yaml
sudo cp /etc/scylla/scylla.yaml-backup /etc/scylla/scylla.yaml
```

### Reload systemd configuration

You must reload the unit file if the systemd unit file is changed.

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

Check the upgrade instructions above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
