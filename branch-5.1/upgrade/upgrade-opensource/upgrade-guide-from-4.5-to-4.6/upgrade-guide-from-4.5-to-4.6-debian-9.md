# Upgrade Guide - Scylla 4.5 to 4.6 for Debian 9

This document is a step by step procedure for upgrading from Scylla 4.5 to Scylla 4.6, and rollback to 4.5 if required.

## Applicable versions

This guide covers upgrading Scylla from the following versions: 4.5.x or later to Scylla version 4.6.y on the following platform:

* Debian 9

## Upgrade Procedure

A Scylla upgrade is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes in the cluster, you will:

* Check cluster schema
* Drain node and backup the data
* Backup configuration file
* Stop Scylla
* Download and install new Scylla packages
* Optional: update 3rd party and OS packages
* Start Scylla
* Validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running with the new version.

**During** the rolling upgrade it is highly recommended:

* Not to use new 4.6 features
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes. See [sctool](https://manager.docs.scylladb.com/stable/sctool/index.html) for suspending Scylla Manager (only available Scylla Enterprise) scheduled or running repairs.
* Not to apply schema changes

#### NOTE
Before upgrading, make sure to use the latest [Scylla Montioring](https://monitoring.docs.scylladb.com/) stack.

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
sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-src
```

### Gracefully stop the node

```sh
sudo service scylla-server stop
```

### Download and install the new release

Before upgrading, check what version you are running now using `dpkg -s scylla-server`. You should use the same version in case you want to [rollback](./#rollback-procedure) the upgrade. If you are not running a 4.5.x version, stop right here! This guide only covers 4.5.x to 4.6.y upgrades.

To upgrade:

1. Update the [Scylla deb repo](https://www.scylladb.com/download/?platform=debian-9&version=scylla-4.6) to 4.6
2. Install

```default
   sudo apt-get clean all
   sudo apt-get update
   sudo apt-get dist-upgrade scylla
```

Answer ‘y’ to the first two questions.

#### NOTE
Alternator users upgrading from Scylla 4.0 to 4.1, need to set [default isolation level](https://opensource.docs.scylladb.com/branch-5.1/upgrade/upgrade-opensource/upgrade-guide-from-4.0-to-4.1/alternator.md)

### Start the node

```sh
sudo service scylla-server start
```

### Validate

1. Check cluster status with `nodetool status` and make sure **all** nodes, including the one you just upgraded, are in UN status.
2. Use `curl -X GET "http://localhost:10000/storage_service/scylla_release_version"` to check scylla version.
3. Check scylla-server log (by `journalctl _COMM=scylla`) and `/var/log/syslog` to validate there are no errors.
4. Check again after 2 minutes, to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

* More on [Scylla Metrics Update - Scylla 4.5 to 4.6](../metric-update-4.5-to-4.6)

## Rollback Procedure

#### NOTE
Execute the following commands one node at the time, moving to the next node only **after** the rollback procedure completed successfully.

The following procedure describes a rollback from Scylla release 4.6.x to 4.5.y. Apply this procedure if an upgrade from 4.5 to 4.6 failed before completing on all nodes. Use this procedure only for nodes you upgraded to 4.6

Scylla rollback is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes rollback to 4.5, you will:

* Drain the node and stop Scylla
* Retrieve the old Scylla packages
* Restore the configuration file
* Restore system tables
* Reload systemd configuration
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

1. Update the [Scylla deb repo](https://www.scylladb.com/download/?platform=debian-9&version=scylla-4.6) to 4.5
2. install

```default
   sudo apt-get update
   sudo apt-get remove scylla* -y
   sudo apt-get install scylla
```

Answer ‘y’ to the first two questions.

### Restore the configuration file

```sh
sudo rm -rf /etc/scylla/scylla.yaml
sudo cp -a /etc/scylla/scylla.yaml.backup-src | /etc/scylla/scylla.yaml
```

### Restore system tables

Restore all tables of **system** and **system_schema** from previous snapshot, 4.6 uses a different set of system tables. Reference doc: [Restore from a Backup and Incremental Backup](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/procedures/backup-restore/restore.md)

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

```sh
sudo service scylla-server start
```

### Validate

Check upgrade instruction above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
