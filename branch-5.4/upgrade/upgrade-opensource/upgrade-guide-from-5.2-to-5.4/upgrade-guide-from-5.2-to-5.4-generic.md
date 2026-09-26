# Upgrade Guide - ScyllaDB 5.2 to 5.4

This document is a step-by-step procedure for upgrading from ScyllaDB 5.2
to ScyllaDB 5.4, and rollback to version 5.2 if required.

This guide covers upgrading ScyllaDB on Red Hat Enterprise Linux (RHEL), CentOS, Debian,
and Ubuntu. See [OS Support by Platform and Version](https://opensource.docs.scylladb.com/branch-5.4/getting-started/os-support.md)
for information about supported versions.

It also applies when using ScyllaDB official image on EC2, GCP, or Azure.

## Before You Upgrade ScyllaDB

**Upgrade Your Driver**

If you’re using a [ScyllaDB driver](https://opensource.docs.scylladb.com/branch-5.4/using-scylla/drivers/cql-drivers/index.md),
upgrade the driver before you upgrade ScyllaDB. The latest two versions of each driver
are supported.

**Upgrade ScyllaDB Monitoring Stack**

If you’re using the ScyllaDB Monitoring Stack, verify that your Monitoring Stack
version supports the ScyllaDB version to which you want to upgrade. See
[ScyllaDB Monitoring Stack Support Matrix](https://monitoring.docs.scylladb.com/stable/reference/matrix.html).

We recommend upgrading the Monitoring Stack to the latest version.

**Check Feature Updates**

See the ScyllaDB Release Notes for the latest updates. The Release Notes are published
at the [ScyllaDB Community Forum](https://forum.scylladb.com/).

#### NOTE
DateTieredCompactionStrategy is removed in 5.4. Migrate to TimeWindowCompactionStrategy
**before** you upgrade from 5.2 to 5.4

#### NOTE
In ScyllaDB 5.4, Raft-based consistent cluster management for existing
deployments is enabled by default.
If you want the consistent cluster management feature to be disabled in
ScyllaDB 5.4, you must update the configuration **before** you upgrade
from 5.2 to 5.4:

1. Set `consistent_cluster_management: false` in the `scylla.yaml`
   configuration file on each node in the cluster.
2. Start the upgrade procedure.

Consistent cluster management **cannot** be disabled in version 5.4 if it was
enabled in version 5.2 in one of the following ways:

* Your cluster was created in version 5.2 with the default `consistent_cluster_management: true`
  configuration in `scylla.yaml`.
* You explicitly set `consistent_cluster_management: true` in
  `scylla.yaml` in an existing cluster in version 5.2.

## Upgrade Procedure

A ScyllaDB upgrade is a rolling procedure that does **not** require full cluster shutdown.
For each of the nodes in the cluster, serially (i.e., one node at a time), you will:

* Check that the cluster’s schema is synchronized
* Drain the node and backup the data
* Backup the configuration file
* Stop ScyllaDB
* Download and install new ScyllaDB packages
* Start ScyllaDB
* Validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to the next
node before validating that the node you upgraded is up and running the new version.

**During** the rolling upgrade, it is highly recommended:

* Not to use the new 5.4 features.
* Not to run administration functions, such as repairs, refresh, rebuild, or add
  or remove nodes. See [sctool](https://manager.docs.scylladb.com/stable/sctool/)
  for suspending ScyllaDB Manager (only available for ScyllaDB Enterprise) scheduled
  or running repairs.
* Not to apply schema changes.

**After** the upgrade, you need to verify that Raft was successfully initiated in
your cluster. You can skip this step only in either of the following cases:

* The `consistent_cluster_management` option was enabled in a previous
  ScyllaDB version.
* You disabled the `consistent_cluster_management` option before upgrading
  to 5.4, as described in the note in the *Before You Upgrade
  ScyllaDB* section.

Otherwise, as soon as every node has been upgraded to the new version,
the cluster will start a procedure that initializes the Raft algorithm for
consistent cluster metadata management. You must then
[verify](#validate-raft-setup-enabled-default-5-4) that the Raft
initialization procedure has successfully finished.

## Upgrade Steps

### Check the cluster schema

Make sure that all nodes have the schema synchronized before the upgrade. The upgrade
procedure will fail if there is a schema disagreement between nodes.

```sh
nodetool describecluster
```

### Drain the nodes and backup data

Before any major procedure, like an upgrade, it is recommended to backup all the data
to an external device. In ScyllaDB, backup is performed using the `nodetool snapshot`
command. For **each** node in the cluster, run the following command:

```sh
nodetool drain
nodetool snapshot
```

Take note of the directory name that nodetool gives you, and copy all the directories
having that name under `/var/lib/scylla` to an external backup device.

When the upgrade is completed on all nodes, remove the snapshot with the
`nodetool clearsnapshot -t <snapshot>` command to prevent running out of space.

### Backup the configuration file

Back up the `scylla.yaml` configuration file and the ScyllaDB packages
in case you need to rollback the upgrade.

Debian/Ubuntu

```sh
sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup
sudo cp /etc/apt/sources.list.d/scylla.list ~/scylla.list-backup
```

RHEL/CentOS

```sh
sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup
sudo cp /etc/yum.repos.d/scylla.repo ~/scylla.repo-backup
```

### Gracefully stop the node

```sh
sudo service scylla-server stop
```

### Download and install the new release

Before upgrading, check what version you are running now using `scylla --version`.
You should take note of the current version in case you want to [rollback](./#rollback-procedure) the upgrade.

Debian/Ubuntu

**To upgrade ScyllaDB:**

1. Update the ScyllaDB deb repo ([Debian](https://www.scylladb.com/download/?platform=debian-10&version=scylla-5.4), [Ubuntu](https://www.scylladb.com/download/?platform=ubuntu-20.04&version=scylla-5.4)) to 5.4.
2. Install the new ScyllaDB version:
   > ```console
   > sudo apt-get clean all
   > sudo apt-get update
   > sudo apt-get dist-upgrade scylla
   > ```

Answer ‘y’ to the first two questions.

RHEL/CentOS

**To upgrade ScyllaDB:**

1. Update the [ScyllaDB rpm repo](https://www.scylladb.com/download/?platform=centos&version=scylla-5.4)  to 5.4.
2. Install the new ScyllaDB version:
   > ```sh
   > sudo yum clean all
   > sudo yum update scylla\* -y
   > ```

#### NOTE
If you are running a ScyllaDB official image (for EC2 AMI, GCP, or Azure),
you need to:

> * Download and install the new ScyllaDB release for Ubuntu; see
>   the Debian/Ubuntu tab above for instructions.
> * Update underlying OS packages.

See [Upgrade ScyllaDB Image](https://opensource.docs.scylladb.com/branch-5.4/upgrade/ami-upgrade.md) for details.

### Start the node

```sh
sudo service scylla-server start
```

### Validate

1. Check cluster status with `nodetool status` and make sure **all** nodes, including
   the one you just upgraded, are in `UN` status.
2. Use `curl -X GET "http://localhost:10000/storage_service/scylla_release_version"`
   to check the ScyllaDB version. Validate that the version matches the one you upgraded to.
3. Check scylla-server log (by `journalctl _COMM=scylla`) and `/var/log/syslog` to
   validate there are no new errors in the log.
4. Check again after two minutes, to validate no new issues are introduced.

Once you are sure the node upgrade was successful, move to the next node in the cluster.

See [Scylla Metrics Update - Scylla 5.2 to 5.4](../metric-update-5.2-to-5.4) for more information.

## After Upgrading Every Node

This section applies to upgrades where Raft is initialized for the first time
in the cluster, which in 5.4 happens by default.

You can skip this step only in either of the following cases:

* The `consistent_cluster_management` option was enabled in a previous
  ScyllaDB version (i.e., Raft was enabled in a version prior to 5.4).
* You disabled the `consistent_cluster_management` option before upgrading
  to 5.4, as described in the note in the Before You Upgrade ScyllaDB section
  (i.e., you prevented Raft from being enabled in 5.4).

<a id="validate-raft-setup-enabled-default-5-4"></a>

### Validate Raft Setup

Enabling Raft causes the ScyllaDB cluster to start an internal Raft
initialization procedure as soon as every node is upgraded to the new version.
The goal of that procedure is to initialize data structures used by the Raft
algorithm to consistently manage cluster-wide metadata, such as table schemas.

Assuming you performed the rolling upgrade procedure correctly (in particular,
ensuring that the schema is synchronized on every step), and if there are no
problems with cluster connectivity, that internal procedure should take a few
seconds to finish. However, the procedure requires full cluster availability.
If one of the nodes fails before the procedure finishes (for example, due to
a hardware problem), the process may get stuck, which may prevent schema or
topology changes in your cluster.

Therefore, following the rolling upgrade, you must verify that the internal
Raft initialization procedure has finished successfully by checking the logs
of every ScyllaDB node. If the process gets stuck, manual intervention is
required.

Refer to the
[Verifying that the internal Raft upgrade procedure finished successfully](https://opensource.docs.scylladb.com/branch-5.4/architecture/raft.md#verify-raft-procedure)
section for instructions on verifying that the procedure was successful and
proceeding if it gets stuck.

## Rollback Procedure

#### NOTE
Execute the following commands one node at a time, moving to the next node only **after** the rollback procedure is completed successfully.

The following procedure describes a rollback from ScyllaDB 5.4.x to
5.2.y. Apply this procedure if an upgrade from 5.2 to
5.4 fails before completing on all nodes.
Use this procedure only for nodes you upgraded to 5.4.

#### WARNING
The rollback procedure can be applied **only** if some nodes have not been
upgraded to 5.4 yet.As soon as the last node in the rolling upgrade
procedure is started with 5.4, rollback becomes impossible. At that
point, the only way to restore a cluster to 5.2 is by restoring it
from backup.

ScyllaDB rollback is a rolling procedure that does **not** require full cluster shutdown.

For each of the nodes you rollback to 5.2, serially (i.e., one node
at a time), you will:

* Drain the node and stop Scylla
* Retrieve the old ScyllaDB packages
* Restore the configuration file
* Reload systemd configuration
* Restart ScyllaDB
* Validate the rollback success

Apply the following procedure **serially** on each node. Do not move to the next
node before validating that the rollback was successful and the node is up and
running the old version.

## Rollback Steps

### Drain and gracefully stop the node

```sh
nodetool drain
nodetool snapshot
sudo service scylla-server stop
```

### Restore and install the old release

Debian/Ubuntu

1. Restore the 5.2 packages backed up during the upgrade.
   > ```sh
   > sudo cp ~/scylla.list-backup /etc/apt/sources.list.d/scylla.list
   > sudo chown root.root /etc/apt/sources.list.d/scylla.list
   > sudo chmod 644 /etc/apt/sources.list.d/scylla.list
   > ```
2. Install:
   > ```default
   > sudo apt-get update
   > sudo apt-get remove scylla\* -y
   > sudo apt-get install scylla
   > ```

Answer ‘y’ to the first two questions.

RHEL/CentOS

1. Restore the 5.2 packages backed up during the upgrade procedure.
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

```sh
sudo service scylla-server start
```

### Validate

Check the upgrade instructions above for validation. Once you are sure the node
rollback is successful, move to the next node in the cluster.
