# Upgrade Guide - ScyllaDB 6.x.y to 6.x.z

This document is a step-by-step procedure for upgrading from ScyllaDB 6.x.y
to ScyllaDB 6.x.z (where “z” is the latest available version).

## Applicable Versions

This guide covers upgrading ScyllaDB on Red Hat Enterprise Linux (RHEL), CentOS, Debian,
and Ubuntu. See [OS Support by Platform and Version](https://opensource.docs.scylladb.com/branch-6.0/getting-started/os-support.md) for
information about supported versions.

This guide also applies when you’re upgrading ScyllaDB Enterprise official image on EC2, GCP,
or Azure.

## Upgrade Procedure

#### NOTE
Apply the following procedure **serially** on each node. Do not move to the next node
before validating the node is up and running the new version.

A ScyllaDB upgrade is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes in the cluster, you will:

* Drain node and backup the data.
* Check your current release.
* Backup configuration file.
* Stop ScyllaDB.
* Download and install new ScyllaDB packages.
* Start ScyllaDB.
* Validate that the upgrade was successful.

**Before** upgrading, check what version you are running now using `scylla --version`.
You should use the same version in case you want to rollback the upgrade.

**During** the rolling upgrade it is highly recommended:

* Not to use new 6.x.z features.
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes.
* Not to apply schema changes.

## Upgrade steps

### Drain node and backup the data

Before any major procedure, like an upgrade, it is recommended to backup all the data to
an external device. In ScyllaDB, backup is done using the `nodetool snapshot` command.
For **each** node in the cluster, run the following command:

```sh
nodetool drain
nodetool snapshot
```

Take note of the directory name that nodetool gives you, and copy all
the directories having this name under `/var/lib/scylla` to a backup device.

When the upgrade is complete (all nodes), the snapshot should be removed by
`nodetool clearsnapshot -t <snapshot>`, or you risk running out of space.

### Backup the configuration file

Back up the scylla.yaml configuration file and the ScyllaDB packages in case
you need to rollback the upgrade.

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

Debian/Ubuntu

1. Update the [ScyllaDB deb repo](https://www.scylladb.com/download/#open-source) to 6.x.
2. Install:
   > ```sh
   > sudo apt-get clean all
   > sudo apt-get update
   > sudo apt-get dist-upgrade scylla
   > ```

   > Answer ‘y’ to the first two questions.

RHEL/CentOS

1. Update the [ScyllaDB Enterprise rpm repo](https://www.scylladb.com/download/#open-source) to 6.x.
2. Install:
   > ```sh
   > sudo yum clean all
   > sudo yum update scylla\* -y
   > ```

EC2/GCP/Azure Ubuntu Image

If you’re using the ScyllaDB official image (recommended), see
the **Debian/Ubuntu** tab for upgrade instructions.

If you’re using your own image and installed ScyllaDB packages for
Ubuntu or Debian, you need to apply an extended upgrade procedure:

1. Update the [ScyllaDB deb repo](https://www.scylladb.com/download/#open-source) to 6.x.
2. Install the new ScyllaDB version with the additional `scylla-machine-image` package:
   > ```console
   > sudo apt-get clean all
   > sudo apt-get update
   > sudo apt-get dist-upgrade scylla
   > sudo apt-get dist-upgrade scylla-machine-image
   > ```
3. Run `scylla_setup` without `running io_setup`.
4. Run `sudo /opt/scylladb/scylla-machine-image/scylla_cloud_io_setup`.

### Start the node

```sh
sudo service start scylla-server
```

### Validate

1. Check cluster status with `nodetool status` and make sure **all** nodes, including the one you just upgraded, are in UN status.
2. Use `curl -X GET "http://localhost:10000/storage_service/scylla_release_version"` to check the ScyllaDB version.
3. Use `journalctl _COMM=scylla` to check there are no new errors in the log.
4. Check again after 2 minutes, to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

## Rollback Procedure

The following procedure describes a rollback from ScyllaDB release 6.x.z to 6.x.y.
Apply this procedure if an upgrade from 6.x.y to 6.x.z failed before
completing on all nodes. Use this procedure only for nodes you upgraded to 6.x.z.

ScyllaDB rollback is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes to rollback to 6.x.y, you will:

* Drain the node and stop ScyllaDB.
* Downgrade to previous release.
* Restore the configuration file.
* Restart ScyllaDB.
* Validate the rollback success.

## Rollback steps

### Gracefully shutdown ScyllaDB

```sh
nodetool drain
sudo service stop scylla-server
```

### Downgrade to the previous release

Debian/Ubuntu

Install:

> ```console
> sudo apt-get install scylla=6.x.y\* scylla-server=6.x.y\* scylla-jmx=6.x.y\* scylla-tools=6.x.y\* scylla-tools-core=6.x.y\* scylla-kernel-conf=6.x.y\* scylla-conf=6.x.y\*
> sudo apt-get install scylla-machine-image=6.x.y\*  # only execute on AMI instance
> ```

Answer ‘y’ to the first two questions.

RHEL/CentOS

Install:

> ```console
>  sudo yum downgrade scylla\*-6.x.y-\* -y
> ```

### Restore the configuration file

```sh
sudo rm -rf /etc/scylla/scylla.yaml
sudo cp -a /etc/scylla/scylla.yaml.backup /etc/scylla/scylla.yaml
```

### Start the node

```sh
sudo service scylla-server start
```

### Validate

Check upgrade instruction above for validation. Once you are sure the node
rollback is successful, move to the next node in the cluster.
