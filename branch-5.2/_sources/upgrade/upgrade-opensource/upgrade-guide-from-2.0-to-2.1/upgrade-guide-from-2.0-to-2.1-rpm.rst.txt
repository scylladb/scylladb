=============================================================================
Upgrade Guide - Scylla 2.0 to 2.1 for Red Hat Enterprise Linux 7 or CentOS 7
=============================================================================

This document is a step by step procedure for upgrading from Scylla 2.0 to Scylla 2.1, and rollback to 2.0 if required.


Applicable versions
===================
This guide covers upgrading Scylla from the following versions: 2.0.x to Scylla version 2.1.y, on the following platforms:

* Red Hat Enterprise Linux, version 7 and later
* CentOS, version 7 and later
* No longer provide packages for Fedora

Upgrade Procedure
=================

.. include:: /upgrade/_common/warning.rst

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

* Not to use new 2.1 features
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes
* Not to apply schema changes

Upgrade steps
=============
Check cluster schema
--------------------
Make sure that all nodes have the schema synched prior to upgrade, we won't survive an upgrade that has schema disagreement between nodes.

.. code:: sh

       nodetool describecluster

Drain node and backup the data
------------------------------
Before any major procedure, like an upgrade, it is recommended to backup all the data to an external device. In Scylla, backup is done using the ``nodetool snapshot`` command. For **each** node in the cluster, run the following command:

.. code:: sh

   nodetool drain
   nodetool snapshot

Take note of the directory name that nodetool gives you, and copy all the directories having this name under ``/var/lib/scylla`` to a backup device.

When the upgrade is complete (all nodes), the snapshot should be removed by ``nodetool clearsnapshot -t <snapshot>``, or you risk running out of space.


Backup configuration file
-------------------------
.. code:: sh

   sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-2.0

Stop Scylla
-----------
.. code:: sh

   sudo systemctl stop scylla-server

Download and install the new release
------------------------------------
Before upgrading, check what version you are running now using ``rpm -qa | grep scylla-server``. You should use the same version in case you want to :ref:`rollback <upgrade-2.0-2.1-rpm-rollback-procedure>` the upgrade. If you are not running a 2.0.x version, stop right here! This guide only covers 2.0.x to 2.1.y upgrades.

To upgrade:

1. Update the `Scylla RPM repo <http://www.scylladb.com/download/?platform=centos>`_ to **2.1**
2. install

.. code:: sh

   sudo yum update scylla\* -y

Start the node
--------------
.. code:: sh

   sudo systemctl start scylla-server

Validate
--------
1. Check cluster status with ``nodetool status`` and make sure **all** nodes, including the one you just upgraded, are in UN status.
2. Use ``curl -X GET "http://localhost:10000/storage_service/scylla_release_version"`` to check scylla version.
3. Use ``journalctl _COMM=scylla`` to check there are no new errors in the log.
4. Check again after 2 minutes, to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

* More on :doc:`Scylla Metrics Update - Scylla 2.0 to 2.1<metric-update-2.0-to-2.1>`

.. _upgrade-2.0-2.1-rpm-rollback-procedure:

Rollback Procedure
==================

.. include:: /upgrade/_common/warning_rollback.rst

The following procedure describes a rollback from Scylla release 2.1.x to 2.0.y. Apply this procedure if an upgrade from 2.0 to 2.1 failed before completing on all nodes. Use this procedure only for nodes you upgraded to 2.1

Scylla rollback is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes rollback to 2.0, you will:

* Drain the node and stop Scylla
* Retrieve the old Scylla packages
* Restore the configuration file
* Restart Scylla
* Validate the rollback success

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running with the new version.

Rollback steps
==============
Gracefully shutdown Scylla
--------------------------
.. code:: sh

   nodetool drain
   sudo systemctl stop scylla-server

Download and install the new release
------------------------------------
1. Remove the old repo file.

.. code:: sh

   sudo rm -rf /etc/yum.repos.d/scylla.repo

2. Update the `Scylla RPM repo <http://www.scylladb.com/download/?platform=centos>`_ to **2.0**
3. Install

.. code:: sh

   sudo yum clean all
   sudo yum remove scylla-tools-core
   sudo yum downgrade scylla\* -y
   sudo yum install scylla

Restore the configuration file
------------------------------

.. code:: sh

   sudo rm -rf /etc/scylla/scylla.yaml
   sudo cp -a /etc/scylla/scylla.yaml.backup-2.0 /etc/scylla/scylla.yaml

Restore system tables
---------------------

Restore all tables of **system** and **system_schema** from previous snapshot, 2.1 uses a different set of system tables. Reference doc: :doc:`Restore from a Backup and Incremental Backup </operating-scylla/procedures/backup-restore/restore/>`

.. code:: sh

    cd /var/lib/scylla/data/keyspace_name/table_name-UUID/snapshots/<snapshot_name>/
    sudo cp -r * /var/lib/scylla/data/keyspace_name/table_name-UUID/

Start the node
--------------

.. code:: sh

   sudo systemctl start scylla-server

Validate
--------
Check upgrade instruction above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
