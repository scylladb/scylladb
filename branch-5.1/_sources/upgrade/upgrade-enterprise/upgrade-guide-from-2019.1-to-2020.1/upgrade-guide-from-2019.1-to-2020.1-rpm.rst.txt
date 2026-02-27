=============================================================================================
Upgrade Guide - Scylla Enterprise 2019.1 to 2020.1 for Red Hat Enterprise Linux 7 or CentOS 7
=============================================================================================

This document is a step by step procedure for upgrading from Scylla Enterprise 2019.1 to Scylla Enterprise 2020.1, and rollback to 2019.1 if required.


Applicable versions
===================
This guide covers upgrading Scylla from the following versions: 2019.1.7 or later to Scylla Enterprise version 2020.1.y, on the following platforms:

.. note::

   This upgrade procedure only works from **2019.1.7** or later. If you have an older Scylla Enterprise 2019.1.x version, please contact the Scylla Support team for advice.

* Red Hat Enterprise Linux, version 7 and later
* CentOS, version 7 and later
* No longer provide packages for Fedora

Upgrade Procedure
=================

.. include:: /upgrade/_common/warning.rst

.. include:: /upgrade/upgrade-enterprise/_common/enterprise_2020.1_warnings.rst

A Scylla upgrade is a rolling procedure that does not require a full cluster shutdown. For each of the nodes in the cluster, serially (i.e. one at a time), you will:

* Check cluster schema
* Drain node and backup the data
* Backup configuration file
* Stop Scylla
* Download and install new Scylla packages
* Start Scylla
* Validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running with the new version.

**During** the rolling upgrade, it is highly recommended:

* Not to use new 2020.1 features.
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes. See `sctool <https://manager.docs.scylladb.com/stable/sctool/index.html>`_ for suspending Scylla Manager scheduled or running repairs.
* Not to apply schema changes.

.. include:: /upgrade/_common/upgrade_to_2020_warning.rst
  

Upgrade steps
=============
Check cluster schema
--------------------
Make sure that all nodes have the schema synched prior to the upgrade. The upgrade will fail if there is a schema disagreement between nodes.

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

   sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-2019.1

Stop Scylla
-----------
.. code:: sh

   sudo systemctl stop scylla-server

Download and install the new release
------------------------------------
Before upgrading, check what version you are running now using ``rpm -qa | grep scylla-server``. You should use the same version in case you want to :ref:`rollback <upgrade-2019.1-2020.1-rpm-rollback-procedure>` the upgrade. If you are not running a 2019.1.x version, stop right here! This guide only covers 2019.1.x to 2020.1.y upgrades.

To upgrade:

1. Update the `Scylla RPM Enterprise repo <https://www.scylladb.com/customer-portal/?product=ent&platform=centos7&version=stable-release-2020.1>`_  to **2020.1**
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
4. Check again after 2 minutes to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

* More on :doc:`Scylla Metrics Update - Scylla Enterprise 2019.1 to 2020.1<metric-update-2019.1-to-2020.1>`

.. _upgrade-2019.1-2020.1-rpm-rollback-procedure:

Rollback Procedure
==================

.. include:: /upgrade/_common/warning_rollback.rst

The following procedure describes a rollback from Scylla Enterprise release 2020.1.x to 2019.1.y. Apply this procedure if an upgrade from 2019.1 to 2020.1 failed before completing on all nodes. Use this procedure only for nodes you upgraded to 2020.1


Scylla rollback is a rolling procedure that does **not** require a  full cluster shutdown.
For each of the nodes rollback to 2019.1, you will:

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

2. Update the `Scylla RPM Enterprise 2019.1 repo <https://www.scylladb.com/customer-portal/?product=ent&platform=centos7&version=stable-release-2019.1>`_  to **2019.1**
3. Install

.. code:: sh

   sudo yum clean all
   sudo rm -rf /var/cache/yum
   sudo yum remove scylla\*tools-core
   sudo yum downgrade scylla\* -y
   sudo yum install scylla-enterprise

Restore the configuration file
------------------------------

.. code:: sh

   sudo rm -rf /etc/scylla/scylla.yaml
   sudo cp -a /etc/scylla/scylla.yaml.backup-2019.1 /etc/scylla/scylla.yaml

Restore system tables
---------------------

Restore all tables of **system** and **system_schema** from previous snapshot, 2020.1 uses a different set of system tables. Reference doc: :doc:`Restore from a Backup and Incremental Backup </operating-scylla/procedures/backup-restore/restore/>`

.. code:: sh

    cd /var/lib/scylla/data/keyspace_name/table_name-UUID/snapshots/<snapshot_name>/
    sudo cp -r * /var/lib/scylla/data/keyspace_name/table_name-UUID/
    sudo chown -R scylla:scylla /var/lib/scylla/data/keyspace_name/table_name-UUID/

Start the node
--------------

.. code:: sh

   sudo systemctl start scylla-server

Validate
--------
Check the upgrade instruction above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
