=============================================================================================================
Upgrade Guide - |SCYLLA_NAME| |SRC_VERSION| to |NEW_VERSION| for Red Hat Enterprise Linux 7 or CentOS 7
=============================================================================================================

This document is a step by step procedure for upgrading from |SCYLLA_NAME| |SRC_VERSION| to |SCYLLA_NAME| |NEW_VERSION|, and rollback to |SRC_VERSION| if required.


Applicable versions
===================
This guide covers upgrading Scylla from the following versions: |SRC_VERSION|.x or later to |SCYLLA_NAME| version |NEW_VERSION|.y, on the following platforms:

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

* Not to use new |NEW_VERSION| features
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes. See :doc:`here </operating-scylla/manager/2.1/sctool>` for suspending Scylla Manager (only available Scylla Enterprise) scheduled or running repairs.
* Not to apply schema changes

.. note:: Before upgrading, make sure to use |SCYLLA_MONITOR|_ or newer, for the Dashboards.

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

   sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-src

Stop Scylla
-----------
.. code:: sh

   sudo systemctl stop scylla-server

Download and install the new release
------------------------------------
Before upgrading, check what version you are running now using ``rpm -qa | grep scylla-server``. You should use the same version in case you want to :ref:`rollback <rollback-procedure>` the upgrade. If you are not running a |SRC_VERSION|.x version, stop right here! This guide only covers |SRC_VERSION|.x to |NEW_VERSION|.y upgrades.

To upgrade:

1. Update the |SCYLLA_REPO|_  to |NEW_VERSION|
2. install

.. code:: sh

   sudo yum update scylla\* -y

.. note::

   Alternator users upgrading from Scylla 4.0 to 4.1, need to set :doc:`default isolation level </upgrade/upgrade-opensource/upgrade-guide-from-4.0-to-4.1/alternator>`


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

* More on |Scylla_METRICS|_

Rollback Procedure
==================

.. include:: /upgrade/_common/warning_rollback.rst

The following procedure describes a rollback from |SCYLLA_NAME| release |NEW_VERSION|.x to |SRC_VERSION|.y. Apply this procedure if an upgrade from |SRC_VERSION| to |NEW_VERSION| failed before completing on all nodes. Use this procedure only for nodes you upgraded to |NEW_VERSION|


Scylla rollback is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes rollback to |SRC_VERSION|, you will:

* Drain the node and stop Scylla
* Retrieve the old Scylla packages
* Restore the configuration file
* Reload systemd configuration
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

2. Update the |SCYLLA_REPO|_  to |SRC_VERSION|
3. Install

.. parsed-literal::
   \    sudo yum clean all
   \    sudo rm -rf /var/cache/yum
   \    sudo yum remove scylla\\*tools-core
   \    sudo yum downgrade scylla\\* -y
   \    sudo yum install |PKG_NAME|
   \    

Restore the configuration file
------------------------------

.. code:: sh

   sudo rm -rf /etc/scylla/scylla.yaml
   sudo cp -a /etc/scylla/scylla.yaml.backup-src| /etc/scylla/scylla.yaml

Restore system tables
---------------------

Restore all tables of **system** and **system_schema** from previous snapshot, |NEW_VERSION| uses a different set of system tables. Reference doc: :doc:`Restore from a Backup and Incremental Backup </operating-scylla/procedures/backup-restore/restore/>`

.. code:: sh

    cd /var/lib/scylla/data/keyspace_name/table_name-UUID/snapshots/<snapshot_name>/
    sudo cp -r * /var/lib/scylla/data/keyspace_name/table_name-UUID/
    sudo chown -R scylla:scylla /var/lib/scylla/data/keyspace_name/table_name-UUID/

Reload systemd configuration
---------------------------------

Require to reload the unit file if the systemd unit file is changed.

.. code:: sh

   sudo systemctl daemon-reload

Start the node
--------------

.. code:: sh

   sudo systemctl start scylla-server

Validate
--------
Check upgrade instruction above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
