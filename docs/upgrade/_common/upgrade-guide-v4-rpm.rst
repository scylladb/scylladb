=============================================================================================================
Upgrade Guide - |SCYLLA_NAME| |SRC_VERSION| to |NEW_VERSION| for |OS|
=============================================================================================================

This document is a step by step procedure for upgrading from |SCYLLA_NAME| |SRC_VERSION| to |SCYLLA_NAME| |NEW_VERSION|, and rollback to version |SRC_VERSION| if required.


Applicable Versions
===================
This guide covers upgrading |SCYLLA_NAME| |SRC_VERSION| to |SCYLLA_NAME| |NEW_VERSION| on |OS|. 
See :doc:`OS Support by Platform and Version </getting-started/os-support>` for information about supported versions.

Upgrade Procedure
=================

Upgrading your ScyllaDB version is a rolling procedure that does not require a full cluster shutdown. For each of the nodes in the cluster, serially (i.e. one at a time), you will:

* Check the cluster's schema
* Drain the node and backup the data
* Backup the configuration file
* Stop ScyllaDB
* Download and install new ScyllaDB packages
* Start ScyllaDB
* Validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running the new version.

**During** the rolling upgrade, it is highly recommended:

* Not to use the new |NEW_VERSION| features
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes. See `sctool <https://manager.docs.scylladb.com/stable/sctool/index.html>`_ for suspending ScyllaDB Manager (only available for ScyllaDB Enterprise) scheduled or running repairs.
* Not to apply schema changes

.. note:: Before upgrading, make sure to use the latest `ScyllaDB Montioring <https://monitoring.docs.scylladb.com/>`_ stack.

Upgrade Steps
=============
Check the cluster schema
------------------------
Make sure that all nodes have the schema synced before the upgrade. The upgrade will fail if there is a schema disagreement between nodes.

.. code:: sh

       nodetool describecluster

Drain the nodes and backup the data
--------------------------------------
Before any major procedure, like an upgrade, it is recommended to backup all the data to an external device. In ScyllaDB, backup is done using the ``nodetool snapshot`` command. For **each** node in the cluster, run the following command:

.. code:: sh

   nodetool drain
   nodetool snapshot

Take note of the directory name that nodetool gives you, and copy all the directories having that name under ``/var/lib/scylla`` to an external backup device.

When the upgrade is completed on all nodes, remove the snapshot with the ``nodetool clearsnapshot -t <snapshot>`` command to prevent running out of space.

Backup the configuration file
------------------------------
.. code:: sh

   sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-src

Stop ScyllaDB
---------------
.. include:: /rst_include/scylla-commands-stop-index.rst


Download and install the new release
------------------------------------
Before upgrading, check what version you are running now using ``rpm -qa | grep scylla-server``. You should use the same version as this version in case you want to :ref:`rollback <rollback-procedure-v4>` the upgrade. If you are not running a |SRC_VERSION|.x version, stop right here! This guide only covers |SRC_VERSION|.x to |NEW_VERSION|.y upgrades.

To upgrade:

#. Update the |SCYLLA_REPO|_  to |NEW_VERSION|.
#. Install the new ScyllaDB version:

    .. code:: sh

       sudo yum clean all
       sudo yum update scylla\* -y


Start the node
--------------
   .. include:: /rst_include/scylla-commands-start-index.rst


Validate
--------
#. Check cluster status with ``nodetool status`` and make sure **all** nodes, including the one you just upgraded, are in UN status.
#. Use ``curl -X GET "http://localhost:10000/storage_service/scylla_release_version"`` to check the ScyllaDB version. Validate that the version matches the one you upgraded to. 
#. Use ``journalctl _COMM=scylla`` to check there are no new errors in the log.
#. Check again after two minutes, to validate no new issues are introduced.

Once you are sure the node upgrade was successful, move to the next node in the cluster.

See |Scylla_METRICS|_ for more information..

.. _rollback-procedure-v4:

Rollback Procedure
==================

.. include:: /upgrade/_common/warning_rollback.rst

The following procedure describes a rollback from |SCYLLA_NAME| release |NEW_VERSION|.x to |SRC_VERSION|.y. Apply this procedure if an upgrade from |SRC_VERSION| to |NEW_VERSION| failed before completing on all nodes. Use this procedure only for the nodes that you upgraded to |NEW_VERSION|.


ScyllaDB rollback is a rolling procedure that does **not** require a full cluster shutdown.
For each of the nodes you rollback to |SRC_VERSION|, you will:

* Drain the node and stop ScyllaDB
* Retrieve the old ScyllaDB packages
* Restore the configuration file
* Reload the systemd configuration
* Restart ScyllaDB
* Validate the rollback success

Apply the following procedure **serially** on each node. Do not move to the next node before validating the rollback was successful and that the node is up and running the old version.

Rollback Steps
==============
Gracefully shutdown ScyllaDB
-----------------------------
.. code:: sh

   nodetool drain
 .. include:: /rst_include/scylla-commands-stop-index.rst

Download and install the old release
------------------------------------
#. Remove the old repo file.

    .. code:: sh

       sudo rm -rf /etc/yum.repos.d/scylla.repo

#. Update the |SCYLLA_REPO|_  to |SRC_VERSION|.
#. Install:

    .. code:: console

       sudo yum clean all
       sudo rm -rf /var/cache/yum
       sudo yum remove scylla\\*tools-core
       sudo yum downgrade scylla\\* -y
       sudo yum install scylla
     

Restore the configuration file
------------------------------

.. code:: sh

   sudo rm -rf /etc/scylla/scylla.yaml
   sudo cp -a /etc/scylla/scylla.yaml.backup-src | /etc/scylla/scylla.yaml

Restore system tables
---------------------

Restore all tables of **system** and **system_schema** from previous snapshot because |NEW_VERSION| uses a different set of system tables. See :doc:`Restore from a Backup and Incremental Backup </operating-scylla/procedures/backup-restore/restore/>` for details.

.. code:: sh

    cd /var/lib/scylla/data/keyspace_name/table_name-UUID/snapshots/<snapshot_name>/
    sudo cp -r * /var/lib/scylla/data/keyspace_name/table_name-UUID/
    sudo chown -R scylla:scylla /var/lib/scylla/data/keyspace_name/table_name-UUID/

Reload systemd configuration
---------------------------------

You must reload the unit file if the systemd unit file is changed.

.. code:: sh

   sudo systemctl daemon-reload

Start the node
--------------

   .. include:: /rst_include/scylla-commands-start-index.rst

Validate
--------
Check the upgrade instructions above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.