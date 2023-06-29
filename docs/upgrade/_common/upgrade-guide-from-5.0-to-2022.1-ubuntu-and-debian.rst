=============================================================================
Upgrade Guide - ScyllaDB 5.0 to ScyllaDB Enterprise 2022.1 for |OS|
=============================================================================

This document is a step-by-step procedure for upgrading from ScyllaDB Open Source 5.0 to ScyllaDB Enterprise 2022.1 and rollback to 5.0 if required.


Applicable Versions
===================
This guide covers upgrading ScyllaDB from version 5.0.x to ScyllaDB Enterprise version 2022.1.y on |OS|. See :doc:`OS Support by Platform and Version </getting-started/os-support>` for information about supported |OS| versions.

Upgrade Procedure
=================

A ScyllaDB upgrade is a rolling procedure that does **not** require a full cluster shutdown.
For each of the nodes in the cluster, you will:

* Check the cluster schema
* Drain the node and backup the data
* Backup the configuration file
* Stop ScyllaDB
* Download and install new ScyllaDB packages
* Start ScyllaDB
* Validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to the next node before validating that the node you upgraded the node is up and running the new version.

**During** the rolling upgrade, it is highly recommended:

* Not to use new 2022.1 features.
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes. See `sctool <https://manager.docs.scylladb.com/stable/sctool/>`_ for suspending Scylla Manager's scheduled or running repairs.
* Not to apply schema changes.


.. include:: /upgrade/_common/upgrade_to_2022_warning.rst
  
Upgrade Steps
=============

Check the cluster schema
--------------------------
Make sure that all nodes have the schema synched before the upgrade. The upgrade will fail if there is a schema disagreement between nodes.

.. code:: sh

       nodetool describecluster

Drain the nodes and backup the data
-------------------------------------

Before any major procedure, like an upgrade, it is recommended to backup all the data to an external device. In ScyllaDB, backup is done using the ``nodetool snapshot`` command. For **each** node in the cluster, run the following command:

.. code:: sh

   nodetool drain
   nodetool snapshot

Take note of the directory name that nodetool gives you, and copy all the directories having this name under ``/var/lib/scylla`` to a backup device.

When the upgrade is completed on all nodes, the snapshot should be removed with the ``nodetool clearsnapshot -t <snapshot>`` command, or you risk running out of space.


Backup the configuration files
--------------------------------

.. code:: sh

   for conf in $(cat /var/lib/dpkg/info/scylla-*server.conffiles /var/lib/dpkg/info/scylla-*conf.conffiles /var/lib/dpkg/info/scylla-*jmx.conffiles | grep -v init ) /etc/systemd/system/{var-lib-scylla,var-lib-systemd-coredump}.mount; do sudo cp -v $conf $conf.backup-5.0; done

Gracefully stop the node
------------------------

.. code:: sh

   sudo service scylla-server stop

Download and install the new release
------------------------------------
Before upgrading, check what version you are running now using ``dpkg -l scylla\*server``. You should use the same version in case you want to |ROLLBACK|_ the upgrade. If you are not running a 5.0.x version, stop right here! This guide only covers 5.0.x to 2022.1.y upgrades.

To upgrade:

#. Update the |APT_ENTERPRISE|_ to **2022.1**.

#. Install:

    .. code:: sh

       sudo apt-get clean all
       sudo apt-get update
       sudo apt-get remove scylla\*
       sudo apt-get install scylla-enterprise
       for conf in $(cat /var/lib/dpkg/info/scylla-*server.conffiles /var/lib/dpkg/info/scylla-*conf.conffiles /var/lib/dpkg/info/scylla-*jmx.conffiles | grep -v init ) /etc/systemd/system/{var-lib-scylla,var-lib-systemd-coredump}.mount; do sudo cp -v $conf.backup-5.0 $conf; done
       sudo systemctl daemon-reload

    Answer ‘y’ to the first two questions.

    If you use a cloud image with a preinstalled version of ScyllaDB (for example, AMI), you need to install an additional 
    package ``scylla-enterprise-machine-image``:

    .. code:: sh

       sudo apt-get install scylla-enterprise-machine-image
       sudo systemctl daemon-reload

Start the node
--------------

New io.conf format was introduced in ScyllaDB 2.3 and 2019.1. If your io.conf doesn't contain `--io-properties-file` option, then it's still the old format. You need to re-run the io setup to generate new io.conf.

.. code:: sh

    sudo scylla_io_setup

.. code:: sh

   sudo service scylla-server start

Validate
--------
#. Check cluster status with ``nodetool status`` and make sure **all** nodes, including the one you just upgraded, are in UN status.
#. Use ``curl -X GET "http://localhost:10000/storage_service/scylla_release_version"`` to check the ScyllaDB version.
#. Check scylla-server log (by ``journalctl _COMM=scylla``) and ``/var/log/syslog`` to validate there are no errors.
#. Check again after 2 minutes to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

More on :doc:`ScyllaDB Metrics Update - ScyllaDB 5.0 to 2022.1<metric-update-5.0-to-2022.1>`

Rollback Procedure
==================

.. include:: /upgrade/_common/warning_rollback.rst

The following procedure describes a rollback from ScyllaDB Enterprise release 2022.1.x to ScyllaDB 5.0.y. Apply this procedure if an upgrade from 5.0 to 2022.1 failed before completing on all nodes. Use this procedure only for nodes you upgraded to 2022.1.

ScyllaDB rollback is a rolling procedure that does **not** require a full cluster shutdown.
For each of the nodes to rollback to 5.0, you will:

* Drain the node and stop ScyllaDB
* Retrieve the old ScyllaDB packages
* Restore the configuration file
* Restart ScyllaDB
* Validate the rollback success

Apply the following procedure **serially** on each node. Do not move to the next node before validating that the node is up and running the new version.

Rollback Steps
==============

Gracefully shutdown ScyllaDB
-----------------------------

.. code:: sh

   nodetool drain
   sudo service scylla-server stop

Download and install the old release
------------------------------------
#. Remove the old repo file.

    .. code:: sh

       sudo rm -rf /etc/apt/sources.list.d/scylla.list

#. Update the |APT|_ to **5.0**.

#. Install:

    .. code:: sh

       sudo apt-get clean all
       sudo apt-get update
       sudo apt-get remove scylla\* -y
       sudo apt-get install scylla

Answer ‘y’ to the first two questions.

Restore the configuration files
-------------------------------
.. code:: sh

   for conf in $(cat /var/lib/dpkg/info/scylla-*server.conffiles /var/lib/dpkg/info/scylla-*conf.conffiles /var/lib/dpkg/info/scylla-*jmx.conffiles | grep -v init ) /etc/systemd/system/{var-lib-scylla,var-lib-systemd-coredump}.mount; do sudo cp -v $conf.backup-5.0 $conf; done
   sudo systemctl daemon-reload

Restore system tables
---------------------

Restore all tables of **system** and **system_schema** from the previous snapshot because 2022.1 uses a different set of system tables. See :doc:`Restore from a Backup and Incremental Backup </operating-scylla/procedures/backup-restore/restore/>` for reference.

.. code:: console

    cd /var/lib/scylla/data/keyspace_name/table_name-UUID/
    sudo find . -maxdepth 1 -type f  -exec sudo rm -f "{}" +
    cd /var/lib/scylla/data/keyspace_name/table_name-UUID/snapshots/<snapshot_name>/
    sudo cp -r * /var/lib/scylla/data/keyspace_name/table_name-UUID/
    sudo chown -R scylla:scylla /var/lib/scylla/data/keyspace_name/table_name-UUID/

Start the node
--------------

.. code:: sh

   sudo service scylla-server start

Validate
--------
Check the upgrade instruction above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
