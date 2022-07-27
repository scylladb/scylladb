=============================================================================
Upgrade Guide - ScyllaDB Enterprise 2021.1 to 2022.1 for |OS|
=============================================================================

This document is a step-by-step procedure for upgrading from ScyllaDB Enterprise 2021.1 to ScyllaDB Enterprise 2022.1, and rollback to 2021.1 if required.


Applicable Versions
===================
This guide covers upgrading ScyllaDB Enterprise from version 2021.1.x to ScyllaDB Enterprise version 2022.1.y on the following platform:

* |OS|

Upgrade Procedure
=================
.. include:: /upgrade/upgrade-enterprise/_common/enterprise_2022.1_warnings.rst

A ScyllaDB upgrade is a rolling procedure that does **not** require a full cluster shutdown.
For each of the nodes in the cluster, you will:

* Check the cluster schema
* Drain the node and backup the data
* Backup the configuration file
* Stop ScyllaDB
* Download and install the new ScyllaDB packages
* Start ScyllaDB
* Validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node that you upgraded is up and running the new version.

**During** the rolling upgrade, it is highly recommended:

* Not to use new 2022.1 features.
* Not to run administration functions, like repairs, refresh, rebuild, or add or remove nodes. See :doc:`here </operating-scylla/manager/2.1/sctool>` for suspending ScyllaDB Manager's scheduled or running repairs.
* Not to apply schema changes.

.. include:: /upgrade/_common/upgrade_to_2022_warning.rst
  
Upgrade Steps
=============
Check the cluster schema
-------------------------
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

When the upgrade is completed on all nodes, the snapshot should be removed with the ``nodetool clearsnapshot -t <snapshot>`` command to prevent running out of space.

Backup the configuration file
------------------------------

.. code:: sh

   sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-2021.1

Gracefully stop the node
------------------------

.. code:: sh

   sudo service scylla-server stop

Download and install the new release
------------------------------------
Before upgrading, check what version you are running now using ``dpkg -l scylla\*server``. You should use the same version in case you want to |ROLLBACK|_ the upgrade. If you are not running a 2021.1.x version, stop right here! This guide only covers 2021.1.x to 2022.1.y upgrades.

**To upgrade ScyllaDB:**

#. Update the |APT|_ to **2022.1** and enable scylla/ppa repo.

   .. code:: sh

       Ubuntu 16:
       sudo add-apt-repository -y ppa:scylladb/ppa

#. Configure Java 1.8, which is requested by ScyllaDB Enterprise 2022.1.

   .. code:: sh
 
      sudo apt-get update
      sudo apt-get install -y openjdk-8-jre-headless
      sudo update-java-alternatives -s java-1.8.0-openjdk-amd64

#. Install:

   .. code:: sh

      sudo apt-get clean all
      sudo apt-get update
      sudo apt-get dist-upgrade scylla-enterprise

Answer ‘y’ to the first two questions.

Start the node
--------------

A new io.conf format was introduced in Scylla 2.3 and 2019.1. If your io.conf doesn't contain `--io-properties-file` option, then it's still the old format. You need to re-run the io setup to generate new io.conf.

.. code:: sh

    sudo scylla_io_setup

.. code:: sh

   sudo service scylla-server start

Validate
--------
#. Check cluster status with ``nodetool status`` and make sure **all** nodes, including the one you just upgraded, are in UN status.
#. Use ``curl -X GET "http://localhost:10000/storage_service/scylla_release_version"`` to check the ScyllaDB version.
#. Check scylla-server log (by ``journalctl _COMM=scylla``) and ``/var/log/syslog`` to validate there are no errors.
#. Check again after two minutes to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

See :doc:`Scylla Metrics Update - Scylla Enterprise 2021.1 to 2022.1<metric-update-2021.1-to-2022.1>` for more information.

Rollback Procedure
==================

.. include:: /upgrade/_common/warning_rollback.rst

The following procedure describes a rollback from ScyllaDB Enterprise release 2022.1.x to 2022.1.y. Apply this procedure if an upgrade from 2021.1 to 2022.1 failed before completing on all nodes. Use this procedure only for nodes you upgraded to 2022.1

ScyllaDB rollback is a rolling procedure that does **not** require a full cluster shutdown.
For each of the nodes you rollback to 2021.1, you will:

* Drain the node and stop ScyllaDB
* Retrieve the old Scylla packages
* Restore the configuration file
* Restart ScyllaDB
* Validate the rollback success

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running with the new version.

Rollback Steps
==============
Gracefully shutdown ScyllaDB
----------------------------

.. code:: sh

   nodetool drain
   sudo service scylla-server stop

Download and install the old release
------------------------------------
#. Remove the old repo file.

    .. code:: sh

       sudo rm -rf /etc/apt/sources.list.d/scylla.list

#. Update the |APT|_ to **2021.1**.
#. Install:

    .. code:: sh

       sudo apt-get clean all
       sudo apt-get update
       sudo apt-get remove scylla\* -y
       sudo apt-get install scylla-enterprise

Answer ‘y’ to the first two questions.

Restore the configuration file
------------------------------
.. code:: sh

   sudo rm -rf /etc/scylla/scylla.yaml
   sudo cp -a /etc/scylla/scylla.yaml.backup-2021.1 /etc/scylla/scylla.yaml

Restore system tables
---------------------

Restore all tables of **system** and **system_schema** from the previous snapshot - 2022.1 uses a different set of system tables. Reference doc: :doc:`Restore from a Backup and Incremental Backup </operating-scylla/procedures/backup-restore/restore/>`.

.. code:: sh

    cd /var/lib/scylla/data/keyspace_name/table_name-UUID/snapshots/<snapshot_name>/
    sudo cp -r * /var/lib/scylla/data/keyspace_name/table_name-UUID/
    sudo chown -R scylla:scylla /var/lib/scylla/data/keyspace_name/table_name-UUID/

Start the node
--------------

.. code:: sh

   sudo service scylla-server start

Validate
--------
Check the upgrade instructions above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
