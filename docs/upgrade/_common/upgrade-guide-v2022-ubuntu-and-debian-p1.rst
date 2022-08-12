=============================================================================
Upgrade Guide - |SCYLLA_NAME| |SRC_VERSION| to |NEW_VERSION| for |OS|
=============================================================================

This document is a step-by-step procedure for upgrading from ScyllaDB Enterprise 2021.1 to ScyllaDB Enterprise 2022.1, and rollback to 2021.1 if required.


Applicable Versions
===================
This guide covers upgrading ScyllaDB Enterprise from version 2021.1.x to ScyllaDB Enterprise version 2022.1.y on |OS|. See :doc:`OS Support by Platform and Version </getting-started/os-support>` for information about supported versions.

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
* Not to run administration functions, like repairs, refresh, rebuild, or add or remove nodes. See `sctool <https://manager.docs.scylladb.com/stable/sctool/index.html>`_ for suspending ScyllaDB Manager's scheduled or running repairs.
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

   sudo service scylla-enterprise-server stop

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

