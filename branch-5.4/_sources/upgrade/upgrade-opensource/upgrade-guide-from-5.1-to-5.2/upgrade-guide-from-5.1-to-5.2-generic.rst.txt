.. |SCYLLA_NAME| replace:: ScyllaDB

.. |SRC_VERSION| replace:: 5.1
.. |NEW_VERSION| replace:: 5.2

.. |DEBIAN_SRC_REPO| replace:: Debian
.. _DEBIAN_SRC_REPO: https://www.scylladb.com/download/?platform=debian-10&version=scylla-5.1

.. |UBUNTU_SRC_REPO| replace:: Ubuntu
.. _UBUNTU_SRC_REPO: https://www.scylladb.com/download/?platform=ubuntu-20.04&version=scylla-5.1

.. |SCYLLA_DEB_SRC_REPO| replace:: ScyllaDB deb repo (|DEBIAN_SRC_REPO|_, |UBUNTU_SRC_REPO|_)

.. |SCYLLA_RPM_SRC_REPO| replace:: ScyllaDB rpm repo
.. _SCYLLA_RPM_SRC_REPO: https://www.scylladb.com/download/?platform=centos&version=scylla-5.1

.. |DEBIAN_NEW_REPO| replace:: Debian
.. _DEBIAN_NEW_REPO: https://www.scylladb.com/download/?platform=debian-10&version=scylla-5.2

.. |UBUNTU_NEW_REPO| replace:: Ubuntu
.. _UBUNTU_NEW_REPO: https://www.scylladb.com/download/?platform=ubuntu-20.04&version=scylla-5.2

.. |SCYLLA_DEB_NEW_REPO| replace:: ScyllaDB deb repo (|DEBIAN_NEW_REPO|_, |UBUNTU_NEW_REPO|_)

.. |SCYLLA_RPM_NEW_REPO| replace:: ScyllaDB rpm repo
.. _SCYLLA_RPM_NEW_REPO: https://www.scylladb.com/download/?platform=centos&version=scylla-5.2

.. |ROLLBACK| replace:: rollback
.. _ROLLBACK: ./#rollback-procedure

.. |SCYLLA_METRICS| replace:: Scylla Metrics Update - Scylla 5.1 to 5.2
.. _SCYLLA_METRICS: ../metric-update-5.1-to-5.2

=============================================================================
Upgrade Guide - |SCYLLA_NAME| |SRC_VERSION| to |NEW_VERSION|
=============================================================================

This document is a step by step procedure for upgrading from |SCYLLA_NAME| |SRC_VERSION| to |SCYLLA_NAME| |NEW_VERSION|, and rollback to version |SRC_VERSION| if required.

This guide covers upgrading Scylla on Red Hat Enterprise Linux (RHEL) 7/8, CentOS 7/8, Debian 10 and Ubuntu 20.04. It also applies when using ScyllaDB official image on EC2, GCP, or Azure; the image is based on Ubuntu 20.04.

See :doc:`OS Support by Platform and Version </getting-started/os-support>` for information about supported versions.

Upgrade Procedure
=================

A ScyllaDB upgrade is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes in the cluster, serially (i.e. one node at a time), you will:

* Check that the cluster's schema is synchronized
* Drain the node and backup the data
* Backup the configuration file
* Stop ScyllaDB
* Download and install new ScyllaDB packages
* (Optional) Enable consistent cluster management in the configuration file
* Start ScyllaDB
* Validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to the next node before validating that the node you upgraded is up and running the new version.

**During** the rolling upgrade, it is highly recommended:

* Not to use the new |NEW_VERSION| features
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes. See `sctool <https://manager.docs.scylladb.com/stable/sctool/>`_ for suspending ScyllaDB Manager (only available for ScyllaDB Enterprise) scheduled or running repairs.
* Not to apply schema changes

If you enabled consistent cluster management in each node's configuration file, then as soon as every node has been upgraded to the new version, the cluster will start a procedure which initializes the Raft algorithm for consistent cluster metadata management.
You must then :ref:`verify <validate-raft-setup>` that this procedure successfully finishes.

.. note:: 

   If you use the `ScyllaDB Monitoring Stack <https://monitoring.docs.scylladb.com/>`_, we recommend upgrading the Monitoring Stack  to the latest version **before** upgrading ScyllaDB.
   
   For ScyllaDB 5.2, you MUST upgrade the Monitoring Stack to version 4.3 or later.

Upgrade Steps
=============
Check the cluster schema
-------------------------
Make sure that all nodes have the schema synchronized before upgrade. The upgrade procedure will fail if there is a schema disagreement between nodes.

.. code:: sh

   nodetool describecluster

Drain the nodes and backup the data
-----------------------------------
Before any major procedure, like an upgrade, it is recommended to backup all the data to an external device. In Scylla, backup is done using the ``nodetool snapshot`` command. For **each** node in the cluster, run the following command:

.. code:: sh

   nodetool drain
   nodetool snapshot

Take note of the directory name that nodetool gives you, and copy all the directories having that name under ``/var/lib/scylla`` to a backup device.

When the upgrade is completed on all nodes, remove the snapshot with the ``nodetool clearsnapshot -t <snapshot>`` command to prevent running out of space.

Backup the configuration file
------------------------------

Back up the ``scylla.yaml`` configuration file and the ScyllaDB packages
in case you need to rollback the upgrade.

.. tabs::

   .. group-tab:: Debian/Ubuntu

      .. code:: sh
         
         sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup
         sudo cp /etc/apt/sources.list.d/scylla.list ~/scylla.list-backup

   .. group-tab:: RHEL/CentOS

      .. code:: sh
         
         sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup
         sudo cp /etc/yum.repos.d/scylla.repo ~/scylla.repo-backup

Gracefully stop the node
------------------------

.. code:: sh

   sudo service scylla-server stop

Download and install the new release
------------------------------------

.. tabs::

   .. group-tab:: Debian/Ubuntu

        Before upgrading, check what version you are running now using ``dpkg -s scylla-server``. You should use the same version as this version in case you want to |ROLLBACK|_ the upgrade. If you are not running a |SRC_VERSION|.x version, stop right here! This guide only covers |SRC_VERSION|.x to |NEW_VERSION|.y upgrades.

        **To upgrade ScyllaDB:**

        #. Update the |SCYLLA_DEB_NEW_REPO| to |NEW_VERSION|.

        #. Install the new ScyllaDB version:

            .. code-block:: console

               sudo apt-get clean all
               sudo apt-get update
               sudo apt-get dist-upgrade scylla


        Answer ‘y’ to the first two questions.

   .. group-tab:: RHEL/CentOS

        Before upgrading, check what version you are running now using ``rpm -qa | grep scylla-server``. You should use the same version as this version in case you want to |ROLLBACK|_ the upgrade. If you are not running a |SRC_VERSION|.x version, stop right here! This guide only covers |SRC_VERSION|.x to |NEW_VERSION|.y upgrades.

        **To upgrade ScyllaDB:**

        #. Update the |SCYLLA_RPM_NEW_REPO|_  to |NEW_VERSION|.
        #. Install the new ScyllaDB version:

            .. code:: sh

               sudo yum clean all
               sudo yum update scylla\* -y

.. note::

   If you are running a ScyllaDB official image (for EC2 AMI, GCP, or Azure), 
   you need to:
   
    * Download and install the new ScyllaDB release for Ubuntu; see 
      the Debian/Ubuntu tab above for instructions.
    * Update underlying OS packages.

   See :doc:`Upgrade ScyllaDB Image </upgrade/ami-upgrade>` for details.

(Optional) Enable consistent cluster management in the node's configuration file
--------------------------------------------------------------------------------

If you enable this option on every node, this will cause the Scylla cluster to enable Raft and use it to consistently manage cluster-wide metadata as soon as you finish upgrading every node to the new version.
Check the :doc:`Raft in ScyllaDB document </architecture/raft/>` to learn more.

.. TODO: include enterprise versions

In 5.2, Raft-based consistent cluster management is disabled by default.
In the following version, it will be enabled by default, but you'll be able to disable it explicitly during upgrade if needed (assuming you haven't previously enabled it on every node).
In further versions the option will be removed and consistent cluster management will be enabled unconditionally.

The option can also be enabled after the cluster is upgraded to |NEW_VERSION| (see :ref:`Enabling Raft in existing cluster <enabling-raft-existing-cluster>`).

To enable the option, modify the ``scylla.yaml`` configuration file in ``/etc/scylla/`` and add the following:

.. code:: yaml

    consistent_cluster_management: true

.. note:: Once you finish upgrading every node with `consistent_cluster_management` enabled, it won't be possible to turn the option back off.

Start the node
--------------

.. code:: sh

   sudo service scylla-server start

Validate
--------
#. Check cluster status with ``nodetool status`` and make sure **all** nodes, including the one you just upgraded, are in ``UN`` status.
#. Use ``curl -X GET "http://localhost:10000/storage_service/scylla_release_version"`` to check the ScyllaDB version. Validate that the version matches the one you upgraded to.
#. Check scylla-server log (by ``journalctl _COMM=scylla``) and ``/var/log/syslog`` to validate there are no new errors in the log.
#. Check again after two minutes, to validate no new issues are introduced.

Once you are sure the node upgrade was successful, move to the next node in the cluster.

See |Scylla_METRICS|_ for more information..

After upgrading every node
==========================

The following section applies only if you enabled the ``consistent_cluster_management`` option on every node when upgrading the cluster.

.. _validate-raft-setup:

Validate Raft setup
-------------------

Enabling ``consistent_cluster_management`` on every node during upgrade will cause the Scylla cluster to start an additional internal procedure as soon as every node is upgraded to the new version.
The goal of this procedure is to initialize data structures used by the Raft algorithm to consistently manage cluster-wide metadata such as table schemas.

Assuming you performed the rolling upgrade procedure correctly, in particular ensuring that schema is synchronized on every step, and if there are no problems with cluster connectivity, then this follow-up internal procedure should take no longer than a few seconds to finish.
However, the procedure requires **full cluster availability**. If an unlucky accident (e.g. a hardware problem) causes one of your nodes to fail before this procedure finishes, the procedure may get stuck. This may cause the cluster to end up in a state where schema change operations are unavailable.

Therefore, following the rolling upgrade, **you must verify** that this internal procedure has finished successfully by checking the logs of every Scylla node.
If the procedure gets stuck, manual intervention is required.

Refer to the following document for instructions on how to verify that the procedure was successful and how to proceed if it gets stuck: :ref:`Verifying that the internal Raft upgrade procedure finished successfully <verify-raft-procedure>`.

Rollback Procedure
==================

.. include:: /upgrade/_common/warning_rollback.rst

The following procedure describes a rollback from |SCYLLA_NAME| |NEW_VERSION|.x to |SRC_VERSION|.y. Apply this procedure if an upgrade from |SRC_VERSION| to |NEW_VERSION| failed before completing on all nodes. Use this procedure only for nodes you upgraded to |NEW_VERSION|.

.. warning::

   The rollback procedure can be applied **only** if some nodes have not been upgraded to |NEW_VERSION| yet.
   As soon as the last node in the rolling upgrade procedure is started with |NEW_VERSION|, rollback becomes impossible.
   At that point, the only way to restore a cluster to |SRC_VERSION| is by restoring it from backup.

ScyllaDB rollback is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes you rollback to |SRC_VERSION|, serially (i.e. one node at a time), you will:

* Drain the node and stop Scylla
* Retrieve the old ScyllaDB packages
* Restore the configuration file
* Reload systemd configuration
* Restart ScyllaDB
* Validate the rollback success

Apply the following procedure **serially** on each node. Do not move to the next node before validating that the rollback was successful and the node is up and running the old version.

Rollback Steps
==============
Drain and gracefully stop the node
----------------------------------

.. code:: sh

   nodetool drain
   nodetool snapshot
   sudo service scylla-server stop

Restore and install the old release
------------------------------------

.. tabs::

   .. group-tab:: Debian/Ubuntu

        #. Restore the |SRC_VERSION| packages backed up during the upgrade.

            .. code:: sh

               sudo cp ~/scylla.list-backup /etc/apt/sources.list.d/scylla.list
               sudo chown root.root /etc/apt/sources.list.d/scylla.list
               sudo chmod 644 /etc/apt/sources.list.d/scylla.list

        #. Install:

            .. code-block::

               sudo apt-get update
               sudo apt-get remove scylla\* -y
               sudo apt-get install scylla

        Answer ‘y’ to the first two questions.

   .. group-tab:: RHEL/CentOS

        #. Restore the |SRC_VERSION| packages backed up during the upgrade.

            .. code:: sh

               sudo cp ~/scylla.repo-backup /etc/yum.repos.d/scylla.repo
               sudo chown root.root /etc/yum.repos.d/scylla.repo
               sudo chmod 644 /etc/yum.repos.d/scylla.repo

        #. Install:

            .. code:: console

               sudo yum clean all
               sudo rm -rf /var/cache/yum
               sudo yum downgrade scylla-\*cqlsh -y
               sudo yum remove scylla-\*cqlsh -y
               sudo yum downgrade scylla\* -y
               sudo yum install scylla -y


Restore the configuration file
------------------------------
.. code:: sh

   sudo rm -rf /etc/scylla/scylla.yaml
   sudo cp /etc/scylla/scylla.yaml-backup /etc/scylla/scylla.yaml

Reload systemd configuration
----------------------------

You must reload the unit file if the systemd unit file is changed.

.. code:: sh

   sudo systemctl daemon-reload

Start the node
--------------

.. code:: sh

   sudo service scylla-server start

Validate
--------
Check the upgrade instructions above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
