.. |SCYLLA_NAME| replace:: ScyllaDB

.. |SRC_VERSION| replace:: 5.2
.. |NEW_VERSION| replace:: 5.3

.. |DEBIAN_SRC_REPO| replace:: Debian
.. _DEBIAN_SRC_REPO: https://www.scylladb.com/download/?platform=debian-10&version=scylla-5.2

.. |UBUNTU_SRC_REPO| replace:: Ubuntu
.. _UBUNTU_SRC_REPO: https://www.scylladb.com/download/?platform=ubuntu-20.04&version=scylla-5.2

.. |SCYLLA_DEB_SRC_REPO| replace:: ScyllaDB deb repo (|DEBIAN_SRC_REPO|_, |UBUNTU_SRC_REPO|_)

.. |SCYLLA_RPM_SRC_REPO| replace:: ScyllaDB rpm repo
.. _SCYLLA_RPM_SRC_REPO: https://www.scylladb.com/download/?platform=centos&version=scylla-5.2

.. |DEBIAN_NEW_REPO| replace:: Debian
.. _DEBIAN_NEW_REPO: https://www.scylladb.com/download/?platform=debian-10&version=scylla-5.3

.. |UBUNTU_NEW_REPO| replace:: Ubuntu
.. _UBUNTU_NEW_REPO: https://www.scylladb.com/download/?platform=ubuntu-20.04&version=scylla-5.3

.. |SCYLLA_DEB_NEW_REPO| replace:: ScyllaDB deb repo (|DEBIAN_NEW_REPO|_, |UBUNTU_NEW_REPO|_)

.. |SCYLLA_RPM_NEW_REPO| replace:: ScyllaDB rpm repo
.. _SCYLLA_RPM_NEW_REPO: https://www.scylladb.com/download/?platform=centos&version=scylla-5.3

.. |ROLLBACK| replace:: rollback
.. _ROLLBACK: ./#rollback-procedure

.. |SCYLLA_METRICS| replace:: Scylla Metrics Update - Scylla 5.2 to 5.3
.. _SCYLLA_METRICS: ../metric-update-5.2-to-5.3

=============================================================================
Upgrade Guide - |SCYLLA_NAME| |SRC_VERSION| to |NEW_VERSION|
=============================================================================

This document is a step-by-step procedure for upgrading from |SCYLLA_NAME| |SRC_VERSION| to |SCYLLA_NAME| |NEW_VERSION|, and rollback to 
version |SRC_VERSION| if required.

This guide covers upgrading ScyllaDB on Red Hat Enterprise Linux (RHEL), CentOS, Debian, and Ubuntu. See 
:doc:`OS Support by Platform and Version </getting-started/os-support>` for information about supported versions.

It also applies when using ScyllaDB official image on EC2, GCP, or Azure. The image is based on Ubuntu 22.04.

Upgrade Procedure
=================

A ScyllaDB upgrade is a rolling procedure that does **not** require full cluster shutdown.
For each of the nodes in the cluster, serially (i.e., one node at a time), you will:

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

* Not to use the new |NEW_VERSION| features.
* Not to run administration functions, like repairs, refresh, rebuild, or add or remove nodes. See `sctool <https://manager.docs.scylladb.com/stable/sctool/>`_ for suspending ScyllaDB Manager (only available for ScyllaDB Enterprise) scheduled or running repairs.
* Not to apply schema changes.

.. note:: 

   If you use the `ScyllaDB Monitoring Stack <https://monitoring.docs.scylladb.com/>`_, we recommend upgrading the Monitoring Stack  to the latest version **before** upgrading ScyllaDB.

Upgrade Steps
=============

Check the cluster schema
-------------------------

Make sure that all nodes have the schema synchronized before the upgrade. The upgrade procedure will fail if there is a schema 
disagreement between nodes.

.. code:: sh

   nodetool describecluster

Drain the nodes and backup data
-----------------------------------

Before any major procedure, like an upgrade, it is recommended to backup all the data to an external device. In ScyllaDB, 
backup is performed using the ``nodetool snapshot`` command. For **each** node in the cluster, run the following command:

.. code:: sh

   nodetool drain
   nodetool snapshot

Take note of the directory name that nodetool gives you, and copy all the directories having that name under ``/var/lib/scylla`` 
to a backup device.

When the upgrade is completed on all nodes, remove the snapshot with the ``nodetool clearsnapshot -t <snapshot>`` command to prevent 
running out of space.

Backup the configuration file
------------------------------
.. code:: sh

   sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-src

Gracefully stop the node
------------------------

.. code:: sh

   sudo service scylla-server stop

Download and install the new release
------------------------------------

.. tabs::

   .. group-tab:: Debian/Ubuntu

        Before upgrading, check what version you are running now using ``scylla --version``. You should use the same version as this version in case you want to |ROLLBACK|_ the upgrade. If you are not running a |SRC_VERSION|.x version, stop right here! This guide only covers |SRC_VERSION|.x to |NEW_VERSION|.y upgrades.

        **To upgrade ScyllaDB:**

        #. Update the |SCYLLA_DEB_NEW_REPO| to |NEW_VERSION|.

        #. Install the new ScyllaDB version:

            .. code-block:: console

               sudo apt-get clean all
               sudo apt-get update
               sudo apt-get dist-upgrade scylla


        Answer ‘y’ to the first two questions.

   .. group-tab:: RHEL/CentOS

        Before upgrading, check what version you are running now using ``scylla --version``. You should use the same version as this version in case you want to |ROLLBACK|_ the upgrade. If you are not running a |SRC_VERSION|.x version, stop right here! This guide only covers |SRC_VERSION|.x to |NEW_VERSION|.y upgrades.

        **To upgrade ScyllaDB:**

        #. Update the |SCYLLA_RPM_NEW_REPO|_  to |NEW_VERSION|.
        #. Install the new ScyllaDB version:

            .. code:: sh

               sudo yum clean all
               sudo yum update scylla\* -y

   .. group-tab:: EC2/GCP/Azure Ubuntu Image

        Before upgrading, check what version you are running now using ``scylla --version``. You should use the same version as this version in case you want to |ROLLBACK|_ the upgrade. If you are not running a |SRC_VERSION|.x version, stop right here! This guide only covers |SRC_VERSION|.x to |NEW_VERSION|.y upgrades.

        There are two alternative upgrade procedures:

        * :ref:`Upgrading ScyllaDB and simultaneously updating 3rd party and OS packages <upgrade-image-recommended-procedure>`. It is recommended if you are running a ScyllaDB official image (EC2 AMI, GCP, and Azure images), which is based on Ubuntu 20.04.

        * :ref:`Upgrading ScyllaDB without updating any external packages <upgrade-image-upgrade-guide-regular-procedure>`.

        .. _upgrade-image-recommended-procedure:

        **To upgrade ScyllaDB and update 3rd party and OS packages (RECOMMENDED):**

        Choosing this upgrade procedure allows you to upgrade your ScyllaDB version and update the 3rd party and OS packages using one command.

        #. Update the |SCYLLA_DEB_NEW_REPO| to |NEW_VERSION|.

        #. Load the new repo:

            .. code:: sh

               sudo apt-get update


        #. Run the following command to update the manifest file:

            .. code:: sh

               cat scylla-packages-<version>-<arch>.txt | sudo xargs -n1 apt-get install -y

            Where:

              * ``<version>`` - The ScyllaDB version to which you are upgrading ( |NEW_VERSION| ).
              * ``<arch>`` - Architecture type: ``x86_64`` or ``aarch64``.

            The file is included in the ScyllaDB packages downloaded in the previous step. The file location is ``http://downloads.scylladb.com/downloads/scylla/aws/manifest/scylla-packages-<version>-<arch>.txt``

            Example:

                .. code:: sh

                   cat scylla-packages-5.2.0-x86_64.txt | sudo xargs -n1 apt-get install -y

                .. note::

                   Alternatively, you can update the manifest file with the following command:

                   ``sudo apt-get install $(awk '{print $1'} scylla-packages-<version>-<arch>.txt) -y``

        .. _upgrade-image-upgrade-guide-regular-procedure:

        **To upgrade ScyllaDB:**

        #. Update the |SCYLLA_DEB_NEW_REPO| to |NEW_VERSION|.

        #. Install the new ScyllaDB version:

            .. code-block:: console

               sudo apt-get clean all
               sudo apt-get update
               sudo apt-get dist-upgrade scylla


        Answer ‘y’ to the first two questions.

.. _upgrade-5.3-enable-raft:

(Optional) Enable consistent cluster management in the node's configuration file
--------------------------------------------------------------------------------

.. note::

   Skip this section if you enabled Raft-based consistent cluster management in version 5.2.

Optionally, you can enable Raft-based consistent cluster management on your cluster with the ``consistent_cluster_management``
option. Setting the option to ``true`` on every node enables the Raft consensus algorithm for your cluster. Raft will be used
to consistently manage cluster-wide metadata as soon as you finish upgrading every node to the new version. See 
:doc:`Raft in ScyllaDB </architecture/raft/>` to learn more.

In ScyllaDB 5.2 and 5.3, Raft-based consistent cluster management is disabled by default. If you didn't enable the feature
in version 5.2, you can enable it during the upgrade to version 5.3: modify the ``scylla.yaml`` configuration file 
in ``/etc/scylla/`` **on every node** and add the following:

.. code:: yaml

    consistent_cluster_management: true

.. note:: Once you finish upgrading every node with ``consistent_cluster_management`` enabled, it won't be possible to disable the option.

The option can also be enabled after the cluster is upgraded to |NEW_VERSION| (see :ref:`Enabling Raft in existing cluster <enabling-raft-existing-cluster>`).

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

Validate Raft Setup After Upgrading Every Node
-----------------------------------------------

.. _validate-raft-setup-5.3:

.. note::

   Skip this section if you enabled Raft-based consistent cluster management in version 5.2.

The following section only applies :ref:`if you enabled <upgrade-5.3-enable-raft>` the ``consistent_cluster_management`` option 
on every node when upgrading the cluster to version 5.3.

When you enable ``consistent_cluster_management`` on every node during an upgrade, the ScyllaDB cluster will start 
an additional internal procedure as soon as every node is upgraded to the new version. The goal of the procedure is to 
initialize data structures used by the Raft algorithm to consistently manage cluster-wide metadata such as table schemas.

If you performed the rolling upgrade procedure correctly (in particular, ensuring that schema is synchronized on every step 
and there are no problems with cluster connectivity), that internal procedure should take no longer than a few seconds 
to finish. However, the procedure requires **full cluster availability**. If one of your nodes fails before this procedure 
finishes (for example, due to a hardware problem), the procedure may get stuck. This may cause the cluster to end up in 
a state where schema change operations are unavailable.

For this reason, **you must verify** that the internal procedure has finished successfully by checking the logs of every 
ScyllaDB node. If the procedure gets stuck, manual intervention is required.

Refer to :ref:`Verifying that the internal Raft upgrade procedure finished successfully <verify-raft-procedure>` for 
instructions on how to verify that the procedure was successful and how to proceed if it gets stuck.

Rollback Procedure
==================

.. include:: /upgrade/_common/warning_rollback.rst

The following procedure describes a rollback from |SCYLLA_NAME| |NEW_VERSION|.x to |SRC_VERSION|.y. Apply this procedure if an upgrade 
from |SRC_VERSION| to |NEW_VERSION| fails before completing on all nodes. Use this procedure only for nodes you upgraded to |NEW_VERSION|.

.. warning::

   The rollback procedure can be applied **only** if some nodes have not been upgraded to |NEW_VERSION| yet.
   As soon as the last node in the rolling upgrade procedure is started with |NEW_VERSION|, rollback becomes impossible.
   At that point, the only way to restore a cluster to |SRC_VERSION| is by restoring it from backup.

ScyllaDB rollback is a rolling procedure that does **not** require full cluster shutdown.
For each of the nodes you rollback to |SRC_VERSION|, serially (i.e., one node at a time), you will:

* Drain the node and stop Scylla
* Retrieve the old ScyllaDB packages
* Restore the configuration file
* Restore system tables
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
   sudo service scylla-server stop

Download and install the old release
------------------------------------

..
    TODO: downgrade for 3rd party packages in EC2/GCP/Azure - like in the upgrade section?

.. tabs::

   .. group-tab:: Debian/Ubuntu

        #. Remove the old repo file.

            .. code:: sh

               sudo rm -rf /etc/apt/sources.list.d/scylla.list

        #. Update the |SCYLLA_DEB_SRC_REPO| to |SRC_VERSION|.
        #. Install:

            .. code-block::

               sudo apt-get update
               sudo apt-get remove scylla\* -y
               sudo apt-get install scylla

        Answer ‘y’ to the first two questions.

   .. group-tab:: RHEL/CentOS

        #. Remove the old repo file.

            .. code:: sh

               sudo rm -rf /etc/yum.repos.d/scylla.repo

        #. Update the |SCYLLA_RPM_SRC_REPO|_  to |SRC_VERSION|.
        #. Install:

            .. code:: console

               sudo yum clean all
               sudo rm -rf /var/cache/yum
               sudo yum remove scylla\\*cqlsh
               sudo yum downgrade scylla\\* -y
               sudo yum install scylla

   .. group-tab:: EC2/GCP/Azure Ubuntu Image

        #. Remove the old repo file.

            .. code:: sh

               sudo rm -rf /etc/apt/sources.list.d/scylla.list

        #. Update the |SCYLLA_DEB_SRC_REPO| to |SRC_VERSION|.
        #. Install:

            .. code-block::

               sudo apt-get update
               sudo apt-get remove scylla\* -y
               sudo apt-get install scylla

        Answer ‘y’ to the first two questions.

Restore the configuration file
------------------------------
.. code:: sh

   sudo rm -rf /etc/scylla/scylla.yaml
   sudo cp -a /etc/scylla/scylla.yaml.backup-src | /etc/scylla/scylla.yaml

Restore system tables
---------------------

Restore all tables of **system** and **system_schema** from the previous snapshot because |NEW_VERSION| uses a different set of system tables. See :doc:`Restore from a Backup and Incremental Backup </operating-scylla/procedures/backup-restore/restore/>` for reference.

.. code:: sh

    cd /var/lib/scylla/data/keyspace_name/table_name-UUID/snapshots/<snapshot_name>/
    sudo cp -r * /var/lib/scylla/data/keyspace_name/table_name-UUID/
    sudo chown -R scylla:scylla /var/lib/scylla/data/keyspace_name/table_name-UUID/

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
