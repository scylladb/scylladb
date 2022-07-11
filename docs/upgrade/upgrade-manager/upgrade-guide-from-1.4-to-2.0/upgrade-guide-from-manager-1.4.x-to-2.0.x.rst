

=============================================
Upgrade Guide - Scylla Manager 1.4.x to 2.0.x 
=============================================

**Supported Operating Systems:** CentOS/RHEL 7, Ubuntu 16 and 18, Debian 9

Enterprise customers who use Scylla Manager 1.4.x are encouraged to upgrade to 2.0.x. For new installations please see `Scylla Manager - Download and Install <https://www.scylladb.com/enterprise-download/#manager>`_. The steps below instruct you how to upgrade the Scylla Manager server while keeping the manager datastore intact. If you are not running Scylla Manager 1.4.x, do not perform this upgrade procedure. This procedure only covers upgrades from Scylla Manager 1.4.x to 2.0.x. If you want to upgrade to 2.0 from lower versions please upgrade to 1.4.x first.


Please contact `Scylla Enterprise Support <https://www.scylladb.com/product/support/>`_ team with any questions. For release information, see the `Release Notes <https://www.scylladb.com/product/release-notes/>`_.

Upgrade Procedure
=================

**Workflow:**

#. `Stop the Scylla Manager Server`_
#. `Backup the Scylla Manager data`_
#. Install the :doc:`Scylla Manager server 2.0 </operating-scylla/manager/2.1/install>`
#. Install the :doc:`Scylla Manager Agent </operating-scylla/manager/2.1/install-agent>` (with the most up to date version)latest agent on eachthe node. Confirm the nodes all start and make sure they are started.
#. `Start the Scylla Manager server`_
#. `Validate`_ that the upgrade was successful
#. (optional) Remove ssh artifacts from the nodes
#. (optional) Configure auth token in the agent configuration, and update cluster in Scylla Manager.


Stop the Scylla Manager Server
------------------------------

**Procedure**

#. Make sure that no task is running (all tasks have DONE status) before stopping the server:

   .. code-block:: none

      sctool task list -c <cluster_id|cluster_name>

#. Stop the Scylla Manager:

   .. code-block:: none

      sudo systemctl stop scylla-manager


Backup the Scylla Manager data
-------------------------------
Scylla Manager server persists its data to a Scylla cluster (data store). Before upgrading, backup the ``scylla_manager`` keyspace from Scylla Manager's backend, following this :doc:`backup procedure </operating-scylla/procedures/backup-restore/backup>`.

Install new Scylla Manager server 2.0
-------------------------------------

.. _upgrade-manager-1.4.x-to-2.0.x-previous-release:

Before upgrading, check what version you are **currently** running:

**CentOS / RHEL** 

.. code-block:: bash
   
   rpm -q scylla-manager


**Ubuntu / Debian**

.. code-block:: bash

   dpkg -s scylla-manager

Save the details of this version so you can :ref:`rollback <upgrade-manager-1.4.x-to-2.0.x-rollback-procedure-centos>` to it.



To upgrade:


Follow the procedure described in: :doc:`Install new Scylla Manager server 2.0 </operating-scylla/manager/2.1/install>`

Download, Install and Start Scylla Manager Agent
------------------------------------------------

Follow the instructions described in :doc:`Install the Scylla Manager Agent </operating-scylla/manager/2.1/install-agent>` for installing the Scylla Manager Agent on every node in the cluster. 

Start the Scylla Manager Server
-------------------------------

From the Scylla Manager Server, run:

.. code-block:: none

   sudo systemctl start scylla-manager


Configure Scylla Manager to work with the authentication token
--------------------------------------------------------------


Copy the authentication :doc:`token </operating-scylla/manager/2.1/install-agent>` you created when installating the scylla-manager-agent:

.. code-block:: none
    
    sctool cluster update --auth-token=6Es3dm24U72NzAu9ANWmU3C4ALyVZhwwPZZPWtK10eYGHJ24wMoh9SQxRZEluWMc0qDrsWCCshvfhk9uewOimQS2x5yNTYUEoIkO1VpSmTFu5fsFyoDgEkmNrCJpXtfM -c cluster-name



Validate
--------
#. Check that Scylla Manager service is running with ``sudo systemctl status scylla-manager.service``. Confirm the service is active (running). If not, then start it with ``systemctl start scylla-manager.service``.
#. Confirm that the upgrade changed the Client and Server version. Run ``sctool version`` and make sure both are 2.0.x version. For example:

   .. code-block:: none

      sctool version
      Client version: 2.0-0.20191220.5407198e
      Server version: 2.0-0.20191220.5407198e

#. Confirm that following the update, that your managed clusters are still present. Run ``sctool cluster list``

   .. code-block:: none
  
      sctool cluster list
      ╭──────────────────────────────────────┬──────────╮
      │ cluster id                           │ name     │
      ├──────────────────────────────────────┼──────────┤
      │ db7faf98-7cc4-4a08-b707-2bc59d65551e │ cluster  │
      ╰──────────────────────────────────────┴──────────╯

#. Confirm that following the upgrade, status is up and running


   .. code-block:: none
  
      sctool status -c <cluster_id|cluster_name>
      Datacenter: AWS_1
      ╭───────────┬─────┬───────────┬───────────────╮
      │ CQL       │ SSL │ REST      │ Host          │
      ├───────────┼─────┼───────────┼───────────────┤
      │ UP (56ms) │ OFF │ UP (37ms) │ 127.0.0.1     │
      │ UP (56ms) │ OFF │ UP (25ms) │ 127.0.0.2     │
      │ UP (56ms) │ OFF │ UP (25ms) │ 127.0.0.3     │
      ╰───────────┴─────┴───────────┴───────────────╯

.. _upgrade-manager-1.4.x-to-2.0.x-rollback-procedure-centos:

Rollback Procedure
==================

The following procedure describes a rollback from Scylla Manager 2.0 to 1.4. Apply this procedure if an upgrade from 1.4 to 2.0 failed for any reason.

**Warning:** note that you may lose the manged clusters after downgrade. Should this happen, you will need to add the managed clusters clusters manually.

* Downgrade to :ref:`previous release <upgrade-manager-1.4.x-to-2.0.x-previous-release>`
* Start Scylla Manager
* Valdate Scylla Manager version

Downgrade to previous release
-----------------------------
#. Stop Scylla Manager

   .. code:: sh

      sudo systemctl stop scylla-manager

#. Drop the ``scylla_manager`` keyspace from the remote datastore

   .. code:: sh

      cqlsh -e "DROP KEYSPACE scylla_manager"

#. Remove Scylla Manager repo

   **CentOS / RHEL**

   .. code:: sh

      sudo rm -f /etc/yum.repos.d/scylla-manager.repo
      sudo yum clean all
      sudo rm -rf /var/cache/yum

   **Ubuntu / Debian**

   .. code:: sh

      sudo rm -f /etc/apt/sources.list.d/scylla-manager.list


#. Update the `Scylla Manager repo <https://www.scylladb.com/enterprise-download/#manager>`_ to **1.4.x**

#. Install previous version

   **CentOS / RHEL**

   .. code:: sh

      sudo yum downgrade scylla-manager scylla-manager-server scylla-manager-client -y
 

   **Ubuntu / Debian**

   .. code:: sh

      sudo apt-get update
      sudo apt-get remove scylla-manager\* -y
      sudo apt-get install scylla-manager scylla-manager-server scylla-manager-client
      sudo systemctl unmask scylla-manager.service


Rollback the Scylla Manager database
------------------------------------

#. Start Scylla Manager to reinitialize the data base schema.

   .. code:: sh

      sudo systemctl start scylla-manager

#. Stop Scylla Manager to avoid issues while restoring the backup. If you did not perform any backup before upgrading then you are done now and can continue at "Start Scylla Manager".

   .. code:: sh

      sudo systemctl stop scylla-manager

#. Restore the database backup if you performed a backup by following the instructions in :doc:`Restore from a Backup </operating-scylla/procedures/backup-restore/restore>`
   You can skip step 1 since the Scylla Manager has done this for you.

Start Scylla Manager
--------------------

.. code:: sh

   sudo systemctl start scylla-manager

Validate Scylla Manager Version
-------------------------------

Validate Scylla Manager version:

.. code:: sh

   sctool version

The version should match with the version infomation you were running :ref:`previously <upgrade-manager-1.4.x-to-2.0.x-previous-release>`.
