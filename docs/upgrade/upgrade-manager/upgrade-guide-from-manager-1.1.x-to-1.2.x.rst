

=============================================
Upgrade Guide - Scylla Manager 1.1.x to 1.2.x
=============================================

Enterprise customers who use Scylla Manager 1.1.x are encouraged to upgrade to 1.2.x.
For new installations please see `Scylla Manager - Download and Install <https://www.scylladb.com/enterprise-download/#manager>`_.
The steps below instruct you how to upgrade the Scylla Manager server while keeping the manager datastore intact.
If you are not running Scylla Manager 1.1.x, do not perform this upgrade procedure. This procedure only covers upgrades from Scylla Manager 1.1.x to 1.2.x.

Please contact `Scylla Enterprise Support <https://www.scylladb.com/product/support/>`_ team with any question.

Upgrade Notes
=================

* This upgrade brings internal changes incompatible with previous versions of Scylla Manager.
  We have removed the repair unit concept and replaced it with a repair task that can be tuned using simple glob based pattern matching.
  The pattern matching that is performed can be applied to filter out multiple keyspaces and tables as well as entire datacenters. 
  A consequence of this is that custom repairs that previously were scheduled are removed during the upgrade and you may need to replace them.
  A weekly repair task will be scheduled for each existing cluster so repairs will be performed automatically.

* The Scylla Manager API is now using HTTPS by default. Their default values have changed to 56443 for HTTPS and 56080 for plain HTTP.
  The port can be changed as needed in ``/etc/scylla-manager/scylla-manager.yaml``

Upgrade Procedure
=================

* Backup the data
* Download and install new packages
* Validate that the upgrade was successful

Backup the data
------------------------------
Before any major procedure, like an upgrade, it is recommended to backup all the data to an external device.  It is recommended to backup the ``scylla_manager`` keyspace from Scylla Manager's backend, following this :doc:`backup procedure </operating-scylla/procedures/backup-restore/backup>`.

Download and install the new release
------------------------------------

.. _upgrade-manager-1.1.x-to-1.2.x-previous-release:

Before upgrading, check what version you are currently running now using ``rpm -q scylla-manager``. You should use the same version that you had previously installed in case you want to :ref:`rollback <upgrade-manager-1.1.x-to-1.2.x-rollback-procedure>` the upgrade.


To upgrade:


1. Update the `Update the Scylla Manager repo: <https://www.scylladb.com/enterprise-download/#manager>`_ to **1.2.x**
2. Run:

.. code:: sh

   sudo yum update scylla-manager -y

3. Reload your shell execute the below command to reload ``sctool`` code completion.

.. code:: sh

   source /etc/bash_completion.d/sctool.bash


Validate
--------
1. Confirm that the upgrade changed the Client and Server version. Run ``sctool version`` and make sure both are 1.2.x version.
2. If you get an error from the version check then make sure that Scylla Manager is running with ``systemctl status scylla-manager.service``. Confirm the service is active (running). If not, then start it with ``systemctl start scylla-manager.service``.
3. Confirm that following the update, that your managed clusters are still present. Run ``sctool cluster list``
4. Confirm that following the upgrade, there is one repair task in a NEW state for each existing cluster. Run ``sctool task list`` to list the repair tasks.

Update scylla-manager.yaml (optional)
-------------------------------------

As part of the upgrade procudre, paramaters were changed in ``/etc/scylla-manager/scylla-manager.yaml``. See the new values below. There is no need to update the ``scylla-manager.yaml`` file as part of the upgrade.

.. code-block:: yaml

   # Bind REST API to the specified TCP address using HTTP protocol.
   # http: 127.0.0.1:56080

   # Bind REST API to the specified TCP address using HTTPS protocol.
   https: 127.0.0.1:56443

   # TLS certificate file to use for HTTPS.
   tls_cert_file: /var/lib/scylla-manager/scylla_manager.crt

   # TLS key file to use for HTTPS.
   tls_key_file: /var/lib/scylla-manager/scylla_manager.key

   # Bind prometheus API to the specified TCP address using HTTP protocol.
   # By default it binds to all network interfaces but you can restrict it
   # by specifying it like this 127:0.0.1:56090 or any other combination
   # of ip and port.
   prometheus: ':56090'

.. _upgrade-manager-1.1.x-to-1.2.x-rollback-procedure:

Rollback Procedure
==================

The following procedure describes a rollback from Scylla Manager 1.2 to 1.1. Apply this procedure if an upgrade from 1.0 to 1.1 failed for any reason.

**Warning:** note that you may lose the manged clusters after downgrade. Should this happen, you will need to add the managed clusters clusters manually.

* Downgrade to :ref:`previous release <upgrade-manager-1.1.x-to-1.2.x-previous-release>`
* Start Scylla Manager
* Valdate Scylla Manager version

Downgrade to previous release
-----------------------------
1. Stop Scylla Manager

.. code:: sh

   sudo systemctl stop scylla-manager

2. Drop the ``scylla_manager`` keyspace from the remote datastore

.. code:: sh

   cqlsh -e "DROP KEYSPACE scylla_manager"

3. Remove Scylla Manager repo

.. code:: sh

   sudo rm -rf /etc/yum.repos.d/scylla-manager.repo
   sudo yum clean all

4. Update the `Scylla Manager repo <https://www.scylladb.com/enterprise-download/#manager>`_ to **1.1.x**

5. Install previous version

.. code:: sh

   sudo yum downgrade scylla-manager scylla-manager-server scylla-manager-client -y

Rollback the Scylla Manager database
------------------------------------

1. Start Scylla Manager to reinitialize the data base schema.

.. code:: sh

   sudo systemctl start scylla-manager

2. Stop Scylla Manager to avoid issues while restoring the backup. If you did not perform any backup before upgrading then you are done now and can continue at "Start Scylla Manager".

.. code:: sh

   sudo systemctl stop scylla-manager

3. Restore the database backup if you performed a backup by following the instructions in :doc:`Restore from a Backup </operating-scylla/procedures/backup-restore/restore>`.
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

The version should match with the results you had :ref:`previously <upgrade-manager-1.1.x-to-1.2.x-previous-release>`.
