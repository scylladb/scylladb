

=============================================
Upgrade Guide - Scylla Manager 1.0.x to 1.1.x
=============================================

Enterprise customers who use Scylla Manager 1.0.x are encouraged to upgrade to 1.1.x.
The steps below instruct you how to upgrade the Scylla Manager server while keeping the manager datastore intact.
If you are not running Scylla Manager 1.0.x, do not perform this upgrade procedure. This procedure only covers upgrades from Scylla Manager 1.0.x to 1.1.x.

Please contact `Scylla Enterprise Support <https://www.scylladb.com/product/support/>`_ team with any question.


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

.. _upgrade-manager-1.0.x-to-1.1.x-previous-release:

Before upgrading, check what version you are running now using ``rpm -qa | grep scylla-manager``. You should use the same version in case you want to :ref:`rollback <upgrade-manager-1.0.x-to-1.1.x-rollback-procedure>` the upgrade.


To upgrade:


1. Update the `Update the Scylla Manager repo: <https://www.scylladb.com/enterprise-download/#manager>`_ to **1.1.x**
2. Run:

.. code:: sh

   sudo yum update scylla-manager -y

Validate
--------
1. Check Scylla Manager status with ``systemctl status scylla-manager.service``. Confirm the service is active (running).
2. Confirm that the upgrade changed the Client and Server version. Run ``sctool version`` and make sure both are 1.1.x version 
3. Confirm that following the update, that your managed clusters are still present. Run ``sctool cluster list``
4. Confirm that following the upgrade, there are no repairs in a stopped state. Run ``sctool task list`` to list the repair tasks in progress. If any are in a stopped state, run ``sctool repair unit schedule <repair-unit-id> --start-date=now`` to resume.

Update scylla-manager.yaml (optional)
-------------------------------------

As part of the upgrade procudre, paramaters were added to ``/etc/scylla-manager/scylla-manager.yaml``. See the new values below. There is no need to update the ``scylla-manager.yaml`` file as part of the upgrade.

.. code-block:: yaml

   # Repair service configuration.
   repair:
     # Granularity of repair. Repair works on segments, segment is a continuous
     # token range.
     #
     # Set the maximal number of tokens in a segment (zero is no limit).
     segment_size_limit: 0

     # Set number of segments to be repaired in one Scylla command.
     segments_per_repair: 1

     # Error tolerance.
     #
     # Set how many segments may fail to repair. Note that the manager would retry
     # to repair the failed segments. If the limit is exceeded, however, repair
     # will stop and the next repair will start from the beginning.
     segment_error_limit: 100

     # Fail-fast, set to true if you want repair to stop on first error. Unlike
     # segment_error_limit this allows for resuming the stopped repair.
     stop_on_error: false

     # Set wait time if Scylla failed to execute a repair command. Note that if
     # stop_on_error is true this has no effect.
     error_backoff: 10s

     # Set how often to poll Scylla node for command status.
     poll_interval: 200ms

     # Set time offset between the automated scheduler run and the scheduled
     # repairs. If scheduler runs at midnight the repairs would start at
     # midnight + this value. This gives you the opportunity to audit and modify
     # the scheduled repairs.
     auto_schedule_delay: 2h

     # Set maximal time after which a restarted repair is forced to start from the
     # beginning.
     max_run_age: 36h

     # Distribution of data among cores (shards) within a node.
     # Copy value from Scylla configuration file.
     murmur3_partitioner_ignore_msb_bits: 12


.. _upgrade-manager-1.0.x-to-1.1.x-rollback-procedure:

Rollback Procedure
==================

The following procedure describes a rollback from Scylla Manager 1.1 to 1.0. Apply this procedure if an upgrade from 1.0 to 1.1 failed for any reason.

**Warning:** note that you may lose the manged clusters after downgrade. Should this happen, you will need to add the managed clusters.

* Downgrade to :ref:`previous release <upgrade-manager-1.0.x-to-1.1.x-previous-release>`
* Start Scylla Manager
* Valdate Scylla Manager version

Downgrade to previous release
-----------------------------
1. Stop Scylla Manager

.. code:: sh

   sudo systemctl stop scylla-manager

2. Drop scylla_manager keyspace from the remote datastore

.. code:: sh

   cqlsh -e "DROP KEYSPACE scylla_manager"

3. Remove Scylla Manager repo

.. code:: sh

   sudo rm -rf /etc/yum.repos.d/scylla-manager.repo
   sudo yum clean all

4. Update the `Scylla Manager repo <https://www.scylladb.com/enterprise-download/#manager>`_ to **1.0.x**

5. Install previous version

.. code:: sh

   sudo yum downgrade scylla-manager scylla-manager-server scylla-manager-client -y


Start Scylla Manager
--------------------
.. code:: sh

   sudo systemctl start scylla-manager

Validate Scylla Manager Version
-------------------------------

Validate Scylla Manager version:

.. code:: sh

   sctool version

The version should match with the results you had :ref:`previously <upgrade-manager-1.0.x-to-1.1.x-previous-release>`.
