**To upgrade ScyllaDB:**

1. Update the |SCYLLA_REPO|_ to |NEW_VERSION|

2. Install

.. code-block::
   
   sudo apt-get clean all
   sudo apt-get update
   sudo apt-get dist-upgrade |PKG_NAME|


Answer ‘y’ to the first two questions.

.. note::

   Alternator users upgrading from Scylla 4.0 to 4.1 need to set :doc:`default isolation level </upgrade/upgrade-opensource/upgrade-guide-from-4.0-to-4.1/alternator>`.


Start the node
--------------

.. code:: sh

   sudo service scylla-server start

Validate
--------
1. Check cluster status with ``nodetool status`` and make sure **all** nodes, including the one you just upgraded, are in UN status.
2. Use ``curl -X GET "http://localhost:10000/storage_service/scylla_release_version"`` to check the Scylla version.
3. Check scylla-server log (by ``journalctl _COMM=scylla``) and ``/var/log/syslog`` to validate there are no errors.
4. Check again after two minutes to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

See |Scylla_METRICS|_ for more information.

Rollback Procedure
==================

.. include:: /upgrade/_common/warning_rollback.rst

The following procedure describes a rollback from |SCYLLA_NAME| release |NEW_VERSION|.x to |SRC_VERSION|.y. Apply this procedure if an upgrade from |SRC_VERSION| to |NEW_VERSION| failed before completing on all nodes. Use this procedure only for nodes you upgraded to |NEW_VERSION|.

Scylla rollback is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes rollback to |SRC_VERSION|, you will:

* Drain the node and stop Scylla
* Retrieve the old Scylla packages
* Restore the configuration file
* Restore system tables
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
   sudo service scylla-server stop

Download and install the old release
------------------------------------
1. Remove the old repo file.

.. code:: sh

   sudo rm -rf /etc/apt/sources.list.d/scylla.list

2. Update the |SCYLLA_REPO|_ to |SRC_VERSION|
3. Install

.. code-block::
   
   sudo apt-get update
   sudo apt-get remove scylla\* -y
   sudo apt-get install |PKG_NAME|

Answer ‘y’ to the first two questions.

Restore the configuration file
------------------------------
.. code:: sh

   sudo rm -rf /etc/scylla/scylla.yaml
   sudo cp -a /etc/scylla/scylla.yaml.backup-src | /etc/scylla/scylla.yaml

Restore system tables
---------------------

Restore all tables of **system** and **system_schema** from the previous snapshot - |NEW_VERSION| uses a different set of system tables. See :doc:`Restore from a Backup and Incremental Backup </operating-scylla/procedures/backup-restore/restore/>` for reference.

.. code:: sh

    cd /var/lib/scylla/data/keyspace_name/table_name-UUID/snapshots/<snapshot_name>/
    sudo cp -r * /var/lib/scylla/data/keyspace_name/table_name-UUID/
    sudo chown -R scylla:scylla /var/lib/scylla/data/keyspace_name/table_name-UUID/

Reload systemd configuration
----------------------------

It is required to reload the unit file if the systemd unit file is changed.

.. code:: sh

   sudo systemctl daemon-reload

Start the node
--------------

.. code:: sh

   sudo service scylla-server start

Validate
--------
Check the upgrade instructions above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
