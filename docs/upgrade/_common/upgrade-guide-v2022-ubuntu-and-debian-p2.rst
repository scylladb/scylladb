Start the node
--------------

A new io.conf format was introduced in Scylla 2.3 and 2019.1. If your io.conf doesn't contain `--io-properties-file` option, then it's still the old format. You need to re-run the io setup to generate new io.conf.

.. code:: sh

    sudo scylla_io_setup

.. code:: sh

   sudo service scylla-enterprise-server start

Validate
--------
#. Check cluster status with ``nodetool status`` and make sure **all** nodes, including the one you just upgraded, are in UN status.
#. Use ``curl -X GET "http://localhost:10000/storage_service/scylla_release_version"`` to check the ScyllaDB version.
#. Check scylla-enterprise-server log (by ``journalctl _COMM=scylla``) and ``/var/log/syslog`` to validate there are no errors.
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
   sudo service scylla-enterprise-server stop

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

   sudo service scylla-enterprise-server start

Validate
--------
Check the upgrade instructions above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
