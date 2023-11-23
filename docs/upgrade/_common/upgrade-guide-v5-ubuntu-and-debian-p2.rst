**To upgrade ScyllaDB:**

#. Update the |SCYLLA_REPO|_ to |NEW_VERSION|.

#. Install:

    .. code-block:: console
   
       sudo apt-get clean all
       sudo apt-get update
       sudo apt-get dist-upgrade scylla


Answer ‘y’ to the first two questions.

Start the node
--------------

.. code:: sh

   sudo service scylla-server start

Validate
--------
#. Check cluster status with ``nodetool status`` and make sure **all** nodes, including the one you just upgraded, are in UN status.
#. Use ``curl -X GET "http://localhost:10000/storage_service/scylla_release_version"`` to check the Scylla version.
#. Check scylla-server log (by ``journalctl _COMM=scylla``) and ``/var/log/syslog`` to validate there are no errors.
#. Check again after two minutes to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

See |Scylla_METRICS|_ for more information.

Rollback Procedure
==================

.. include:: /upgrade/_common/warning_rollback.rst

The following procedure describes a rollback from |SCYLLA_NAME| |NEW_VERSION|.x to |SRC_VERSION|.y. Apply this procedure if an upgrade from |SRC_VERSION| to |NEW_VERSION| failed before completing on all nodes. Use this procedure only for nodes you upgraded to |NEW_VERSION|.

ScyllaDB rollback is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes you rollback to |SRC_VERSION|, you will:

* Drain the node and stop Scylla
* Retrieve the old ScyllaDB packages
* Restore the configuration file
* Reload systemd configuration
* Restart ScyllaDB
* Validate the rollback success

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running the old version.

Rollback Steps
==============
Gracefully shutdown ScyllaDB
----------------------------

.. code:: sh

   nodetool drain
   nodetool snapshot
   sudo service scylla-server stop

Restore and install the old release
------------------------------------
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

Restore the configuration file
------------------------------
.. code:: sh

   sudo rm -rf /etc/scylla/scylla.yaml
   sudo cp /etc/scylla/scylla.yaml-backup /etc/scylla/scylla.yaml

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
