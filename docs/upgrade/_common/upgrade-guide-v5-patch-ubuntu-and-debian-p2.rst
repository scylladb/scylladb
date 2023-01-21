**To upgrade ScyllaDB:**

#. Update the |APT|_ to |NEW_VERSION|.
#. Install:

    .. code:: sh

       sudo apt-get update
       sudo apt-get dist-upgrade scylla

    Answer ‘y’ to the first two questions.

Start the node
^^^^^^^^^^^^^^^^

.. code:: sh

   sudo service scylla-server start

Validate
^^^^^^^^^^^^^^^^
#. Check cluster status with ``nodetool status`` and make sure **all** nodes, including the one you just upgraded, are in UN status.
#. Use ``curl -X GET "http://localhost:10000/storage_service/scylla_release_version"`` to check the ScyllaDB version.
#. Check the scylla-server log (execute ``journalctl _COMM=scylla``) and ``/var/log/syslog`` to validate there are no errors.
#. Check again after 2 minutes, to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

Rollback Procedure
-----------------------

.. include:: /upgrade/_common/warning_rollback.rst

The following procedure describes a rollback from ScyllaDB release |TO| to |FROM|. Apply this procedure if an upgrade from |FROM| to |TO| failed before completing on all nodes. Use this procedure only for nodes you upgraded to |TO|.

ScyllaDB rollback is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes rollback to |FROM|, you will:

* Drain the node and stop ScyllaDB.
* Downgrade to previous release.
* Restore the configuration file.
* Restart ScyllaDB.
* Validate the rollback success.

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running with the new version.

Rollback steps
------------------------
Gracefully shutdown ScyllaDB
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: sh

   nodetool drain
   sudo service scylla-server stop

Downgrade to previous release
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Install:

.. code:: sh

   sudo apt-get install scylla=5.x.y\* scylla-server=5.x.y\* scylla-jmx=5.x.y\* scylla-tools=5.x.y\* scylla-tools-core=5.x.y\* scylla-kernel-conf=5.x.y\* scylla-conf=5.x.y\*

Answer ‘y’ to the first two questions.

Restore the configuration file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: sh

   sudo rm -rf /etc/scylla/scylla.yaml
   sudo cp -a /etc/scylla/scylla.yaml.backup-5.x.z /etc/scylla/scylla.yaml

Start the node
^^^^^^^^^^^^^^^^^^^

.. code:: sh

   sudo service scylla-server start

Validate
^^^^^^^^^^^^^^^^^^
Check upgrade instruction above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
