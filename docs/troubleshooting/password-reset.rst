Reset Authenticator Password
============================

This procedure describes what to do when a user loses their password and can not reset it with a superuser role.
If a node has maintenance socket available, there is no node or cluster downtime required.
If a node does not have maintenance socket available, node has to be restarted with maintenance socket enabled, but no cluster downtime is required.


Prerequisites
.............

Enable maintenance socket on the node. If already done, skip to the ``Procedure`` section.
To check if the maintenance socket is enabled, see :doc:`Admin Tools: Maintenance Socket </operating-scylla/admin-tools/maintenance-socket/>` for details.

1. Stop the ScyllaDB node.

.. code-block:: shell

   sudo systemctl stop scylla-server

2. Edit ``/etc/scylla/scylla.yaml`` file and configure the maintenance socket.
   See :doc:`Admin Tools: Maintenance Socket </operating-scylla/admin-tools/maintenance-socket/>` for details.

3. Start the ScyllaDB node.

.. code-block:: shell

   sudo systemctl start scylla-server


Procedure
.........

1. Connect to the node using ``cqlsh`` over the maintenance socket.

.. code-block:: shell

   cqlsh <maintenance_socket_path>

Replace ``<maintenance_socket_path>`` with the socket path configured in ``scylla.yaml``.

2. Reset the password for the user using ``ALTER ROLE`` command.

.. code-block:: cql

   ALTER ROLE username WITH PASSWORD '<new_password>';

3. Verify that you can log in to your node using ``cqlsh`` command with the new password.

.. code-block:: shell

   cqlsh -u username
   Password: 

.. note::

   Enter the value of `<new_password>` password when prompted. The input is not displayed.

.. include:: /troubleshooting/_common/ts-return.rst
