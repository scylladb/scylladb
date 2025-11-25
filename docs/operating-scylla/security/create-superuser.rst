================================
Creating a Superuser
================================

There is no default superuser role in ScyllaDB.
Users with a superuser role have full access to the database and can run 
any CQL command on the database resources.

There are two ways you can create a superuser in ScyllaDB:

- :ref:`Using the ScyllaDB Maintenance Socket to create a superuser role <create-superuser-using-maintenance-socket>`
- :ref:`Using an existing superuser account to create a new superuser role <create-superuser-using-existing-superuser>`

When setting up a new cluster, use the maintenance socket approach to create the first superuser.


.. _create-superuser-using-maintenance-socket:

Setting Up a Superuser Using the Maintenance Socket
------------------------------------------------------

If no superuser account exists in the cluster, which is the case for new clusters, you can create a superuser using the ScyllaDB Maintenance Socket.
In order to do that, the node must have the maintenance socket enabled.
See :doc:`Admin Tools: Maintenance Socket </operating-scylla/admin-tools/maintenance-socket/>`.

To create a superuser using the maintenance socket, you should:

1. Connect to the node using ``cqlsh`` over the maintenance socket.

.. code-block:: shell

   cqlsh <maintenance_socket_path>

Replace ``<maintenance_socket_path>`` with the socket path configured in ``scylla.yaml``.

2. Create new superuser role using ``CREATE ROLE`` command.

.. code-block:: cql

   CREATE ROLE <new_superuser>  WITH SUPERUSER = true AND LOGIN = true and PASSWORD = '<new_superuser_password>';

3. Verify that you can log in to your node using ``cqlsh`` command with the new password.

.. code-block:: shell

   cqlsh -u <new_superuser>
   Password: 

.. note::

   Enter the value of `<new_superuser_password>` password when prompted. The input is not displayed.

4. Show all the roles to verify that the new superuser was created:

.. code-block:: cql

   LIST ROLES;


.. _create-superuser-using-existing-superuser:

Setting Up a Superuser Using an Existing Superuser Account
-------------------------------------------------------------

To create a superuser using an existing superuser account, you should:

1. Log in to cqlsh using an existing superuser account.

.. code-block:: shell

   cqlsh -u existing_superuser -p existing_superuser_password

2. Create a new superuser.

.. code-block:: cql

   CREATE ROLE <new_superuser>  WITH SUPERUSER = true AND LOGIN = true and PASSWORD = '<new_superuser_password>';

3. Verify that you can log in to your node using ``cqlsh`` command with the new password.

.. code-block:: shell

   cqlsh -u <new_superuser>
   Password: 

.. note::

   Enter the value of `<new_superuser_password>` password when prompted. The input is not displayed.

4. Show all the roles to verify that the new superuser was created:

.. code-block:: cql

   LIST ROLES;

