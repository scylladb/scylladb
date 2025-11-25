================================
Creating a Superuser
================================

There is no default superuser role in ScyllaDB.
Users with a superuser role have full access to the database and can run 
any CQL command on the database resources.

There are three ways you can create a superuser in ScyllaDB:

- :ref:`Using an existing superuser account to create a new superuser role <create-superuser-using-existing-superuser>`
- :ref:`Configure the initial custom superuser in the scylla.yaml configuration file <create-superuser-in-config-file>`
- :ref:`Using the ScyllaDB Maintenance Socket to create a superuser role <create-superuser-using-maintenance-socket>`


.. _create-superuser-using-existing-superuser:

Setting Up a Superuser Using an Existing Superuser Account
-------------------------------------------------------------

To create a superuser using an existing superuser account, you should:

1. Log in to cqlsh using an existing superuser account.

.. code-block:: shell

   cqlsh -u existing_superuser -p existing_superuser_password

2. Create a custom superuser.

.. code-block:: cql

   CREATE ROLE <custom_superuser>  WITH SUPERUSER = true AND LOGIN = true and PASSWORD = '<custom_superuser_password>';

3. Verify that you can log in to your node using ``cqlsh`` command with the new password.

.. code-block:: shell

   cqlsh -u <custom_superuser> -p <custom_superuser_password>

4. Show all the roles to verify that the new superuser was created:

.. code-block:: cql

   LIST ROLES;


.. _create-superuser-in-config-file:

Setting Custom Superuser Credentials in scylla.yaml
------------------------------------------------------

To create a superuser using the scylla.yaml configuration file, you can configure
the custom superuser name and password in the ``scylla.yaml`` configuration file:

.. code-block:: yaml
   
   auth_superuser_name: <superuser name>
   auth_superuser_salted_password: <superuser salted password - cleartext is not allowed>

To generate a salted password, you can use CLI tools such as ``mkpasswd``:

.. code-block:: shell

   mkpasswd -m SHA-512

.. caution::

    The superuser credentials in the ``scylla.yaml`` file will be ignored if any superuser with `can_login` set to true already exists in the cluster, no matter who and when created it.


.. _create-superuser-using-maintenance-socket:

Setting Up a Superuser Using the Maintenance Socket
------------------------------------------------------

If no superuser account exists in the cluster, you can create a superuser using the ScyllaDB Maintenance Socket.
In order to do that, the node must have the maintenance socket enabled.
See :doc:`Admin Tools: Maintenance Socket </operating-scylla/admin-tools/maintenance-socket/>`.

To create a superuser using the maintenance socket, you should:

1. Connect to the node using ``cqlsh`` over the maintenance socket.

.. code-block:: shell

   cqlsh <maintenance_socket_path>

Replace ``<maintenance_socket_path>`` with the socket path configured in ``scylla.yaml``.

2. Create new superuser role using ``CREATE ROLE`` command.

.. code-block:: cql

   CREATE ROLE <custom_superuser>  WITH SUPERUSER = true AND LOGIN = true and PASSWORD = '<custom_superuser_password>';

3. Verify that you can log in to your node using ``cqlsh`` command with the new password.

.. code-block:: shell

   cqlsh -u <custom_superuser> -p <custom_superuser_password>

4. Show all the roles to verify that the new superuser was created:

.. code-block:: cql

   LIST ROLES;

