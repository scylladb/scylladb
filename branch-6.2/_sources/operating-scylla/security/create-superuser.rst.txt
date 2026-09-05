================================
Creating a Custom Superuser
================================

The default ScyllaDB superuser role is ``cassandra`` with password ``cassandra``. 
Users with the ``cassandra`` role have full access to the database and can run 
any CQL command on the database resources.

To improve security, we recommend creating a custom superuser. You should:

#. Use the default ``cassandra`` superuser to log in.
#. Create a custom superuser.
#. Log in as the custom superuser.
#. Remove the ``cassandra`` role.

In the above procedure, you only need to use the ``cassandra`` superuser once, during 
the initial RBAC set up. 
To completely eliminate the need to use ``cassandra``, you can :ref:`configure the initial 
custom superuser in the scylla.yaml configuration file <create-superuser-in-config-file>`. 

.. _create-superuser-procedure:

Procedure
-----------

#. Start cqlsh with the default superuser settings:

   .. code::

    cqlsh -u cassandra -p cassandra

#. Create a new superuser:

   .. code::

    CREATE ROLE <custom_superuser name>  WITH SUPERUSER = true AND LOGIN = true and PASSWORD = '<custom_superuser_password>';

   For example:

   .. code::
    :class: hide-copy-button

    CREATE ROLE dba WITH SUPERUSER = true AND LOGIN = true and PASSWORD = '39fksah!';

   .. warning::
    
    You must set a PASSWORD when creating a role with LOGIN privileges. 
    Otherwise, you will not be able to log in to the database using that role.

#. Exit cqlsh:

   .. code::

    EXIT;

#. Log in as the new superuser:

   .. code::

    cqlsh -u <custom_superuser name> -p <custom_superuser_password>

   For example:

   .. code::
    :class: hide-copy-button

    cqlsh -u dba -p 39fksah!

#. Show all the roles to verify that the new superuser was created:

   .. code::

    LIST ROLES;

#. Remove the cassandra superuser:

   .. code::

    DROP ROLE cassandra;

#. Show all the roles to verify that the cassandra role was deleted:

   .. code::

    LIST ROLES;

.. _create-superuser-in-config-file:

Setting Custom Superuser Credentials in scylla.yaml
------------------------------------------------------

Operating ScyllaDB using the default superuser ``cassandra`` with password ``cassandra`` 
is insecure and impacts performance. For this reason, the default should be used only once - 
to create a custom superuser role, following the CQL :ref:`procedure <create-superuser-procedure>` above. 

To avoid executing with the default credentials for the period before you can make 
the CQL modifications, you can configure the custom superuser name and password
in the ``scylla.yaml`` configuration file:

.. code-block:: yaml
   
   auth_superuser_name: <superuser name>
   auth_superuser_salted_password: <superuser salted password as processed by mkpassword or similar - cleartext is not allowed>

.. caution::

    The superuser credentials in the ``scylla.yaml`` file will be ignored:

    * If any superuser other than ``cassandra`` is already defined in the cluster.
    * After you create a custom superuser with the CQL :ref:`procedure <create-superuser-procedure>`.

