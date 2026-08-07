Enable Authentication
=====================

Authentication is the process where login accounts and their passwords are verified, and the user is allowed access to the database. Authentication is done internally within Scylla and is not done with a third party. Users and passwords are created with roles using a ``CREATE ROLE`` statement. Refer to :doc:`Grant Authorization CQL Reference </operating-scylla/security/authorization>` for details.  

The procedure described below enables Authentication on the Scylla servers. It is intended to be used when you do **not** have applications running with Scylla/Cassandra drivers.

.. warning:: Once you enable authentication, all clients (such as applications using Scylla/Apache Cassandra drivers) will **stop working** until they are updated or reconfigured to work with authentication.

If this downtime is not an option, you can follow the instructions in :doc:`Enable and Disable Authentication Without Downtime </operating-scylla/security/runtime-authentication>`, which using a transient state, allows clients to work with or without Authentication at the same time. In this state, you can update the clients (application using Scylla/Apache Cassandra drivers) one at the time. Once all the clients are using Authentication, you can enforce Authentication on all Scylla nodes as well.

Prerequisites
--------------
Set the ``system_auth`` keyspace replication factor to the number of nodes in the datacenter via cqlsh. It allows you to ensure that 
the user's information is kept highly available for the cluster. If ``system_auth`` is not equal to the number of nodes
and a node fails, the user whose information is on that node will be denied access.

For **production environments** use only ``NetworkTopologyStrategy``.

* Single DC (SimpleStrategy)

.. code-block:: cql

   ALTER KEYSPACE system_auth WITH REPLICATION =
     { 'class' : 'SimpleStrategy', 'replication_factor' : <new_rf> };

For example:

.. code-block:: cql

   ALTER KEYSPACE system_auth WITH REPLICATION =
     { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

* Multi - DC (NetworkTopologyStrategy)

.. code-block:: cql

   ALTER KEYSPACE system_auth WITH REPLICATION =
     {'class' : 'NetworkTopologyStrategy', '<name of DC 1>' : <new RF>, '<name of DC 2>' : <new RF>};

For example:

.. code-block:: cql

   ALTER KEYSPACE system_auth WITH REPLICATION =
     {'class' : 'NetworkTopologyStrategy', 'dc1' : 3, 'dc2' : 3};

The names of the DCs must match the datacenter names specified in the rack & DC configuration file: ``/etc/scylla/cassandra-rackdc.properties``.


Procedure
----------

#. For each Scylla node in the cluster, edit the ``/etc/scylla/scylla.yaml`` file to change the ``authenticator`` parameter from ``AllowAllAuthenticator`` to ``PasswordAuthenticator``.

   .. code-block:: yaml

       authenticator: PasswordAuthenticator


#. Restart  Scylla.

    .. include:: /rst_include/scylla-commands-restart-index.rst

#. Start cqlsh with the default superuser username and password. 

    .. code-block:: cql

       cqlsh -u cassandra -p cassandra

   .. warning::

      Before proceeding  to the next step, we highly recommend creating a custom superuser 
      to ensure security and prevent performance degradation.
      See :doc:`Creating a Custom Superuser </operating-scylla/security/create-superuser/>` for instructions.

#. Run a repair on the ``system_auth`` keyspace on **all** the nodes in the cluster.
	
    For example:
	
    .. code-block:: none
	
       nodetool repair -pr system_auth

6. If you want to create users and roles, continue to :doc:`Enable Authorization </operating-scylla/security/enable-authorization>`.


Additional Resources
--------------------

* :doc:`Enable and Disable Authentication Without Downtime </operating-scylla/security/runtime-authentication/>`
* :doc:`Enable Authorization </operating-scylla/security/enable-authorization/>` 
* :doc:`Authorization </operating-scylla/security/authorization/>` 



