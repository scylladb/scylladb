Enable Authentication
=====================

Authentication is the process where login accounts and their passwords are verified, and the user is allowed access to the database. Authentication is done internally within ScyllaDB and is not done with a third party. Users and passwords are created with roles using a ``CREATE ROLE`` statement. Refer to :doc:`Grant Authorization CQL Reference </operating-scylla/security/authorization>` for details.  

The procedure described below enables Authentication on the ScyllaDB servers. It is intended to be used when you do **not** have applications running with ScyllaDB/Cassandra drivers.

.. warning:: Once you enable authentication, all clients (such as applications using ScyllaDB/Apache Cassandra drivers) will **stop working** until they are updated or reconfigured to work with authentication.

If this downtime is not an option, you can follow the instructions in :doc:`Enable and Disable Authentication Without Downtime </operating-scylla/security/runtime-authentication>`, which using a transient state, allows clients to work with or without Authentication at the same time. In this state, you can update the clients (application using ScyllaDB/Apache Cassandra drivers) one at the time. Once all the clients are using Authentication, you can enforce Authentication on all ScyllaDB nodes as well.

Procedure
----------

#. For each ScyllaDB node in the cluster, edit the ``/etc/scylla/scylla.yaml`` file to change the ``authenticator`` parameter from ``AllowAllAuthenticator`` to ``PasswordAuthenticator``.

   .. code-block:: yaml

       authenticator: PasswordAuthenticator


#. Restart  ScyllaDB.

    .. include:: /rst_include/scylla-commands-restart-index.rst

#. Start cqlsh with the default superuser username and password. 

    .. code-block:: cql

       cqlsh -u cassandra -p cassandra

   .. note::

      Before proceeding  to the next step, we recommend creating a custom superuser
      to improve security.
      See :doc:`Creating a Custom Superuser </operating-scylla/security/create-superuser/>` for instructions.

#. If you want to create users and roles, continue to :doc:`Enable Authorization </operating-scylla/security/enable-authorization>`.

Additional Resources
--------------------

* :doc:`Enable and Disable Authentication Without Downtime </operating-scylla/security/runtime-authentication/>`
* :doc:`Enable Authorization </operating-scylla/security/enable-authorization/>` 
* :doc:`Authorization </operating-scylla/security/authorization/>` 



