Enable and Disable Authentication Without Downtime
==================================================

Authentication is the process where login accounts and their passwords are verified, and the user is allowed access into the database. Authentication is done internally within ScyllaDB and is not done with a third party. Users and passwords are created with :doc:`roles </operating-scylla/security/authorization>` using a ``CREATE ROLE`` statement. This procedure enables Authentication on the ScyllaDB servers using a transit state, allowing clients to work with or without Authentication at the same time. In this state, you can update the clients (application using ScyllaDB/Apache Cassandra drivers) one at the time. Once all the clients are using Authentication, you can enforce Authentication on all ScyllaDB nodes as well. If you would rather perform a faster authentication procedure where all clients (application using ScyllaDB/Apache Cassandra drivers) will stop working until they are updated to work with Authentication, refer to :doc:`Enable Authentication </operating-scylla/security/runtime-authentication>`.



Enable Authentication Without Downtime
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This procedure allows you to enable authentication on a live ScyllaDB cluster without downtime.

Procedure
---------

#. Update the ``authenticator`` parameter in ``scylla.yaml`` for all the nodes in the cluster: Change ``authenticator: AllowAllAuthenticator`` to ``authenticator: com.scylladb.auth.TransitionalAuthenticator``.

   .. code-block:: yaml

       authenticator:  com.scylladb.auth.TransitionalAuthenticator

#. Run the :doc:`nodetool drain </operating-scylla/nodetool-commands/drain>` command (ScyllaDB stops listening to its connections from the client and other nodes).

#. Restart the nodes one by one to apply the effect.

   .. include:: /rst_include/scylla-commands-restart-index.rst

#. Login with the default superuser credentials and create an authenticated user with strong password.

   For example:

   .. code-block:: cql

       cqlsh -ucassandra -pcassandra

       cassandra@cqlsh> CREATE ROLE scylla WITH PASSWORD = '123456' AND LOGIN = true AND SUPERUSER = true;
       cassandra@cqlsh> LIST ROLES;

       name      |super
       ----------+-------
       cassandra |True
       scylla    |True

   Optionally, assign the role to your user. For example:

   .. code-block:: cql

      cassandra@cqlsh> GRANT scylla TO myuser

#. Login with the new user created and drop the superuser cassandra.

   .. code-block:: cql

      cqlsh -u scylla -p 123456

      scylla@cqlsh> DROP ROLE cassandra;

      scylla@cqlsh> LIST ROLES;

      name      |super
      ----------+-------
      scylla    |True

#. Update the ``authenticator`` parameter in ``scylla.yaml`` for all the nodes in the cluster: Change ``authenticator: com.scylladb.auth.TransitionalAuthenticator`` to ``authenticator: PasswordAuthenticator``.

     .. code-block:: yaml
 
        authenticator: PasswordAuthenticator

#. Restart the nodes one by one to apply the effect.

   .. include:: /rst_include/scylla-commands-restart-index.rst

#. Verify that all the client applications are working correctly with authentication enabled.
                              

Disable Authentication Without Downtime
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This procedure allows you to disable authentication on a live ScyllaDB cluster without downtime. Once disabled, you will have to re-enable authentication where required. 

Procedure
---------

#. Update the ``authenticator`` parameter in ``scylla.yaml`` for all the nodes in the cluster: Change ``authenticator: PasswordAuthenticator`` to ``authenticator: com.scylladb.auth.TransitionalAuthenticator``.

     .. code-block:: yaml

        authenticator: com.scylladb.auth.TransitionalAuthenticator

#. Restart the nodes one by one to apply the effect.

   .. code-block:: shell

      sudo systemctl restart scylla-server

#. Update the ``authenticator`` parameter in ``scylla.yaml`` for all the nodes in the cluster: Change ``authenticator: com.scylladb.auth.TransitionalAuthenticator`` to ``authenticator: AllowAllAuthenticator``.
 
   .. code-block:: yaml

      authenticator: AllowAllAuthenticator

#. Restart the nodes one by one to apply the effect.

   .. include:: /rst_include/scylla-commands-restart-index.rst

#. Verify that all the client applications are working correctly with authentication disabled.

