Reset Authenticator Password
============================

This procedure describes what to do when a user loses his password and can not reset it with a superuser role. 
The procedure requires cluster downtime and as a result, all of the system_auth data is deleted.

Procedure
.........

| 1. Stop Scylla nodes (**Stop all the nodes in the cluster**).

.. code-block:: shell 

   sudo systemctl stop scylla-server

| 2. Remove your tables under ``/var/lib/scylla/data/system_auth/``.

.. code-block:: shell  

   rm -rf /var/lib/scylla/data/system_auth/

| 3. Start Scylla nodes.

.. code-block:: shell 

   sudo systemctl start scylla-server

| 4. Verify that you can log in to your node using ``cqlsh`` command.
| The access is only possible using Scylla superuser.

.. code-block:: cql
 
   cqlsh -u cassandra -p cassandra

| 5. Recreate the users

.. include:: /troubleshooting/_common/ts-return.rst
