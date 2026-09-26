# Enable and Disable Authentication Without Downtime

#### NOTE
If you upgraded your cluster from version 5.4, see [After Upgrading from 5.4](#runtime-authentication-upgrade-info).

Authentication is the process where login accounts and their passwords are verified, and the user is allowed access into the database. Authentication is done internally within Scylla and is not done with a third party. Users and passwords are created with [roles](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/security/authorization.md) using a `CREATE ROLE` statement. This procedure enables Authentication on the Scylla servers using a transit state, allowing clients to work with or without Authentication at the same time. In this state, you can update the clients (application using Scylla/Apache Cassandra drivers) one at the time. Once all the clients are using Authentication, you can enforce Authentication on all Scylla nodes as well. If you would rather perform a faster authentication procedure where all clients (application using Scylla/Apache Cassandra drivers) will stop working until they are updated to work with Authentication, refer to [Enable Authentication](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/security/runtime-authentication.md).

## Enable Authentication Without Downtime

This procedure allows you to enable authentication on a live Scylla cluster without downtime.

### Procedure

1. Update the `authenticator` parameter in `scylla.yaml` for all the nodes in the cluster: Change `authenticator: AllowAllAuthenticator` to `authenticator: com.scylladb.auth.TransitionalAuthenticator`.
   ```yaml
   authenticator:  com.scylladb.auth.TransitionalAuthenticator
   ```
2. Run the [nodetool drain](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/nodetool-commands/drain.md) command (Scylla stops listening to its connections from the client and other nodes).
3. Restart the nodes one by one to apply the effect.

   Supported OS
   ```shell
   sudo systemctl restart scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl restart scylla
   ```

   (without restarting *some-scylla* container)
4. Login with the default superuser credentials and create an authenticated user with strong password.

   For example:
   ```cql
   cqlsh -ucassandra -pcassandra

   cassandra@cqlsh> CREATE ROLE scylla WITH PASSWORD = '123456' AND LOGIN = true AND SUPERUSER = true;
   cassandra@cqlsh> LIST ROLES;

   name      |super
   ----------+-------
   cassandra |True
   scylla    |True
   ```

   Optionally, assign the role to your user. For example:
   ```cql
   cassandra@cqlsh> GRANT scylla TO myuser
   ```
5. Login with the new user created and drop the superuser cassandra.
   ```cql
   cqlsh -u scylla -p 123456

   scylla@cqlsh> DROP ROLE cassandra;

   scylla@cqlsh> LIST ROLES;

   name      |super
   ----------+-------
   scylla    |True
   ```
6. Update the `authenticator` parameter in `scylla.yaml` for all the nodes in the cluster: Change `authenticator: com.scylladb.auth.TransitionalAuthenticator` to `authenticator: PasswordAuthenticator`.
   > ```yaml
   > authenticator: PasswordAuthenticator
   > ```
7. Restart the nodes one by one to apply the effect.

   Supported OS
   ```shell
   sudo systemctl restart scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl restart scylla
   ```

   (without restarting *some-scylla* container)
8. Verify that all the client applications are working correctly with authentication enabled.

## Disable Authentication Without Downtime

This procedure allows you to disable authentication on a live Scylla cluster without downtime. Once disabled, you will have to re-enable authentication where required.

### Procedure

1. Update the `authenticator` parameter in `scylla.yaml` for all the nodes in the cluster: Change `authenticator: PasswordAuthenticator` to `authenticator: com.scylladb.auth.TransitionalAuthenticator`.
   > ```yaml
   > authenticator: com.scylladb.auth.TransitionalAuthenticator
   > ```
2. Restart the nodes one by one to apply the effect.
   ```shell
   sudo systemctl restart scylla-server
   ```
3. Update the `authenticator` parameter in `scylla.yaml` for all the nodes in the cluster: Change `authenticator: com.scylladb.auth.TransitionalAuthenticator` to `authenticator: AllowAllAuthenticator`.
   ```yaml
   authenticator: AllowAllAuthenticator
   ```
4. Restart the nodes one by one to apply the effect.

   Supported OS
   ```shell
   sudo systemctl restart scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl restart scylla
   ```

   (without restarting *some-scylla* container)
5. Verify that all the client applications are working correctly with authentication disabled.

<a id="runtime-authentication-upgrade-info"></a>

## After Upgrading from 5.4

The procedures described above apply to clusters where consistent topology updates
are enabled. The feature is automatically enabled in new clusters.

If you’ve upgraded an existing cluster from version 5.4, ensure that you
[manually enabled consistent topology updates](https://opensource.docs.scylladb.com/branch-6.0/upgrade/upgrade-opensource/upgrade-guide-from-5.4-to-6.0/enable-consistent-topology.md).
Without consistent topology updates enabled, you must take additional steps
to enable or disable authentication without downtime:

* Before you enable authentication without downtime, set the `system_auth`
  keyspace replication factor to the number of nodes in the datacenter via cqlsh.
  It allows you to ensure that the user’s information is kept highly available
  for the cluster. If `system_auth` is not equal to the number of nodes and
  a node fails, the user whose information is on that node will be denied access.
* After you restart the nodes when you enable or disable authentication without
  downtime, run repair on the `system_auth` keyspace, one node at a time on
  all the nodes in the cluster.
