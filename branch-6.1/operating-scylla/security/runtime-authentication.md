# Enable and Disable Authentication Without Downtime

Authentication is the process where login accounts and their passwords are verified, and the user is allowed access into the database. Authentication is done internally within ScyllaDB and is not done with a third party. Users and passwords are created with [roles](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/security/authorization.md) using a `CREATE ROLE` statement. This procedure enables Authentication on the ScyllaDB servers using a transit state, allowing clients to work with or without Authentication at the same time. In this state, you can update the clients (application using ScyllaDB/Apache Cassandra drivers) one at the time. Once all the clients are using Authentication, you can enforce Authentication on all ScyllaDB nodes as well. If you would rather perform a faster authentication procedure where all clients (application using ScyllaDB/Apache Cassandra drivers) will stop working until they are updated to work with Authentication, refer to [Enable Authentication](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/security/runtime-authentication.md).

## Enable Authentication Without Downtime

This procedure allows you to enable authentication on a live ScyllaDB cluster without downtime.

### Procedure

1. Update the `authenticator` parameter in `scylla.yaml` for all the nodes in the cluster: Change `authenticator: AllowAllAuthenticator` to `authenticator: com.scylladb.auth.TransitionalAuthenticator`.
   ```yaml
   authenticator:  com.scylladb.auth.TransitionalAuthenticator
   ```
2. Run the [nodetool drain](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/nodetool-commands/drain.md) command (ScyllaDB stops listening to its connections from the client and other nodes).
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

This procedure allows you to disable authentication on a live ScyllaDB cluster without downtime. Once disabled, you will have to re-enable authentication where required.

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
