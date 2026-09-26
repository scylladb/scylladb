# Enable Authentication

Authentication is the process where login accounts and their passwords are verified, and the user is allowed access to the database. Authentication is done internally within ScyllaDB and is not done with a third party. Users and passwords are created with roles using a `CREATE ROLE` statement. Refer to [Grant Authorization CQL Reference](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/security/authorization.md) for details.

The procedure described below enables Authentication on the ScyllaDB servers. It is intended to be used when you do **not** have applications running with ScyllaDB/Cassandra drivers.

#### WARNING
Once you enable authentication, all clients (such as applications using ScyllaDB/Apache Cassandra drivers) will **stop working** until they are updated or reconfigured to work with authentication.

If this downtime is not an option, you can follow the instructions in [Enable and Disable Authentication Without Downtime](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/security/runtime-authentication.md), which using a transient state, allows clients to work with or without Authentication at the same time. In this state, you can update the clients (application using ScyllaDB/Apache Cassandra drivers) one at the time. Once all the clients are using Authentication, you can enforce Authentication on all ScyllaDB nodes as well.

## Procedure

1. For each ScyllaDB node in the cluster, edit the `/etc/scylla/scylla.yaml` file to change the `authenticator` parameter from `AllowAllAuthenticator` to `PasswordAuthenticator`.
   ```yaml
   authenticator: PasswordAuthenticator
   ```
2. Restart  ScyllaDB.
   > Supported OS
   > ```shell
   > sudo systemctl restart scylla-server
   > ```

   > Docker
   > ```shell
   > docker exec -it some-scylla supervisorctl restart scylla
   > ```

   > (without restarting *some-scylla* container)
3. Start cqlsh with the default superuser username and password.
   > ```cql
   > cqlsh -u cassandra -p cassandra
   > ```

   #### NOTE
   Before proceeding  to the next step, we recommend creating a custom superuser
   to improve security.
   See [Creating a Custom Superuser](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/security/create-superuser.md) for instructions.
4. If you want to create users and roles, continue to [Enable Authorization](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/security/enable-authorization.md).

## Additional Resources

* [Enable and Disable Authentication Without Downtime](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/security/runtime-authentication.md)
* [Enable Authorization](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/security/enable-authorization.md)
* [Authorization](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/security/authorization.md)
