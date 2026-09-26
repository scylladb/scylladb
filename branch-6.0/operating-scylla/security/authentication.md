# Enable Authentication

#### NOTE
If you upgraded your cluster from version 5.4, see [After Upgrading from 5.4](#authentication-upgrade-info).

Authentication is the process where login accounts and their passwords are verified, and the user is allowed access to the database. Authentication is done internally within Scylla and is not done with a third party. Users and passwords are created with roles using a `CREATE ROLE` statement. Refer to [Grant Authorization CQL Reference](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/security/authorization.md) for details.

The procedure described below enables Authentication on the Scylla servers. It is intended to be used when you do **not** have applications running with Scylla/Cassandra drivers.

#### WARNING
Once you enable authentication, all clients (such as applications using Scylla/Apache Cassandra drivers) will **stop working** until they are updated or reconfigured to work with authentication.

If this downtime is not an option, you can follow the instructions in [Enable and Disable Authentication Without Downtime](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/security/runtime-authentication.md), which using a transient state, allows clients to work with or without Authentication at the same time. In this state, you can update the clients (application using Scylla/Apache Cassandra drivers) one at the time. Once all the clients are using Authentication, you can enforce Authentication on all Scylla nodes as well.

## Procedure

1. For each Scylla node in the cluster, edit the `/etc/scylla/scylla.yaml` file to change the `authenticator` parameter from `AllowAllAuthenticator` to `PasswordAuthenticator`.
   ```yaml
   authenticator: PasswordAuthenticator
   ```
2. Restart  Scylla.
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
   See [Creating a Custom Superuser](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/security/create-superuser.md) for instructions.
4. If you want to create users and roles, continue to [Enable Authorization](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/security/enable-authorization.md).

<a id="authentication-upgrade-info"></a>

## After Upgrading from 5.4

The procedure described above applies to clusters where consistent topology updates
are enabled. The feature is automatically enabled in new clusters.

If you’ve upgraded an existing cluster from version 5.4, ensure that you
[manually enabled consistent topology updates](https://opensource.docs.scylladb.com/branch-6.0/upgrade/upgrade-opensource/upgrade-guide-from-5.4-to-6.0/enable-consistent-topology.md).
Without consistent topology updates enabled, you must take additional steps
to enable authentication:

* Before you start the procedure, set the `system_auth` keyspace replication factor
  to the number of nodes in the datacenter via cqlsh. It allows you to ensure that
  the user’s information is kept highly available for the cluster. If `system_auth`
  is not equal to the number of nodes and a node fails, the user whose information
  is on that node will be denied access.
* After you start cqlsh with the default superuser username and password, run
  a repair on the `system_auth` keyspace on all the nodes in the cluster, for example:
  `nodetool repair -pr system_auth`

## Additional Resources

* [Enable and Disable Authentication Without Downtime](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/security/runtime-authentication.md)
* [Enable Authorization](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/security/enable-authorization.md)
* [Authorization](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/security/authorization.md)
