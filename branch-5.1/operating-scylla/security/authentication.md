# Enable Authentication

Authentication is the process where login accounts and their passwords are verified, and the user is allowed access to the database. Authentication is done internally within Scylla and is not done with a third party. Users and passwords are created with roles using a `CREATE ROLE` statement. Refer to [Grant Authorization CQL Reference](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/authorization.md) for details.

The procedure described below enables Authentication on the Scylla servers. It is intended to be used when you do **not** have applications running with Scylla/Cassandra drivers.

#### WARNING
Once you enable authentication, all clients (such as applications using Scylla/Apache Cassandra drivers) will **stop working** until they are updated or reconfigured to work with authentication.

If this downtime is not an option, you can follow the instructions in [Enable and Disable Authentication Without Downtime](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/runtime-authentication.md), which using a transient state, allows clients to work with or without Authentication at the same time. In this state, you can update the clients (application using Scylla/Apache Cassandra drivers) one at the time. Once all the clients are using Authentication, you can enforce Authentication on all Scylla nodes as well.

## Prerequisites

Set the `system_auth` keyspace replication factor to the number of nodes in the datacenter via cqlsh. It allows you to ensure that
the user’s information is kept highly available for the cluster. If `system_auth` is not equal to the number of nodes
and a node fails, the user whose information is on that node will be denied access.

For **production environments** use only `NetworkTopologyStrategy`.

* Single DC (SimpleStrategy)

```cql
ALTER KEYSPACE system_auth WITH REPLICATION =
  { 'class' : 'SimpleStrategy', 'replication_factor' : <new_rf> };
```

For example:

```cql
ALTER KEYSPACE system_auth WITH REPLICATION =
  { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
```

* Multi - DC (NetworkTopologyStrategy)

```cql
ALTER KEYSPACE system_auth WITH REPLICATION =
  {'class' : 'NetworkTopologyStrategy', '<name of DC 1>' : <new RF>, '<name of DC 2>' : <new RF>};
```

For example:

```cql
ALTER KEYSPACE system_auth WITH REPLICATION =
  {'class' : 'NetworkTopologyStrategy', 'dc1' : 3, 'dc2' : 3};
```

The names of the DCs must match the datacenter names specified in the rack & DC configuration file: `/etc/scylla/cassandra-rackdc.properties`.

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
3. Start cqlsh with the default superuser username and password. The default username is `cassandra`, the default password is `cassandra`. You can change it later if you are enabling authorization.
   > ```cql
   > cqlsh -u cassandra -p cassandra
   > ```
4. Run a repair on the `system_auth` keyspace on **all** the nodes in the cluster.
   > For example:
   > ```none
   > nodetool repair system_auth
   > ```

1. If you want to create users and roles, continue to [Enable Authorization](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/enable-authorization.md).

## Additional Resources

* [Enable and Disable Authentication Without Downtime](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/runtime-authentication.md)
* [Enable Authorization](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/enable-authorization.md)
* [Authorization](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/authorization.md)
