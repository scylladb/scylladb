# Enable Authorization

Authorization is the process by where users are granted permissions, which entitle them to access or change data on specific keyspaces, tables, or an entire datacenter. Authorization for Scylla is done internally within Scylla and is not done with a third party such as LDAP or OAuth. Granting permissions to users requires the use of a role such as Database Administrator and requires a user who has been [authenticated](https://opensource.docs.scylladb.com/branch-5.4/operating-scylla/security/authentication.md).

Authorization is enabled using the authorizer setting in scylla.yaml. Scylla has two authorizers available:

* `AllowAllAuthorizer` (default setting) - which performs no checking and so effectively grants all permissions to all roles. This must be used if AllowAllAuthenticator is the configured [authenticator](https://opensource.docs.scylladb.com/branch-5.4/operating-scylla/security/authentication.md).
* `CassandraAuthorizer` - which implements permission management functionality and stores its data in Scylla system tables.

#### NOTE
Once Authorization is enabled, **all users must**:

* Have [roles](#roles) and permissions (set by a DBA with [superuser](#superuser) credentials) configured.
* Use a user/password to [connect](#access) to Scylla.

## Enabling Authorization

Permissions are modeled as a whitelist, and as such, a given role has **no access** to **any** database resource, unless specified. The implication of this is that once authorization is enabled on a node, all requests will be rejected until the required permissions have been granted. For this reason, it is strongly recommended to perform the initial setup on a node that is not processing client requests.

The following assumes that Authentication has already been enabled via the process outlined in [Enable Authentication](https://opensource.docs.scylladb.com/branch-5.4/operating-scylla/security/authentication.md). Perform these steps to enable internal authorization across the cluster:

1. Configure the [authorizer]() as CassandraAuthorizer
2. Set your credentials as the [superuser]()
3. Login to cqlsh as the superuser and set [roles]() and privileges for your users
4. Confirm users can [access]() the client with their new credentials.
5. [Remove]() Cassandra default user / passwords

<a id="authorizer"></a>

### Configure the Authorizer

It is highly recommended to perform this action on a node that is not processing client requests.

**Procedure**

1. On the selected node, edit scylla.yaml to change the authorizer option to CassandraAuthorizer:

```yaml
authorizer: CassandraAuthorizer
```

1. Restart the node. This will set the authorization.

Supported OS

```shell
sudo systemctl restart scylla-server
```

Docker

```shell
docker exec -it some-scylla supervisorctl restart scylla
```

(without restarting *some-scylla* container)

<a id="superuser"></a>

### Set a Superuser

The default ScyllaDB superuser role is `cassandra` with password `cassandra`. Using the default
superuser is unsafe and may significantly impact performance.

If you haven’t created a custom superuser while enablint authentication, you should create a custom superuser
before creating additional roles.
See [Creating a Custom Superuser](https://opensource.docs.scylladb.com/branch-5.4/operating-scylla/security/create-superuser.md) for instructions.

#### WARNING
We highly recommend creating a custom superuser to ensure security and avoid performance degradation.

<a id="roles"></a>

### Create Additional Roles

In order for the users on your system to be able to login and perform actions, you as the DBA will have to create roles and privileges.

**Before you Begin**
Validate you have set the authenticator as described in [Authentication](https://opensource.docs.scylladb.com/branch-5.4/operating-scylla/security/authentication.md).
Validate you have the credentials for the superuser for your system for yourself.

1. Open a new cqlsh session using the credentials of a role with [superuser]() credentials. For example:

```none
cqlsh -u dba -p 39fksah!
```

1. Configure the appropriate access privileges for clients using [GRANT PERMISSION](https://opensource.docs.scylladb.com/branch-5.4/operating-scylla/security/authorization.md#grant-permission-statement) statements.  For additional examples, consult the [RBAC example](https://opensource.docs.scylladb.com/branch-5.4/operating-scylla/security/rbac-usecase.md).

In this example, you are creating a user (`db_user`) who can access with password (`password`). You are also granting `db_user` with the role named `client` who has SELECT permissions on the ks.t1 table.

```cql
CREATE ROLE db_user WITH PASSWORD = 'password' AND LOGIN = true;
CREATE ROLE client;
GRANT SELECT ON ks.t1 TO client;
GRANT client TO db_user;
```

1. Continue in this manner to grant permissions for all users.

<a id="access"></a>

### Clients Resume Access with New Permissions

1. Restart Scylla. As each node restarts and clients reconnect, the enforcement of the granted permissions will begin.

Supported OS

```shell
sudo systemctl restart scylla-server
```

Docker

```shell
docker exec -it some-scylla supervisorctl restart scylla
```

(without restarting *some-scylla* container)

The following should be noted:

* Clients are not able to connect until you setup roles as users with passwords  using [GRANT PERMISSION](https://opensource.docs.scylladb.com/branch-5.4/operating-scylla/security/authorization.md#grant-permission-statement) statements (using the superuser). Refer to the example in [Role Based Access Control (RBAC)](https://opensource.docs.scylladb.com/branch-5.4/operating-scylla/security/rbac-usecase.md) for details.
* When initiating a connection, clients will need to use the user name and password that you assign
* Confirm all clients can connect before removing the Cassandra default password and user.

1. To remove permission from any role or user, see [REVOKE PERMISSION](https://opensource.docs.scylladb.com/branch-5.4/operating-scylla/security/authorization.md#revoke-permission-statement).

<a id="remove"></a>

### Remove Cassandra Default Password and User

To prevent others from entering with the old superuser password, you can and should delete it.

```cql
DROP ROLE [ IF EXISTS ] 'old-username';
```

For example

```cql
DROP ROLE [ IF EXISTS ] 'cassandra';
```

## Additional References

* [Role Based Access Control (RBAC)](https://opensource.docs.scylladb.com/branch-5.4/operating-scylla/security/rbac-usecase.md)
* [Authorization](https://opensource.docs.scylladb.com/branch-5.4/operating-scylla/security/authorization.md) - CQL Reference for authorizing users
* [Authentication](https://opensource.docs.scylladb.com/branch-5.4/operating-scylla/security/authentication.md) - Enable Authentication
