# Enable Authorization

Authorization is the process by where users are granted permissions, which entitle them to access or change data on specific keyspaces, tables, or an entire datacenter. Authorization for Scylla is done internally within Scylla and is not done with a third party such as LDAP or OAuth. Granting permissions to users requires the use of a role such as Database Administrator and requires a user who has been [authenticated](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/authentication.md).

Authorization is enabled using the authorizer setting in scylla.yaml. Scylla has two authorizers available:

* `AllowAllAuthorizer` (default setting) - which performs no checking and so effectively grants all permissions to all roles. This must be used if AllowAllAuthenticator is the configured [authenticator](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/authentication.md).
* `CassandraAuthorizer` - which implements permission management functionality and stores its data in Scylla system tables.

#### NOTE
Once Authorization is enabled, **all users must**:

* Have [roles](#roles) and permissions (set by a DBA with [superuser](#superuser) credentials) configured.
* Use a user/password to [connect](#access) to Scylla.

## Enabling Authorization

Permissions are modeled as a whitelist, and as such, a given role has **no access** to **any** database resource, unless specified. The implication of this is that once authorization is enabled on a node, all requests will be rejected until the required permissions have been granted. For this reason, it is strongly recommended to perform the initial setup on a node that is not processing client requests.

The following assumes that Authentication has already been enabled via the process outlined in [Enable Authentication](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/authentication.md). Perform these steps to enable internal authorization across the cluster:

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

By default, the superuser credentials are username cassandra, password cassandra. This is not secure. It is highly advised to change this to a unique username and password combination.

**Procedure**

1. Start cqlsh with the default superuser settings.

```cql
cqlsh -u cassandra -p cassandra
```

#### NOTE
The cassandra user is special. When you try to login with this username, it is required to usen EACH_QUORUM consistency level(CL) for replies. On the other hand, your own user requires LOCAL_ONE consistency level.
This can be a problematic in certain situations, such as adding or removing DCs. In such cases the cassandra user won’t be able to login.
Creating a superuser role and assigning yourself to the role is definitely the best way forward. Refer to [RBAC](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/rbac-usecase.md) for an example of how to create roles and refer to [Grant Authorization](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/authorization.md) for information on using the grant clause.

1. Create a role for the superuser which has all privileges

```cql
CREATE ROLE <role-name> WITH SUPERUSER = true;
```

```cql
CREATE ROLE DBA WITH SUPERUSER = true;
```

#### NOTE
This role already has complete read and write permissions on all tables and keyspaces and does not need to be granted anything else. The superuser permission setting is by default, disabled. Only for the administrator does it need to be enabled.

1. Assign that role to yourself and grant login privileges

```cql
CREATE ROLE <user> WITH PASSWORD = 'password' AND SUPERUSER = true AND LOGIN = true;
```

#### WARNING
It is **highly** recommended to set a password when creating a role with login privileges.  If you are using password authentication and you create a role with `LOGIN` privileges and a blank `PASSWORD` or no password, the user assigned to this role will not be able to login to the database.

For example (John is the DBA)

```cql
CREATE ROLE john WITH PASSWORD = '39fksah!' AND LOGIN = true;
GRANT DBA TO john;
```

1. Exit cqlsh and login again with the new credentials

```none
cqlsh> exit
cqlsh -u new-username -p new-password
```

For example:

```none
cqlsh> exit
cqlsh -u john -p 39fksah!
```

#### NOTE
To guarantee new authorization values (like a password) are visible across the cluster, make sure to run a repair on table system_auth after updating or adding users.

<a id="roles"></a>

### Create Additional Roles

In order for the users on your system to be able to login and perform actions, you as the DBA will have to create roles and privileges.

**Before you Begin**
Validate you have set the authenticator as described in [Authentication](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/authentication.md).
Validate you have the credentials for the superuser for your system for yourself.

1. Open a new cqlsh session using the credentials of a role with [superuser]() credentials. For example:

```none
cqlsh -u dba -p 39fksah!
```

1. Configure the appropriate access privileges for clients using [GRANT PERMISSION](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/authorization.md#grant-permission-statement) statements.  For additional examples, consult the [RBAC example](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/rbac-usecase.md).

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

* Clients are not able to connect until you setup roles as users with passwords  using [GRANT PERMISSION](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/authorization.md#grant-permission-statement) statements (using the superuser). Refer to the example in [Role Based Access Control (RBAC)](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/rbac-usecase.md) for details.
* When initiating a connection, clients will need to use the user name and password that you assign
* Confirm all clients can connect before removing the Cassandra default password and user.

1. To remove permission from any role or user, see [REVOKE PERMISSION](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/authorization.md#revoke-permission-statement).

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

* [Role Based Access Control (RBAC)](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/rbac-usecase.md)
* [Authorization](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/authorization.md) - CQL Reference for authorizing users
* [Authentication](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/authentication.md) - Enable Authentication
