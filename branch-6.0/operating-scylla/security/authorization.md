<a id="cql-security"></a>

# Grant Authorization CQL Reference

Authorization is the process by where users are granted permissions, which entitle them to access, or permission to change data on specific keyspaces, tables or an entire datacenter. Authorization for Scylla is done internally within Scylla and is not done with a third-party such as LDAP or OAuth. Granting permissions to users requires the use of a role such as a Database Administrator as well as [enabling the CassandraAuthorizer](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/security/enable-authorization.md). It also requires a user who has been [authenticated](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/security/authentication.md).

This reference covers CQL specification version 3.3.1

<a id="db-roles"></a>

## Database Roles

<a id="database-roles-statement"></a>

Database roles should be used instead of USERS.

CQL uses database roles to represent users and groups of users. Syntactically, a role is defined by:

```cql
role_name: `identifier` | `string`
```

<a id="create-role-statement"></a>

### CREATE ROLE

<a id="create-role-statment"></a>

Creating a role uses the `CREATE ROLE` statement:

```cql
create_role_statement: CREATE ROLE [ IF NOT EXISTS ] `role_name`
                     :     [ WITH `role_options` ]
role_options: `role_option` ( AND `role_option` )*
role_option: PASSWORD '=' `string`
           :| LOGIN '=' `boolean`
           :| SUPERUSER '=' `boolean`
           :| OPTIONS '=' `map_literal`
```

For instance:

```cql
CREATE ROLE new_role;
CREATE ROLE alice WITH PASSWORD = 'password_a' AND LOGIN = true;
CREATE ROLE bob WITH PASSWORD = 'password_b' AND LOGIN = true AND SUPERUSER = true;
CREATE ROLE carlos WITH OPTIONS = { 'custom_option1' : 'option1_value', 'custom_option2' : 99 };
```

#### WARNING
It is **highly** recommended to set a password when creating a role with login privileges.  If you are using password authentication and you create a role with `LOGIN` privileges and a blank `PASSWORD` or no password, the user assigned to this role will not be able to login to the database.

By default, roles do not possess `LOGIN` privileges or `SUPERUSER` status.

[Permissions](#cql-permissions) on database resources are granted to roles; types of resources include keyspaces,
tables, functions, and roles themselves. Roles may be granted to other roles to create hierarchical permissions
structures; in these hierarchies, permissions and `SUPERUSER` status are inherited, but the `LOGIN` privilege is
not.

If a role has the `LOGIN` privilege, clients may identify as that role when connecting. For the duration of that
connection, the client will acquire any roles and privileges granted to that role.

Only a client with the `CREATE` permission on the database roles resource may issue `CREATE ROLE` requests (see
the [relevant section](#cql-permissions) below) unless the client is a `SUPERUSER`. Role management in Scylla
is pluggable, and custom implementations may support only a subset of the listed options.

Role names should be quoted if they contain non-alphanumeric characters.

<a id="setting-credentials-for-internal-authentication"></a>

#### Setting credentials for internal authentication

Use the `WITH PASSWORD` clause to set a password for internal authentication, enclosing the password in single
quotation marks.

If internal authentication has not been set up or the role does not have `LOGIN` privileges, the `WITH PASSWORD`
clause is not necessary.

#### Creating a role conditionally

Attempting to create an existing role results in an invalid query condition unless the `IF NOT EXISTS` option is used.
If the option is used and the role exists, the statement is a no-op:

```cql
CREATE ROLE other_role;
CREATE ROLE IF NOT EXISTS other_role;
```

<a id="alter-role-statement"></a>

### ALTER ROLE

<a id="alter-role-statment"></a>

Altering role options uses the `ALTER ROLE` statement:

```cql
alter_role_statement: ALTER ROLE `role_name` WITH `role_options`
```

For instance:

```cql
ALTER ROLE bob WITH PASSWORD = 'PASSWORD_B' AND SUPERUSER = false;
```

Conditions on executing `ALTER ROLE` statements:

- A client must have `SUPERUSER` status to alter the `SUPERUSER` status of another role
- A client cannot alter the `SUPERUSER` status of any role it currently holds
- A client can only modify certain properties of the role with which it identified at login (e.g. `PASSWORD`)
- To modify properties of a role, the client must be granted `ALTER` [permission](#cql-permissions) on that role

<a id="drop-role-statement"></a>

### DROP ROLE

<a id="drop-role-statment"></a>

Dropping a role uses the `DROP ROLE` statement:

```cql
drop_role_statement: DROP ROLE [ IF EXISTS ] `role_name`
```

`DROP ROLE` requires the client to have `DROP` [permission](#cql-permissions) on the role in question. In
addition, client may not `DROP` the role with which it identified at login. Finally, only a client with `SUPERUSER`
status may `DROP` another `SUPERUSER` role.

Attempting to drop a role that does not exist results in an invalid query condition unless the `IF EXISTS` option is
used. If the option is used and the role does not exist, the statement is a no-op.

<a id="grant-role-statement"></a>

### GRANT ROLE

<a id="grant-role-statment"></a>

Granting a role to another uses the `GRANT ROLE` statement:

```cql
grant_role_statement: GRANT `role_name` TO `role_name`
```

For instance:

```cql
GRANT report_writer TO alice;
```

This statement grants the `report_writer` role to `alice`. Any permissions granted to `report_writer` are also
acquired by `alice`.

Roles are modelled as a directed acyclic graph, so circular grants are not permitted. The following examples result in
error conditions:

```cql
GRANT role_a TO role_b;
GRANT role_b TO role_a;

GRANT role_a TO role_b;
GRANT role_b TO role_c;
GRANT role_c TO role_a;
```

<a id="revoke-role-statement"></a>

### REVOKE ROLE

<a id="revoke-role-statment"></a>

Revoking a role uses the `REVOKE ROLE` statement:

```cql
revoke_role_statement: REVOKE `role_name` FROM `role_name`
```

For instance:

```cql
REVOKE report_writer FROM alice;
```

This statement revokes the `report_writer` role from `alice`. Any permissions that `alice` has acquired via the
`report_writer` role are also revoked.

<a id="list-roles-statement"></a>

### LIST ROLES

<a id="list-roles-statment"></a>

All the known roles (in the system or granted to the specific role) can be listed using the `LIST ROLES` statement:

```cql
list_roles_statement: LIST ROLES [ OF `role_name` ] [ NORECURSIVE ]
```

For instance:

```cql
LIST ROLES;
```

returns all known roles in the system, this requires `DESCRIBE` permission on the database roles resource. And:

```cql
LIST ROLES OF alice;
```

enumerates all roles granted to `alice`, including those transitively acquired. But:

```cql
LIST ROLES OF bob NORECURSIVE
```

lists all roles directly granted to `bob` without including any of the transitively acquired ones.

## Users

Prior to the introduction of roles in Scylla 2.2, authentication and authorization were based around the concept of a
`USER`. For backward compatibility, this syntax has been preserved. From Scylla 2.2 and onward, it is recommended to use [roles](#db-roles).

<a id="create-user-statement"></a>

### CREATE USER

Creating a user uses the `CREATE USER` statement:

```cql
create_user_statement: CREATE USER [ IF NOT EXISTS ] [ WITH PASSWORD `string` ] [ `user_option` ]
user_option: SUPERUSER | NOSUPERUSER
```

For instance:

```cql
CREATE USER alice WITH PASSWORD 'password_a' SUPERUSER;
CREATE USER bob WITH PASSWORD 'password_b' NOSUPERUSER;
```

`CREATE USER` where the `LOGIN` option is `true`. So, the following pairs of
statements are equivalent:

```cql
CREATE USER alice WITH PASSWORD 'password_a' SUPERUSER;
CREATE USER IF NOT EXISTS alice WITH PASSWORD 'password_a' SUPERUSER;
CREATE USER alice WITH PASSWORD 'password_a' NOSUPERUSER;
CREATE USER alice WITH PASSWORD 'password_a';
```

<!-- CREATE ROLE alice WITH PASSWORD = 'password_a' AND LOGIN = true AND SUPERUSER = true; -->
<!-- CREATE ROLE IF EXISTS alice WITH PASSWORD = 'password_a' AND LOGIN = true AND SUPERUSER = true; -->
<!-- CREATE ROLE alice WITH PASSWORD = 'password_a' AND LOGIN = true AND SUPERUSER = false; -->
<!-- CREATE ROLE alice WITH PASSWORD = 'password_a' WITH LOGIN = true; -->
<!-- CREATE ROLE alice WITH PASSWORD = 'password_a' WITH LOGIN = true; -->

#### WARNING
It is **highly** recommended to set a password when creating a role with login privileges.  If you are using password authentication and you create a role with `LOGIN` privileges and a blank `PASSWORD` or no password, the user assigned to this role will not be able to login to the database.

<a id="alter-user-statement"></a>

### ALTER USER

Altering the options of a user uses the `ALTER USER` statement:

```cql
alter_user_statement: ALTER USER `user_name` [ WITH PASSWORD `string` ] [ `user_option` ]
```

For instance:

```cql
ALTER USER alice WITH PASSWORD 'PASSWORD_A';
ALTER USER bob SUPERUSER;
```

<a id="drop-user-statement"></a>

### DROP USER

Dropping a user uses the `DROP USER` statement:

```cql
drop_user_statement: DROP USER [ IF EXISTS ]
```

<a id="list-users-statement"></a>

### LIST USERS

Existing users can be listed using the `LIST USERS` statement:

```cql
list_users_statement: LIST USERS
```

<!-- Note that this statement is equivalent to:: -->
<!-- ##    LIST ROLES; -->
<!-- ## but only roles with the ``LOGIN`` privilege are included in the output. -->

<a id="data-control"></a>

## Data Control

<a id="cql-permissions"></a>

### Permissions

Permissions on resources are granted to users; there are several different types of resources in Scylla, and each type
is modelled hierarchically:

- The hierarchy of Data resources, Keyspaces, and Tables has the structure `ALL KEYSPACES` -> `KEYSPACE` ->
  `TABLE`.

<!-- - Function resources have the structure ``ALL FUNCTIONS`` -> ``KEYSPACE`` -> ``FUNCTION``
- Resources representing roles have the structure ``ALL ROLES`` -> ``ROLE``
- Resources representing JMX ObjectNames, which map to sets of MBeans/MXBeans, have the structure ``ALL MBEANS`` ->
``MBEAN`` -->

Permissions can be granted at any level of these hierarchies, and they flow downwards. So granting permission on a
resource higher up the chain automatically grants that same permission on all resources lower down. For example,
granting `SELECT` on a `KEYSPACE` automatically grants it on all `TABLES` in that `KEYSPACE`.

<!-- Likewise, granting a permission on ``ALL FUNCTIONS`` grants it on every defined function, regardless of which keyspace it is scoped in. It is also possible to grant permissions on all functions scoped to a particular keyspace. -->

Modifications to permissions are visible to existing client sessions; that is, connections need not be re-established
following permissions changes.

The full set of available permissions is:

- `CREATE`
- `ALTER`
- `DROP`
- `SELECT`
- `MODIFY`
- `AUTHORIZE`
- `DESCRIBE`

<!-- - ``EXECUTE`` -->
<!-- Not all permissions are applicable to every type of resource. For instance, ``EXECUTE`` is only relevant in the context -->
<!-- of functions or mbeans; granting ``EXECUTE`` on a resource representing a table is nonsensical. -->

Attempting to `GRANT` permission on a resource to which it cannot be applied results in an error response. The following illustrates which
permissions can be granted on which types of resources, and which statements are enabled by that permission.

| Permission   | Resource                 | Operations                                                                         |
|--------------|--------------------------|------------------------------------------------------------------------------------|
| `CREATE`     | `ALL KEYSPACES`          | `CREATE KEYSPACE` and `CREATE TABLE` in any keyspace                               |
| `CREATE`     | `KEYSPACE keyspace_name` | `CREATE TABLE` in specified keyspace                                               |
| `ALTER`      | `ALL KEYSPACES`          | `ALTER KEYSPACE` and `ALTER TABLE` in any keyspace                                 |
| `ALTER`      | `KEYSPACE keyspace_name` | `ALTER KEYSPACE` and `ALTER TABLE` in specified keyspace                           |
| `ALTER`      | `TABLE table_name`       | `ALTER TABLE` on specified table                                                   |
| `DROP`       | `ALL KEYSPACES`          | `DROP KEYSPACE` and `DROP TABLE` in any keyspace                                   |
| `DROP`       | `KEYSPACE keyspace_name` | `DROP TABLE` and `DROP KEYSPACE` in specified keyspace                             |
| `DROP`       | `TABLE table_name`       | `DROP TABLE`                                                                       |
| `SELECT`     | `ALL KEYSPACES`          | `SELECT` on any table                                                              |
| `SELECT`     | `KEYSPACE keyspace_name` | `SELECT` on any table in specified keyspace                                        |
| `SELECT`     | `TABLE table_name`       | `SELECT` on specified table                                                        |
| `MODIFY`     | `ALL KEYSPACES`          | `INSERT`, `UPDATE`, `DELETE` and `TRUNCATE` on any table                           |
| `MODIFY`     | `KEYSPACE keyspace_name` | `INSERT`, `UPDATE`, `DELETE` and `TRUNCATE` on any table in the specified keyspace |
| `MODIFY`     | `TABLE table_name`       | `INSERT`, `UPDATE`, `DELETE` and `TRUNCATE` on specified table                     |
| `AUTHORIZE`  | `ALL KEYSPACES`          | `GRANT PERMISSION` and `REVOKE PERMISSION` on any table                            |
| `AUTHORIZE`  | `KEYSPACE keyspace_name` | `GRANT PERMISSION` and `REVOKE PERMISSION` on any table in the specified keyspace  |
| `AUTHORIZE`  | `TABLE table_name`       | `GRANT PERMISSION` and `REVOKE PERMISSION` on specified table                      |
| `DESCRIBE`   | `ALL ROLES`              | `LIST ROLES` on all roles or only roles granted to another specified role          |

<a id="grant-permission-statement"></a>

### GRANT PERMISSION

Granting permission uses the `GRANT PERMISSION` statement:

```cql
grant_permission_statement: GRANT `permissions` ON `resource` TO `user_name`
permissions: ALL [ PERMISSIONS ] | `permission` [ PERMISSION ]
permission: CREATE | ALTER | DROP | SELECT | MODIFY | AUTHORIZE | DESCRIBE
resource: ALL KEYSPACES
        :| KEYSPACE `keyspace_name`
        :| [ TABLE ] `table_name`
        :| ALL USERS
        :| USER `user_name`
```

For instance:

```cql
GRANT SELECT ON ALL KEYSPACES TO data_reader;
```

This gives any user with the `data_reader` role, permission to execute `SELECT` statements on any table across all
keyspaces:

```cql
GRANT MODIFY ON KEYSPACE keyspace1 TO data_writer;
```

This gives any user with the `data_writer` role, permission to perform `UPDATE`, `INSERT`, `DELETE`,
and `TRUNCATE` queries on all tables in the `keyspace1` keyspace:

```cql
GRANT DROP ON keyspace1.table1 TO schema_owner;
```

This gives any user with the `schema_owner` role, permissions to `DROP` `keyspace1.table1`

<!-- GRANT EXECUTE ON FUNCTION keyspace1.user_function( int ) TO report_writer; -->
<!-- This grants any user with the ``report_writer`` permission to execute ``SELECT``, ``INSERT`` and ``UPDATE`` queries -->
<!-- which use the function ``keyspace1.user_function( int )``:: -->
<!-- GRANT DESCRIBE ON ALL USERS TO role_admin; -->
<!-- This grants any user with the ``user_admin`` permission to view any and all users in the system with a ``LIST -->
<!-- USERS`` statement -->

<a id="grant-all"></a>

#### GRANT ALL

When the `GRANT ALL` form is used, the appropriate set of permissions is determined automatically based on the target
resource.

#### Automatic Granting

When a resource is created, via a `CREATE KEYSPACE`, `CREATE TABLE` or `CREATE USER` statement, the creator (the role the database user who issues the statement is identified as) is
automatically granted all applicable permissions on the new resource.

<a id="revoke-permission-statement"></a>

### REVOKE PERMISSION

Revoking permission from a user uses the `REVOKE PERMISSION` statement:

```cql
revoke_permission_statement: REVOKE `permissions` ON `resource` FROM `user_name`
```

For instance:

```cql
REVOKE SELECT ON ALL KEYSPACES FROM data_reader;
REVOKE MODIFY ON KEYSPACE keyspace1 FROM data_writer;
REVOKE DROP ON keyspace1.table1 FROM schema_owner;
REVOKE DESCRIBE ON ALL USERS FROM user_admin;
```

<!-- REVOKE EXECUTE ON FUNCTION keyspace1.user_function( int ) FROM report_writer; -->

<a id="list-permissions-statement"></a>

### LIST PERMISSIONS

Listing granted permissions uses the `LIST PERMISSIONS` statement:

```cql
list_permissions_statement: LIST `permissions` [ ON `resource` ] [ OF `user_name` [ NORECURSIVE ] ]
```

For instance:

```cql
LIST ALL PERMISSIONS OF alice;
```

Show all permissions granted to `alice`, including those acquired transitively from any other users:

```cql
LIST ALL PERMISSIONS ON keyspace1.table1 OF bob;
```

Show all permissions on `keyspace1.table1` granted to `bob`, including those acquired transitively from any other
users. This also includes any permissions higher up the resource hierarchy, which can be applied to `keyspace1.table1`.
For example, should `bob` have `ALTER` permission on `keyspace1`, that would be included in the results of this
query. Adding the `NORECURSIVE` switch restricts the results to only those permissions which were directly granted to
`bob`:

```cql
LIST SELECT PERMISSIONS OF carlos;
```

Show any permissions granted to `carlos`, limited to `SELECT` permissions on any resource.

#### Related Topics

* [Apache Cassandra Query Language (CQL) Reference](https://opensource.docs.scylladb.com/branch-6.0/cql/index.md)
* [Role Based Access Control (RBAC)](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/security/rbac-usecase.md)

Copyright

© 2016, The Apache Software Foundation.

Apache®, Apache Cassandra®, Cassandra®, the Apache feather logo and the Apache Cassandra® Eye logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. No endorsement by The Apache Software Foundation is implied by the use of these marks.

<!-- Licensed to the Apache Software Foundation (ASF) under one -->
<!-- or more contributor license agreements.  See the NOTICE file -->
<!-- distributed with this work for additional information -->
<!-- regarding copyright ownership.  The ASF licenses this file -->
<!-- to you under the Apache License, Version 2.0 (the -->
<!-- "License"); you may not use this file except in compliance -->
<!-- with the License.  You may obtain a copy of the License at -->
<!-- http://www.apache.org/licenses/LICENSE-2.0 -->
<!-- Unless required by applicable law or agreed to in writing, software -->
<!-- distributed under the License is distributed on an "AS IS" BASIS, -->
<!-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. -->
<!-- See the License for the specific language governing permissions and -->
<!-- limitations under the License. -->
