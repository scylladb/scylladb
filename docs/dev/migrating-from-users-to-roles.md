# Migrating from users to roles

Previously, Scylla's access-control system ("auth", which lives in the `auth/` directory) operated based on users. A user could log in to the system and could be granted permissions to perform certain functions on resources in the database: particular keyspaces and tables.

With the introduction of roles, access-control rules are more flexible. A role is an entity that has an associated set of granted permissions. Unlike the old users, however, a role can be granted another role. If role `a` is granted to role `b`, then all the permissions of `a` are also inherited by `b`. All users are roles, but not all roles are users: we distinguish a user from a role based on whether or not the role can log in to the system to perform queries.

The change to roles from users required a change in the schema of the metadata tables populated internally by Scylla. Nonetheless, the auth. system includes code to perform this migration automatically in the background. The rest of this page describes the migration procedure in detail.

## Strategy

Generally, Scylla supports cluster upgrades with no down-time when nodes are upgraded one at a time. We assume that the replication factor of the `system_auth` keyspace is equal to the size of the cluster, as this is a prerequisite for enabling access-control for all versions of Scylla.

We also require that during a cluster upgrade from users to roles, no changes to access-control are made (to users, roles, or permissions).

In a cluster consisting of `n` nodes running an older version of Scylla, a single node is stopped. It's `scylla-server` executable is upgraded and then the node is restarted. After the node restarts, each of the modules that encompass access-control (role-management, authentication, and authorization) performs its own migration.

For each module, migration follows the same process:

- If any non-default metadata exists in the new tables, then nothing happens: no migration takes place. If the legacy table exists, a warning is printed to the log.
- If the legacy table exists, then each entry in the old table is transformed and written to the new table. Existing entries in the new table are overwritten (see below for an explanation). A log message indicates that the migration process is starting and when it has finished.
- If there is an error migrating data, an error is written to the log and the exception is allowed to propagate.

The reason that existing entries in the new table are overwritten during migration is that if two nodes are restarted at once (though we do not support this) and both observe the non-existence of non-default metadata and start migrating, we still ensure that all data are copied over.

After a single node has been upgraded, a client may connect to an old node, or to the new one.

If the client connects to an old node, any changes to access-control will succeed (because the code accesses the old tables, which still exist), but are unsupported: changes will not necessarily be reflected in the new tables.

If a client connects to a new node, all access-control CQL statements will access the new tables. A new gossiper feature flag, `ROLES`, helps to enforce the restriction that no changes to access-control can be made during an upgrade. Unless all nodes in the cluster advertise their support for `ROLES`, CQL statements which modify access-control will log an error and not succeed. 

Once all nodes have been upgraded, it is important to verify the contents of the roles-related tables (`system_auth.{roles, role_permissions, role_members}`) as a superuser and compare them to the old users tables (`system_auth.{users,credentials,permissions}`). Alternatively, you may explore the migrated access-control data through the usual CQL statements: LIST PERMISSIONS, LIST ROLES, LIST USERS, etc.

After you are confident all the metadata has migrated successfully and the system is operating as expected, you may drop the legacy tables.

## Recovery

If a particular node fails to migrate metadata, it will log an error message. The best way to move forward in this case is to drop any entries in the new tables and to restart the node.
