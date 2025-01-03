===============
DESCRIBE SCHEMA
===============

When using ScyllaDB, you may want to back up your data. In case something unexpected happens, you will
be able to restore it. However, you cannot insert data into the void; because of that, you need to
first restore the schema.

That is the main motivation for an introduced statement in ScyllaDB: ``DESCRIBE SCHEMA``, which is
responsible for producing a sequence of CQL commands that can be executed in order to recreate the schema.

Aside from elements of schema, like keyspaces and tables, it also allows for restoring entities such as
roles and service levels.

For more context, see the articles covering :doc:`backing up </operating-scylla/procedures/backup-restore/backup>`
and :doc:`restoring </operating-scylla/procedures/backup-restore/restore>` the schema.

Syntax and semantics
--------------------

``DESCRIBE SCHEMA`` comes in three forms that differ in semantics. We will refer to those forms as "tiers".

* ``DESCRIBE [FULL] SCHEMA``: describe elements of the non-system schema: keyspaces, tables, views, UDTs, etc.
  When `FULL` is used, it also includes the elements of the system schema, e.g. system tables.

* ``DESCRIBE [FULL] SCHEMA WITH INTERNALS``: in addition to the output of the previous tier, the statement
  also describes authentication and authorization, as well as service levels. The statements corresponding to
  restoring roles do *not* contain any information about their passwords. Aside from that, additional information
  about tables, materialized views, and secondary indices may be provided, e.g. the ID of a table.

* ``DESCRIBE [FULL] SCHEMA WITH INTERNALS AND PASSWORDS``: aside from the information retrieved as part of the
  previous tier, the statements corresponding to restoring roles *do* contain information about their
  passwords; namely—their hashed passwords. For more information regarding hashed passwords, see the relevant section
  below.

Instead of ``DESCRIBE``, you can use its shortened form: ``DESC``.

Output
------

As a result of the query, you will obtain a set of rows, each of which consists of four values:

* ``keyspace_name``: the name of the keyspace the entity is part of,
* ``type``: the type of the entity,
* ``name``: the name of the entity,
* ``create_statement``: the statement that can be used to restore the entity.

All of those values are always present and represent strings using UTF-8 encoding.
The value ``keyspace_name`` can be equal to ``null`` if the corresponding entity is not part of any keyspace,
e.g. in the case of a role or a service level.

Required permissions
--------------------

No permissions are necessary to execute the first two tiers. However, executing the last tier requires that the user
performing the query be a superuser.

Restoring the schema by executing the consecutive statements returned by ``DESCRIBE SCHEMA`` should be conducted by
a superuser—some of the statements may require that status. It's advised to use the default superuser and not modify
the schema, roles, permissions, or service levels beforehand.

Relation to authentication and authorization
--------------------------------------------

It's important to note that the information returned by ``DESCRIBE [FULL] SCHEMA WITH INTERNALS [AND PASSWORDS]``
depends on the currently used authenticator and authorizer. If the used authenticator doesn't use passwords to
authenticate a role, the hashed passwords won't be returned even if they're present in the database. That scenario
may happen if, for example, you start using `AllowAllAuthenticator`.

Similarly, permission grants may not be returned if they're not used, e.g. when ScyllaDB is configured to use
`AllowAllAuthorizer`.

Since ScyllaDB may also use third party software service for authentication and authorization, the result of the query
will depend on what information that service will provide the database with.

That's why it's *crucial* for you to take into consideration the current configuration of your cluster.

Restoring process and its side effects
--------------------------------------

When a resource is created, the current role is always granted full permissions to that resource. As a consequence, the role
used to restore the backup will gain permissions to all of the resources that will be recreated. That's one of the reasons
why using a superuser to restore the schema is advised—a superuser has full access to all resources anyway.

cqlsh support
-------------

    +---------------------------------------------------------+---------------------------+
    | Statement                                               | Required version of cqlsh |
    +=========================================================+===========================+
    | ``DESCRIBE [FULL] SCHEMA``                              | 6.0.19                    |
    +---------------------------------------------------------+---------------------------+
    | ``DESCRIBE [FULL] SCHEMA WITH INTERNALS``               | 6.0.19                    |
    +---------------------------------------------------------+---------------------------+
    | ``DESCRIBE [FULL] SCHEMA WITH INTERNALS AND PASSWORDS`` | 6.0.23                    |
    +---------------------------------------------------------+---------------------------+

Appendix I, hashed passwords
----------------------------

A hashed password is an encrypted form of a password stored by ScyllaDB to authenticate roles. The statements returned
by ``DESCRIBE [FULL] SCHEMA WITH INTERNALS AND PASSWORDS`` corresponding to recreating the roles will be of the
following form:

.. code-block:: cql

    CREATE ROLE [IF NOT EXISTS] <role_name> WITH HASHED PASSWORD = '<hashed_password>' AND LOGIN = <boolean> AND SUPERUSER = <boolean>

The semantics of this statement is analogous to the regular ``CREATE ROLE`` statement except that it circumvents
the encryption phase of the execution and inserts the hashed password directly into ``system.roles``. You should not use
this statement unless it was returned by ``DESCRIBE SCHEMA``.
