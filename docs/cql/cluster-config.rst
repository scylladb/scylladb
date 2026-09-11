.. _cluster-config:

Configuring a Cluster with CQL
==============================

CQL cluster configuration provides an additional way to configure ScyllaDB, next to
``scylla.yaml``. An option set through CQL is stored once and applied by all nodes in the
cluster, without editing ``scylla.yaml`` on every node or restarting anything. An option is set
at a *scope*, which determines what it applies to: the whole cluster, a datacenter, a rack, a
node, a keyspace, or a table; see :ref:`Scopes <cql-cluster-config-scopes>`. The stored values
are part of the schema: they are replicated to every node through the same path as tables and
keyspaces, and they appear in ``DESCRIBE`` output.

The options and scopes currently available through CQL are listed in
:ref:`Available options <cql-cluster-config-options>` at the end of this page. They are a
separate set from the ``scylla.yaml`` parameters; more options will be added over time.

.. note::

   You can set a cluster configuration option only when all nodes in the cluster run a ScyllaDB
   version that supports it. During a rolling upgrade, attempts to set an option that is not
   supported by every node are rejected. Retry the statement when every node supports the
   option. Existing option values continue to apply during the upgrade.

.. _cql-cluster-config-scopes:

Scopes
------

An option can be set at several *scopes*. A value set at a narrower scope takes precedence over a
value set at a broader one:

* ``TABLE`` overrides ``KEYSPACE``, which overrides ``CLUSTER``.
* ``NODE`` overrides ``RACK``, which overrides ``DATACENTER``, which overrides ``CLUSTER``.

Each option supports a fixed set of scopes. Options that describe a table (such as
``auto_repair_enabled``) can be set at ``TABLE``, ``KEYSPACE`` and ``CLUSTER`` scope. No option
currently supports the ``DATACENTER``, ``RACK`` or ``NODE`` scopes.

Each scope stores only the values that were explicitly set there. The *effective* value of an
option for a table is the value stored at the narrowest scope that has one: the table's own value
if it has one, otherwise the keyspace's, otherwise the cluster's. If no scope stores a value, the
option's built-in default applies.

Setting an option
-----------------

For a table or a keyspace, use the ``WITH`` clause of the usual statements. The option name is used
directly as a property name:

.. code-block:: cql

   CREATE TABLE ks.tbl (pk int PRIMARY KEY) WITH auto_repair_enabled = true;
   ALTER TABLE ks.tbl WITH auto_repair_enabled = false;
   ALTER KEYSPACE ks WITH auto_repair_enabled = true;

Cluster configuration options can be combined with other properties in the same statement, for
example ``WITH comment = 'orders' AND auto_repair_enabled = true``. The one exception is a
tablets replication-factor change, which cannot be combined with a cluster configuration option
in the same ``ALTER KEYSPACE`` statement. This is rejected:

.. code-block:: cql

   ALTER KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3} AND auto_repair_enabled = true;

Use separate statements instead:

.. code-block:: cql

   ALTER KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};
   ALTER KEYSPACE ks WITH auto_repair_enabled = true;

For the cluster as a whole, use ``ALTER CLUSTER``:

.. code-block:: cql

   ALTER CLUSTER WITH auto_repair_enabled = true;

The statements for the node-oriented scopes exist for options that will support them:

.. code-block:: cql

   alter_cluster_statement:    ALTER CLUSTER WITH `option` '=' `value`
   alter_datacenter_statement: ALTER DATACENTER `datacenter_name` WITH `option` '=' `value`
   alter_rack_statement:       ALTER RACK `datacenter_name` `rack_name` WITH `option` '=' `value`
   alter_node_statement:       ALTER NODE `host_id` WITH `option` '=' `value`

The datacenter, rack, or node named in the statement must exist in the cluster.

Values are validated against the option's type before anything is stored: a boolean option accepts
``true`` and ``false`` (in any letter case), an integer option accepts a signed 64-bit integer, and
so on, and a text option accepts at most 4096 bytes. An unknown option name, an option used at a
scope it does not support, or a value of the wrong type is rejected and nothing changes.

Removing an option
------------------

Setting an option to ``NULL`` removes the value stored at that scope, so the option falls back to
the next broader scope, or to its default:

.. code-block:: cql

   ALTER TABLE ks.tbl WITH auto_repair_enabled = NULL;   -- ks.tbl now follows its keyspace
   ALTER CLUSTER WITH auto_repair_enabled = NULL;        -- back to the built-in default

Only the bare ``NULL`` keyword removes a value. The string ``'null'`` is an ordinary value and is
validated like any other, so a boolean option rejects it.

Permissions
-----------

The permission required depends on the scope, not on the option:

* ``ALTER TABLE ... WITH`` requires ``ALTER`` permission on the table.
* ``ALTER KEYSPACE ... WITH`` requires ``ALTER`` permission on the keyspace.
* ``ALTER CLUSTER``, ``ALTER DATACENTER``, ``ALTER RACK`` and ``ALTER NODE`` require a superuser.

Inspecting effective values
---------------------------

``DESCRIBE KEYSPACE`` and ``DESCRIBE TABLE`` show the cluster configuration options that are in
force for the described object, in the ``WITH`` clause alongside the other properties. Each such
line ends with a comment that says where the value comes from and what every scope stores
(``NULL`` means nothing is stored at that scope).

A value stored at the described object's own scope is shown as a regular property:

.. code-block:: cql

   ... AND auto_repair_enabled = true  -- from table (table=true, keyspace=false, cluster=NULL)

A value inherited from a broader scope is shown as a commented-out property, so replaying the
``DESCRIBE`` output recreates exactly what was stored and nothing more:

.. code-block:: cql

   ... -- AND auto_repair_enabled = false  -- from keyspace (table=NULL, keyspace=false, cluster=NULL)

Removing the leading ``--`` marker and running the statement pins the inherited value at that
scope as an explicit setting. An option that no scope stores is not shown at all.

``DESCRIBE SCHEMA`` lists the cluster-scope values first, before any keyspace, as
``ALTER CLUSTER WITH ...`` statements, so a schema dump carries them. Under
``DESCRIBE SCHEMA WITH INTERNALS`` the trailing comments are omitted and only stored values are
emitted, which is the form used for backup and restore. See :doc:`DESCRIBE SCHEMA </cql/describe-schema>`.

.. _cql-cluster-config-options:

Available options
-----------------

.. list-table::
   :widths: 25 10 25 40
   :header-rows: 1

   * - Option
     - Type
     - Scopes
     - Description
   * - ``auto_repair_enabled``
     - boolean
     - ``CLUSTER``, ``KEYSPACE``, ``TABLE``
     - Enable automatic repair for tablet-based tables. Default: ``false``.
