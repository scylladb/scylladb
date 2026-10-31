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
separate set from the ``scylla.yaml`` parameters, except for a few ``scylla.yaml`` parameters that
can also be set through CQL; see :ref:`Options also set in scylla.yaml <cql-cluster-config-yaml-options>`.
More options will be added over time.

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
``auto_repair_enabled``) can be set at ``TABLE``, ``KEYSPACE`` and ``CLUSTER`` scope. Options that
describe the cluster as a whole (such as ``repair_hints_batchlog_flush_discard_unreplayed_hints``)
can be set at ``CLUSTER`` scope only. No option currently supports the ``DATACENTER``, ``RACK`` or
``NODE`` scopes.

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
``true`` and ``false`` (in any letter case), an integer option accepts a signed 64-bit integer
within the option's range, and so on, and a text option accepts at most 4096 bytes. An option may
introduce further restrictions (see ``auto_scrub_period_seconds`` in
:ref:`Available options <cql-cluster-config-options>`). An unknown
option name, an option used at a scope it does not support, or a value of the wrong type is
rejected and nothing changes.

Some options take a structured value: a set of objects rather than a single scalar. Such a value
can be written as a CQL literal - a map, a list, or the ``{name: value}`` form whose names need no
quotes - nested as deep as the option needs, for example
``{first: {text: 'abc', number: 1, flag: true}, second: {list: [1, 2]}}``. String values are quoted
as usual; numbers, ``true``, ``false`` and ``null`` are not. The same value can also be given as a
string holding its YAML or JSON text. Only a structured option takes a literal; any other option
rejects one.

Removing an option
------------------

Setting an option to ``NULL`` removes the value stored at that scope, so the option falls back to
the next broader scope, or to its default:

.. code-block:: cql

   ALTER TABLE ks.tbl WITH auto_repair_enabled = NULL;   -- ks.tbl now follows its keyspace
   ALTER CLUSTER WITH auto_repair_enabled = NULL;        -- back to the default

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
     - Enable :doc:`automatic repair </features/automatic-repair>` for tablet-based tables. Default:
       ``false``. Until the deprecated ``auto_repair_enabled_default`` option is removed, a node
       that sets it in ``scylla.yaml`` uses that value when no scope sets this option.
   * - ``auto_repair_threshold_in_seconds``
     - integer
     - ``CLUSTER``, ``KEYSPACE``, ``TABLE``
     - Time in seconds since the start of the last successful repair of a tablet after which it is
       eligible for automatic repair. A repair restricted to some hosts or datacenters does not count,
       and a tablet that was never repaired is eligible immediately. ``0`` disables time-based
       automatic repair. Default: ``86400``. Until the deprecated
       ``auto_repair_threshold_default_in_seconds`` option is removed, a node that sets it in
       ``scylla.yaml`` uses that value when no scope sets this option.
   * - ``repair_hints_batchlog_flush_discard_unreplayed_hints``
     - boolean
     - ``CLUSTER``
     - A repair of a table with ``tombstone_gc`` in ``repair`` mode waits for the hints written
       before it to be sent, and records no repair time if that times out. With this option on,
       such a timeout discards those hints, so that the next repair does not wait for them again.
       This drops every regular hint on the node, for all tables and destinations; a write
       acknowledged at consistency level ``ANY`` that only hints hold is lost. Hints for
       materialized views are never discarded. Default: ``true``, in effect once every node knows
       the option.
   * - ``auto_scrub_period_seconds``
     - integer
     - ``CLUSTER``, ``KEYSPACE``, ``TABLE``
     - Schedule every sstable written or validated more than this many seconds ago for scrub.
       Accepts ``0`` or ``3600`` to ``4294967295``. Default: ``0``. ``0`` disables automatic scrub
       at that scope even if a broader scope enabled it, whereas ``NULL`` removes the value and
       falls back to the broader scope. See
       :doc:`Automatic Scrub </operating-scylla/procedures/maintenance/automatic-scrub>`.

.. _cql-cluster-config-yaml-options:

Options also set in scylla.yaml
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following options are also ``scylla.yaml`` parameters. A value set through CQL overrides the
node's own value on every node it applies to:

* While the value is stored, it is the option's effective value on the node, and ``system.config``
  shows it with the source ``cluster_config``.
* Writing the option through the REST API or ``UPDATE system.config`` is rejected, and a
  ``scylla.yaml`` reload does not change it.
* Removing the value with ``= NULL`` returns the node to the value it had when the option was
  first set through CQL. A ``scylla.yaml`` change made while the value was stored is not applied
  until the next ``scylla.yaml`` reload or restart.

.. list-table::
   :widths: 25 10 25 40
   :header-rows: 1

   * - Option
     - Type
     - Scopes
     - Description
   * - ``tablets_per_shard_goal``
     - integer
     - ``CLUSTER``
     - The goal for the maximum number of tablet replicas per shard that the tablet load balancer
       tries to keep; see :doc:`Tablets </architecture/tablets>`. Range: 1 to 4294967295. Without
       a stored value, each node uses its ``scylla.yaml`` value (default ``100``).
