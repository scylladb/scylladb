.. highlight:: cql

.. _cql-guardrails:

CQL Guardrails
==============

ScyllaDB provides a set of configurable guardrail parameters that help operators
enforce best practices and prevent misconfigurations that could degrade cluster
health, availability, or performance. Guardrails operate at two severity levels:

* **Warn**: The request succeeds, but the server includes a warning in the CQL
  response. Depending on the specific guardrail, the warning may also be logged on the server side.
* **Fail**: The request is rejected with an error/exception (the specific type
  depends on the guardrail). The user must correct the request or adjust the
  guardrail configuration to proceed.

.. note::

   Guardrails are checked only when a statement is
   executed. They do not retroactively validate existing keyspaces, tables, or
   previously completed writes.

For the full list of configuration properties, including types, defaults, and
liveness information, see :doc:`Configuration Parameters </reference/configuration-parameters>`.

.. _guardrails-replication-factor:

Replication Factor Guardrails
-----------------------------

These four parameters control the minimum and maximum allowed replication factor
(RF) values. They are evaluated whenever a ``CREATE KEYSPACE`` or
``ALTER KEYSPACE`` statement is executed. Each data center's RF is checked
individually.

An RF of ``0`` — which means "do not replicate to this data center" — is
always allowed and never triggers a guardrail.

A threshold value of ``-1`` disables the corresponding check.

``minimum_replication_factor_warn_threshold``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If any data center's RF is set to a value greater than ``0`` and lower than
this threshold, the server attaches a warning to the CQL response identifying
the offending data center and RF value.

**When to use.** The default of ``3`` is the standard recommendation for
production clusters. An RF below ``3`` means that the cluster cannot tolerate
even a single node failure without data loss or read unavailability (assuming
``QUORUM`` consistency). Keep this at ``3`` unless your deployment has specific
constraints (e.g., a development or test cluster with fewer than 3 nodes).

``minimum_replication_factor_fail_threshold``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If any data center's RF is set to a value greater than ``0`` and lower than
this threshold, the request is rejected with a ``ConfigurationException``
identifying the offending data center and RF value.

**When to use.** Enable this parameter (e.g., set to ``3``) in production
environments where allowing a low RF would be operationally dangerous. Unlike
the warn threshold, this provides a hard guarantee that no keyspace can be
created or altered to have an RF below the limit.

``maximum_replication_factor_warn_threshold``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If any data center's RF exceeds this threshold, the server attaches a warning to the CQL response identifying
the offending data center and RF value.

**When to use.** An excessively high RF increases write amplification and
storage costs proportionally. For example, an RF of ``5`` means every write
is replicated to five nodes. Set this threshold to alert operators who
may unintentionally set an RF that is too high.

``maximum_replication_factor_fail_threshold``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If any data center's RF exceeds this threshold, the request is rejected with a ``ConfigurationException``
identifying the offending data center and RF value.

**When to use.** Enable this parameter to prevent accidental creation of
keyspaces with an unreasonably high RF. An extremely high RF wastes storage and
network bandwidth and can lead to write latency spikes. This is a hard limit —
the keyspace creation or alteration will not proceed until the RF is lowered.

**Metrics.** ScyllaDB exposes per-shard metrics that track the number of
times each replication factor guardrail has been triggered:

* ``scylla_cql_minimum_replication_factor_warn_violations``
* ``scylla_cql_minimum_replication_factor_fail_violations``
* ``scylla_cql_maximum_replication_factor_warn_violations``
* ``scylla_cql_maximum_replication_factor_fail_violations``

A sustained increase in any of these metrics indicates that
``CREATE KEYSPACE`` or ``ALTER KEYSPACE`` requests are hitting the configured
thresholds.

.. _guardrails-replication-strategy:

Replication Strategy Guardrails
-------------------------------

These two parameters control which replication strategies trigger warnings or
are rejected when a keyspace is created or altered.

``replication_strategy_warn_list``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If the replication strategy used in a ``CREATE KEYSPACE`` or ``ALTER KEYSPACE``
statement is on this list, the server attaches a warning to the CQL response
identifying the discouraged strategy and the affected keyspace.

**When to use.** ``SimpleStrategy`` is not recommended for production use.
It places replicas without awareness of data center or rack topology, which
can undermine fault tolerance in multi-DC deployments. Even in single-DC
deployments, ``NetworkTopologyStrategy`` is recommended because it keeps the
schema ready for future topology changes.

The default configuration warns on ``SimpleStrategy``, which is appropriate
for most deployments. If you have existing keyspaces that use
``SimpleStrategy``, see :doc:`Update Topology Strategy From Simple to Network
</operating-scylla/procedures/cluster-management/update-topology-strategy-from-simple-to-network>`
for the migration procedure.

``replication_strategy_fail_list``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If the replication strategy used in a ``CREATE KEYSPACE`` or ``ALTER KEYSPACE``
statement is on this list, the request is rejected with a
``ConfigurationException`` identifying the forbidden strategy and the affected
keyspace.

**When to use.** In production environments, add ``SimpleStrategy`` to this
list to enforce ``NetworkTopologyStrategy`` across all keyspaces. This helps
prevent new production keyspaces from being created with a topology-unaware
strategy.

**Metrics.** The following per-shard metrics track replication strategy
guardrail violations:

* ``scylla_cql_replication_strategy_warn_list_violations``
* ``scylla_cql_replication_strategy_fail_list_violations``

.. _guardrails-write-consistency-level:

Write Consistency Level Guardrails
----------------------------------

These two parameters control which consistency levels (CL) are allowed for
write operations (``INSERT``, ``UPDATE``, ``DELETE``, and ``BATCH``
statements). They apply only to writes sent by clients. Writes that ScyllaDB
issues internally are not affected.

Be aware that adding warnings to CQL responses can significantly increase
network traffic and reduce overall throughput.

``write_consistency_levels_warned``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If a write operation uses a consistency level on this list, the server attaches
a warning to the CQL response identifying the discouraged consistency level.

**When to use.** Use this parameter to alert application developers when they
use a consistency level that, while technically functional, is not recommended
for the workload. Common examples:

* **Warn on** ``ANY``: writes at ``ANY`` are acknowledged as soon as at least
  one node (including a coordinator acting as a hinted handoff store) receives
  the mutation. This means data may not be persisted on any replica node at
  the time of acknowledgement, risking data loss if the coordinator fails
  before hinted handoff completes.
* **Warn on** ``ALL``: writes at ``ALL`` require every replica to acknowledge
  the write. If any single replica is down, the write fails. This significantly
  reduces write availability.

``write_consistency_levels_disallowed``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If a write operation uses a consistency level on this list, the request is
rejected with an ``InvalidRequestException`` identifying the forbidden
consistency level.

**When to use.** Use this parameter to hard-block consistency levels that are
considered unsafe for your deployment:

* **Disallow** ``ANY``: in production environments, ``ANY`` is almost never
  appropriate. It provides the weakest durability guarantee and is a common
  source of data-loss incidents when operators or application developers use it
  unintentionally.
* **Disallow** ``ALL``: in clusters where high write availability is critical,
  blocking ``ALL`` prevents a single node failure from causing write
  unavailability.

**Metrics.** The following per-shard metrics track write consistency level
guardrail violations:

* ``scylla_cql_write_consistency_levels_warned_violations``
* ``scylla_cql_write_consistency_levels_disallowed_violations``

Additionally, ScyllaDB exposes the
``scylla_cql_writes_per_consistency_level`` metric, labeled by consistency
level, which tracks the total number of client write requests per CL. This metric is
useful for understanding the current write-CL distribution across the cluster
*before* deciding which levels to warn on or disallow. For example, querying
this metric can reveal whether any application is inadvertently using ``ANY``
or ``ALL`` for writes.

.. _guardrails-compact-storage:

Compact Storage Guardrail
-------------------------

``enable_create_table_with_compact_storage``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This boolean parameter controls whether ``CREATE TABLE`` statements with the
deprecated ``COMPACT STORAGE`` option are allowed. Unlike the other guardrails,
it acts as a simple on/off switch rather than using separate warn and fail
thresholds.

**When to use.** Leave this at the default (``false``) for all new
deployments. ``COMPACT STORAGE`` is a legacy feature that will be permanently
removed in a future version of ScyllaDB. Set to ``true`` only if you have a specific,
temporary need to create compact storage tables (e.g., compatibility with legacy
applications during a migration). For details on the ``COMPACT STORAGE`` option, see
:ref:`Compact Tables <compact-tables>` in the Data Definition documentation.

Additional References
---------------------

* :doc:`Consistency Level </cql/consistency>`
* :doc:`Data Definition (CREATE/ALTER KEYSPACE) </cql/ddl>`
* :doc:`How to Safely Increase the Replication Factor </kb/rf-increase>`
* :doc:`Metrics Reference </reference/metrics>`
