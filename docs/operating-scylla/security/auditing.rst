========================
ScyllaDB Auditing Guide
========================

Auditing allows the administrator to monitor activities on a ScyllaDB cluster, including CQL queries and data changes, as well as Alternator (DynamoDB-compatible API) requests.
The information is stored in a Syslog or a ScyllaDB table.

Prerequisite
------------

Enable ScyllaDB :doc:`Authentication </operating-scylla/security/authentication>` and :doc:`Authorization </operating-scylla/security/enable-authorization>`.


Enabling Audit
---------------

By default, auditing is **enabled** with the ``table`` backend. Enabling auditing is controlled by the ``audit:`` parameter in the ``scylla.yaml`` file.
You can set the following options:

* ``none`` - Audit is disabled.
* ``table`` - Audit is enabled, and messages are stored in a ScyllaDB table (default).
* ``syslog`` - Audit is enabled, and messages are sent to Syslog.
* ``syslog,table`` - Audit is enabled, and messages are stored in a ScyllaDB table and sent to Syslog.

Configuring any other value results in an error at ScyllaDB startup.

Configuring Audit
-----------------

The audit can be tuned using the following flags or ``scylla.yaml`` entries:

==================  ========================================================================================================================
Flag                Description
==================  ========================================================================================================================
audit_categories    Comma-separated list of statement categories that should be audited
------------------  ------------------------------------------------------------------------------------------------------------------------
audit_tables        Comma-separated list of table names that should be audited, in the format ``<keyspace_name>.<table_name>``.

                    For Alternator tables use the ``alternator.<table_name>`` format (see :ref:`alternator-auditing`).
------------------  ------------------------------------------------------------------------------------------------------------------------
audit_keyspaces     Comma-separated list of keyspaces that should be audited. You must specify at least one keyspace.
                    If you leave this option empty, no keyspace will be audited.
==================  ========================================================================================================================

The default value and liveness of each option are listed in
:doc:`/reference/configuration-parameters`: see
:ref:`audit_categories <confprop_audit_categories>`,
:ref:`audit_tables <confprop_audit_tables>` and
:ref:`audit_keyspaces <confprop_audit_keyspaces>`.

To audit all the tables in a keyspace, set the ``audit_keyspaces`` with the keyspace you want to audit and leave ``audit_tables`` empty.

You can use DCL, AUTH, and ADMIN audit categories without including any keyspace or table.

audit_categories parameter description
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

=========  =========================================================================================  ====================
Parameter  Logs Description                                                                           Applies To
=========  =========================================================================================  ====================
AUTH       Logs login events                                                                           CQL
---------  -----------------------------------------------------------------------------------------  --------------------
DML        Logs insert, update, delete, and other data manipulation language (DML) events              CQL, Alternator
---------  -----------------------------------------------------------------------------------------  --------------------
DDL        Logs object and role create, alter, drop, and other data definition language (DDL) events   CQL, Alternator
---------  -----------------------------------------------------------------------------------------  --------------------
DCL        Logs grant, revoke, create role, drop role, and list roles events                           CQL
---------  -----------------------------------------------------------------------------------------  --------------------
QUERY      Logs all queries                                                                            CQL, Alternator
---------  -----------------------------------------------------------------------------------------  --------------------
ADMIN      Logs service level operations: create, alter, drop, attach, detach, list.                   CQL
           For :ref:`service level <workload-priorization-service-level-management>`
           auditing.
=========  =========================================================================================  ====================

For details on auditing Alternator operations, see :ref:`alternator-auditing`.

Note that enabling audit may negatively impact performance and audit-to-table may consume extra storage. That's especially true when auditing DML and QUERY categories, which generate a high volume of audit messages.

.. _alternator-auditing:

Auditing Alternator Requests
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When auditing is enabled, Alternator (DynamoDB-compatible API) requests are audited using the same
backends and filtering configuration as CQL operations. There are no separate Alternator audit settings.
The filtering configuration includes ``audit_categories``, ``audit_keyspaces``, ``audit_tables``,
and, for operations with table context, ``audit_rules``.

Both successful and failed Alternator requests are audited.

Alternator Operation Categories
""""""""""""""""""""""""""""""""

Each Alternator API operation is assigned to one of the standard audit categories:

=========  ====================================================================================================
Category   Alternator Operations
=========  ====================================================================================================
DDL        CreateTable, DeleteTable, UpdateTable, TagResource, UntagResource, UpdateTimeToLive
---------  ----------------------------------------------------------------------------------------------------
DML        PutItem, UpdateItem, DeleteItem, BatchWriteItem
---------  ----------------------------------------------------------------------------------------------------
QUERY      GetItem, BatchGetItem, Query, Scan, DescribeTable, ListTables, DescribeEndpoints,
           ListTagsOfResource, DescribeTimeToLive, DescribeContinuousBackups,
           ListStreams, DescribeStream, GetShardIterator, GetRecords
=========  ====================================================================================================

.. note:: AUTH, DCL, and ADMIN categories do not apply to Alternator operations. These categories
   are specific to CQL authentication, authorization, and service-level management.

Operation Field Format
"""""""""""""""""""""""

For CQL operations, the ``operation`` field in the audit log contains the raw CQL query string.
For Alternator operations, the format is:

.. code-block:: none

   <OperationName>|<JSON request body>

For example:

.. code-block:: none

   PutItem|{"TableName":"my_table","Item":{"p":{"S":"pk_val"},"c":{"S":"ck_val"},"v":{"S":"data"}}}

.. note:: The full JSON request body is included in the ``operation`` field. For batch operations
   (such as BatchWriteItem), this can be very large (up to 16 MB).

Keyspace and Table Filtering for Alternator
""""""""""""""""""""""""""""""""""""""""""""

The real keyspace name of an Alternator table ``T`` is ``alternator_T``.
The ``audit_tables`` config flag uses the shorthand format ``alternator.T`` to refer to such
tables -- the parser expands it to the real keyspace name automatically.
For ``audit_keyspaces``, use the real keyspace name directly.

For example, to audit an Alternator table called ``my_table_name`` use either of the below:

.. code-block:: yaml

   # Using audit_tables - use 'alternator' as the keyspace name:
   audit_tables: "alternator.my_table_name"

   # Using audit_keyspaces - use the real keyspace name:
   audit_keyspaces: "alternator_my_table_name"

**Global and batch operations**: Some Alternator operations are not scoped to a single table:

* ``ListTables`` and ``DescribeEndpoints`` have no associated keyspace or table.
  These operations are logged whenever their category matches ``audit_categories``, regardless of
  ``audit_keyspaces`` or ``audit_tables`` filters. Their ``keyspace_name`` field is empty.
* ``BatchWriteItem`` and ``BatchGetItem`` may span multiple tables. Therefore, the audit log
  entries for these operations have the ``keyspace_name`` field empty. These operations still
  match each table in the batch against ``audit_keyspaces``, ``audit_tables``, and table-qualified
  ``audit_rules`` filters. The ``table_name`` field in the audit entry contains only the audited
  tables, listed in pipe-separated (``|``) format. The JSON request in the audit entry's
  ``operation`` field is also stripped of entries belonging to non-audited tables, so that data
  from these tables is not leaked into audit records. If none of the tables in a batch match the
  audit filter, no audit entry is produced for that batch operation. When different tables in a
  batch match different audit sinks, each sink receives at most one audit entry with the tables
  matching that sink.

For example, given ``audit_tables: "alternator.my_table"`` or an ``audit_rules`` entry whose
``qualified_table_names`` match only ``alternator_my_table.*``, and a single ``BatchWriteItem`` that
targets both ``my_table`` and ``other_table``, the resulting audit entry will list only
``my_table`` in its ``table_name`` field -- ``other_table`` is filtered out. Also, the contents of
the JSON request body for ``other_table`` will not be present in the audit entry. If the same
``BatchWriteItem`` targeted only ``other_table``, no audit entry would be produced at all.

**DynamoDB Streams operations**: For streams-related operations (``DescribeStream``, ``GetShardIterator``,
``GetRecords``), the ``table_name`` field contains the base table name and the CDC log table name
separated by a pipe (e.g., ``my_table|my_table_scylla_cdc_log``).

Alternator Audit Log Examples
""""""""""""""""""""""""""""""

Syslog output example (PutItem):

.. code-block:: shell

   Mar 18 10:15:03 ip-10-143-2-108 scylla-audit[28387]: node="10.143.2.108", category="DML", cl="LOCAL_QUORUM", error="false", keyspace="alternator_my_table", query="PutItem|{\"TableName\":\"my_table\",\"Item\":{\"p\":{\"S\":\"pk_val\"}}}", client_ip="127.0.0.1", table="my_table", username="anonymous"

Table output example (PutItem):

.. code-block:: shell

   SELECT * FROM audit.audit_log ;

returns:

.. code-block:: none

    date                    | node         | event_time                           | category | consistency  | error | keyspace_name         | operation                                                                        | source    | table_name | username  |
   -------------------------+--------------+--------------------------------------+----------+--------------+-------+-----------------------+----------------------------------------------------------------------------------+-----------+------------+-----------+
   2026-03-18 00:00:00+0000 | 10.143.2.108 | 3429b1a5-2a94-11e8-8f4e-000000000001 |      DML | LOCAL_QUORUM | False | alternator_my_table   | PutItem|{"TableName":"my_table","Item":{"p":{"S":"pk_val"}}}                     | 127.0.0.1 |   my_table | anonymous |
   (1 row)

Audit Rules
^^^^^^^^^^^^^

Audit rules provide fine-grained, role-aware auditing. They can be used together
with ``audit_categories``, ``audit_tables``, and ``audit_keyspaces`` to extend an
existing audit configuration with more granular rules. The settings do not
override each other: for each event, ScyllaDB evaluates both mechanisms
independently, and the event is audited if either one matches.

For new configurations, prefer ``audit_rules`` because they provide more precise
matching options, including table patterns and role patterns.

Each rule contains the following fields:

- **sinks** — target sinks for matched events (e.g. ``table``, ``syslog``).
  Must be a subset of the global ``audit`` setting; a rule referencing a sink
  not enabled globally will log an error and events for that sink will not be
  written.
- **categories** — which operation types to audit: ``DML``, ``DDL``,
  ``QUERY``, ``AUTH``, ``ADMIN``, ``DCL``. An empty list matches nothing.
- **qualified_table_names** — ``keyspace.table`` patterns to match. For
  table-independent categories (``AUTH``, ``ADMIN``, ``DCL``) this field is
  ignored. An empty list prevents matching for table-scoped categories
  (``DML``, ``DDL``, ``QUERY``).
- **roles** — role patterns to match. An empty list matches nothing.

The ``qualified_table_names`` and ``roles`` fields support fnmatch glob patterns
with extended syntax (``FNM_EXTMATCH``), including negation ``!(…)``,
alternation ``@(a|b)``, and quantifiers ``+(…)``, ``*(…)``, ``?(…)``. For
example, ``"prod_ks.*"`` or ``"!(system).*"`` for tables and ``"admin_*"`` for
roles.

.. note::

   A login attempt that establishes no role name, such as one whose client
   certificate was rejected, is audited with an empty user name. ``roles: ["*"]``
   and ``roles: [""]`` match it; a rule listing only specific patterns, such as
   ``["admin_*"]``, does not record such attempts.

``audit_rules`` is a live-updatable parameter. To apply changes at runtime, edit
``scylla.yaml`` and send ``SIGHUP`` to the ScyllaDB process. See
:doc:`/reference/configuration-parameters` for details on live updates and
configuration precedence.

Example ``scylla.yaml`` configuration:

.. code-block:: yaml

   audit_rules:
     - sinks: [table]
       categories: [DML, DDL]
       qualified_table_names: ["prod_ks.*"]
       roles: ["admin_*"]
     - sinks: [syslog]
       categories: [AUTH]
       qualified_table_names: []
       roles: ["*"]

Configuring Audit Storage
---------------------------

Auditing messages can be sent to :ref:`Syslog <auditing-syslog-storage>` or stored in a ScyllaDB :ref:`table <auditing-table-storage>` or both.

.. _auditing-syslog-storage:

Storing Audit Messages in Syslog
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Procedure**

#. Set the ``audit`` parameter in the ``scylla.yaml`` file to ``syslog``.

   For example:

   .. code-block:: shell

      # audit setting
      # 'audit' config option controls if and where to output audited events:
      audit: "syslog"
      #
      # List of statement categories that should be audited.
      audit_categories: "DCL,DDL,AUTH"
      # 
      # List of tables that should be audited.
      audit_tables: "mykespace.mytable"
      #
      # List of keyspaces that should be fully audited.
      # All tables in those keyspaces will be audited
      audit_keyspaces: "mykespace"

#. Restart the ScyllaDB node.

.. include:: /rst_include/scylla-commands-restart-index.rst

By default, audit messages are written to the same destination as ScyllaDB :doc:`logging </getting-started/logging>`, with ``scylla-audit`` as the process name.

Logging output example (CQL drop table):

.. code-block:: shell

   Mar 18 09:53:52 ip-10-143-2-108 scylla-audit[28387]: node="10.143.2.108", category="DDL", cl="ONE", error="false", keyspace="nba", query="DROP TABLE nba.team_roster ;", client_ip="127.0.0.1", table="team_roster", username="anonymous"

To redirect the Syslog output to a file, follow the steps below (available only for CentOS) :

#. Install rsyslog sudo ``dnf install rsyslog``.
#. Edit ``/etc/rsyslog.conf`` and append the following to the file: ``if $programname contains 'scylla-audit' then /var/log/scylla-audit.log``.
#. Start rsyslog ``systemctl start rsyslog``.
#. Enable rsyslog ``systemctl enable rsyslog``.

.. _auditing-table-storage:

Storing Audit Messages in a Table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Messages are stored in a ScyllaDB table named ``audit.audit_log``. 

For example:

.. code-block:: shell   
   
   CREATE TABLE IF NOT EXISTS audit.audit_log (
         date timestamp,
         node inet,
         event_time timeuuid,
         category text,
         consistency text,
         table_name text,
         keyspace_name text,
         operation text,
         source inet,
         username text,
         error boolean,
         PRIMARY KEY ((date, node), event_time));

.. note:: The schema of ``audit.audit_log`` has been migrated in the 2024.2 version from ``SimpleStrategy RF=1`` to ``NetworkTopologyStrategy RF=3``:

   * On vnodes, by default every DC will contain 3 audit replicas. If a new DC is added, in order for it to also contain audit replicas, audit's schema has to be manually altered.
     On clusters where new keyspaces use tablets, the replication is managed automatically, see :ref:`audit-table-replication`.
   * CL for writes is still equal to ``1``, which implies that reading audit rows with CL=Quorum may fail, which is especially true for clusters with less than 3 nodes.

**Procedure**

#. Set the ``audit`` parameter in the ``scylla.yaml`` file to ``table``.

   For example:

   .. code-block:: shell

      # audit setting
      # 'audit' config option controls if and where to output audited events:
      audit: "table"
      #
      # List of statement categories that should be audited.
      audit_categories: "DCL,DDL,AUTH"
      # 
      # List of tables that should be audited.
      audit_tables: "mykespace.mytable"
      #
      # List of keyspaces that should be fully audited.
      # All tables in those keyspaces will be audited
      audit_keyspaces: "mykespace"

#. Restart the ScyllaDB node.

   .. include:: /rst_include/scylla-commands-restart-index.rst

   Table output example (CQL drop table):

   .. code-block:: shell

      SELECT * FROM audit.audit_log ;

   returns:

   .. code-block:: none

       date                    | node         | event_time                           | category | consistency | error | keyspace_name | operation                    | source          | table_name  | username |
      -------------------------+--------------+--------------------------------------+----------+-------------+-------+---------------+------------------------------+-----------------+-------------+----------+
      2018-03-18 00:00:00+0000 | 10.143.2.108 | 3429b1a5-2a94-11e8-8f4e-000000000001 |      DDL |         ONE | False |           nba | DROP TABLE nba.team_roster ; | 127.0.0.1       | team_roster | Scylla   | 
      (1 row)

.. _auditing-table-and-syslog-storage:

Storing Audit Messages in a Table and Syslog Simultaneously
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Procedure**

#. Follow both procedures from above, and set the ``audit`` parameter in the ``scylla.yaml`` file to both ``syslog`` and ``table``. You need to restart ScyllaDB only once.

   To have both syslog and table you need to specify both backends separated by a comma:

   .. code-block:: shell

      audit: "syslog,table"


.. _audit-table-replication:

Replication of the Audit Table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``audit`` keyspace is created by ScyllaDB when a node starts with table auditing enabled
(``audit: table``, the default), if it does not exist yet. How it is replicated depends on whether
new keyspaces use tablets by default (``tablets_mode_for_new_keyspaces``, enabled in the shipped
``scylla.yaml``).

Clusters where new keyspaces use vnodes
"""""""""""""""""""""""""""""""""""""""

The keyspace uses vnodes with ``NetworkTopologyStrategy`` and a replication factor of 3 in every
data center, including a data center of zero-token nodes only. Its replication is yours to change.

The same applies to a cluster upgraded from a release where the ``audit`` keyspace already existed
on vnodes, and to an ``audit`` keyspace created while the cluster was being upgraded from a release
without automatic replication: the keyspace stays on vnodes, with the replication it has.

Clusters where new keyspaces use tablets
""""""""""""""""""""""""""""""""""""""""

The keyspace uses tablets and ScyllaDB manages its replication. It follows the other tablets
keyspaces of the cluster, which are called the *application keyspaces* below. The keyspace starts
with a single replica in each data center that has token-owning nodes; a data center of zero-token
nodes only, such as an arbiter data center, is skipped. From then on, in each data center:

* **Where the application keyspaces use rack lists**, or where ``rf_rack_valid_keyspaces`` or
  ``enforce_rack_list`` is enabled, the audit table uses a rack list. A rack becomes *eligible* once
  an application keyspace replicates to it (a numeric replication factor makes every rack of the
  data center eligible). The audit table is extended into eligible racks, one rack at a time, up to
  three racks, and a rack that is no longer eligible is given up again, one rack at a time.
* **Where the application keyspaces all use numeric replication factors** and neither option is
  enabled (the defaults), the audit table keeps a numeric replication factor too: the largest one of
  the application keyspaces in the data center, at most three, and at most the number of racks.
  It follows that replication factor up and down, one step at a time. A numeric replication factor
  names no rack, so the audit table can always leave a rack the application keyspaces can leave.
* The audit table is extended into eligible data centers it does not replicate to yet. A data
  center is never given up: once no application keyspace uses it, the audit table keeps a single
  replica there. Nothing is given up while no application keyspace exists at all.

Use ``DESCRIBE KEYSPACE audit`` to see the current replication. Every step is an ordinary
replication factor change: its tablets are rebuilt on the new replica (a repair followed by
streaming), which the tablet load balancer carries out along with its other migrations. The
steps appear in the task manager's ``global_topology_requests`` module as ``keyspace_rf_change``
tasks for the ``audit`` keyspace. With a large audit table (for example, with the ``DML`` or
``QUERY`` categories audited) a step takes correspondingly longer.

The automatic management pauses:

* while tablet load balancing is disabled (the ``storage_service/tablets/balancing`` REST API), and
* while a node of the cluster is down, unless that node has been excluded.

Whether rack lists are required is read from the configuration of the node that coordinates
topology changes, so set ``rf_rack_valid_keyspaces`` and ``enforce_rack_list`` on every node.

Changing the replication by hand
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Manual changes are kept as long as they agree with the rules above, and undone where they do not:

* A rack that no application keyspace uses is removed again.
* While the audit table is below its goal in an eligible data center, it is extended again (with a
  rack list, not necessarily onto the rack you removed).
* Racks above the goal, within the eligible racks, are kept. A numeric replication factor above the
  goal is kept as long as it is not above the largest one of the application keyspaces in the
  data center, nor above the number of racks.
* A data center you remove (``'<dc>': 0``, or ``'<dc>': []`` for a rack list) stays removed as long
  as no application keyspace replicates to it; while one does, it is added back.

A manual ``ALTER KEYSPACE audit`` is rejected while one of the automatic steps is in progress
(``Another RF change for this keyspace ... ongoing``); retry it once the step has finished. As for
any tablets keyspace, list every data center in the statement and change one rack, or the numeric
replication factor by one, at a time, for example:

.. code-block:: cql

   ALTER KEYSPACE audit WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': ['rack1', 'rack2'], 'dc2': ['rack4']};

Things to keep in mind
~~~~~~~~~~~~~~~~~~~~~~

* Until the application keyspaces use more than one replica, or make more than one rack eligible,
  in a data center, the audit table has a single replica there. An audit record whose replica node
  is down at that moment is dropped, and the node reports it, for example: ``audit - Unexpected
  exception when writing login log ... Cannot achieve consistency level for cl ONE``. Create the
  application keyspaces before relying on the audit table's durability.
* The replication of the audit table is only ever as wide as the application keyspaces make it. A
  cluster whose application keyspaces are all on vnodes leaves the audit table at a single replica.
* While a step that adds a replica runs, reads of ``audit.audit_log`` at ``QUORUM`` or ``ALL`` can
  fail with ``Unavailable``: the consistency level already counts the new replica, which cannot
  serve reads yet. Retry such reads, or read at ``ONE``.
* To remove the nodes of a lost rack, exclude them first, then drop the rack from the application
  keyspaces, then remove the nodes. With a rack list, the audit table gives the rack up in between.
* Dropping a rack from the application keyspaces and decommissioning its last node right away
  works with ``rf_rack_valid_keyspaces`` enabled: the decommission waits, up to two minutes, for the
  audit table to give the rack up. Without that option the decommission is accepted and then fails
  while draining the node if the audit table still replicates there; retry it once the rack has
  been given up.
* Enabling ``rf_rack_valid_keyspaces`` or ``enforce_rack_list`` later converts the audit table to
  rack lists of the same size. Nodes started with either option let the audit and ``system_traces``
  keyspaces through the startup checks, with a warning, until that has happened.
* Adding or removing a data center: see
  :doc:`Add a Data Center </operating-scylla/procedures/cluster-management/add-dc-to-existing-dc>`
  and :doc:`Remove a Data Center </operating-scylla/procedures/cluster-management/decommissioning-data-center>`.

.. _audit-table-replication-limitation:

Known limitation: mixed numeric and rack-list replication
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In a data center where the application keyspaces mix numeric replication factors and rack lists,
every rack is eligible and the audit table uses a rack list, so it never gives a rack up on its
own. Decommissioning or removing all nodes of a rack it lists is then blocked: the operation is
rejected as RF-rack-invalid, or fails while draining the last node. To remove such a rack:

#. Disable tablet load balancing, which pauses the automatic management.
#. Remove the rack from the rack lists of the ``audit`` and ``system_traces`` keyspaces with
   ``ALTER KEYSPACE``, one rack per statement.
#. Remove the nodes.
#. Enable tablet load balancing again.

Alternatively, leave one node of the rack in the cluster.


Handling Audit Failures
---------------------------

In some cases, auditing may not be possible, for example, when:

* A table is used as the audit’s backend, and the partitions where the audit rows are saved are unavailable because the nodes holding those partitions are down or unreachable due to network issues.
* Syslog is used as the audit’s backend, and the Syslog sink (a regular Unix socket) is unresponsive or unavailable.

If the audit fails and audit messages are not stored in the configured audit’s backend, you can still review the audit log in the regular ScyllaDB logs.

The following example shows audit information in the regular ScyllaDB logs in the case when the Syslog backend is broken (for example, because the socket was closed) and you tried to connect to a node with incorrect credentials:

   .. code-block:: shell

      ERROR 2024-01-15 14:09:41,516 [shard 0:sl:d] audit - Unexpected exception when writing login log with: node_ip <IP:port> client_ip <IP:port> username <username> error true exception audit::audit_exception (Starting syslog audit backend failed (sending a message to <socket_path> resulted in sendto: No such file or directory).)

Additional Resources
-----------------------------------

* :doc:`Authorization</operating-scylla/security/authorization>` 

* :doc:`Authentication</operating-scylla/security/authentication>` 




