========================
ScyllaDB Auditing Guide
========================

Auditing allows the administrator to monitor activities on a ScyllaDB cluster, including CQL queries and data changes, as well as Alternator (DynamoDB-compatible API) requests.
The information is stored in a Syslog, a ScyllaDB table, or the process standard output.

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
* ``stdout`` - Audit is enabled, and messages are written to the process standard output.

Multiple backends can be combined with a comma, for example ``syslog,table`` or ``stdout,table``.

Configuring any other value results in an error at ScyllaDB startup.

Configuring Audit
-----------------

The audit can be tuned using the following flags or ``scylla.yaml`` entries:

==================  ==================================  ========================================================================================================================
Flag                Default Value                       Description
==================  ==================================  ========================================================================================================================
audit_categories    "DCL,AUTH,ADMIN"                                  Comma-separated list of statement categories that should be audited
------------------  ----------------------------------  ------------------------------------------------------------------------------------------------------------------------
audit_tables        “”                                  Comma-separated list of table names that should be audited, in the format ``<keyspace_name>.<table_name>``.
                                                        
                                                        For Alternator tables use the ``alternator.<table_name>`` format (see :ref:`alternator-auditing`).
------------------  ----------------------------------  ------------------------------------------------------------------------------------------------------------------------
audit_keyspaces     “”                                  Comma-separated list of keyspaces that should be audited. You must specify at least one keyspace.
                                                        If you leave this option empty, no keyspace will be audited.
==================  ==================================  ========================================================================================================================

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
backends and the same filtering configuration (``audit_categories``, ``audit_keyspaces``,
``audit_tables``) as CQL operations. No additional configuration is needed.

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
* ``BatchWriteItem`` and ``BatchGetItem`` may span multiple tables.

These operations are logged whenever their category matches ``audit_categories``, regardless of
``audit_keyspaces`` or ``audit_tables`` filters. Their ``keyspace_name`` field is empty, and for
batch operations the ``table_name`` field contains a pipe-separated (``|``) list of all involved table names.

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

Auditing messages can be sent to :ref:`Syslog <auditing-syslog-storage>`, stored in a ScyllaDB :ref:`table <auditing-table-storage>`, written to :ref:`stdout <auditing-stdout-storage>`, or any combination of these.

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

.. _auditing-stdout-storage:

Storing Audit Messages in Stdout
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The stdout backend writes audit events to the process standard output. It is
intended for containerised deployments (Kubernetes, Docker) where the container
runtime captures stdout and forwards it to a centralised log aggregator such as
FluentBit, Vector, or Loki. It avoids the need for a syslog daemon or a
privileged ``/dev/log`` hostPath mount.

.. note:: The stdout backend requires the process standard output to be a pipe
   or a character device (a terminal) — which is what a container runtime
   provides. Redirecting Scylla's stdout to a regular file (for example
   ``scylla > audit.log``) or to a socket is **not** supported: the node will
   fail to start with an explanatory error. Use the ``syslog`` or ``table``
   backend if you need to persist audit events to a file.

.. note:: In a non-root container, the stdout pipe may be created by the
   container runtime under a different UID. In that case Scylla can write to the
   inherited stdout but may be unable to re-open it, and the node will fail to
   start with an actionable error. If you hit this, run Scylla as the owner of
   the stdout pipe, or use the ``syslog``/``table`` backend instead.

**Procedure**

#. Set the ``audit`` parameter in the ``scylla.yaml`` file to ``stdout``.

   For example:

   .. code-block:: shell

      # audit setting
      # 'audit' config option controls if and where to output audited events:
      audit: "stdout"
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

#. Restart the Scylla node.

.. include:: /rst_include/scylla-commands-restart-index.rst

Audit messages are written directly to the process standard output with
``scylla-audit`` as an identifier.

Logging output example (drop table):

.. code-block:: shell

   Mar 18 09:53:52 scylla-audit: node="10.143.2.108", category="DDL", cl="ONE", error="false", keyspace="nba", query="DROP TABLE nba.team_roster ;", client_ip="127.0.0.1", table="team_roster", username="anonymous"

.. note:: The stdout backend does not JSON-escape field values. Embedded
   quotes or newlines in CQL queries can break the ``key="value"`` layout.
   If your log pipeline requires strict JSON, consider post-processing the
   output or using the syslog backend instead.

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

   * By default every DC will contain 3 audit replicas. If a new DC is added, in order for it to also contain audit replicas, audit's schema has to be manually altered.
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



Handling Audit Failures
---------------------------

In some cases, auditing may not be possible, for example, when:

* A table is used as the audit’s backend, and the partitions where the audit rows are saved are unavailable because the nodes holding those partitions are down or unreachable due to network issues.
* Syslog is used as the audit’s backend, and the Syslog sink (a regular Unix socket) is unresponsive or unavailable.

* Stdout is used as the audit’s backend, and the standard output file descriptor is closed or the pipe is broken (for example, the container runtime terminated the log collector).

If the audit fails and audit messages are not stored in the configured audit’s backend, you can still review the audit log in the regular ScyllaDB logs.

The following example shows audit information in the regular ScyllaDB logs in the case when the Syslog backend is broken (for example, because the socket was closed) and you tried to connect to a node with incorrect credentials:

   .. code-block:: shell

      ERROR 2024-01-15 14:09:41,516 [shard 0:sl:d] audit - Unexpected exception when writing login log with: node_ip <IP:port> client_ip <IP:port> username <username> error true exception audit::audit_exception (Starting syslog audit backend failed (sending a message to <socket_path> resulted in sendto: No such file or directory).)

Additional Resources
-----------------------------------

* :doc:`Authorization</operating-scylla/security/authorization>` 

* :doc:`Authentication</operating-scylla/security/authentication>` 







