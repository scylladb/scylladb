===============
Virtual Tables
===============

Virtual tables expose system-level information in the familiar CQL or Alternator interfaces.
Virtual tables are not backed by physical storage (sstables) and generate their content on-the-fly when 
queried. 

ScyllaDB supports:

* Virtual tables for retrieving system-level information, such as the cluster status, version-related information, etc. The range of information they can expose partially overlaps with the information you can obtain via :doc:`nodetool </operating-scylla/nodetool>` (unlike ``nodetool``, virtual tables permit remote access over CQL).
* The virtual table for querying and updating configuration over CQL (the ``system.config`` table).

See `Virtual tables in the system keyspace <https://github.com/scylladb/scylla/blob/branch-5.0/docs/design-notes/system_keyspace.md#virtual-tables-in-the-system-keyspace>`_ for the list of available virtual tables.

