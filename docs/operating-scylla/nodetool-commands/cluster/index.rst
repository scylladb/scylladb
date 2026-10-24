Nodetool cluster
================

.. toctree::
   :hidden:

   repair <repair>
   compact <compact>
   cleanup <cleanup>

**cluster** - Nodetool supercommand for running cluster operations.

Supported cluster suboperations
-------------------------------

* :doc:`repair </operating-scylla/nodetool-commands/cluster/repair>`  :code:`<keyspace>` :code:`<table>` - Repair one or more tablet tables.
* :doc:`compact </operating-scylla/nodetool-commands/cluster/compact>`  :code:`<keyspace>` :code:`<table>` - Compact one or more tablet tables, tablet by tablet.
* :doc:`cleanup </operating-scylla/nodetool-commands/cluster/cleanup>`  - Clean up all non tablet (vnode-based) keyspaces in a cluster
