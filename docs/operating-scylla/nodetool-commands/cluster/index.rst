Nodetool cluster
================

.. toctree::
   :hidden:

   repair <repair>
   compact <compact>
   cleanup <cleanup>
   snapshot <snapshot>
   backup <backup>

**cluster** - Nodetool supercommand for running cluster operations.

Supported cluster suboperations
-------------------------------

* :doc:`repair </operating-scylla/nodetool-commands/cluster/repair>`  :code:`<keyspace>` :code:`<table>` - Repair one or more tablet tables.
* :doc:`compact </operating-scylla/nodetool-commands/cluster/compact>`  :code:`<keyspace>` :code:`<table>` - Compact one or more tablet tables, tablet by tablet.
* :doc:`cleanup </operating-scylla/nodetool-commands/cluster/cleanup>`  - Clean up all non tablet (vnode-based) keyspaces in a cluster
* :doc:`snapshot </operating-scylla/nodetool-commands/cluster/snapshot>`  - Take a cluster-wide snapshot of one or more keyspaces or tables.
* :doc:`backup </operating-scylla/nodetool-commands/cluster/backup>`  - Back up a cluster-wide snapshot to object storage.
