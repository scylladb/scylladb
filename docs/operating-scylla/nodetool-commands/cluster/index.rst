Nodetool cluster
================

.. toctree::
   :hidden:

   repair <repair>
   cleanup <cleanup>
   snapshot <snapshot>
   backup <backup>

**cluster** - Nodetool supercommand for running cluster operations.

Supported cluster suboperations
-------------------------------

* :doc:`repair </operating-scylla/nodetool-commands/cluster/repair>`  :code:`<keyspace>` :code:`<table>` - Repair one or more tablet tables.
* :doc:`cleanup </operating-scylla/nodetool-commands/cluster/cleanup>`  - Clean up all non tablet (vnode-based) keyspaces in a cluster
<<<<<<< HEAD
||||||| parent of 261f5063dd (docs, tools: add nodetool cluster snapshot and cluster backup pages)
* :doc:`snapshot </operating-scylla/nodetool-commands/snapshot>` - Take a cluster-wide snapshot of keyspaces or tables.
* :doc:`backup </operating-scylla/nodetool-commands/backup>` - Back up a cluster-wide snapshot to object storage.
=======
* :doc:`snapshot </operating-scylla/nodetool-commands/cluster/snapshot>`  - Take a cluster-wide snapshot of one or more keyspaces or tables.
* :doc:`backup </operating-scylla/nodetool-commands/cluster/backup>`  - Back up a cluster-wide snapshot to object storage.
>>>>>>> 261f5063dd (docs, tools: add nodetool cluster snapshot and cluster backup pages)
