Nodetool cluster cleanup
========================

**cluster cleanup** - A process that runs in the background and removes data no longer owned by nodes. Used for non tablet (vnode-based) tables only.

Running ``cluster cleanup`` on a **single node** cleans up all non tablet tables on all nodes in the cluster (tablet enabled tables are cleaned up automatically).


  For example:

  ::

     nodetool cluster cleanup

See also `ScyllaDB Manager <https://manager.docs.scylladb.com/>`_.
