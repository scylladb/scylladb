Nodetool cleanup
================
**cleanup** ``[<keyspace> <tablename ...>]``- triggers the immediate removal of data from node(s) that "lose" part of their token range due to a range movement operation (node addition or node replacement).

You should run nodetool cleanup whenever you scale-out (expand) your cluster, and new nodes are added to the same DC. The scale out process causes the token ring to get re-distributed. As a result, some of the nodes will have replicas for tokens that they are no longer responsible for (taking up disk space). This data continues to consume diskspace until you run nodetool cleanup. The cleanup operation deletes these replicas and frees up disk space.

In addition, the following should be noted:

- An optional keyspace and column family (table) can be specified to restrict the cleanup action. 
- If no keyspace is specified, it will perform cleanup in all keyspaces
- There is no need to run cleanup when nodes are being removed permanently

For example:

To clean up the data of a specific node and specific keyspace, use this command:

.. code-block:: shell

   nodetool -h <host name> cleanup <keyspace>

.. warning::

   Make sure there are no topology changes before running cleanup. To validate, run ``nodetool status``, all nodes should be in status Up Normal (``UN``).

.. include:: nodetool-index.rst
