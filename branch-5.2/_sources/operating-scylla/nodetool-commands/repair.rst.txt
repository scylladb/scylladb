Nodetool repair
===============

**Repair** - a process that runs in the background and synchronizes the data between nodes.

When running ``nodetool repair`` on a **single node**, it acts as the **repair master**. Only the data contained in the master node and its replications will be repaired.
Typically, this subset of data is replicated on many nodes in the cluster, often all, and the repair process syncs between all the replicas until the master data subset is in-sync.

To repair **all** of the data in the cluster, you need to run a repair on **all** of the nodes in the cluster, or let `ScyllaDB Manager <https://manager.docs.scylladb.com/>`_ do it for you.

.. note:: Run the :doc:`nodetool repair </operating-scylla/nodetool-commands/repair/>` command regularly. If you delete data frequently, it should be more often than the value of ``gc_grace_seconds`` (by default: 10 days), for example, every week. Use the **nodetool repair -pr** on each node in the cluster, sequentially.

  
.. warning::

  It is strongly recommended to **not** do **any** maintenance operations (add/remove/decommission/replace/rebuild) **or** schema changes (CREATE/DROP/TRUNCATE/ALTER CQL commands) while repairs are running. Repairs running during any of these operations are likely ro result in an error.


Scylla nodetool repair command supports the following options:
                                                                                                         

- ``-dc`` ``--in-dc`` syncs the **repair master** data subset between all nodes in one Data Center (DC). 

  The DC *must* be the **repair master** DC.


  .. warning:: This command leaves part of the data subset (all remaining DCs) out of sync.

     For example: 

     ::

        nodetool repair -dc US_DC
        nodetool repair --in-dc US_DC

- ``-et`` ``--end-token`` executes a repair on the specified node/s starting with the first token and ending with the specified UUID token. 

  For example: 

  ::

     nodetool repair -et 90874935784
     nodetool repair --end-token 90874935784
     
- ``-hosts`` ``--in-hosts`` syncs the **repair master** data subset only between a list of nodes, using host ID or Address. The list *must* include the **repair master**.

  .. warning:: this command leaves part of the data subset (on nodes that are *not* listed) out of sync.
  
  For example: 

  ::

     nodetool repair -hosts 172.17.0.2,172.17.0.3,172.17.0.4
     nodetool repair --in-hosts 172.17.0.2,172.17.0.3,172.17.0.4
     nodetool repair --in-hosts cdc295d7-c076-4b07-af69-1385fefdb40b,2dbdf288-9e73-11ea-bb37-0242ac130002,3a5993f8-9e73-11ea-bb37-0242ac130002

- ``-local`` ``--in-local-dc`` executes a repair on the nodes in the local datacenter only.                                                                                    

  For example: 

  ::

     nodetool repair -local
     nodetool repair --in-local-dc

- ``-pr`` ``--partitioner-range`` executes a repair only on the primary replica returned by the partitioner.                                                                          

  For example: 
  
  ::

     nodetool repair -pr
     nodetool repair --partitioner-range

- ``-st`` ``--start-token`` executes a repair on a range of node/s starting with the start token (the token specified).                                                               

  For example: 

  ::

     nodetool repair -st 10474535988
     nodetool repair --start-token

- ``keyspace`` executes a repair on a specific keyspace. The default is all keyspaces.   

  For example: 

  ::

     nodetool repair <my_keyspace>


- ``table`` executes a repair on a specific table or a list of space-delimited table names. The default is all tables.


  For example: 

  
  ::

     nodetool repair <my_keyspace> <my_table>

See also `ScyllaDB Manager <https://manager.docs.scylladb.com/>`_.

.. include:: nodetool-index.rst
