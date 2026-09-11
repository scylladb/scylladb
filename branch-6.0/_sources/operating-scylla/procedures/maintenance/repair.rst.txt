==============
Scylla Repair
==============

During the regular operation, a Scylla cluster continues to function and remains ‘always-on’ even in the face of failures such as:

* A down node
* A network partition
* Complete datacenter failure
* Dropped mutations due to timeouts
* Process crashes (before a flush)
* A replica that cannot write due to a lack of resources

As long as the cluster can satisfy the required consistency level (usually quorum), availability and consistency will be maintained. However, in order to automatically mitigate data inconsistency (entropy), Scylla uses three processes:

* :doc:`Hinted Handoff </architecture/anti-entropy/hinted-handoff>`
* :doc:`Read Repair </architecture/anti-entropy/read-repair>`
* :doc:`Repair Based Node Operations </operating-scylla/procedures/cluster-management/repair-based-node-operation>`
* Repair - described in the following sections

Repair Overview
----------------

Data stored on nodes may become inconsistent with other replicas over time. For this reason, repairs are a necessary part of database maintenance.

Scylla repair is a process that runs in the background and synchronizes the data between nodes so that all the replicas hold the same data.
Running repairs is necessary to ensure that data on a given node is consistent with the other nodes in the cluster. 
You can manually run the ``nodetool repair`` command or schedule repair with `Scylla Manager <https://manager.docs.scylladb.com/stable/repair>`_, 
which can run repairs for you.

.. note:: Run the :doc:`nodetool repair </operating-scylla/nodetool-commands/repair/>` command regularly. If you delete data frequently, it should be more often than the value of ``gc_grace_seconds`` (by default: 10 days), for example, every week. Use the **nodetool repair -pr** on each node in the cluster, sequentially.

In most cases, the proportion of data that is out of sync is very small.  In a few cases, for example, if a node was down for a day, the difference might be more significant.

.. _row-level-repair:

Row-level Repair
----------------

ScyllaDB uses row-level repair.

Row-level repair improves Scylla in two ways:

* Minimizes data transfer. With row-level repair, Scylla calculates the checksum for each row and uses set reconciliation algorithms to find the mismatches between nodes. As a result, only the mismatched rows are exchanged, which eliminates unnecessary data transmission over the network.

* Minimize disk reads by :

  * reading the data only once.
  * keeping the data in a temporary buffer.
  * using the cached data to calculate the checksum and send it to the replicas.

See also

* `Scylla Manager documentation <https://manager.docs.scylladb.com/>`_

* `Blog: Scylla Open Source 3.1: Efficiently Maintaining Consistency with Row-Level Repair <https://www.scylladb.com/2019/08/13/scylla-open-source-3-1-efficiently-maintaining-consistency-with-row-level-repair/>`_


