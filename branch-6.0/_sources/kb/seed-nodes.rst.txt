=================
Scylla Seed Nodes
=================

**Topic: Scylla Seed Nodes Overview**

**Learn: What a seed node is, and how they should be used in a Scylla Cluster**

**Audience: Scylla Administrators**


What is the Function of a Seed Node in Scylla?
----------------------------------------------

.. note:: 
    Seed nodes function was changed in Scylla Open Source 4.3 and Scylla Enterprise 2021.1; if you are running an older version, see :ref:`Older Version Of Scylla <seeds-older-versions>`.

A Scylla seed node is a node specified with the ``seeds`` configuration parameter in ``scylla.yaml``. It is used by new node joining as the first contact point.
It allows nodes to discover the cluster ring topology on startup (when joining the cluster). This means that any time a node is joining the cluster, it needs to learn the cluster ring topology, meaning:

-  What are the IPs of the nodes in the cluster are
-  Which token ranges are available
-  Which nodes will own which tokens when a new node joins the cluster

**Once the nodes have joined the cluster, seed node has no function.**
     
The first node in a new cluster needs to be a seed node.

.. _seeds-older-versions:

Older Version Of Scylla
----------------------------

In Scylla releases older than Scylla Open Source 4.3 and Scylla Enterprise 2021.1, seed node has one more function: it assists with :doc:`gossip </kb/gossip>` convergence.
Gossiping with other nodes ensures that any update to the cluster is propagated across the cluster. This includes detecting and alerting whenever a node goes down, comes back, or is removed from the cluster.

This functions was removed, as described in `Seedless NoSQL: Getting Rid of Seed Nodes in ScyllaDB <https://www.scylladb.com/2020/09/22/seedless-nosql-getting-rid-of-seed-nodes-in-scylla/>`_.

If you run an older Scylla release, we recommend upgrading to version 4.3 (Scylla Open Source) or 2021.1 (Scylla Enterprise) or later. If you choose to run an older version, it is good practice to follow these guidelines:

* The first node in a new cluster needs to be a seed node.
* Ensure that all nodes in the cluster have the same seed nodes listed in each node's scylla.yaml.
* To maintain resiliency of the cluster, it is recommended to have more than one seed node in the cluster.
* If you have more than one seed in a DC with multiple racks (or availability zones), make sure to put your seeds in different racks.
* You must have at least one node that is not a seed node. You cannot create a cluster where all nodes are seed nodes.
* You should have more than one seed node.


