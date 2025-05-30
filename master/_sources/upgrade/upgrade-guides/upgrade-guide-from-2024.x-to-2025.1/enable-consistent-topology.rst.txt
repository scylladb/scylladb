=====================================
Enable Consistent Topology Updates
=====================================

.. note::

    The following procedure only applies if:

    * You're upgrading **from ScyllaDB Enterprise 2024.1** to ScyllaDB 2025.1.
    * You previously upgraded from 2024.1 to 2024.2 without enabling consistent
      topology updates (see the `2024.2 upgrade guide <https://enterprise.docs.scylladb.com/branch-2024.2/upgrade/upgrade-enterprise/upgrade-guide-from-2024.1-to-2024.2/enable-consistent-topology.html>`_
      for reference). 

Introduction
============

ScyllaDB 2025.1 has :ref:`consistent topology changes based on Raft <raft-topology-changes>`.
Clusters created with version 2025.1 use consistent topology changes right
from the start. However, consistent topology changes are *not* automatically
enabled in clusters upgraded from version 2024.1. In such clusters, you need to
enable consistent topology changes manually by following the procedure described in this article.

Before you start, you **must** check that the cluster meets the prerequisites
and ensure that some administrative procedures will not be run while
the procedure is in progress.

.. _enable-raft-topology-2025.1-prerequisites:

Prerequisites
=============

* Make sure that all nodes in the cluster are upgraded to ScyllaDB 2025.1.
* Verify that :ref:`schema on raft is enabled <schema-on-raft-enabled>`.
* Make sure that all nodes enabled ``SUPPORTS_CONSISTENT_TOPOLOGY_CHANGES`` cluster feature.
  One way to verify it is to look for the following message in the log:

  .. code-block:: none

    features - Feature SUPPORTS_CONSISTENT_TOPOLOGY_CHANGES is enabled

  Alternatively, it can be verified programmatically by checking whether the ``value``
  column under the ``enabled_features`` key contains the name of the feature in
  the ``system.scylla_local`` table. One way to do it is with the following bash script:

  .. code-block:: bash

    until cqlsh -e "select value from system.scylla_local where key = 'enabled_features'" | grep "SUPPORTS_CONSISTENT_TOPOLOGY_CHANGES"
    do
        echo "Upgrade didn't finish yet on the local node, waiting 10 seconds before checking again..."
        sleep 10
    done
    echo "Upgrade completed on the local node"

* Make sure that all nodes are alive for the duration of the procedure.

.. _enable-raft-topology-2025.1-forbidden-operations:

Administrative operations that must not be running during the procedure
=========================================================================

Make sure that administrative operations will not be running while
the procedure is in progress. In particular, you must abstain from:

* :doc:`Cluster management procedures </operating-scylla/procedures/cluster-management/index>`
  (adding, replacing, removing, decommissioning nodes, etc.).
* Running :doc:`nodetool repair </operating-scylla/nodetool-commands/repair>`.
* Running :doc:`nodetool checkAndRepairCdcStreams </operating-scylla/nodetool-commands/checkandrepaircdcstreams>`.
* Any modifications of :doc:`authentication </operating-scylla/security/authentication>` and :doc:`authorization </operating-scylla/security/enable-authorization>` settings.
* Any change of authorization via :doc:`CQL API </operating-scylla/security/authorization>`.
* Schema changes.

Running the procedure
=====================

.. warning::

  Before proceeding, make sure that all the :ref:`prerequisites <enable-raft-topology-2025.1-prerequisites>` are met
  and no :ref:`forbidden administrative operations <enable-raft-topology-2025.1-forbidden-operations>` will run
  during the procedure. Failing to do so may put the cluster in an inconsistent state.

#. Issue a POST HTTP request to the ``/storage_service/raft_topology/upgrade``
   endpoint to any of the nodes in the cluster.
   For example, you can do it with ``curl``:

   .. code-block:: bash

	   curl -X POST "http://127.0.0.1:10000/storage_service/raft_topology/upgrade"

#. Wait until all nodes report that the procedure is complete. You can check
   whether a node finished the procedure in one of two ways:

   * By sending a HTTP ``GET`` request on the ``/storage_service/raft_topology/upgrade``
     endpoint. For example, you can do it with ``curl``:

     .. code-block:: bash
      
      curl -X GET "http://127.0.0.1:10000/storage_service/raft_topology/upgrade"

     It will return a JSON string that will be equal to ``done`` after the procedure is complete on that node.

   * By querying the ``upgrade_state`` column in the ``system.topology`` table.
     You can use ``cqlsh`` to get the value of the column:

     .. code-block:: bash
      
      cqlsh -e "select upgrade_state from system.topology"

     The ``upgrade_state`` column should be set to ``done`` after the procedure
     is complete on that node:

After the procedure is complete on all nodes, wait at least one minute before
issuing any topology changes in order to avoid data loss from writes that were
started before the procedure.

What if the procedure gets stuck?
===================================

If the procedure gets stuck at some point, first check the status of your cluster:

- If there are some nodes that are not alive, try to restart them.
- If all nodes are alive, ensure that the network is healthy and every node can reach all other nodes.
- If all nodes are alive and the network is healthy, perform
  a :doc:`rolling restart </operating-scylla/procedures/config-change/rolling-restart/>` of the cluster.

If none of the above solves the issue, perform :ref:`the Raft recovery procedure <recovery-procedure>`.
During recovery, the cluster will switch back to the gossip-based topology management mechanism.

After exiting recovery, you should retry enabling consistent topology updates using
the procedure described in this document.