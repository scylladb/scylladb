=====================================
Enable Consistent Topology Updates
=====================================

This article explains how to enable consistent topology changes
when you upgrade from version 5.4 to 6.0.

Introduction
============

ScyllaDB Open Source 6.0 introduces :ref:`consistent topology changes based on Raft <raft-topology-changes>`.
Newly created clusters use consistent topology changes right from the start. However - unlike in the case
of schema managed on Raft - consistent topology changes are *not* automatically enabled after the cluster
was upgraded from an older version of ScyllaDB. If you have such a cluster, then you need to enable
consistent topology changes manually with a dedicated upgrade procedure.

Before running the procedure, you **must** check that the cluster meets some prerequisites
and you **must** ensure that some administrative procedures will not be run
while the upgrade procedure is in progress.

.. _enable-raft-topology-6.0-prerequisites:

Prerequisites
=============

* Make sure that all nodes in the cluster are upgraded to ScyllaDB Open Source 6.0.
* Verify that :ref:`schema on raft is enabled <schema-on-raft-enabled>`.
* Make sure that all nodes enabled ``SUPPORTS_CONSISTENT_TOPOLOGY_CHANGES`` cluster feature.
  One way to verify this is to look for the following message in the log:

  .. code-block:: none

    features - Feature SUPPORTS_CONSISTENT_TOPOLOGY_CHANGES is enabled

  Alternatively, this can be verified programmatically by checking whether ``value`` column under the key ``enabled_features`` contains the name of the feature in the ``system.scylla_local`` table.
  For example, this can be done with the following bash script:

  .. code-block:: bash

    until cqlsh -e "select value from system.scylla_local where key = 'enabled_features'" | grep "SUPPORTS_CONSISTENT_TOPOLOGY_CHANGES"
    do
        echo "Upgrade didn't finish yet on the local node, waiting 10 seconds before checking again..."
        sleep 10
    done
    echo "Upgrade completed on the local node"

* Make sure that all nodes are alive for the duration of the upgrade.

.. _enable-raft-topology-6.0-forbidden-operations:

Administrative operations which must not be running during upgrade
==================================================================

Make sure that administrative operations will not be running while upgrade is in progress.
In particular, you must abstain from:

* :doc:`Cluster management procedures </operating-scylla/procedures/cluster-management/index>` (adding, replacing, removing, decommissioning nodes etc.).
* :doc:`Version upgrade procedures </upgrade/index>`. You must ensure that all Scylla nodes are running the same version before proceeding.
* Running :doc:`nodetool repair </operating-scylla/nodetool-commands/repair>`.
* Running :doc:`nodetool checkAndRepairCdcStreams </operating-scylla/nodetool-commands/checkandrepaircdcstreams>`.
* Any modifications of :doc:`authentication </operating-scylla/security/authentication>` and :doc:`authorization </operating-scylla/security/enable-authorization>` settings.
* Any change of authorization via :doc:`CQL API </operating-scylla/security/authorization>`.
* Doing schema changes.

Running the procedure
=====================

.. warning::

  Before proceeding, make sure that all the :ref:`prerequisites <enable-raft-topology-6.0-prerequisites>` are met
  and no :ref:`forbidden administrative operations <enable-raft-topology-6.0-forbidden-operations>` will run
  during upgrade. Failing to do so may put the cluster in an inconsistent state.

Starting the upgrade procedure is done by issuing an POST HTTP request to the ``/storage_service/raft_topology/upgrade`` endpoint,
to any of the nodes in the cluster.

For example, you can do it via ``curl``, like this:

.. code-block:: bash

	curl -X POST "http://127.0.0.1:10000/storage_service/raft_topology/upgrade"

Next, wait until all nodes report that upgrade is complete. You can check that a single node finished upgrade in one of two ways:

* By sending a HTTP ``GET`` request on the ``/storage_service/raft_topology/upgrade`` endpoint. For example, you can do it with ``curl`` like this:

  .. code-block:: bash

      curl -X GET "http://127.0.0.1:10000/storage_service/raft_topology/upgrade"

  It will return a JSON string which will be equal to ``done`` after the upgrade is complete on this node.

* By querying the ``upgrade_state`` column in the ``system.topology`` table. You can use ``cqlsh`` to get the value of the column like this:

  .. code-block:: bash

      cqlsh -e "select upgrade_state from system.topology"

  The ``upgrade_state`` column should be set to ``done`` after the upgrade is complete on this node:

After the upgrade is complete on all nodes, wait at least one minute before issuing any topology changes in order to avoid data loss from writes that were started before the upgrade.

What if upgrade gets stuck?
===========================

If the process gets stuck at some point, first check the status of your cluster:

- If there are some nodes that are not alive, try to restart them.
- If all nodes are alive, ensure that the network is healthy and every node can reach all other nodes.
- If all nodes are alive and the network is healthy, perform a :doc:`rolling restart </operating-scylla/procedures/config-change/rolling-restart/>` of the cluster.

If none of the above solves the issue, perform :ref:`the Raft recovery procedure <recovery-procedure>`.
During recovery, the cluster will switch back to gossip-based topology management mechanism.
After exiting recovery, you should upgrade the cluster to consistent topology updates using the procedure described in this document.
