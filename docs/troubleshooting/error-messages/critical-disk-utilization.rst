Critical disk utilization
=========================

Issue
^^^^^

SELECT/INSERT/UPDATE/DELETE cqlsh operations fail with ``Critical disk utilization: rejected write mutation`` error message.


For example

.. code-block:: shell

   WriteFailure: Error from server: code=1500 [Replica(s) failed to execute write] message="Critical disk utilization: rejected write mutation" info={'consistency': 'QUORUM', 'required_responses': 2, 'received_responses': 1, 'failures': 2}

Problem
^^^^^^^

Scaling out a cluster failed or is delayed. As a consequence the disk space becomes critically low and mechanisms preventing nodes going out of space have been activated. User write activity was paused, but the node remains able to migrate data to other nodes.

How to Verify
^^^^^^^^^^^^^

Run ``nodetool status`` to check nodes are up and running.

Run ``df -h /var/lib/scylla/`` to verify that the current disk utilization (in %) is higher than the configured threshold :ref:`critical_disk_utilization_level <confprop_critical_disk_utilization_level>`.

Solution
^^^^^^^^

Scale out the cluster. Adding new nodes to the same rack as the node that reached critical condition, will bring the disk utilization down below the critical threshold and the prevention mechanisms will be deactivated.
