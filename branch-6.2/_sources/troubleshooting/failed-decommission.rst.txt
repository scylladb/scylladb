Failed Decommission
===================

This article describes the troubleshooting procedure when node decommission fails.

During decommissioning, the streaming process starts, and the node streams its data to the other nodes in the ScyllaDB cluster.
The process may fail if the node fails to read from the HDD or a network problem occurs.


Problem
^^^^^^^
The node is stuck in the decommission status.

How to Verify
^^^^^^^^^^^^^
Run the ``nodetool status`` command to check the node status. The expected result is ``UL`` (Up Leaving).

Check the node status from the other nodes in the cluster to verify that the status of the decommissioned node status is ``UL``.

.. note::
  The ``nodetool netstats`` command does not show ongoing streaming. 

The following error message will appear in the logs_:

.. _logs: /getting-started/logging/ 

.. code-block:: shell

   nodetool: ScyllaDB API server HTTP POST to URL '/storage_service/decommission' failed: stream_ranges failed

Solution
^^^^^^^^

#. Restart the node you are decommissioning.

    .. include:: /rst_include/scylla-commands-restart-index.rst

#. Run the ``nodetool status`` command to verify the node is in ``UN`` status.

#. Run ``nodetool decommission`` again.

.. include:: /troubleshooting/_common/ts-return.rst
