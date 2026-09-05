nodetool decommission
=====================

**decommission** - Deactivate a selected node by streaming its data to the next node in the ring.

For example:

``nodetool decommission``

.. include:: /operating-scylla/_common/decommission_warning.rst

Use the ``nodetool netstats`` command to monitor the progress of the token reallocation.             

.. include:: nodetool-index.rst
