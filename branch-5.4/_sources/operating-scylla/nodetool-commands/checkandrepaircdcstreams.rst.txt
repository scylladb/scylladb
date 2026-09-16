Nodetool checkAndRepairCdcStreams
===================================

Checks if CDC streams reflect the current cluster topology, and regenerates them if they don't.


.. warning::

    Do not use this operation while performing other administrative tasks, such as 
    bootstrapping or decommissioning a node.


Usage
------

.. code:: console

    nodetool checkAndRepairCdcStreams

   
See Also
----------

:doc:`Change Data Capture (CDC) </using-scylla/cdc/index>`

:doc:`Upgrading from experimental CDC </kb/cdc-experimental-upgrade>`

.. include:: nodetool-index.rst

