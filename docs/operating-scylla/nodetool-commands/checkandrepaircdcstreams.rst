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

:doc:`Change Data Capture (CDC) </features/cdc/index>`

.. include:: nodetool-index.rst

