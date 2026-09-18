============================
Nodetool gettraceprobability
============================

**gettraceprobability** - Displays the current trace probability value. This value is the probability for tracing a request.
To change this value see :doc:`settraceprobability </operating-scylla/nodetool-commands/settraceprobability/>`.


For example:

.. code-block:: none

   nodetool gettraceprobability

returns:

.. code-block:: none

   Current trace probability: 0.0



Additional Information
----------------------

* :doc:`settraceprobability </operating-scylla/nodetool-commands/settraceprobability/>` - Nodetool Reference
* `CQL tracing in ScyllaDB blog <https://www.scylladb.com/2016/08/04/cql-tracing/>`_

