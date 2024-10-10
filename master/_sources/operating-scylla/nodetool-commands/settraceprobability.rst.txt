Nodetool settraceprobability
============================

**settraceprobability** -  ``<value>`` - Sets the probability for tracing a request. Value is trace probability between 0 and 1. 0 the trace will never happen and 1 the trace will always happen.
Anything in between is a percentage of the time, converted into a decimal. For example, 60% would be 0.6.

This command is useful to determine the cause of intermittent query performance problems by identifying which queries are responsible.
It can trace some or all the queries sent to the cluster, setting the probability to 1.0 will trace everything, set to a lower number will reduce the traced queries.
Use caution when setting the ``settraceprobability`` high, it can affect active systems, as system-wide tracing will have a performance impact.
Trace information is stored under ``system_traces`` keyspace for more information you can read our `CQL tracing in ScyllaDB`_ blog

..  _`CQL tracing in ScyllaDB`: https://www.scylladb.com/2016/08/04/cql-tracing/

For example, to set the probability to 10%:

.. code-block:: shell
   
   nodetool settraceprobability 0.1



Additional Information
----------------------

* :doc:`gettraceprobability </operating-scylla/nodetool-commands/gettraceprobability/>` - Nodetool Reference

