
REST
------

ScyllaDB exposes a REST API to retrieve administrative information from a node and execute administrative operations.
For example, it allows you to check or update configuration, retrieve cluster-level information, and more.

The :doc:`nodetool </operating-scylla/nodetool>` CLI tool interacts with a *scylla-jmx* process using JMX. The process, in turn, uses the REST API to interact with the ScyllaDB process.

You can interact with the REST API directly using :code:`curl` or using the Swagger UI available at :code:`your-ip:10000/ui`.

.. warning::

   Do not expose the REST API externally for production systems.
   
   The following example is only for demonstration, as it exposes the REST API externally and will allow external access to administration-level information and operations on your cluster. 
   The API is default bound to an internal IP (127.0.0.1), blocked for external connections.

For example, using the Swagger UI with Scylla Docker:

.. code:: sh

  docker run --name some-scylla -p 10000:10000 -d scylladb/scylla:5.0.4 --api-address 0.0.0.0

Then visit http://localhost:10000/ui/.


.. image:: swagger.jpeg
   :alt: Swagger screen capture
