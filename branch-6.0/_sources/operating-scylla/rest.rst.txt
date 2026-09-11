
Admin REST API
--------------

ScyllaDB exposes a REST API to retrieve administrative information from a node and execute 
administrative operations. For example, it allows you to check or update configuration, 
retrieve cluster-level information, and more.

The :doc:`nodetool </operating-scylla/nodetool>` CLI tool interacts with a *scylla-jmx* process using JMX. 
The process, in turn, uses the REST API to interact with the ScyllaDB process.

You can interact with the REST API directly using :code:`curl`, ScyllaDB's CLI for REST API, or the Swagger UI.


Swagger UI
^^^^^^^^^^^^

.. warning::

   Do not expose the REST API externally for production systems.

   The following example is only for demonstration, as it exposes the REST API externally and will allow 
   external access to administration-level information and operations on your cluster. 
   The API is by default bound to an internal IP (127.0.0.1), blocked for external connections.


You can interact with the REST API using the Swagger UI at :code:`your-ip:10000/ui`. 

The following example shows using the Swagger UI with Docker.

#. Run:

    .. code:: sh

      docker run --name some-scylla -p 10000:10000 -d scylladb/scylla:5.0.4 --api-address 0.0.0.0


#. Go to http://localhost:10000/ui/.

    .. image:: scylla_swagger_ui.jpeg
       :alt: Swagger screen capture



CLI for REST API
^^^^^^^^^^^^^^^^^

ScyllaDB ships with ``scylla-api-client``, a lightweight tool that provides a command-line interface 
for the ScyllaDB API. The tool allows you to:

* List the API functions with their parameters and print detailed help for each function.
* Invoke the API commands using the command line. 

When invoking a function, ``scylla-api-client`` performs basic validation of the function arguments and 
prints the result to the standard output. You can use commonly available command line utilities
to print the results in the JSON format.

To avoid errors, you should prefer ``scylla-api-client`` over ``curl`` and similar HTTP tools for interacting 
with the ScyllaDB REST API.

Usage
======

Run ``scylla-api-client --help`` for information about all the available options.

**Examples:**

* ``scylla-api-client --list-modules`` - Shows all the API modules.
* ``scylla-api-client --list-module-commands system`` - Shows all the API commands for the ``system`` module.
* ``scylla-api-client system uptime_ms`` - Gets the system uptime (in milliseconds).
* ``scylla-api-client system log POST --message "hello world" --level warn`` - Writes the "hello world" message to the ScyllaDB log using the "warn" logging level.
