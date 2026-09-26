
REST
----

ScyllaDB exposes a REST API to retrieve administrative information from a node and execute 
administrative operations. For example, it allows you to check or update configuration, 
retrieve cluster-level information, and more.


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
