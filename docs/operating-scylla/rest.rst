
REST
----

ScyllaDB exposes a REST API to retrieve administrative information from a node and execute 
administrative operations. For example, it allows you to check or update configuration, 
retrieve cluster-level information, and more.


CLI for REST API
^^^^^^^^^^^^^^^^^

ScyllaDB ships with ``scylla-api-cli``, a lightweight tool that provides a command-line interface 
for the ScyllaDB API.

The ``scylla-api-cli`` tool allows you to:

* List the API functions with their parameters and print detailed help for each function.
* Invoke the API commands using the command line. 

When invoking a function, ``scylla-api-cli`` performs basic validation of the function arguments and 
prints the result to the standard output. You can use commonly available command line utilities
to print the results in the JSON format.

To avoid errors, you should prefer ``scylla-api-cli`` over cURL and similar HTTP tools for interacting 
with the ScyllaDB REST API


