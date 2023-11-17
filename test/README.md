Scylla in-source tests.

For details on how to run the tests, see [docs/dev/testing.md](../docs/dev/testing.md)

Shared C++ utils, libraries are in lib/, for Python - pylib/

alternator - Python tests which connect to a single server and use the DynamoDB API
unit, boost, raft - unit tests in C++
cql-pytest - Python tests which connect to a single server and use CQL
topology* - tests that set up clusters and add/remove nodes
cql - approval tests that use CQL and pre-recorded output
rest\_api - tests for Scylla REST API Port 9000
scylla-gdb - tests for scylla-gdb.py helper script
nodetool - tests for C++ implementation of nodetool

If you can use an existing folder, consider adding your test to it.
New folders should be used for new large categories/subsystems, 
or when the test environment is significantly different from some existing
suite, e.g. you plan to start scylladb with different configuration,
and you intend to add many tests and would like them to reuse an existing
Scylla cluster (clusters can be reused for tests within the same folder).

To add a new folder, create a new directory, and then
copy & edit its `suite.ini`.


