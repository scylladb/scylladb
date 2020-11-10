Single-node funtional tests for Scylla's CQL features.
Tests use the Python CQL library and the pytest frameworks.
By using an actual CQL library for the tests, they can be run against *any*
implementation of CQL - both Scylla and Cassandra. Most tests - except in
rare cases - should pass on both, to ensure that Scylla is compatible with
Cassandra in most features.

To run all tests against an already-running local installation of Scylla
or Cassandra on localhost, just run `pytest`. The "--host" and "--port"
can be used to give a different location for the running Scylla or Cassanra.

More conveniently, we have two scripts - "run" and "run-cassandra" - which
do all the work necessary to start Scylla or Cassandra (respectively),
and run the tests on them. The Scylla or Cassandra process is run in a
temporary directory which is automatically deleted when the test ends.

Additional options can be passed to "pytest" or to "run" / "run-cassandra"
to control which tests to run:

* To run all tests in a single file, do `pytest test_table.py`.
* To run a single specific test, do `pytest test_table.py::test_create_table_unsupported_names`.

Additional useful pytest options, especially useful for debugging tests:

* -v: show the names of each individual test running instead of just dots.
* -s: show the full output of running tests (by default, pytest captures the test's output and only displays it if a test fails)
