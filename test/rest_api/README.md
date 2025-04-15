Tests for the Scylla REST API.

Tests use the requests library and the pytest frameworks
(both are available from Linux distributions, or with "pip install").

To run all tests using test.py, just run `./test.py api/run`.

To run all tests against an already-running local installation of Scylla,
just run `pytest`. The "--host" and "--api-port"
can be used to give a different location for the running Scylla.

More conveniently, we have a script - "run" - which
does all the work necessary to start Scylla,
and run the tests on it. The Scylla process is run in a
temporary directory which is automatically deleted when the test ends.

"run" automatically picks the most recently compiled version of Scylla in
`build/*/scylla` - but this choice of Scylla executable can be overridden with
the `SCYLLA` environment variable.

Additional options can be passed to "pytest" or to "run"
to control which tests to run:

* To run all tests in a single file, do `pytest test_system.py`.
* To run a single specific test, do `pytest test_system.py::test_system_uptime_ms`.
* To run the same test or tests 100 times, add the `--count=100` option.
  This is faster than running `run` 100 times, because Scylla is only run
  once, and also counts for you how many of the runs failed.
  For `pytest` to support the `--count` option, you need to install a
  pytest extension: `pip install pytest-repeat`

Additional useful pytest options, especially useful for debugging tests:

* -v: show the names of each individual test running instead of just dots.
* -s: show the full output of running tests (by default, pytest captures the test's output and only displays it if a test fails)
