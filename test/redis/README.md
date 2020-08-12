Tests for Scylla with Redis API that should also pass, identically, against Redis.

Tests use the redis library for Redis API, and the pytest frameworks
(both are available from Linux distributions, or with "pip install").

To run all tests against the local installation of Scylla with Redis API on
localhost:6379, just run `pytest`.

Some additional pytest options:
* To run all tests in a single file, do `pytest test_strings.py`.
* To run a single specific test, do `pytest test_strings.py::set`.
* Additional useful pytest options, especially useful for debugging tests:
  * -v: show the names of each individual test running instead of just dots.
  * -s: show the full output of running tests (by default, pytest captures the test's output and only displays it if a test fails)
