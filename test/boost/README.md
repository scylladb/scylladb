# Scylla unit tests using C++ and the Boost test framework

The source files in this directory are Scylla unit tests written in C++
using the Boost.Test framework. These unit tests come in three flavors:

1. Some simple tests that check stand-alone C++ functions or classes use
   Boost's `BOOST_AUTO_TEST_CASE`.

2. Some tests require Seastar features, and need to be declared with Seastar's
   extensions to Boost.Test, namely `SEASTAR_TEST_CASE`.

3. Even more elaborate tests require not just a functioning Seastar environment
   but also a complete (or partial) Scylla environment. Those tests use the
   `do_with_cql_env()` or `do_with_cql_env_thread()` function to set up a
   mostly-functioning environment behaving like a single-node Scylla, in
   which the test can run.

While we have many tests of the third flavor, writing new tests of this
type should be reserved to *white box* tests - tests where it is necessary
to inspect or control Scylla internals that do not have user-facing APIs
such as CQL. In contrast, black-box tests - tests that **can** be written
only using user-facing APIs, should be written in one of newer test frameworks
that we offer - such as test/cqlpy or test/alternator (in Python, using
the CQL or DynamoDB APIs respectively) or test/cql (using textual CQL
commands), or - if more than one Scylla node is needed for a test - using
the test/topology* framework.

# Running tests

Because these are C++ tests, they need to be compiled before running.
To compile a single test executable `row_cache_test`, use a command like
```
ninja build/dev/test/boost/row_cache_test
```
You can also use `ninja dev-test` to build _all_ C++ tests, or use
`ninja deb-build` to build the C++ tests and also the full Scylla executable
(however, note that full Scylla executable isn't needed to run Boost tests).

Replace "dev" by "debug" or "release" in the examples above and below to
use the "debug" build mode (which, importantly, compiles the test with
ASAN and UBSAN enabling on and helps catch difficult-to-catch use-after-free
bugs) or the "release" build mode (optimized for run speed).

To run an entire test file `row_cache_test`, including all its test
functions, use a command like:
```
build/dev/test/boost/row_cache_test -- -c1 -m1G 
```

to run a single test function `test_reproduce_18045()` from the longer test
file, use a command like:
```
build/dev/test/boost/row_cache_test -t test_reproduce_18045 -- -c1 -m1G 
```

In these command lines, the parameters before the `--` are passed to
Boost.Test, while the parameters after the `--` are passed to the test code,
and in particular to Seastar. In this example Seastar is asked to run on one
CPU (`-c1`) and use 1G of memory (`-m1G`) instead of hogging the entire
machine. The Boost.Test option `-t test_reproduce_18045` asks it to run just
this one test function instead of all the test functions in the executable.

Unfortunately, interrupting a running test with control-C while doesn't
work. This is a known bug (#5696). Kill a test with SIGKILL (`-9`) if you
need to kill it while it's running.

Boost tests can also be run using test.py - which is a script that provides
a uniform way to run all tests in scylladb.git - C++ tests, Python tests,
etc.

## Execution with pytest

To run all tests with pytest execute 
```bash
pytest test/boost
```

To execute all tests in one file, provide the path to the source filename as a parameter
```bash
pytest test/boost/aggregate_fcts_test.cc
```
Since it's a normal path, autocompletion works in the terminal out of the box.

To execute only one test function, provide the path to the source file and function name
```bash
pytest --mode dev test/boost/aggregate_fcts_test.cc::test_aggregate_avg
```

To provide a specific mode, use the next parameter `--mode dev`,
if parameter isn't provided pytest tries to use `ninja mode_list` to find out the compiled modes.

Parallel execution is controlled by `pytest-xdist` and the parameter `-n auto`.
This command starts tests with the number of workers equal to CPU cores.
The useful command to discover the tests in the file or directory is 
```bash
pytest --collect-only -q --mode dev test/boost/aggregate_fcts_test.cc
```
That will return all test functions in the file.
To execute only one function from the test, you can invoke the output from the previous command.
However, suffix for mode should be skipped.
For example,
output shows in the terminal something like this `test/boost/aggregate_fcts_test.cc::test_aggregate_avg.dev`.
So to execute this specific test function, please use the next command 
```bash
pytest --mode dev test/boost/aggregate_fcts_test.cc::test_aggregate_avg
```

# Writing tests

Because of the large build time and build size of each separate test
executable, it is recommended to put test functions into relatively large
source files. But not too large - to keep compilation time of a single
source file (during development) at reasonable levels.

When adding new source files in test/boost, don't forget to list the new
source file in configure.py and also in CMakeLists.txt. The former is
needed by our CI, but the latter is preferred by some developers.
