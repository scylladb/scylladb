# Testing

## Introduction

`test.py` is a regression testing harness shipped along with
scylla.git, which runs C++, unit, CQL and python tests.

This is a manual for `test.py`.


## Installation

To run `test.py`, Python 3.11 or higher is required.
`./install-dependencies.sh` should install all the required Python
modules. If `install-dependencies.sh` does not support your distribution,
please manually install all Python modules it lists with `pip`.

Additionally, `toolchain/dbuild` could be used to run `test.py`. In this
case you don't need to run `./install-dependencies.sh`

By default `test.py` has `--gather-metrics` parameter, that is used to gather
CPU/RAM usage during tests from the cgroup.
This means that before execute `test.py` current terminal process should be located in
the correct cgroup where the **current user** have RW access to the group.
Some desktop environments (DE) handle this automatically by putting the process
to the user-owned scope or slice.
Some DE don't do this.
To check if the terminal has the correct cgroup, do next:
1. Check current cgroup
    ```shell
    $ cat /proc/self/cgroup
    ```
    The output will be a string with a cgroup something like this:
    ```
    0::/user.slice/user-1000.slice/user@1000.service/app.slice/
    ```
2. Check the permission of the cgroup. Get the path after `::` from the previous
command and add at the beginning `/sys/fs/cgroup` and check the permissions:
   ```shell
   $ ls -la /sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice/
   ```
   All items should have the owner set to the current user

If this requirement is satisfied you're good to go to run `test.py` with metrics.

If the requirement isn't satisfied, here is how you can do to run `test.py`.
1. Switch off the metric gathering.
   1. Use toolchain to run `test.py`
   2. Add `--no-gather-metrics` to the `test.py` during the run.
      To make it persistent, it's possible to create an alias
      ```shell
      $ echo 'alias testpy="./test.py --no-gather-metrics"' > ~/.profile
      $ source ~/.profile 
      ```
      Now it's possible to invoke `testpy` alias that will switch off 
      gathering metrics.
   3. Run the `test.py` with `systemd-run`
      ```shell
      $ systemd-run --user --scope ./test.py
      ```
      This can be created as an alias as well.
   4. Create a cgroup manually and put the current terminal process to it
      ```shell
      $ mkdir /sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/test_py.slice 
      $ echo $! | sudo tee /sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/test_py.slice/cgroup.procs
      ```
      This solution requires `sudo` permissions, because, to change a process' cgroup 
      user needs RW permissions to both cgroups: the old one and the new one.
      ***NOTE:*** This operation needs to be executed each time the terminal is opened

Some tests utilize (nested) docker images to provide mock/test services against which to 
run scylla features. In general, these images will be pulled on first usage by the test.
Some images used:
    
    * docker.io/fsouza/fake-gcs-server:1.52.3
    * (add as needed)

## Usage

In order to invoke `test.py`, you need to build first. `./test.py` will
run all existing tests in all configured build modes:

    $ ./test.py

In order to invoke `test.py` with `toolchain/dbuild`, you need to run

    $ ./tools/toolchain/dbuild ./test.py

If you want to specify a specific build mode:

    $ ./test.py --mode=dev

If you want to run only tests in a specific directory:

    $ ./test.py test/cqlpy/

If you want to run a specific test file:

    $ ./test.py test/cqlpy/test_null.py

If you want to run a specific test case within a file:

    $ ./test.py test/boost/aggregate_fcts_test.cc::test_aggregate_avg

You can also use `-k` for expression-based filtering by test name:

    $ ./test.py -k 'test_null or test_empty'

This matches all test functions and classes whose name contains
`test_null` or `test_empty`. Use `not` to exclude:

    $ ./test.py -k 'not test_slow'

To skip tests matching a pattern, use `--skip`:

    $ ./test.py --skip test_slow

Multiple paths and options can be combined freely.

Build artefacts, such as test output and harness output is stored
in `./testlog`. Scylla data files are stored in `/tmp`.

Test directories under `./test/` may contain a `test_config.yaml` file
that configures suite-specific behaviour such as server pool size,
extra Scylla command-line options, and cluster topology. All test
execution is delegated to pytest; `test.py` itself is a thin wrapper
that builds the appropriate pytest arguments.

## How it works

`test.py` is a thin wrapper around `pytest`. On start it invokes `ninja`
to find out configured build modes, builds a list of pytest arguments,
and calls `pytest.main()`.

`HOST_ID` is a short hash derived from the hostname and the current
time (or set via the `SCYLLA_TEST_HOST_ID` environment variable).
It is appended to log filenames, report paths, and the metrics database
so that results from different CI builder nodes are not overwritten
when Jenkins collects them into a single directory.

Test discovery, collection and execution are handled entirely by pytest
and its collectors (defined in `test/pylib/`). Each test directory
contains a `test_config.yaml` that declares the suite type and options.
Pytest collectors use these configs to locate test files — e.g. files
ending with `_test.cc` or `_test.cql` for C++ and CQL suites,
and `*_test.py` / `test_*.py` for Python suites.

Test names or paths given on the command line are forwarded to pytest
for selection. For example:

    $ ./test.py test/cqlpy/test_null.py

runs only `test/cqlpy/test_null.py`. You can also pass a substring via
`-k` to filter by test name.

The `./testlog` directory is created if it doesn't exist, otherwise it is
cleared from the previous run artefacts.

Tests are run concurrently via `pytest-xdist`, with the number of worker
processes determined by available CPU cores, system memory, and build mode
(see `ThreadsCalculator` in `test.py`). `test.py`
continues until all tests are run even if any one of them fails.

## CQL tests

The main idea of CQL tests is that test writer specifies CQL
statements run against Scylla, and (almost) everything else is done by
`test.py`: statement output is recorded in a dedicated file, and
later used to validate correctness of the test. The initial validation
must be of course performed by the author of the test. This approach
is sometimes called "approval testing" and discussion
about pros and cons of this methodology is widely available online.

To run CQL tests, `test.py` uses the custom pytest file collector implemented
in `test/pylib/cql_repl.py` file.  More specifically, the test execution done
in `CqlTest.runtest()` method: read CQL input file, evaluate it against a
pre-started Scylla using CQL database connection, and print output in tabular
format to a temporary output file in `testlog` directory.  A default keyspace
is created automatically.  At the end, the test compares the output stored in
the temporary file with a pre-recorded output stored in
`test/suitename/testname_test.result`.

The test is considered failed if executing any CQL statement produced an
error (e.g.  because the server crashed during execution) or server output
does not match one recorded in `testname.result`, or there is no
`testname.result`. The latter is possible when it's the first invocation of
the test ever.

In the event of output mismatch file `test/suitename/testname_test.reject`
is created, and first lines of the diff between the two files are output.
To update `.result` file with new output, simply overwrite it with the
reject file:

    mv test/suitename/testname.re*

*Note* Since the result file is effectively part of the test, developers
must thoroughly examine the diff and understand the reasons for every
change before overwriting `.result` files.

### Debugging CQL tests

To debug CQL tests, one can run CQL against a standalone
Scylla (possibly started in debugger) using `cqlsh`.

## Unit tests

The same unit test can be run in different seastar configurations, i.e. with
different command line arguments. The custom arguments can be set in
`custom_args` key of the `test_config.yaml` file.

Tests from boost suite are divided into test-cases. These are top-level
functions wrapped by `BOOST_AUTO_TEST_CASE`, `SEASTAR_TEST_CASE` or alike.
Boost tests support `path/to/file_name.cc::casename` selection described above.

### Debugging unit tests

If a test fails, its log can be found in
`testlog/{mode}/{suitename}.{testname}.{casename}_stdout.{run_id}.log`
(e.g. `testlog/dev/boost.aggregate_fcts_test.test_aggregate_avg_stdout.1.log`).
By default, all unit tests are built stripped. To build non-stripped tests,
`./configure` with `--tests-debuginfo list-of-tests`.
`test.py` adds some command line arguments to unit tests.

## Python tests

`test.py` supports pytest standard of tests, for suites (directories)
specifying `Python` test type in their test_config.yaml. For such tests,
a standalone server instance is created, and a connection URI to the
server is passed to the test. Thanks to convenience fixtures,
test writers don't need to create or cleanup connections or keyspaces.
`test.py` will also keep track of the used server(s) and will shut
down the server when all tests  using it end.

Note that some suites have a convenience helper script called `run`. Find
more information about it in [test/cqlpy](../../test/cqlpy/README.md) and [test/alternator](../../test/alternator/README.md).

All tests in pytest suites consist of test-cases -- top-level functions
starting with test_ -- and thus support the `path/to/test_file.py::casename`
selection described in the Usage section.

## Sharing and pooling servers

Since there can be many pytests in a single directory (e.g. cqlpy)
`test.py` creates multiple servers to parallelize their execution.
Each server is also shared among many tests, to save on setup/teardown
steps. While this speeds up execution, sharing servers complicates debugging
if a test fails.

Specifically, you should avoid leaving global artifacts in your test, even
if it fails. Typically, you could use a built-in `keyspace()` fixture
to create a randomly named keyspace.

At start and end of each test, `test.py` performs a number of sanity checks
of the used server:
- it should be up and running,
- it should not contain non-system keyspaces.

### Debugging a pytest.

To have a full picture for a failing pytest it is necessary to identify
the server which was used to run it and the relevant fragment in the server
log.

Tests run in parallel via `pytest-xdist` workers (`gw0`, `gw1`, ...).
Each worker writes its log to `testlog/pytest_log/pytest_gw{N}_{HOST_ID}.log`.
This log contains cluster lifecycle messages and test pass/fail status.

For example, imagine `cqlpy/test_null.py` fails. The relevant lines
in the worker log will be:

```
INFO> installing Scylla server in .../testlog/dev/scylla-gw0-1...
INFO> starting server at host 127.1.191.1 in scylla-gw0-1...
INFO> started server at host 127.1.191.1 in scylla-gw0-1, pid 675
INFO> Leasing Scylla cluster ... for test gw0.cqlpy.test_null.1
INFO> Test gw0.cqlpy.test_null.1 failed
```

From these messages you can identify the server working directory
(`testlog/dev/scylla-gw0-1/`) and the server log
(`testlog/dev/scylla-gw0-1.log`).

The server log contains special markers, written at test start and end:

```
------ Starting test gw0.cqlpy.test_null.1 ------
...
------ Ending test gw0.cqlpy.test_null.1 ------
```

The per-test failure output (pytest traceback) is captured in
`testlog/pytest_tests_logs/`. For example:

```
testlog/pytest_tests_logs/cqlpy-test_null.py-test_insert_null_key.dev.1-call-c8a46.log
```

To extend logging, you can use the standard `logging` module API.

When finished debugging, you don't have to worry about deleting the remains
of a previous run, `test.py` will clean them up on the next execution
automatically.

### Pooling implementation details

When pooling and running multiple servers, we want to avoid host/port or
temporary directory clashes. We also want to make sure that `test.py`
doesn't leave any running servers around, even when it's interrupted
by user or with an exception. This is why `test.py` has a special
registry where it tracks all servers, in which each server gets a unique
address in a subnet of network `127.*.*.*`. Unless killed with `SIGKILL`,
`test.py` kills all servers it creates at shutdown.

The servers created by the pool use a pre-defined set of options
to speed up boot. Some of these options are developer-only, such as
`flush_schema_tables_after_modification: false`. If you wish to
extend the options of a used server, you can do it by adding
`extra_scylla_cmdline_options` or `extra_scylla_config_options`
to your test_config.yaml.

## Topology pytests

Some pytest suites run against Scylla clusters
and support topology operations. A standard `manager`
fixture is available for these. Through this fixture,
you can access individual nodes, start, stop
and restart instances, add and remove instances from the cluster.
The `manager` fixture connects to the cluster by sending
`test.py` HTTP/REST commands over a unix-domain socket.
This guarantees that `test.py` is fully aware of all topology
operations and can clean up resources, including added
servers, when tests end.
`test.py` automatically detects if a cluster can not be shared with a
subsequent test because it was manipulated with. Today the check
is quite simple: any cluster that has nodes added or removed,
started or stopped, even if it ended up in the same state
as it was at the beginning of the test, is considered "dirty".
Such clusters are not returned to the pool, but destroyed, and
the pool is replenished with a new cluster instead.

## Test metrics

The parameter `--gather-metrics` is used to gather CPU/RAM usage during tests from the cgroup and system overall CPU/RAM
usage.
For that, SQLite database is used to store the metrics in `testlog/sqlite_{HOST_ID}.db`.
The database is created in the `testlog` directory and contains the following tables:

- `tests` - contains the list of tests that were executed with information about the test name, directory, architecture,
  and mode
- `test_metrics` - contains the metrics for each test, such as memory peak usage, CPU usage, and duration
- `system_resource_metrics` - contains system CPU and memory utilization in percents during the whole run
- `cgroup_memory_metrics` - contains cgroup memory usage during the test run

## Automation, CI, and Jenkins

If any of the tests fails, `test.py` returns a non-zero exit status.
A consolidated JUNIT XML report is written to
`testlog/report/pytest_cpp_{HOST_ID}.xml`. This file is used
by Jenkins to produce formatted build reports.

If that's not enough, a debugging journey, similar to a local one, is
available in CI if you navigate to 'Build artifacts' at the Jenkins build
page. This will bring you to a folder with all the test logs, preserved by
Jenkins in event of test failure.

## Stability

Testing is hard. Testing ScyllaDB is even harder, but we strive to ensure our testing
suite is as solid as possible. The first step is contributing a stable (read: non-flaky) test.
To do so, when developing tests, please run them (1) in debug mode and (2) 100 times in a row (using `--repeat 100`),
and see that they pass successfully.

## Allure reporting

To make analyzing of the test results more convenient, an allure reporting tool is introduced.
Python module allure-pytest is included in the toolchain image and an additional parameter added to gather allure
data for the test run.
However, the allure binary is not a part of the toolchain image.
So to have the full benefit of the new reporting tool allure binary should be installed locally.

### Allure installation

To install allure tool, please follow [official documentation](https://allurereport.org/docs/install-for-linux/)

**Note:** rpm package requires dependency `default-jre-headless` but on Fedora 38,39 none of the packages provides it.
So manual installation of allure is required.

### Basic allure usage

1. Open directory with the test results, e.g. testlog/report/
2. Execute allure serve to show report
```shell
$ allure serve -h localhost .
```
This will open the default system browser with an interactive allure report.

For more information please refer to `allure -h` or [official documentation](https://allurereport.org/docs/)


## See also

For command line help and available options, please see also:

        $ ./test.py --help

