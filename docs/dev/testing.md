# Testing

## Introduction

`test.py` is a regression testing harness shipped along with
scylla.git, which runs C++, unit, CQL and python tests.

This is a manual for `test.py`.


## Installation

To run `test.py`, Python 3.7 or higher is required.
`./install-dependencies.sh` should install all the required Python
modules. If `install-dependencies.sh` does not support your distribution,
please manually install all Python modules it lists with `pip`.

Additionally, `toolchain/dbuid` could be used to run `test.py`. In this
case you don't need to run `./install-dependencies.sh`


## Usage

In order to invoke `test.py`, you need to build first. `./test.py` will
run all existing tests in all configured build modes:

    $ ./test.py

In order to invoke `test.py` with `toolchain/dbuild`, you need to run

    $ ./tools/toolchain/dbuild ./test.py

If you want to specify a specific build mode:

    $ ./test.py --mode=dev

If you want to run only a specific test:

    $ ./test.py suitename/testname

This will select and run all tests having suitename/testname as substring,
e.g. suitename/testname_ext and others.

If you want to run only a specific test-case from a specific test:

    $ ./test.py suitename/testname::casename

This will select only the test with the suitename/testname name, no
substring search is performed in this case. If the casename is `*`, then
all test-cases will be selected.

Note that not all tests are divided into cases. Below sections will
shed more light on this.

Build artefacts, such as test output and harness output is stored
in `./testlog`. Scylla data files are stored in `/tmp`.

## How it works

On start, `test.py` invokes `ninja` to find out configured build modes. Then
it searches all subdirectories of `./test/` for `suite.yaml` files: each
directory containing `suite.yaml` is a test suite, in which `test.py` then looks
for tests. All files ending with `_test.cc` or `_test.cql` are considered
tests.

A suite must contain tests of the same type, as configured in `suite.yaml`.
The list of found tests is matched with the optional command line test name
filter. A match is registered if filter substring exists anywhere in test
full name. For example:

    $ ./test.py cql

runs `cql/lwt_test`, `cql/lwt_batch_test`, as well as
`boost/cql_query_test`.

The `./testlog` directory is created if it doesn't exist, otherwise it is
cleared from the previous run artefacts.

Matched tests are run concurrently, with concurrency factor set to the
number of available CPU cores. `test.py` continues until all tests are run
even if any one of them fails.

## CQL tests

The main idea of CQL tests is that test writer specifies CQL
statements run against Scylla, and (almost) everything else is done by
`test.py`: statement output is recorded in a dedicated file, and
later used to validate correctness of the test. The initial validation
must be of course performed by the author of the test. This approach
is sometimes called "approval testing" and discussion
about pros and cons of this methodology is widely available online.

To run CQL tests, `test.py` uses an auxiliary program,
`test/pylib/cql_repl/cql_repl.py`.
This program reads CQL input file, evaluates it against a pre-started
Scylla using CQL database connection, and prints output in tabular format to
stdout. A default keyspace is created automatically.

`test.py` invokes `cql_repl.py` as a pytest providing the test file
and redirecting its output to a temporary file in `testlog` directory.

After `cql_repl.py` finishes, `test.py` compares the output stored in the
temporary file with a pre-recorded output stored in
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
`custom_args` key of the `suite.yaml` file.

Tests from boost suite are divided into test-cases. These are top-level
functions wrapped by `BOOST_AUTO_TEST_CASE`, `SEASTAR_TEST_CASE` or alike.
Boost tests support `suitename/testname::casename` selection described above.

### Debugging unit tests

If a test fails, its log can be found in `testlog/${mode}/testname.log`.
By default, all unit tests are built stripped. To build non-stripped tests,
`./configure` with `--tests-debuginfo list-of-tests`.
`test.py` adds some command line arguments to unit tests. The exact way in
which the test is invoked is recorded in `testlog/test.py.log`.

## Python tests

`test.py` supports pytest standard of tests, for suites (directories)
specifying `Python` test type in their suite.yaml. For such tests,
a standalone server instance is created, and a connection URI to the
server is passed to the test. Thanks to convenience fixtures,
test writers don't need to create or cleanup connections or keyspaces.
`test.py` will also keep track of the used server(s) and will shut
down the server when all tests  using it end.

Note that some suites have a convenience helper script called `run`. Find
more information about it in [test/cql-pytest](../../test/cql-pytest/README.md) and [test/alternator](../../test/alternator/README.md).

All tests in pytest suites consist of test-cases -- top-level functions
starting with test_ -- and thus support the `suitename/testname::casename`
selection described above.

## Sharing and pooling servers

Since there can be many pytests in a single directory (e.g. cql-pytest)
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
log. For this, `test.py` maintains this link through relevant log
messages and preserves Scylla output on test failure.

A typical debugging journey should start with looking at `test.py.log` in
`testlog` where, for each test it runs, `test.py` prints all relevant paths
to server log and pytest output.

To extend `test.py` logging, you can use the standard 'logging' module API.
Individual pytests are programmed to not gobble stdout, so you can can also
add prints to pytests, and they will end up in the test' log.

For example, imagine `cql-pytest/test_null.py` fails. The relevant lines
in `test.py.log` will be:

```
21:53:04.789 INFO> Created cluster {127.101.161.1}
21:53:04.790 INFO> Leasing Scylla cluster {127.101.161.1} for test test_null.1
21:53:04.790 INFO> Starting test test_null.1: pytest --host=127.101.161.1 -s ...test/cql-pytest/test_null.py
21:53:05.533 INFO> Test test_null.1 failed
```
To find out the working directory of instance 127.101.161.1 search
for its initialization message in `test.py.log`:

```
10:05:51.722 INFO> installing Scylla server in /opt/local/work/scylla/scylla/testlog/dev/scylla-1...
10:05:51.722 INFO> starting server at host 127.159.235.1 in scylla-1...
10:05:52.688 INFO> started server at host 127.159.235.1 in scylla-1, pid 2165602
```

Next, we can take a look at the server log, which is at
`/opt/local/work/scylla/scylla/testlog/dev/scylla-1.log`:

The log contains special markers, written at test start and end:

```
INFO  2022-08-18 10:05:52,598 [shard 0] schema_tables - Schema version changed to 8b5e9c73-7c1c-3b28-8c31-c1359210c484
------ Starting test test_null.1 ------
...
INFO  2022-08-18 10:05:53,297 [shard 0] schema_tables - Dropping keyspace cql_test_1660806353124
INFO  2022-08-18 10:05:53,304 [shard 0] schema_tables - Schema version changed to 8b5e9c73-7c1c-3b28-8c31-c1359210c484
------ Ending test test_null.1 ------
```

Most often there are no errors in Scylla log, so next we inspect
the test' log, which is next to the server's at
`/opt/local/scylla/scylla/testlog/dev/test_null.1.log`:
```
        cql.execute(f"INSERT INTO {table1} (p,c) VALUES ('{p}', '3')")
>       assert False
E       assert False
```

What does number 1 mean in the log file name? Since `test.py` can
run parallel jobs and run each test multiple times with `--repeat`,
each execution is assigned a unique sequence number, allowing to
distinguish artifacts of different execution.

When finished debugging, you don't have to worry about deleting the remains
of a previous run, `test.py` will clean then up on the next execution
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
to your suite.yaml.

## Topology pytests

In addition to the standard 'Python' suite type, `test.py`
supports an extended pytest suite, `Topology`. Unlike
`Python` tests, `Topology` tests run against Scylla clusters,
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
is quite simple: any cluster that has has nodes added or removed,
started or stopped, even if it ended up in the same state
as it was at the beginning of the test, is considered "dirty".
Such clusters are not returned to the pool, but destroyed, and
the pool is replenished with a new cluster instead.

## Automation, CI, and Jenkins

If any of the tests fails, `test.py` returns a non-zero exit status.
JUNIT and XUNIT execution status XML files can be found in
`testlog/${mode}/xml/` directory. These files are used
by Jenkins to produce formatted build reports. `test.py` will
try to add as much context information, such as fragments of log
files, exceptions, to the test XML output.

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

1. Open directory with the Junit xml test results, e.g. testlog/dev/xml
2. Execute allure serve to show report
```shell
$ allure serve -h localhost .
```
This will open the default system browser with an interactive allure report.

For more information please refer to `allure -h` or [official documentation](https://allurereport.org/docs/)


## See also

For command line help and available options, please see also:

        $ ./test.py --help

