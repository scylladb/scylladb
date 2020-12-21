# Testing

## Introduction

`test.py` is a simple regression testing harness shipped along with
scylla.git, which runs C++ unit and CQL tests.

This is a manual for `test.py`.


## Installation

To run `test.py`, Python 3.7 or higher is required.
`./install-dependencies.sh` should install all the required Python
modules. If `install-dependencies.sh` does not support your distribution,
please manually install all Python modules it lists with `pip`.


## Usage

In order to invoke `test.py`, you need to build first. `./test.py` will
run all existing tests in all configured build modes:

    $ ./test.py

If you want to specify a specific build mode:

    $ ./test.py --mode=dev

If you want to run only a specific test:

    $ ./test.py suitename/testname

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

To run CQL tests, `test.py` uses an auxiliary program, `test/tools/cql_repl.cc`.
This program reads CQL input from stdin, evaluates it using Scylla CQL
database front-end, and prints output in JSON format to stdout. A default
keyspace is created automatically.

`test.py` invokes `cql_repl` providing the test file for its standard
input and redirecting its output to a temporary file in `testlog` directory.

After `cql_repl` finishes, `test.py` compares the output stored in the
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
must thorougly examine the diff and understand the reasons for every
change before overwriting `.result` files.

## Unit tests

The same unit test can be run in different seastar configurations, i.e. with
different command line arguments. The custom arguments can be set in
`custom_args` key of the `suite.yaml` file.


## Debugging

If a test fails, its log can be found in `testlog/${mode}/testname.log`.
By default, all unit tests are built stripped. To build non-stripped tests,
`./configure` with `--tests-debuginfo list-of-tests`.
`test.py` adds some command line arguments to unit tests. The exact way in
which the test is invoked is recorded in `testlog/test.py.log`.

To debug CQL tests, one can invoke `gdb cql_repl`, which will start
`cql_repl` in interactive mode, and then supply faulty CQL statements
on the terminal.


## Automation

If any of the tests fails, `test.py` returns a non-zero exit status.
JUNIT and XUNIT execution status XML files can be found in
`testlog/${mode}/xml/` directory.


## See also

For command line help and available options, please see also:

        $ ./test.py --help

