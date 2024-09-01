# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use. A "fixture" is some
# setup which an individual test requires to run; The fixture has setup code
# and teardown code, and if multiple tests require the same fixture, it can
# be set up only once - while still allowing the user to run individual tests
# and automatically setting up the fixtures they need.

import pytest
import os
import sys

try:
    import gdb as gdb_library
except:
    print('This test must be run inside gdb. Run ./run instead.')
    exit(1)


def pytest_addoption(parser):
    parser.addoption('--scylla-pid', action='store', default=None,
        help='Process ID of running Scylla to attach gdb to')
    parser.addoption('--scylla-tmp-dir', action='store', default=None,
        help='Temporary directory where Scylla runs')

# Scylla's "scylla-gdb.py" does two things: It configures gdb to add new
# "scylla" commands, and it implements a bunch of useful functions in Python.
# Doing just the former is easy (just add "-x scylla-gdb.py" when running
# gdb), but we also want the latter - we want to be able to use some of those
# extra functions in the test code. For that, we need to actually import the
# scylla-gdb.py module from the test code here - and remember the module
# object.
@pytest.fixture(scope="session")
def scylla_gdb(request):
    save_sys_path = sys.path
    sys.path.insert(1, sys.path[0] + '/../..')
    # Unfortunately, the file's name includes a dash which requires some
    # funky workarounds to import.
    import importlib
    try:
        mod = importlib.import_module("scylla-gdb")
    except Exception as e:
        pytest.exit(f'Failed to load scylla-gdb: {e}')
    sys.path = save_sys_path
    yield mod

# "gdb" fixture, attaching to a running Scylla and letting the tests
# run gdb commands on it. The fixture returns a module 
# The gdb fixture depends on scylla_gdb, to ensure that the "scylla"
# subcommands are loaded into gdb.
@pytest.fixture(scope="session")
def gdb(request, scylla_gdb):
    try:
        gdb_library.lookup_type('size_t')
    except:
        print('ERROR: Scylla executable was compiled without debugging information (-g)')
        print('so cannot be used to test gdb. Please set SCYLLA environment variable.')
        sys.exit(1)

    # The gdb tests are known to be broken on aarch64 (see
    # https://sourceware.org/bugzilla/show_bug.cgi?id=27886) and untested
    # on anything else. So skip them.
    if os.uname().machine != 'x86_64':
        pytest.skip('test/scylla-gdb/conftest.py: gdb tests skipped for non-x86_64')
    gdb_library.execute('set python print-stack full')
    scylla_pid = request.config.getoption('scylla_pid')
    gdb_library.execute(f'attach {scylla_pid}')
    # FIXME: We can start the test here, but at this point Scylla may be
    # completely idle. To make the situation more interesting (and, e.g., have
    # live live tasks for test_misc.py::task()), we can set a breakpoint and
    # let Scylla run a bit more and stop in the middle of its work. However,
    # I'm not sure where to set a break point that is actually guaranteed to
    # happen :(
    #gdb_library.execute('handle SIG34 SIG35 SIGUSR1 nostop noprint pass')
    #gdb_library.execute('break sstables::compact_sstables')
    #gdb_library.execute('continue')
    yield gdb_library
