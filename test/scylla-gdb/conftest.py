# Copyright 2022-present ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use. A "fixture" is some
# setup which an invididual test requires to run; The fixture has setup code
# and teardown code, and if multiple tests require the same fixture, it can
# be set up only once - while still allowing the user to run individual tests
# and automatically setting up the fixtures they need.

import pytest
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
    import sys
    save_sys_path = sys.path
    sys.path.insert(1, sys.path[0] + '/../..')
    # Unfortunately, the file's name includes a dash which requires some
    # funky workarounds to import.
    import importlib
    mod = importlib.import_module("scylla-gdb")
    sys.path = save_sys_path
    yield mod

# "gdb" fixture, attaching to a running Scylla and letting the tests
# run gdb commands on it. The fixture returns a module 
# The gdb fixture depends on scylla_gdb, to ensure that the "scylla"
# subcommands are loaded into gdb.
@pytest.fixture(scope="session")
def gdb(request, scylla_gdb):
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
