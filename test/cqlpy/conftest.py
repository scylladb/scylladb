# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use. A "fixture" is some
# setup which an individual test requires to run; The fixture has setup code
# and teardown code, and if multiple tests require the same fixture, it can
# be set up only once - while still allowing the user to run individual tests
# and automatically setting up the fixtures they need.

import pytest

from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.connection import DRIVER_NAME, DRIVER_VERSION
import json
import os
import ssl
import subprocess
import tempfile
import time
import random

import sys
sys.path.insert(1, sys.path[0] + '/../..')
from test.pylib.report_plugin import ReportPlugin
from util import unique_name, new_test_keyspace, keyspace_has_tablets, cql_session, local_process_id, is_scylla


print(f"Driver name {DRIVER_NAME}, version {DRIVER_VERSION}")


# By default, tests run against a CQL server (Scylla or Cassandra) listening
# on localhost:9042. Add the --host and --port options to allow overriding
# these defaults.
def pytest_addoption(parser):
    parser.addoption('--host', action='store', default='localhost',
        help='CQL server host to connect to')
    parser.addoption('--port', action='store', default='9042',
        help='CQL server port to connect to')
    parser.addoption('--ssl', action='store_true',
        help='Connect to CQL via an encrypted TLSv1.2 connection')
    # Used by the wrapper script only, not by pytest, added here so it appears
    # in --help output and so that pytest's argparser won't protest against its
    # presence.
    parser.addoption('--omit-scylla-output', action='store_true',
        help='Omit scylla\'s output from the test output')
    parser.addoption('--mode', action='store', default='no_mode',
                     help='Scylla build mode. Tests can use it to adjust their behavior.')
    parser.addoption('--run_id', action='store', default=1,
                     help='Run id for the test run')

# "cql" fixture: set up client object for communicating with the CQL API.
# The host/port combination of the server are determined by the --host and
# --port options, and defaults to localhost and 9042, respectively.
# We use scope="session" so that all tests will reuse the same client object.
@pytest.fixture(scope="session")
def cql(request):
    try:
        # Use the default superuser credentials, which work for both Scylla and Cassandra
        with cql_session(request.config.getoption('host'),
            request.config.getoption('port'),
            request.config.getoption('ssl'),
            username="cassandra",
            password="cassandra"
        ) as session:
            yield session
            session.shutdown()
    except NoHostAvailable:
        # We couldn't create a cql connection. Instead of reporting that
        # each individual test failed, let's just exit immediately.
        pytest.exit(f"Cannot connect to Scylla at --host={request.config.getoption('host')} --port={request.config.getoption('port')}", returncode=pytest.ExitCode.INTERNAL_ERROR)

# A function-scoped autouse=True fixture allows us to test after every test
# that the CQL connection is still alive - and if not report the test which
# crashed Scylla and stop running any more tests.
@pytest.fixture(scope="function", autouse=True)
def cql_test_connection(cql, request):
    yield
    try:
        # We want to run a do-nothing CQL command. 
        # "BEGIN BATCH APPLY BATCH" is the closest to do-nothing I could find...
        cql.execute("BEGIN BATCH APPLY BATCH")
    except:
        pytest.exit(f"Scylla appears to have crashed in test {request.node.parent.name}::{request.node.name}")

# Until Cassandra 4, NetworkTopologyStrategy did not support the option
# replication_factor (https://issues.apache.org/jira/browse/CASSANDRA-14303).
# We want to allow these tests to run on Cassandra 3.* (for the convenience
# of developers who happen to have it installed), so we'll use the older
# syntax that needs to specify a DC name explicitly. For this, will have
# a "this_dc" fixture to figure out the name of the current DC, so it can be
# used in NetworkTopologyStrategy.
@pytest.fixture(scope="session")
def this_dc(cql):
    yield cql.execute("SELECT data_center FROM system.local").one()[0]

@pytest.fixture(scope="session")
def test_keyspace_tablets(cql, this_dc, has_tablets):
    if not is_scylla(cql) or not has_tablets:
        yield None
        return

    name = unique_name()
    cql.execute("CREATE KEYSPACE " + name + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 1 } AND TABLETS = {'enabled': true}")
    yield name
    cql.execute("DROP KEYSPACE " + name)

@pytest.fixture(scope="session")
def test_keyspace_vnodes(cql, this_dc, has_tablets):
    name = unique_name()
    if has_tablets:
        cql.execute("CREATE KEYSPACE " + name + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 1 } AND TABLETS = {'enabled': false}")
    else:
        # If tablets are not available or not enabled, we just create a regular keyspace
        cql.execute("CREATE KEYSPACE " + name + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 1 }")
    yield name
    cql.execute("DROP KEYSPACE " + name)

# "test_keyspace" fixture: Creates and returns a temporary keyspace to be
# used in tests that need a keyspace. The keyspace is created with RF=1,
# and automatically deleted at the end. We use scope="session" so that all
# tests will reuse the same keyspace.
@pytest.fixture(scope="session")
def test_keyspace(request, test_keyspace_vnodes, test_keyspace_tablets, cql, this_dc):
    if hasattr(request, "param"):
        if request.param == "vnodes":
            yield test_keyspace_vnodes
        elif request.param == "tablets":
            if not test_keyspace_tablets:
                pytest.skip("tablet-specific test skipped")
            yield test_keyspace_tablets
        else:
            pytest.fail(f"test_keyspace(): invalid request parameter: {request.param}")
    else:
        name = unique_name()
        cql.execute("CREATE KEYSPACE " + name + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 1 }")
        yield name
        cql.execute("DROP KEYSPACE " + name)

# The "scylla_only" fixture can be used by tests for Scylla-only features,
# which do not exist on Apache Cassandra. A test using this fixture will be
# skipped if running with "run-cassandra".
@pytest.fixture(scope="session")
def scylla_only(cql):
    # We recognize Scylla by checking if there is any system table whose name
    # contains the word "scylla":
    if not is_scylla(cql):
        pytest.skip('Scylla-only test skipped')

# "cassandra_bug" is similar to "scylla_only", except instead of skipping
# the test, it is expected to fail (xfail) on Cassandra. It should be used
# in rare cases where we consider Scylla's behavior to be the correct one,
# and Cassandra's to be the bug.
@pytest.fixture(scope="session")
def cassandra_bug(cql):
    # We recognize Scylla by checking if there is any system table whose name
    # contains the word "scylla":
    names = [row.table_name for row in cql.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = 'system'")]
    if not any('scylla' in name for name in names):
        pytest.xfail('A known Cassandra bug')

# Consistent schema change feature is optionally enabled and
# some tests are expected to fail on Scylla without this
# option enabled, and pass with it enabled (and also pass on Cassandra).
# These tests should use the "fails_without_consistent_cluster_management"
# fixture. When consistent mode becomes the default, this fixture can be removed.
@pytest.fixture(scope="function")
def check_pre_consistent_cluster_management(cql):
    # If not running on Scylla, return false.
    names = [row.table_name for row in cql.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = 'system'")]
    if not any('scylla' in name for name in names):
        return False
    # In Scylla, we check Raft mode by inspecting the configuration via CQL.
    consistent = list(cql.execute("SELECT value FROM system.config WHERE name = 'consistent_cluster_management'"))
    return len(consistent) == 0 or consistent[0].value == "false"


@pytest.fixture(scope="function")
def fails_without_consistent_cluster_management(request, check_pre_consistent_cluster_management):
    if check_pre_consistent_cluster_management:
        request.node.add_marker(pytest.mark.xfail(reason="Test expected to fail without consistent cluster management "
                                                         "feature on"))
# Older versions of the Cassandra driver had a bug where if Scylla returns
# an empty page, the driver would immediately stop reading even if this was
# not the last page. Some tests which filter out most of the results can end
# up with some empty pages, and break on buggy versions of the driver. These
# tests should be skipped when using a buggy version of the driver. This is
# the purpose of the following fixture.
# This driver bug was fixed in Scylla driver 3.24.5 and Datastax driver
# 3.25.1, in the following commits:
# https://github.com/scylladb/python-driver/commit/6ed53d9f7004177e18d9f2ea000a7d159ff9278e,
# https://github.com/datastax/python-driver/commit/1d9077d3f4c937929acc14f45c7693e76dde39a9
@pytest.fixture(scope="function")
def driver_bug_1():
    scylla_driver = 'Scylla' in DRIVER_NAME
    driver_version = tuple(int(x) for x in DRIVER_VERSION.split('.'))
    if (scylla_driver and driver_version < (3, 24, 5) or
            not scylla_driver and driver_version <= (3, 25, 0)):
        pytest.skip("Python driver too old to run this test")

# `random_seed` fixture should be used when the test uses random module.
# If the fixture is used, the seed is visible in case of test's failure,
# so it can be easily recreated.
# The state of random module is restored to before-test state after the test finishes.
@pytest.fixture(scope="function")
def random_seed():
    state = random.getstate()
    seed = time.time()
    print(f"Using seed {seed}")
    random.seed(seed)
    yield seed
    random.setstate(state)

# TODO: use new_test_table and "yield from" to make shared test_table
# fixtures with some common schemas.

# To run the Scylla tools, we need to run Scylla executable itself, so we
# need to find the path of the executable that was used to run Scylla for
# this test. We do this by trying to find a local process which is listening
# to the address and port to which our our CQL connection is connected.
# If such a process exists, we verify that it is Scylla, and return the
# executable's path. If we can't find the Scylla executable we use
# pytest.skip() to skip tests relying on this executable.
@pytest.fixture(scope="session")
def scylla_path(cql):
    pid = local_process_id(cql)
    if not pid:
        pytest.skip("Can't find local Scylla process")
    # Now that we know the process id, use /proc to find the executable.
    try:
        path = os.readlink(f'/proc/{pid}/exe')
    except:
        pytest.skip("Can't find local Scylla executable")
    # Confirm that this executable is a real tool-providing Scylla by trying
    # to run it with the "--list-tools" option
    try:
        subprocess.check_output([path, '--list-tools'])
    except:
        pytest.skip("Local server isn't Scylla")
    return path

# A fixture for finding Scylla's data directory. We get it using the CQL
# interface to Scylla's configuration. Note that if the server is remote,
# the directory retrieved this way may be irrelevant, whether or not it
# exists on the local machine... However, if the same test that uses this
# fixture also uses the scylla_path fixture, the test will anyway be skipped
# if the running Scylla is not on the local machine local.
@pytest.fixture(scope="module")
def scylla_data_dir(cql):
    try:
        dir = json.loads(cql.execute("SELECT value FROM system.config WHERE name = 'data_file_directories'").one().value)[0]
        return dir
    except:
        pytest.skip("Can't find Scylla sstable directory")

@pytest.fixture(scope="function")
def temp_workdir():
    """ Creates a temporary work directory, for the scope of a single test. """
    with tempfile.TemporaryDirectory() as workdir:
        yield workdir

@pytest.fixture(scope="session")
def has_tablets(cql):
    with new_test_keyspace(cql, " WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor': 1}") as keyspace:
        return keyspace_has_tablets(cql, keyspace)

@pytest.fixture(scope="function")
def skip_without_tablets(scylla_only, has_tablets):
    if not has_tablets:
        pytest.skip("Test needs tablets experimental feature on")


def pytest_configure(config):
    config.pluginmanager.register(ReportPlugin())
