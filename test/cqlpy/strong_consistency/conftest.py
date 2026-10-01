# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#############################################################################
# Fixtures shared by the strongly-consistent test modules in this directory.
#############################################################################

import pytest
from cassandra.protocol import ConfigurationException

from test.pylib.skip_types import skip_env

from ..util import unique_name


# A keyspace whose tables are strongly consistent. Cassandra and the --vnodes
# mode have nothing to say about it, and neither has a build which runs without
# the strongly-consistent-tables experimental feature. Every other rejection of
# the keyspace is a regression, so it has to reach the test as a failure rather
# than as a skip.
@pytest.fixture(scope="module")
def sc_keyspace(cql, scylla_only, has_tablets):
    if not has_tablets:
        skip_env('Strongly consistent tables need a tablet based keyspace')
    keyspace = unique_name()
    try:
        cql.execute(f"CREATE KEYSPACE {keyspace} WITH replication = "
                    "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
                    "AND tablets = {'initial': 1} AND consistency = 'global'")
    except ConfigurationException as e:
        if 'strongly_consistent_tables' not in str(e):
            raise
        skip_env('Strongly consistent tables need the strongly-consistent-tables feature on')
    yield keyspace
    cql.execute(f"DROP KEYSPACE {keyspace}")
