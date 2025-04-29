#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.pylib.suite.python import add_host_option, add_cql_connection_options

# Add required fixtures:
from test.cqlpy.conftest import host, cql
from test.pylib.cql_repl.conftest import cql_test_connection


def pytest_addoption(parser):
    add_host_option(parser)
    add_cql_connection_options(parser)
