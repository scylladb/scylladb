#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.pylib.cql_repl import CQL_TEST_SUFFIX
from test.pylib.suite.python import PythonTestSuite


class CQLApprovalTestSuite(PythonTestSuite):
    """Run CQL commands against a single Scylla instance."""

    test_file_ext = ".cql"

    @property
    def pattern(self) -> str:
        return f"*{CQL_TEST_SUFFIX}"
