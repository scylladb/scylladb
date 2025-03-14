# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#############################################################################
# Tests asserting correct values are kept in system keyspaces
#############################################################################

import pytest

# By default, tests run CQL over unencrypted sockets,
# so no extra SSL config is required in this testcase.
# The opposite testcase is test_ssl.py::test_tls_versions
def test_system_clients_keeps_correct_tls_entries_when_tls_disabled(cql):
    table_result = cql.execute("SELECT * FROM system.clients")
    for row in table_result:
        assert row.hostname == '127.0.0.1' # is that always the case?
        assert row.ssl_enabled == False
        assert row.ssl_protocol is None
        assert row.ssl_cipher_suite is None
