# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

# Test that the internal system-distributed tables for alternator export to S3 exist and are queryable.
@pytest.mark.parametrize("table_name", ['alternator_export_to_s3_exports', 'alternator_export_to_s3_client_tokens'])
def test_export_to_s3_checks_if_internal_tables_exist(cql, table_name):
    statement = SimpleStatement(f"SELECT * FROM system_distributed.{table_name} LIMIT 1", consistency_level=ConsistencyLevel.ONE)
    # we don't care about the results, we just want to make sure the read succeeds
    cql.execute(statement)
