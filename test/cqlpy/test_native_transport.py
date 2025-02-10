# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest
import time
import nodetool
from util import new_test_table
from cassandra.cluster import NoHostAvailable

def test_enable_disable_binary(cql, test_keyspace):
    schema = 'k text, v text, primary key (k)'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"INSERT INTO {table} (k,v) VALUES ('foo', 'bar')")
        cql.execute(f"SELECT * FROM {table}")

        nodetool.disablebinary(cql)
        with pytest.raises(NoHostAvailable):
            cql.execute(f"SELECT * FROM {table}")

        nodetool.enablebinary(cql)
        pause = 0.1
        while pause < 100:
            try:
                cql.execute(f"SELECT * FROM {table}")
                break
            except NoHostAvailable:
                time.sleep(pause)
                pause += pause
        else:
            assert False
