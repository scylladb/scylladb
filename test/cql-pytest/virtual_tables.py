# -*- coding: utf-8 -*-
# Copyright 2021-present ScyllaDB
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

import pytest
import util
import nodetool

def test_snapshots_table(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, 'pk int PRIMARY KEY, v int') as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (0, 0)")
        nodetool.take_snapshot(cql, table, 'my_tag', False)
        res = list(cql.execute(f"SELECT keyspace_name, table_name, snapshot_name, live, total FROM system.snapshots"))
        assert len(res) == 1
        ks, tbl = table.split('.')
        assert res[0][0] == ks
        assert res[0][1] == tbl
        assert res[0][2] == 'my_tag'

# We only want to check that the table exists with the listed columns, to assert
# backwards compatibility.
def _check_exists(cql, table_name, columns):
    cols = ", ".join(columns)
    assert list(cql.execute(f"SELECT {cols} FROM system.{table_name}"))

def test_protocol_servers(scylla_only, cql):
    _check_exists(cql, "protocol_servers", ("name", "listen_addresses", "protocol", "protocol_version"))

def test_runtime_info(scylla_only, cql):
    _check_exists(cql, "runtime_info", ("group", "item", "value"))

def test_versions(scylla_only, cql):
    _check_exists(cql, "versions", ("key", "build_id", "build_mode", "version"))
