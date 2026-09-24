# -*- coding: utf-8 -*-
# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for user-defined functions (UDF) written in Lua. Lua is a
# Scylla-only UDF language, so all tests here are Scylla-only.
# Ported from test/boost/user_function_test.cc.
#############################################################################

import math
import re
from decimal import Decimal
from uuid import UUID

import pytest
from cassandra.cluster import NoHostAvailable
from cassandra.protocol import InvalidRequest, SyntaxException, Unauthorized
from cassandra.util import Date, Duration

from .util import new_test_table, new_type, new_function, unique_name

# Some errors (e.g., marshaling errors) are returned by the server as a
# SERVER_ERROR, which the driver converts to NoHostAvailable.

# Create a Lua function with the given signature (argument list, null
# handling and return type) and body, call it with the given arguments
# on all rows of the table, and return the resulting values.
def call_lua(cql, keyspace, table, signature, body, args="val"):
    with new_function(cql, keyspace, f"{signature} LANGUAGE lua AS '{body}'") as f:
        return [row[0] for row in cql.execute(f"SELECT {keyspace}.{f}({args}) FROM {table}")]

def test_lua_out_of_memory(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val int") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', null)")
        with pytest.raises(InvalidRequest, match="lua execution failed: not enough memory"):
            call_lua(cql, test_keyspace, table, "(val int) CALLED ON NULL INPUT RETURNS int",
                     'a = "foo" while true do a = a .. a end')
