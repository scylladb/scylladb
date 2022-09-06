# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest
import re
from cassandra.protocol import InvalidRequest
from util import new_test_table

def assert_invalid_message(cql, table, message, cmd):
    with pytest.raises(InvalidRequest, match=re.escape(message)):
        cql.execute(cmd)

# Reproduces issue #11061
# Tests message when we compare collection column with integer value
def testInvalidCollectionIntegerOperandRelation(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int PRIMARY KEY, b set<int>, c list<int>, d map<int, int>") as table:
        operations = ['=', '>', '>=', '<', '<=']

        for oper in operations:
            assert_invalid_message(cql, table, "Collection column 'b' (set<int>) cannot be restricted by a '" + oper + "' relation",
                                            "SELECT * FROM " + table + " WHERE b " + oper + "1")
            assert_invalid_message(cql, table, "Collection column 'c' (list<int>) cannot be restricted by a '" + oper + "' relation",
                                            "SELECT * FROM " + table + " WHERE c " + oper + "1")
            assert_invalid_message(cql, table, "Collection column 'd' (map<int, int>) cannot be restricted by a '" + oper + "' relation",
                                            "SELECT * FROM " + table + " WHERE d " + oper + "1")
