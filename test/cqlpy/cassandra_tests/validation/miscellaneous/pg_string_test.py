# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from ...porting import *
from ....util import is_scylla

# Scylla doesn't support Java as a UDF language, so on Scylla this test uses
# an equivalent function written in Lua. The point of this test is the
# pg-style $$ string syntax, not the function's language.
def testPgSyleFunction(cql, test_keyspace):
    if is_scylla(cql):
        language, body = "lua", 'return "foobar"'
    else:
        language, body = "java", 'return "foobar";'
    fun = test_keyspace + "." + unique_name()
    cql.execute("create or replace function " + fun + " ( input double ) called on null input returns text language " + language + "\n" +
                "AS $$" + body + "$$")
    cql.execute("DROP FUNCTION " + fun)
