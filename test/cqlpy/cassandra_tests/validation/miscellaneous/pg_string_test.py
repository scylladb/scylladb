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

def testPgSyleInsert(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(key ascii primary key, val text)") as table:
        # some non-terminated pg-strings
        assert_invalid_syntax(cql, table, "INSERT INTO %s (key, val) VALUES ($ $key_empty$$, $$'' value for empty$$)")
        assert_invalid_syntax(cql, table, "INSERT INTO %s (key, val) VALUES ($$key_empty$$, $$'' value for empty$ $)")
        assert_invalid_syntax(cql, table, "INSERT INTO %s (key, val) VALUES ($$key_empty$ $, $$'' value for empty$$)")

        # different pg-style markers for multiple strings
        execute(cql, table, "INSERT INTO %s (key, val) VALUES ($$prim$ $ $key$$, $$some '' arbitrary value$$)")
        # same empty pg-style marker for multiple strings
        execute(cql, table, "INSERT INTO %s (key, val) VALUES ($$key_empty$$, $$'' value for empty$$)")
        # stange but valid pg-style
        execute(cql, table, "INSERT INTO %s (key, val) VALUES ($$$foo$_$foo$$, $$$'' value for empty$$)")
        # these are conventional quoted strings
        execute(cql, table, "INSERT INTO %s (key, val) VALUES ('$txt$key$$$$txt$', '$txt$'' other value$txt$')")

        assert_rows(execute(cql, table, "SELECT key, val FROM %s WHERE key='prim$ $ $key'"),
                   row("prim$ $ $key", "some '' arbitrary value")
        )
        assert_rows(execute(cql, table, "SELECT key, val FROM %s WHERE key='key_empty'"),
                   row("key_empty", "'' value for empty")
        )
        assert_rows(execute(cql, table, "SELECT key, val FROM %s WHERE key='$foo$_$foo'"),
                   row("$foo$_$foo", "$'' value for empty")
        )
        assert_rows(execute(cql, table, "SELECT key, val FROM %s WHERE key='$txt$key$$$$txt$'"),
                   row("$txt$key$$$$txt$", "$txt$' other value$txt$")
        )

        # invalid syntax
        assert_invalid_syntax(cql, table, "INSERT INTO %s (key, val) VALUES ($ascii$prim$$$key$invterm$, $txt$some '' arbitrary value$txt$)")

def testMarkerPgFail(cql, test_keyspace):
    # must throw SyntaxException - not StringIndexOutOfBoundsException or similar
    with pytest.raises(SyntaxException):
        cql.execute("create function " + test_keyspace + "." + unique_name() + " ( input double ) called on null input returns bigint language java\n" +
                "AS $javasrc$return 0L;$javasrc$;")
