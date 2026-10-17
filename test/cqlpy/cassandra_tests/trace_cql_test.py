# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from .porting import *

# The Java test makes Cassandra wait for trace events to complete (see
# CASSANDRA-12754) through Cassandra's internal TraceStateImpl class. The
# Python driver's get_query_trace() waits for the trace to be complete.

# Scylla and Cassandra both record the bound values of a prepared statement
# in the trace's parameters, but in different formats: Cassandra names the
# i'th value "bound_var_<i>_<name>" and formats it as a CQL literal (e.g.,
# 'lukasz' with quotes, or (3, 'bar', 2.1)), truncated to 1000 characters,
# while Scylla names it "param[<i>]" and formats it in its own way (e.g.,
# lukasz without quotes, or 3:bar:2.1), truncated to 64 characters. The trace
# parameters aren't a documented interface, so the translated test only checks
# what the two have in common, and leaves the original checks of the exact
# format in comments.
def getQueryTrace(cql, stmt, args):
    return cql.execute(stmt, args, trace=True).get_query_trace()

def boundValue(trace, i, name):
    params = trace.parameters
    return params.get(f"bound_var_{i}_{name}", params.get(f"param[{i}]"))

LONG_BOUND_VALUE = ("Indulgence announcing uncommonly met she continuing two unpleasing terminated. Now " +
                    "busy say down the shed eyes roof paid her. Of shameless collected suspicion existence " +
                    "in. Share walls stuff think but the arise guest. Course suffer to do he sussex it " +
                    "window advice. Yet matter enable misery end extent common men should. Her indulgence " +
                    "but assistance favourable cultivated everything collecting." +
                    "On projection apartments unsatiable so if he entreaties appearance. Rose you wife " +
                    "how set lady half wish. Hard sing an in true felt. Welcomed stronger if steepest " +
                    "ecstatic an suitable finished of oh. Entered at excited at forming between so " +
                    "produce. Chicken unknown besides attacks gay compact out you. Continuing no " +
                    "simplicity no favourable on reasonably melancholy estimating. Own hence views two " +
                    "ask right whole ten seems. What near kept met call old west dine. Our announcing " +
                    "sufficient why pianoforte. Full age foo set feel her told. Tastes giving in passed" +
                    "direct me valley as supply. End great stood boy noisy often way taken short. Rent the " +
                    "size our more door. Years no place abode in ﻿no child my. Man pianoforte too " +
                    "solicitude friendship devonshire ten ask. Course sooner its silent but formal she " +
                    "led. Extensive he assurance extremity at breakfast. Dear sure ye sold fine sell on. " +
                    "Projection at up connection literature insensible motionless projecting." +
                    "Nor hence hoped her after other known defer his. For county now sister engage had " +
                    "season better had waited. Occasional mrs interested far expression acceptance. Day " +
                    "either mrs talent pulled men rather regret admire but. Life ye sake it shed. Five " +
                    "lady he cold in meet up. Service get met adapted matters offence for. Principles man " +
                    "any insipidity age you simplicity understood. Do offering pleasure no ecstatic " +
                    "whatever on mr directly. ")

COMPLEX_TABLE = "(id int primary key, v1 text, v2 tuple<int, text, float>, v3 map<int, text>)"

def testCqlStatementTracing(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id int primary key, v1 text, v2 text)") as table:
        execute(cql, table, "INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 1, "Apache", "Cassandra")
        execute(cql, table, "INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 2, "trace", "test")

        cql_ = f"SELECT id, v1, v2 FROM {table} WHERE id = ?"
        pstmt = cql.prepare(cql_)
        trace = getQueryTrace(cql, pstmt, [1])
        assert cql_ == trace.parameters.get("query")
        assert "1" == boundValue(trace, 0, "id")

        cql2 = f"SELECT id, v1, v2 FROM {table} WHERE id IN (?, ?, ?)"
        pstmt = cql.prepare(cql2)
        trace = getQueryTrace(cql, pstmt, [19, 15, 16])
        assert cql2 == trace.parameters.get("query")
        assert "19" == boundValue(trace, 0, "id")
        assert "15" == boundValue(trace, 1, "id")
        assert "16" == boundValue(trace, 2, "id")

    #some more complex tests for tables with map and tuple data types and long bound values
    with create_table(cql, test_keyspace, COMPLEX_TABLE) as table:
        execute(cql, table, "INSERT INTO %s (id, v1, v2, v3) values (?, ?, ?, ?)", 12, "mahdix", (3, "bar", 2.1),
                {1290: "birthday", 39: "anniversary"})
        execute(cql, table, "INSERT INTO %s (id, v1, v2, v3) values (?, ?, ?, ?)", 274, "CassandraRocks", (9, "foo", 3.14),
                {9181: "statement", 716: "public speech"})

        cql_ = f"SELECT id, v1, v2, v3 FROM {table} WHERE v2 = ? ALLOW FILTERING"
        pstmt = cql.prepare(cql_)
        value = (3, "bar", 2.1)
        trace = getQueryTrace(cql, pstmt, [value])
        assert cql_ == trace.parameters.get("query")
        assert boundValue(trace, 0, "v2") is not None
        # Cassandra-specific format:
        # assert "(3, 'bar', 2.1)" == trace.parameters.get("bound_var_0_v2")

        cql2 = f"SELECT id, v1, v2, v3 FROM {table} WHERE v3 CONTAINS KEY ? ALLOW FILTERING"
        pstmt = cql.prepare(cql2)
        trace = getQueryTrace(cql, pstmt, [9181])
        assert cql2 == trace.parameters.get("query")
        assert "9181" == boundValue(trace, 0, "key(v3)")

        cql3 = f"SELECT id, v1, v2, v3 FROM {table} WHERE v3 CONTAINS ? ALLOW FILTERING"
        pstmt = cql.prepare(cql3)
        trace = getQueryTrace(cql, pstmt, [LONG_BOUND_VALUE])
        assert cql3 == trace.parameters.get("query")

        #when tracing is done, this boundValue will be surrounded by single quote, and first 1000 characters
        #will be filtered. Here we take into account single quotes by adding them to the expected output
        # Cassandra-specific format:
        # assert "'" + LONG_BOUND_VALUE[0:999] + "...'" == trace.parameters.get("bound_var_0_value(v3)")
        traced = boundValue(trace, 0, "value(v3)")
        assert LONG_BOUND_VALUE[0:50] in traced
        assert len(traced) < len(LONG_BOUND_VALUE)
        assert traced.rstrip("'").endswith("...")

# The end of the Java test testCqlStatementTracing, checking that the trace
# shows which bound values are unset. Cassandra shows an unset value as
# "<unset>", and we check that it is at least shown differently from a null.
# Reproduces SCYLLADB-5188 (unset bound value is traced as "null").
@pytest.mark.xfail(reason="SCYLLADB-5188")
def testCqlStatementTracingUnset(cql, test_keyspace):
    with create_table(cql, test_keyspace, COMPLEX_TABLE) as table:
        value = (3, "bar", 2.1)
        pstmt = cql.prepare(f"INSERT INTO {table} (id, v1, v2, v3) values (?, ?, ?, ?)")
        names = ["id", "v1", "v2", "v3"]
        def boundParameters(trace):
            return [boundValue(trace, i, name) for i, name in enumerate(names)]
        nullParameters = boundParameters(getQueryTrace(cql, pstmt, [13, "lukasz", None, None]))

        # test query tracing after UNSET collection type
        boundParameters1 = boundParameters(getQueryTrace(cql, pstmt, [13, "lukasz", value, UNSET_VALUE]))
        # Cassandra-specific format:
        # assert ["13", "'lukasz'", "(3, 'bar', 2.1)", "<unset>"] == boundParameters1
        assert "13" == boundParameters1[0]
        assert boundParameters1[3] is not None and boundParameters1[3] != nullParameters[3]

        # test query tracing after UNSET tuple type
        boundParameters2 = boundParameters(getQueryTrace(cql, pstmt, [13, "lukasz", UNSET_VALUE, UNSET_VALUE]))
        # Cassandra-specific format:
        # assert ["13", "'lukasz'", "<unset>", "<unset>"] == boundParameters2
        assert "13" == boundParameters2[0]
        assert boundParameters2[2] is not None and boundParameters2[2] != nullParameters[2]
        assert boundParameters2[3] is not None and boundParameters2[3] != nullParameters[3]
