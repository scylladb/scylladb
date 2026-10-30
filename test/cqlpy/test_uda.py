# -*- coding: utf-8 -*-
# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for user-defined aggregates (UDA)
#############################################################################

import pytest
from cassandra.protocol import InvalidRequest, ConfigurationException
from .util import unique_name, new_test_keyspace, new_test_table, new_function, new_aggregate, new_type

# Test that computing an average by hand works the same as
# the built-in function
def test_custom_avg(scylla_only, cql, test_keyspace):
    schema = 'id bigint primary key'
    with new_test_table(cql, test_keyspace, schema) as table:
        for i in range(8):
            cql.execute(f"INSERT INTO {table} (id) VALUES ({10**i})")
        avg_partial_body = "(state tuple<bigint, bigint>, val bigint) CALLED ON NULL INPUT RETURNS tuple<bigint, bigint> LANGUAGE lua AS 'return {state[1] + val, state[2] + 1}'"
        div_body = "(state tuple<bigint, bigint>) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return state[1]//state[2]'"
        with new_function(cql, test_keyspace, avg_partial_body) as avg_partial, new_function(cql, test_keyspace, div_body) as div_fun:
            custom_avg_body = f"(bigint) SFUNC {avg_partial} STYPE tuple<bigint, bigint> FINALFUNC {div_fun} INITCOND (0,0)"
            with new_aggregate(cql, test_keyspace, custom_avg_body) as custom_avg:
                custom_res = [row for row in cql.execute(f"SELECT {test_keyspace}.{custom_avg}(id) AS result FROM {table}")]
                avg_res = [row for row in cql.execute(f"SELECT avg(id) AS result FROM {table}")]
                assert custom_res == avg_res

# Test that computing an aggregate which takes 2 parameters works fine.
# In this case - it's a simple map literal builder.
def test_map_literal_builder(scylla_only, cql, test_keyspace):
    schema = 'id int, k text, val int, primary key (id, k)'
    with new_test_table(cql, test_keyspace, schema) as table:
        for i in range(8):
            cql.execute(f"INSERT INTO {table} (id, k, val) VALUES (0, '{chr(ord('a') + i)}', {i})")
        map_literal_partial_body = "(state text, id text, val int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE lua AS 'return state..id..\":\"..tostring(val)..\",\"'"
        finish_body = "(state text) CALLED ON NULL INPUT RETURNS text LANGUAGE lua AS 'return state..\"}\"'"
        with new_function(cql, test_keyspace, map_literal_partial_body) as map_literal_partial, new_function(cql, test_keyspace, finish_body) as finish_fun:
            map_literal_body = f"(text, int) SFUNC {map_literal_partial} STYPE text FINALFUNC {finish_fun} INITCOND '{{'"
            with new_aggregate(cql, test_keyspace, map_literal_body) as map_literal:
                map_res = [row for row in cql.execute(f"SELECT {test_keyspace}.{map_literal}(k, val) AS result FROM {table}")]
                assert len(map_res) == 1 and map_res[0].result == '{a:0,b:1,c:2,d:3,e:4,f:5,g:6,h:7,}'

# Test that the state function and final function must exist and have correct signatures
def test_wrong_sfunc_or_ffunc(scylla_only, cql, test_keyspace):
    avg_partial_body = "(state tuple<bigint, text>, val bigint) CALLED ON NULL INPUT RETURNS tuple<bigint, text> LANGUAGE lua AS 'return {state[1] + val, \"hello\"}'"
    correct_avg_partial_body = "(state tuple<bigint, bigint>, val bigint) CALLED ON NULL INPUT RETURNS tuple<bigint, bigint> LANGUAGE lua AS 'return {state[1] + val, state[2]+1}'"
    partial_invalid_state_body = "(state tuple<bigint, bigint>, val bigint) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return state[1] + val'"
    div_body = "(state tuple<bigint, bigint>) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return state[1]//state[2]'"
    div_body2 = "(state tuple<bigint, bigint>) CALLED ON NULL INPUT RETURNS float LANGUAGE lua AS 'return state[1]/state[2]'"
    with new_function(cql, test_keyspace, avg_partial_body) as avg_partial, new_function(cql, test_keyspace, div_body) as div_fun, new_function(cql, test_keyspace, div_body2) as div_fun2:
      with new_function(cql, test_keyspace, correct_avg_partial_body) as correct_avg_partial, new_function(cql, test_keyspace, partial_invalid_state_body) as inv_state_fun:
        custom_avg_body = f"(bigint) SFUNC {avg_partial} STYPE tuple<bigint, bigint> FINALFUNC {div_fun} INITCOND (0,0)"
        with pytest.raises(InvalidRequest, match="not found"):
            cql.execute(f"CREATE AGGREGATE {test_keyspace}.{unique_name()} {custom_avg_body}")
        custom_avg_body = f"(bigint) SFUNC {avg_partial} STYPE tuple<bigint, bigint> FINALFUNC {div_fun2} INITCOND (0,0)"
        with pytest.raises(InvalidRequest, match="not found"):
            cql.execute(f"CREATE AGGREGATE {test_keyspace}.{unique_name()} {custom_avg_body}")
        custom_avg_body = f"(bigint) SFUNC i_do_not_exist STYPE tuple<bigint, bigint> FINALFUNC {div_fun2} INITCOND (0,0)"
        with pytest.raises(InvalidRequest, match="not found"):
            cql.execute(f"CREATE AGGREGATE {test_keyspace}.{unique_name()} {custom_avg_body}")
        custom_avg_body = f"(bigint) SFUNC {correct_avg_partial} STYPE tuple<bigint, bigint> FINALFUNC i_do_not_exist_either INITCOND (0,0)"
        with pytest.raises(InvalidRequest, match="not found"):
            cql.execute(f"CREATE AGGREGATE {test_keyspace}.{unique_name()} {custom_avg_body}")
        custom_avg_body = f"(bigint) SFUNC {inv_state_fun} STYPE tuple<bigint, bigint> FINALFUNC {div_fun} INITCOND (0,0)"
        with pytest.raises(InvalidRequest, match="doesn't return state"):
            cql.execute(f"CREATE AGGREGATE {test_keyspace}.{unique_name()} {custom_avg_body}")

# Test that dropping the state function, reduce function or the final function is not allowed if it's used by an aggregate
def test_drop_sfunc_reducefunc_or_ffunc(scylla_only, cql, test_keyspace):
    avg_partial_body = "(state tuple<bigint, bigint>, val bigint) CALLED ON NULL INPUT RETURNS tuple<bigint, bigint> LANGUAGE lua AS 'return {state[1] + val, state[2] + 1}'"
    avg_reduce_body = "(state1 tuple<bigint, bigint>, state2 tuple<bigint, bigint>) CALLED ON NULL INPUT RETURNS tuple<bigint, bigint> LANGUAGE lua AS 'return {state1[1] + state2[1], state1[2] + state2[2]}'"
    div_body = "(state tuple<bigint, bigint>) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return state[1]//state[2]'"
    with new_function(cql, test_keyspace, avg_partial_body, args="tuple<bigint, bigint>, bigint") as avg_partial,\
         new_function(cql, test_keyspace, avg_reduce_body, args="tuple<bigint, bigint>, tuple<bigint, bigint>") as avg_reduce,\
         new_function(cql, test_keyspace, div_body, args="tuple<bigint, bigint>") as div_fun:
        custom_avg_body = f"(bigint) SFUNC {avg_partial} STYPE tuple<bigint, bigint> REDUCEFUNC {avg_reduce} FINALFUNC {div_fun} INITCOND (0,0)"
        with new_aggregate(cql, test_keyspace, custom_avg_body) as custom_avg:
            with pytest.raises(InvalidRequest, match="it is used"):
                cql.execute(f"DROP FUNCTION {test_keyspace}.{avg_partial}")
            with pytest.raises(InvalidRequest, match="it is used"):
                cql.execute(f"DROP FUNCTION {test_keyspace}.{avg_reduce}")
            with pytest.raises(InvalidRequest, match="it is used"):
                cql.execute(f"DROP FUNCTION {test_keyspace}.{div_fun}")
            avg_partial_body2 = "(state bigint, val bigint) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return 42'"
            div_body2 = "(state bigint) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return 4'"
            with new_function(cql, test_keyspace, avg_partial_body2, avg_partial, "bigint, bigint"),\
                 new_function(cql, test_keyspace, avg_partial_body2, avg_reduce, "bigint, bigint"),\
                 new_function(cql, test_keyspace, div_body2, div_fun, "bigint"):
                with pytest.raises(InvalidRequest, match="There are multiple"):
                    cql.execute(f"DROP FUNCTION {test_keyspace}.{avg_partial}")
                with pytest.raises(InvalidRequest, match="There are multiple"):
                    cql.execute(f"DROP FUNCTION {test_keyspace}.{avg_reduce}")
                with pytest.raises(InvalidRequest, match="There are multiple"):
                    cql.execute(f"DROP FUNCTION {test_keyspace}.{div_fun}")
                with pytest.raises(InvalidRequest, match="it is used"):
                    cql.execute(f"DROP FUNCTION {test_keyspace}.{avg_partial}(tuple<bigint, bigint>, bigint)")
                with pytest.raises(InvalidRequest, match="it is used"):
                    cql.execute(f"DROP FUNCTION {test_keyspace}.{avg_reduce}(tuple<bigint, bigint>, tuple<bigint, bigint>)")
                with pytest.raises(InvalidRequest, match="it is used"):
                    cql.execute(f"DROP FUNCTION {test_keyspace}.{div_fun}(tuple<bigint, bigint>)")

# Test that CREATE OR REPLACE FUNCTION can't change the return type of a
# function used by an aggregate, even to a compatible type (ascii to text).
# The aggregate's state type must be exactly its state function's return
# type, and an aggregate whose state function returns a different type
# would fail to load from the schema tables (e.g., after a restart).
# Cassandra allows this change, because it doesn't store the state type
# separately from the state function.
def test_replace_sfunc_return_type(scylla_only, cql, test_keyspace):
    concat_body = "(state ascii, val ascii) CALLED ON NULL INPUT RETURNS ascii LANGUAGE lua AS 'return state .. val'"
    concat_body2 = "(state ascii, val ascii) CALLED ON NULL INPUT RETURNS text LANGUAGE lua AS 'return state .. val'"
    with new_function(cql, test_keyspace, concat_body) as concat:
        with new_aggregate(cql, test_keyspace, f"(ascii) SFUNC {concat} STYPE ascii INITCOND ''"):
            # Check that the return type can't change while the aggregate
            # uses the function
            with pytest.raises(InvalidRequest, match='used by user-defined aggregate'):
                cql.execute(f'CREATE OR REPLACE FUNCTION {test_keyspace}.{concat} {concat_body2}')
            # Check that replacing just the body is still allowed
            cql.execute(f'CREATE OR REPLACE FUNCTION {test_keyspace}.{concat} {concat_body}')
        # Check that without the aggregate, the compatible return type is
        # allowed
        cql.execute(f'CREATE OR REPLACE FUNCTION {test_keyspace}.{concat} {concat_body2}')

# Test that the state function takes a correct number of arguments - the state and the new input
def test_incorrect_state_func(scylla_only, cql, test_keyspace):
    avg_partial_body = "(state tuple<bigint, bigint>, val bigint, redundant int) CALLED ON NULL INPUT RETURNS tuple<bigint, bigint> LANGUAGE lua AS 'return {state[1] + val, state[2] + 1}'"
    div_body = "(state tuple<bigint, bigint>) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return state[1]//state[2]'"
    with new_function(cql, test_keyspace, avg_partial_body) as avg_partial, new_function(cql, test_keyspace, div_body) as div_fun:
        custom_avg_body = f"(bigint) SFUNC {avg_partial} STYPE tuple<bigint, bigint> FINALFUNC {div_fun} INITCOND (0,0)"
        with pytest.raises(InvalidRequest, match="State function .* not found"):
            cql.execute(f"CREATE AGGREGATE {test_keyspace}.{unique_name()} {custom_avg_body}")
    avg2_partial_body = "(state tuple<bigint, bigint>) CALLED ON NULL INPUT RETURNS tuple<bigint, bigint> LANGUAGE lua AS 'return {state[1] + 42, state[2] + 1}'"
    div_body = "(state tuple<bigint, bigint>) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return state[1]//state[2]'"
    with new_function(cql, test_keyspace, avg2_partial_body) as avg2_partial, new_function(cql, test_keyspace, div_body) as div_fun:
        custom_avg_body = f"(bigint) SFUNC {avg2_partial} STYPE tuple<bigint, bigint> FINALFUNC {div_fun} INITCOND (0,0)"
        with pytest.raises(InvalidRequest, match="State function .* not found"):
            cql.execute(f"CREATE AGGREGATE {test_keyspace}.{unique_name()} {custom_avg_body}")

# Test that UDA works without final function and returns accumulator then
def test_no_final_func(scylla_only, cql, test_keyspace):
    schema = "id int primary key"
    sum_partial_body = "(acc int, val int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE lua AS 'return acc + val'"
    with new_test_table(cql, test_keyspace, schema) as table:
        for i in range(5):
            cql.execute(f"INSERT INTO {table} (id) VALUES ({i})")
        with new_function(cql, test_keyspace, sum_partial_body) as sum_partial:
            custom_sum_body = f"(int) SFUNC {sum_partial} STYPE int INITCOND 0"
            with new_aggregate(cql, test_keyspace, custom_sum_body) as custom_sum:
                custom_res = cql.execute(f"SELECT {custom_sum}(id) AS result FROM {table}").one()
                avg_res = cql.execute(f"SELECT sum(id) AS result FROM {table}").one()
                assert custom_res == avg_res

# Test that UDA works without initcond and inits acc as null
def test_no_initcond(scylla_only, cql, test_keyspace):
    rows = 5
    schema = "id int primary key"
    # This aggregate will return `1000 - #rows` if there is no initcond and `initcond - #rows` otherwise
    state_body = "(acc int, val int) CALLED ON NULL INPUT RETURNS int LANGUAGE lua AS 'if acc == null then return 999 else return acc - 1 end'"
    final_body = "(acc int) CALLED ON NULL INPUT RETURNS int LANGUAGE lua AS 'return acc'"
    with new_test_table(cql, test_keyspace, schema) as table:
        for i in range(rows):
            cql.execute(f"INSERT INTO {table} (id) VALUES ({i})")
        with new_function(cql, test_keyspace, state_body) as state_func, new_function(cql, test_keyspace, final_body) as final_func:
            aggr_body = f"(int) SFUNC {state_func} STYPE int FINALFUNC {final_func}"
            with new_aggregate(cql, test_keyspace, aggr_body) as aggr_func:
                result = cql.execute(f"SELECT {aggr_func}(id) AS result FROM {table}").one()
                assert result.result == (1000 - rows)

# Test if reduce function is assigned to UDA
def test_reduce_function(scylla_only, cql, test_keyspace):
    row_body = "(acc bigint, val int) RETURNS NULL ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return acc+val'"
    reduce_body = "(acc1 bigint, acc2 bigint) RETURNS NULL ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return acc1+acc2'"
    final_body = "(acc bigint) RETURNS NULL ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return -acc'"

    with new_function(cql, test_keyspace, row_body) as row_f:
        with new_function(cql, test_keyspace, reduce_body) as reduce_f:
            with new_function(cql, test_keyspace, final_body) as final_f:
                aggr_body = f"(int) SFUNC {row_f} STYPE bigint REDUCEFUNC {reduce_f} FINALFUNC {final_f} INITCOND 0"
                with new_aggregate(cql, test_keyspace, aggr_body) as aggr_f:
                    result = cql.execute(f"SELECT aggregate_name, reduce_func, state_type FROM system_schema.scylla_aggregates WHERE keyspace_name = '{test_keyspace}' AND aggregate_name = '{aggr_f}' AND argument_types = ['int']").one()
                    assert result == (aggr_f, reduce_f, 'bigint')

def test_collection_initcond(scylla_only, cql, test_keyspace):
    schema = "id int primary key"
    row_body = "(acc list<int>, val int) RETURNS NULL ON NULL INPUT RETURNS list<int> LANGUAGE lua AS 'table.insert(acc, val); return acc'"
    final_body = "(acc list<int>) RETURNS NULL ON NULL INPUT RETURNS list<int> LANGUAGE lua AS 'table.sort(acc); return acc'"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"INSERT INTO {table} (id) VALUES (5)")
        cql.execute(f"INSERT INTO {table} (id) VALUES (3)")
        cql.execute(f"INSERT INTO {table} (id) VALUES (8)")
        cql.execute(f"INSERT INTO {table} (id) VALUES (2)")
        with new_function(cql, test_keyspace, row_body) as row_f:
            with new_function(cql, test_keyspace, final_body) as final_f:
                aggr_body = f"(int) SFUNC {row_f} STYPE list<int> finalfunc {final_f} INITCOND [6,12,9,7,4,1]"
                with new_aggregate(cql, test_keyspace, aggr_body) as aggr_f:
                    result = cql.execute(f"SELECT {aggr_f}(id) AS result FROM {table}").one()
                    assert result.result == [1,2,3,4,5,6,7,8,9,12]

def test_nested_collection_initcond(scylla_only, cql, test_keyspace):
    with new_type(cql, test_keyspace, "(a int, b int)") as typ:
        schema = "id tuple<int,int,int> primary key"
        row_body = f"""(acc frozen<list<set<{typ}>>>, val tuple<int,int,int>) RETURNS NULL ON NULL INPUT RETURNS frozen<list<set<{typ}>>> LANGUAGE lua AS '
        while acc[val[1]] == nil do
            table.insert(acc, {{}});
        end
        acc[val[1]][{{a=val[2], b=val[3]}}] = true;
        return acc'"""
        final_body = f"""(acc frozen<list<set<{typ}>>>) RETURNS NULL ON NULL INPUT RETURNS list<double> LANGUAGE lua AS '
        local ret = {{}}
        for i, k in ipairs(acc) do
            ret[i] = 0.0;
            siz = 0;
            for x, v in pairs(k) do
                ret[i] = ret[i] + x.a * x.b;
                siz = siz + x.b;
            end
            if siz > 0 then
                ret[i] = ret[i] / siz;
            else
                ret[i] = 0;
            end
        end
        return ret '"""
        with new_test_table(cql, test_keyspace, schema) as table:
                cql.execute(f"INSERT INTO {table} (id) VALUES ((1,2,3))")
                cql.execute(f"INSERT INTO {table} (id) VALUES ((1,3,4))")
                cql.execute(f"INSERT INTO {table} (id) VALUES ((2,2,3))")
                cql.execute(f"INSERT INTO {table} (id) VALUES ((2,1,2))")
                cql.execute(f"INSERT INTO {table} (id) VALUES ((3,3,3))")
                cql.execute(f"INSERT INTO {table} (id) VALUES ((3,1,4))")
                with new_function(cql, test_keyspace, row_body) as row_f:
                    with new_function(cql, test_keyspace, final_body) as final_f:
                        aggr_body = f"(tuple<int,int,int>) SFUNC {row_f} STYPE frozen<list<set<{typ}>>> finalfunc {final_f} INITCOND []"
                        with new_aggregate(cql, test_keyspace, aggr_body) as aggr_f:
                            result = cql.execute(f"SELECT {aggr_f}(id) AS result FROM {table}").one()
                            assert result.result == [18/7,8/5,13/7]

def test_drop_keyspace_with_uda(scylla_only, cql):
    ks = unique_name()
    cql.execute(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}}")
    f1 = unique_name()
    cql.execute(f"CREATE FUNCTION {ks}.{f1}(acc int, val int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE lua AS $$ return acc $$")
    f2 = unique_name()
    cql.execute(f"CREATE FUNCTION {ks}.{f2}(acc int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE lua AS $$ return acc $$")
    agg = unique_name()
    cql.execute(f"CREATE AGGREGATE {ks}.{agg}(int) SFUNC {f1} STYPE int FINALFUNC {f2} INITCOND 0")
    aggregates_before = cql.execute(f"SELECT COUNT(*) AS result FROM system_schema.aggregates").one()
    cql.execute(f"DROP KEYSPACE {ks}")
    aggregates_after = cql.execute(f"SELECT COUNT(*) AS result FROM system_schema.aggregates").one()
    assert aggregates_before.result - aggregates_after.result == 1
    with pytest.raises(ConfigurationException, match="Cannot drop non existing keyspace"):
        cql.execute(f"DROP KEYSPACE {ks}")

# Test that replacing the state function, reduce function or the final function successfully changes the function used by the aggregate.
# When the state or final function is replaced, the new function should be used in following calls to the aggregate. Cassandra keeps using
# the old function, which we consider a cassandra bug.
def test_replace_sfunc_ffunc(cql, test_keyspace, cassandra_bug, has_java_udf):
    schema = "id bigint primary key"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"INSERT INTO {table} (id) VALUES (1)")
        cql.execute(f"INSERT INTO {table} (id) VALUES (2)")
        cql.execute(f"INSERT INTO {table} (id) VALUES (3)")
        if has_java_udf:
            sum_partial_body = "(state bigint, val bigint) CALLED ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return state + val'"
            sum_partial_body2 = "(state bigint, val bigint) CALLED ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return state + 2 * val'"
            sum_final_body = "(state bigint) CALLED ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return state'"
            sum_final_body2 = "(state bigint) CALLED ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return 3 * state'"
        else:
            sum_partial_body = "(state bigint, val bigint) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return state + val'"
            sum_partial_body2 = "(state bigint, val bigint) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return state + 2 * val'"
            sum_final_body = "(state bigint) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return state'"
            sum_final_body2 = "(state bigint) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return 3 * state'"

        with new_function(cql, test_keyspace, sum_partial_body) as sum_partial, new_function(cql, test_keyspace, sum_final_body) as sum_final:
            custom_sum_body = f"(bigint) SFUNC {sum_partial} STYPE bigint FINALFUNC {sum_final} INITCOND 0"
            with new_aggregate(cql, test_keyspace, custom_sum_body) as custom_sum:
                result = cql.execute(f"SELECT {custom_sum}(id) AS result FROM {table}").one()
                assert result.result == 6
                cql.execute(f"CREATE OR REPLACE FUNCTION {test_keyspace}.{sum_partial} {sum_partial_body2}")
                result = cql.execute(f"SELECT {custom_sum}(id) AS result FROM {table}").one()
                assert result.result == 12
                cql.execute(f"CREATE OR REPLACE FUNCTION {test_keyspace}.{sum_final} {sum_final_body2}")
                result = cql.execute(f"SELECT {custom_sum}(id) AS result FROM {table}").one()
                assert result.result == 36

# Test that an aggregate cannot reference a UDT from another keyspace.
# Ported from scylla-dtest user_functions_test.py::test_aggregate_with_udt_keyspace_isolation (SCYLLADB-1928).
# Reproduces CASSANDRA-9409.
def test_aggregate_with_udt_keyspace_isolation(cql, test_keyspace):
    with new_type(cql, test_keyspace, "(a int)") as udt:
        _, udt_name = udt.split('.')
        with new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as other_ks:
            # The SFUNC 'plus' doesn't need to exist — the cross-keyspace
            # UDT reference error fires during type validation, before
            # function lookup.
            with pytest.raises(InvalidRequest, match="cannot refer to a user type in keyspace"):
                cql.execute(
                    f"CREATE AGGREGATE {other_ks}.suma ({test_keyspace}.{udt_name}) "
                    f"SFUNC plus STYPE int INITCOND 10")

# Test an aggregate whose state type is a UDT with a case-sensitive field
# name, with an initial condition. The initial condition is saved in the
# schema as a CQL literal, which must quote this field name so it can be read
# back. Cassandra has a similar bug, CASSANDRA-21747: CREATE AGGREGATE fails
# with "Unknown field 'a'", so this test is marked cassandra_bug.
# Reproduces SCYLLADB-5270 (the field name wasn't quoted, so reading the saved
# literal back failed, which stopped group 0).
def test_uda_initcond_udt_case_sensitive_field(cql, test_keyspace, cassandra_bug, has_java_udf):
    # A state function adding x to the UDT's only field.
    if has_java_udf:
        body = "LANGUAGE java AS 's.setInt(0, s.getInt(0) + x); return s;'"
    else:
        body = "LANGUAGE lua AS 'for k, v in pairs(s) do s[k] = v + x end return s'"
    with new_type(cql, test_keyspace, '("A" int)') as udt:
        with new_test_table(cql, test_keyspace, 'p int PRIMARY KEY, x int') as table:
            cql.execute(f'INSERT INTO {table} (p, x) VALUES (1, 2)')
            cql.execute(f'INSERT INTO {table} (p, x) VALUES (2, 3)')
            with new_function(cql, test_keyspace, f'(s {udt}, x int) RETURNS NULL ON NULL INPUT RETURNS {udt} {body}') as sf:
                with new_aggregate(cql, test_keyspace, f'(int) SFUNC {sf} STYPE {udt} INITCOND {{"A": 10}}') as agg:
                    select = f'SELECT {test_keyspace}.{agg}(x) FROM {table}'
                    assert [tuple(r[0]) for r in cql.execute(select)] == [(15,)]
                    # DESCRIBE AGGREGATE prints the initial condition in the
                    # same way, and its output must re-create the aggregate.
                    desc = cql.execute(f'DESCRIBE AGGREGATE {test_keyspace}.{agg}').one().create_statement
                    cql.execute(f'DROP AGGREGATE {test_keyspace}.{agg}')
                    cql.execute(desc)
                    assert [tuple(r[0]) for r in cql.execute(select)] == [(15,)]
