# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of Cassandra 4.1.1 (commit f54ef5e824daf758087ff4efa94041467d46f8eb)
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

import datetime
import uuid
from cassandra_tests.porting import *

from cassandra.util import Date
from util import new_function, unique_name, new_secondary_index
import os

def is_scylla(cql, test_keyspace):
    try:
        with new_function(cql, test_keyspace, "() RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return 1;'"):
            pass
        return True
    except:
        return False

def read_function_from_file(file_name, wasm_name=None, udf_name=None):
    wat_path = os.path.realpath(os.path.join(__file__, f'../../../../../../build/wasm/{file_name}.wat'))
    wasm_name = wasm_name or file_name
    udf_name = udf_name or wasm_name
    try:
      with open(wat_path, "r") as f:
          return f.read().replace("'", "''").replace(f'export "{wasm_name}"', f'export "{udf_name}"')
    except:
        print(f"Can't open {wat_path}.\nPlease build Wasm examples.")
        exit(1)

def test_complex_null_values(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(txt text, i int)") as type:
        schema = f"(key int primary key, lst list<double>, st set<text>, mp map<int, boolean>, tup frozen<tuple<double, text, int, boolean>>, udt frozen<{type}>)"
        with create_table(cql, test_keyspace, schema) as table:
            use_wasm = is_scylla(cql, test_keyspace)
            lang = "wasm" if use_wasm else "java"
            flist_name = unique_name()
            flist_wasm_src = read_function_from_file("test_complex_null_values", "return_input_flist", flist_name) if use_wasm else 'return input;'
            flist_src = f"(input list<double>) CALLED ON NULL INPUT RETURNS list<double> LANGUAGE {lang} AS '{flist_wasm_src}'"
            fset_name = unique_name()
            fset_wasm_src = read_function_from_file("test_complex_null_values", "return_input_fset", fset_name) if use_wasm else 'return input;'
            fset_src = f"(input set<text>) CALLED ON NULL INPUT RETURNS set<text> LANGUAGE {lang} AS '{fset_wasm_src}'"
            fmap_name = unique_name()
            fmap_wasm_src = read_function_from_file("test_complex_null_values", "return_input_fmap", fmap_name) if use_wasm else 'return input;'
            fmap_src = f"(input map<int, boolean>) CALLED ON NULL INPUT RETURNS map<int, boolean> LANGUAGE {lang} AS '{fmap_wasm_src}'"
            ftup_name = unique_name()
            ftup_wasm_src = read_function_from_file("test_complex_null_values", "return_input_ftup", ftup_name) if use_wasm else 'return input;'
            ftup_src = f"(input tuple<double, text, int, boolean>) CALLED ON NULL INPUT RETURNS tuple<double, text, int, boolean> LANGUAGE {lang} AS '{ftup_wasm_src}'"
            fudt_name = unique_name()
            fudt_wasm_src = read_function_from_file("test_complex_null_values", "return_input_fudt", fudt_name) if use_wasm else 'return input;'
            fudt_src = f"(input {type}) CALLED ON NULL INPUT RETURNS {type} LANGUAGE {lang} AS '{fudt_wasm_src}'"

            cql.execute(f"INSERT INTO {table} (key, lst, st, mp, tup, udt) VALUES (1, [1.0, 2.0, 3.0], {{'one', 'three', 'two'}}, {{1:true, 2:false, 3:true}}, (1.0, 'one', 42, false), {{txt:'one', i:1}})")
            cql.execute(f"INSERT INTO {table} (key, lst, st, mp, tup, udt) VALUES (2, null, null, null, null, null)")
            with new_function(cql, test_keyspace, flist_src, flist_name), new_function(cql, test_keyspace, fset_src, fset_name), \
            new_function(cql, test_keyspace, fmap_src, fmap_name), new_function(cql, test_keyspace, ftup_src, ftup_name), \
            new_function(cql, test_keyspace, fudt_src, fudt_name):
                cql.execute(f"SELECT {flist_name}(lst) FROM {table} WHERE key = 1")
                cql.execute(f"SELECT {fset_name}(st) FROM {table} WHERE key = 1")
                cql.execute(f"SELECT {fmap_name}(mp) FROM {table} WHERE key = 1")
                cql.execute(f"SELECT {ftup_name}(tup) FROM {table} WHERE key = 1")
                cql.execute(f"SELECT {fudt_name}(udt) FROM {table} WHERE key = 1")
                row = cql.execute(f"SELECT {flist_name}(lst) as l, {fset_name}(st) as s, {fmap_name}(mp) as m, {ftup_name}(tup) as t, {fudt_name}(udt) as u FROM {table} WHERE key = 1").one()
                assert row.l
                assert row.s
                assert row.m
                assert row.t
                assert row.u
                row = cql.execute(f"SELECT {flist_name}(lst) as l, {fset_name}(st) as s, {fmap_name}(mp) as m, {ftup_name}(tup) as t, {fudt_name}(udt) as u FROM {table} WHERE key = 2").one()
                assert not row.l
                assert not row.s
                assert not row.m
                assert not row.t
                assert not row.u
                # Cassandra tests query execution both using internal commands and using the API for all protocol versions.
                # We only test the API here, but if we add more ABI versions we should test them as well.

class TypesTestDef:
    def __init__(self, udf_type, table_type, column_name, reference_value):
        self.udf_type = udf_type
        self.table_type = table_type
        self.column_name = column_name
        self.reference_value = reference_value

def test_types_with_and_without_nulls(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(txt text, i int)") as type:
        now = datetime.datetime.now()
        # Cassandra only supports milliseconds in timestamps, so we need to round down to the nearest millisecond.
        micros = now.microsecond // 1000 * 1000
        type_defs = [
            # udf type, table type, column, reference value
            TypesTestDef("timestamp", "timestamp", "ts", now.replace(microsecond=micros)),
            TypesTestDef("date", "date", "dt", Date(12345)),
            TypesTestDef("time", "time", "tim", 12345),
            TypesTestDef("uuid", "uuid", "uu", uuid.uuid4()),
            TypesTestDef("timeuuid", "timeuuid", "tu", uuid.uuid1()),
            TypesTestDef("tinyint", "tinyint", "ti", 42),
            TypesTestDef("smallint", "smallint", "si", 43),
            TypesTestDef("int", "int", "i", 44),
            TypesTestDef("bigint", "bigint", "bi", 45),
            TypesTestDef("float", "float", "f", 46.0),
            TypesTestDef("double", "double", "d", 47.0),
            TypesTestDef("boolean", "boolean", "x", True),
            TypesTestDef("ascii", "ascii", "a", "tqbfjutld"),
            TypesTestDef("text", "text", "txt", "k\u00f6lsche jung"),
            # TypesTestDef(type, f"frozen<{type}>", "u", null),
            TypesTestDef("tuple<int, text>", "frozen<tuple<int, text>>", "tup", (1, "foo")),
        ]

        schema = "(key int PRIMARY KEY"
        values = [1]
        for type_def in type_defs:
            schema += ", " + type_def.column_name + ' ' + type_def.table_type
            values.append(type_def.reference_value)
        schema += ")"

        with create_table(cql, test_keyspace, schema) as table:
            insert_str = f"INSERT INTO {table} (key"
            for type_def in type_defs:
                insert_str += ", " + type_def.column_name
            insert_str += ") VALUES (?"
            for type_def in type_defs:
                insert_str += ", ?"
            insert_str += ")"
            insert_stmt = cql.prepare(insert_str)
            cql.execute(insert_stmt, values)

            for i in range(len(values)):
                values[i] = None
            values[0] = 2
            cql.execute(insert_stmt, values)

            for type_def in type_defs:
                use_wasm = is_scylla(cql, test_keyspace)
                lang = "wasm" if use_wasm else "java"
                fun_name = unique_name()
                fun_wasm_src = read_function_from_file("test_types_with_and_without_nulls", f"check_arg_and_return_{type_def.column_name}", fun_name) if use_wasm else 'return input;'
                fun_src = f"(input {type_def.udf_type}) CALLED ON NULL INPUT RETURNS {type_def.udf_type} LANGUAGE {lang} AS '{fun_wasm_src}'"
                with new_function(cql, test_keyspace, fun_src, fun_name):
                    # Cassandra returns timeuuids returned from UDFs as bytes (even though UUIDs are UUIDs), so if we're running Cassandra we should also compare the result to bytes.
                    ref_val = type_def.reference_value if use_wasm or type_def.udf_type != "timeuuid" else type_def.reference_value.bytes
                    assertRows(execute(cql, table, f"SELECT {fun_name}({type_def.column_name}) FROM %s WHERE key = 1"), row(ref_val))

                fun_name = unique_name()
                fun_wasm_src = read_function_from_file("test_types_with_and_without_nulls", f"called_on_null_{type_def.column_name}", fun_name) if use_wasm else 'return \"called\";'
                fun_src = f"(input {type_def.udf_type}) CALLED ON NULL INPUT RETURNS text LANGUAGE {lang} AS '{fun_wasm_src}'"
                with new_function(cql, test_keyspace, fun_src, fun_name):
                    assertRows(execute(cql, table, f"SELECT {fun_name}({type_def.column_name}) FROM %s WHERE key = 1"), row("called"))
                    assertRows(execute(cql, table, f"SELECT {fun_name}({type_def.column_name}) FROM %s WHERE key = 2"), row("called"))

                fun_name = unique_name()
                fun_wasm_src = read_function_from_file("test_types_with_and_without_nulls", f"returns_null_on_null_{type_def.column_name}", fun_name) if use_wasm else 'return \"called\";'
                fun_src = f"(input {type_def.udf_type}) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE {lang} AS '{fun_wasm_src}'"
                with new_function(cql, test_keyspace, fun_src, fun_name):
                    assertRows(execute(cql, table, f"SELECT {fun_name}({type_def.column_name}) FROM %s WHERE key = 1"), row("called"))
                    assertRows(execute(cql, table, f"SELECT {fun_name}({type_def.column_name}) FROM %s WHERE key = 2"), row(None))

@pytest.mark.skip(reason="Issue #13746")
@pytest.mark.xfail(reason="Issue #13855")
@pytest.mark.xfail(reason="Issue #13860")
@pytest.mark.xfail(reason="Issue #13866")
def test_function_with_frozen_set_type(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b frozen<set<int>>)") as table:
        with new_secondary_index(cql, table, "FULL(b)"):
            use_wasm = is_scylla(cql, test_keyspace)
            lang = "wasm" if use_wasm else "java"
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", 0, set())
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", 1, set([1, 2, 3]))
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", 2, set([4, 5, 6]))
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", 3, set([7, 8, 9]))

            fun_name = unique_name()
            sum_set_src = read_function_from_file("test_functions_with_frozen_types", "sum_set", fun_name) if use_wasm else 'int sum = 0; for (int value : values) {sum += value;} return sum;'
            frozen_arg_src = f"(values frozen<set<int>>) CALLED ON NULL INPUT RETURNS int LANGUAGE {lang} AS '{sum_set_src}'"
            assertInvalidMessage(cql, "", "cannot be frozen", f"CREATE FUNCTION {test_keyspace}.{fun_name} {frozen_arg_src}")

            ret_name = unique_name()
            return_set_src = read_function_from_file("test_functions_with_frozen_types", "return_set", ret_name) if use_wasm else 'return values;'
            frozen_ret_src = f"(values set<int>) CALLED ON NULL INPUT RETURNS frozen<set<int>> LANGUAGE {lang} AS '{return_set_src}'"
            assertInvalidMessage(cql, "", "cannot be frozen", f"CREATE FUNCTION {test_keyspace}.{ret_name} {frozen_ret_src}")

            fun_src = f"(values set<int>) CALLED ON NULL INPUT RETURNS int LANGUAGE {lang} AS '{sum_set_src}'"
            with new_function(cql, test_keyspace, fun_src, fun_name):
                assertRows(execute(cql, table, f"SELECT a, {fun_name}(b) FROM %s WHERE a = 0"), row(0, 0))
                assertRows(execute(cql, table, f"SELECT a, {fun_name}(b) FROM %s WHERE a = 1"), row(1, 6))
                assertRows(execute(cql, table, f"SELECT a, {fun_name}(b) FROM %s WHERE a = 2"), row(2, 15))
                assertRows(execute(cql, table, f"SELECT a, {fun_name}(b) FROM %s WHERE a = 3"), row(3, 24))

            fun_src = f"(values set<int>) CALLED ON NULL INPUT RETURNS set<int> LANGUAGE {lang} AS '{return_set_src}'"
            with new_function(cql, test_keyspace, fun_src, ret_name):
                assertRows(execute(cql, table, f"SELECT a FROM %s WHERE b = {ret_name}(?)", set([1, 2, 3])), row(1))

            assertInvalidMessage(cql, "", "cannot be frozen", f"DROP FUNCTION {test_keyspace}.{ret_name}(frozen<set<int>>)")

@pytest.mark.skip(reason="Issue #13746")
@pytest.mark.xfail(reason="Issue #13855")
@pytest.mark.xfail(reason="Issue #13860")
@pytest.mark.xfail(reason="Issue #13866")
def test_function_with_frozen_list_type(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b frozen<list<int>>)") as table:
        with new_secondary_index(cql, table, "FULL(b)"):
            use_wasm = is_scylla(cql, test_keyspace)
            lang = "wasm" if use_wasm else "java"
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", 0, [])
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", 1, [1, 2, 3])
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", 2, [4, 5, 6])
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", 3, [7, 8, 9])

            fun_name = unique_name()
            sum_list_src = read_function_from_file("test_functions_with_frozen_types", "sum_list", fun_name) if use_wasm else 'int sum = 0; for (int value : values) {sum += value;} return sum;'
            frozen_arg_src = f"(values frozen<list<int>>) CALLED ON NULL INPUT RETURNS int LANGUAGE {lang} AS '{sum_list_src}'"
            assertInvalidMessage(cql, "", "cannot be frozen", f"CREATE FUNCTION {test_keyspace}.{fun_name} {frozen_arg_src}")

            ret_name = unique_name()
            return_list_src = read_function_from_file("test_functions_with_frozen_types", "return_list", ret_name) if use_wasm else 'return values;'
            frozen_ret_src = f"(values list<int>) CALLED ON NULL INPUT RETURNS frozen<list<int>> LANGUAGE {lang} AS '{return_list_src}'"
            assertInvalidMessage(cql, "", "cannot be frozen", f"CREATE FUNCTION {test_keyspace}.{ret_name} {frozen_ret_src}")

            fun_src = f"(values list<int>) CALLED ON NULL INPUT RETURNS int LANGUAGE {lang} AS '{sum_list_src}'"
            with new_function(cql, test_keyspace, fun_src, fun_name):
                assertRows(execute(cql, table, f"SELECT a, {fun_name}(b) FROM %s WHERE a = 0"), row(0, 0))
                assertRows(execute(cql, table, f"SELECT a, {fun_name}(b) FROM %s WHERE a = 1"), row(1, 6))
                assertRows(execute(cql, table, f"SELECT a, {fun_name}(b) FROM %s WHERE a = 2"), row(2, 15))
                assertRows(execute(cql, table, f"SELECT a, {fun_name}(b) FROM %s WHERE a = 3"), row(3, 24))

            fun_src = f"(values list<int>) CALLED ON NULL INPUT RETURNS list<int> LANGUAGE {lang} AS '{return_list_src}'"
            with new_function(cql, test_keyspace, fun_src, ret_name):
                assertRows(execute(cql, table, f"SELECT a FROM %s WHERE b = {ret_name}(?)", [1, 2, 3]), row(1))

            assertInvalidMessage(cql, "", "cannot be frozen", f"DROP FUNCTION {test_keyspace}.{ret_name}(frozen<list<int>>)")

@pytest.mark.skip(reason="Issue #13746")
@pytest.mark.xfail(reason="Issue #13855")
@pytest.mark.xfail(reason="Issue #13860")
@pytest.mark.xfail(reason="Issue #13866")
def test_function_with_frozen_map_type(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b frozen<map<int, int>>)") as table:
        with new_secondary_index(cql, table, "FULL(b)"):
            use_wasm = is_scylla(cql, test_keyspace)
            lang = "wasm" if use_wasm else "java"
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", 0, {})
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", 1, {1: 1, 2: 2, 3: 3})
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", 2, {4: 4, 5: 5, 6: 6})
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", 3, {7: 7, 8: 8, 9: 9})

            fun_name = unique_name()
            sum_map_src = read_function_from_file("test_functions_with_frozen_types", "sum_map", fun_name) if use_wasm else 'int sum = 0; for (int value : values.values()) {sum += value;} return sum;'
            frozen_arg_src = f"(values frozen<map<int, int>>) CALLED ON NULL INPUT RETURNS int LANGUAGE {lang} AS '{sum_map_src}'"
            assertInvalidMessage(cql, "", "cannot be frozen", f"CREATE FUNCTION {test_keyspace}.{fun_name} {frozen_arg_src}")

            ret_name = unique_name()
            return_map_src = read_function_from_file("test_functions_with_frozen_types", "return_map", ret_name) if use_wasm else 'return values;'
            frozen_ret_src = f"(values map<int, int>) CALLED ON NULL INPUT RETURNS frozen<map<int, int>> LANGUAGE {lang} AS '{return_map_src}'"
            assertInvalidMessage(cql, "", "cannot be frozen", f"CREATE FUNCTION {test_keyspace}.{ret_name} {frozen_ret_src}")

            fun_src = f"(values map<int, int>) CALLED ON NULL INPUT RETURNS int LANGUAGE {lang} AS '{sum_map_src}'"
            with new_function(cql, test_keyspace, fun_src, fun_name):
                assertRows(execute(cql, table, f"SELECT a, {fun_name}(b) FROM %s WHERE a = 0"), row(0, 0))
                assertRows(execute(cql, table, f"SELECT a, {fun_name}(b) FROM %s WHERE a = 1"), row(1, 6))
                assertRows(execute(cql, table, f"SELECT a, {fun_name}(b) FROM %s WHERE a = 2"), row(2, 15))
                assertRows(execute(cql, table, f"SELECT a, {fun_name}(b) FROM %s WHERE a = 3"), row(3, 24))

            fun_src = f"(values map<int, int>) CALLED ON NULL INPUT RETURNS map<int, int> LANGUAGE {lang} AS '{return_map_src}'"
            with new_function(cql, test_keyspace, fun_src, ret_name):
                assertRows(execute(cql, table, f"SELECT a FROM %s WHERE b = {ret_name}(?)", {1: 1, 2: 2, 3: 3}), row(1))

            assertInvalidMessage(cql, "", "cannot be frozen", f"DROP FUNCTION {test_keyspace}.{ret_name}(frozen<map<int, int>>)")

@pytest.mark.skip(reason="Issue #13746")
def test_function_with_frozen_tuple_type(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b frozen<tuple<int, int>>)") as table:
        with new_secondary_index(cql, table, "b"):
            use_wasm = is_scylla(cql, test_keyspace)
            lang = "wasm" if use_wasm else "java"
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", 0, (None, None))
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", 1, (1, 2))
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", 2, (4, 5))
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", 3, (7, 8))

            ret_name = unique_name()
            return_tuple_src = read_function_from_file("test_functions_with_frozen_types", "return_tuple", ret_name) if use_wasm else 'return values;'
            cql.execute(f"CREATE FUNCTION {test_keyspace}.{ret_name} (values frozen<tuple<int, int>>) CALLED ON NULL INPUT RETURNS frozen<tuple<int, int>> LANGUAGE {lang} AS '{return_tuple_src}'")

            assertRowCount(cql.execute(f"SELECT * from system_schema.functions WHERE keyspace_name = '{test_keyspace}' AND function_name = '{ret_name}'"), 1)
            cql.execute(f"DROP FUNCTION {test_keyspace}.{ret_name} (frozen<tuple<int, int>>)")
            assertRowCount(cql.execute(f"SELECT * from system_schema.functions WHERE keyspace_name = '{test_keyspace}' AND function_name = '{ret_name}'"), 0)

            fun_name = unique_name()
            tostring_tuple_src = read_function_from_file("test_functions_with_frozen_types", "tostring_tuple", fun_name) if use_wasm else 'return values.toString();'
            cql.execute(f"CREATE FUNCTION {test_keyspace}.{fun_name}(values frozen<tuple<int, int>>) CALLED ON NULL INPUT RETURNS text LANGUAGE {lang} AS '{tostring_tuple_src}'")

            # In the future, we may want to decide on a string representation of UDTs in Wasm UDFs that's
            # independent of the language used to implement them, for now we use the Rust debug representation.
            assertRows(execute(cql, table, f"SELECT a, {fun_name}(b) FROM %s WHERE a = 0"), row(0, "Some((None, None))" if use_wasm else "(NULL,NULL)"))
            assertRows(execute(cql, table, f"SELECT a, {fun_name}(b) FROM %s WHERE a = 1"), row(1, "Some((Some(1), Some(2)))" if use_wasm else "(1,2)"))
            assertRows(execute(cql, table, f"SELECT a, {fun_name}(b) FROM %s WHERE a = 2"), row(2, "Some((Some(4), Some(5)))" if use_wasm else "(4,5)"))
            assertRows(execute(cql, table, f"SELECT a, {fun_name}(b) FROM %s WHERE a = 3"), row(3, "Some((Some(7), Some(8)))" if use_wasm else "(7,8)"))

            with new_function(cql, test_keyspace, f"(values tuple<int, int>) CALLED ON NULL INPUT RETURNS tuple<int, int> LANGUAGE {lang} AS '{return_tuple_src}'", ret_name):
                assertRows(execute(cql, table, f"SELECT a FROM %s WHERE b = {ret_name}(?)", (1, 2)), row(1))

            assertRowCount(cql.execute(f"SELECT * from system_schema.functions WHERE keyspace_name = '{test_keyspace}' AND function_name = '{fun_name}'"), 1)
            cql.execute(f"DROP FUNCTION {test_keyspace}.{fun_name} (frozen<tuple<int, int>>)")
            assertRowCount(cql.execute(f"SELECT * from system_schema.functions WHERE keyspace_name = '{test_keyspace}' AND function_name = '{fun_name}'"), 0)

@pytest.mark.skip(reason="Issue #13746")
def test_function_with_frozen_udt_type(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(f int)") as type:
        with create_table(cql, test_keyspace, f"(a int PRIMARY KEY, b frozen<{type}>)") as table:
            with new_secondary_index(cql, table, "b"):
                use_wasm = is_scylla(cql, test_keyspace)
                lang = "wasm" if use_wasm else "java"
                execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, {f : ?})", 0, 0)
                execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, {f : ?})", 1, 1)
                execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, {f : ?})", 2, 4)
                execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, {f : ?})", 3, 7)

                fun_name = unique_name()
                tostring_udt_src = read_function_from_file("test_functions_with_frozen_types", "tostring_udt", fun_name) if use_wasm else 'return values.toString();'
                frozen_arg_src = f"(values frozen<{type}>) CALLED ON NULL INPUT RETURNS text LANGUAGE {lang} AS '{tostring_udt_src}'"
                assertInvalidMessage(cql, "", "not be frozen", f"CREATE FUNCTION {test_keyspace}.{fun_name} {frozen_arg_src}")

                ret_name = unique_name()
                return_udt_src = read_function_from_file("test_functions_with_frozen_types", "return_udt", ret_name) if use_wasm else 'return values;'
                frozen_ret_src = f"(values {type}) CALLED ON NULL INPUT RETURNS frozen<{type}> LANGUAGE {lang} AS '{return_udt_src}'"
                assertInvalidMessage(cql, "", "not be frozen", f"CREATE FUNCTION {test_keyspace}.{ret_name} {frozen_ret_src}")

                tostring_src = f"(values {type}) CALLED ON NULL INPUT RETURNS text LANGUAGE {lang} AS '{tostring_udt_src}'"
                with new_function(cql, test_keyspace, tostring_src, fun_name):

                    # In the future, we may want to decide on a string representation of UDTs in Wasm UDFs that's
                    # independent of the language used to implement them, for now we use the Rust debug representation.
                    assertRows(execute(cql, table, f"SELECT a, {fun_name}(b) FROM %s WHERE a = 0"), row(0, "Some(Udt { f: Some(0) })" if use_wasm else "{f:0}"))
                    assertRows(execute(cql, table, f"SELECT a, {fun_name}(b) FROM %s WHERE a = 1"), row(1, "Some(Udt { f: Some(1) })" if use_wasm else "{f:1}"))
                    assertRows(execute(cql, table, f"SELECT a, {fun_name}(b) FROM %s WHERE a = 2"), row(2, "Some(Udt { f: Some(4) })" if use_wasm else "{f:4}"))
                    assertRows(execute(cql, table, f"SELECT a, {fun_name}(b) FROM %s WHERE a = 3"), row(3, "Some(Udt { f: Some(7) })" if use_wasm else "{f:7}"))

                    nonfrozen_ret_src = f"(values {type}) CALLED ON NULL INPUT RETURNS {type} LANGUAGE {lang} AS '{return_udt_src}'"
                    with new_function(cql, test_keyspace, nonfrozen_ret_src, ret_name):
                        assertRows(execute(cql, table, f"SELECT a FROM %s WHERE b = {ret_name}({{f : ?}})", 1), row(1))
                    assertInvalidMessage(cql, "", "not be frozen", f"DROP FUNCTION {test_keyspace}.{fun_name} (frozen<{type}>)")
