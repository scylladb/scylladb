# Copyright 2018-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests of CAST on many values of every castable type, also inside aggregates
# (avg/sum/min/max of a cast, and a cast of avg/sum/min/max) and a cast of a
# cast. Every query goes through cqlsh, and the expected values are cqlsh's
# output. A test runs all its queries in one cqlsh call and reports every check
# that fails, not only the first.
#
# The expected values are Scylla's, so the tests are Scylla-only.
#
# See also test_cast_data.py.
###############################################################################

import re
import subprocess
import tempfile

import pytest

from test import TOP_SRC_DIR
from test.pylib.skip_types import skip_env
from .util import new_test_table, new_type

COLUMN_NAME_TEMPLATE = "{}_clmn"

# The expected result of a cast of a value that does not fit the target type:
# Scylla fails the cast, as SQL databases do.  Cassandra (and the original
# test) wraps around or gives an infinity.
OUT_OF_RANGE = "<out of range>"


@pytest.fixture(scope="module")
def cqlsh(request, host):
    """Run CQL statements, one per line, in one cqlsh call.

    Returns, for each statement, the first value row cqlsh printed for it (or
    "" when it printed none) and its error (or None).
    """
    port = request.config.getoption("--port")
    if request.config.getoption("--ssl"):
        skip_env("cqlsh is not set up for SSL here")
    user = request.config.getoption("--auth_username") or "cassandra"
    password = request.config.getoption("--auth_password") or "cassandra"

    def run(statements):
        # After each statement, a marker query: what cqlsh printed between two
        # markers belongs to the statement between them.
        with tempfile.NamedTemporaryFile("w", suffix=".cql") as f:
            f.write("".join(f"{s};\nSELECT 'marker{i}' FROM system.local;\n" for i, s in enumerate(statements)))
            f.flush()
            p = subprocess.run([TOP_SRC_DIR / "bin/cqlsh", host, str(port), "-u", user, "-p", password, "-f", f.name],
                               capture_output=True, text=True, timeout=600)
        # cqlsh exits with 2 after statement errors; anything else but 0 means it
        # failed to run at all.
        assert p.returncode in (0, 2), f"cqlsh failed with exit status {p.returncode}:\n{p.stdout}\n{p.stderr}"
        # The first value row of each result: the line after the dashes
        # under the column header.
        lines = p.stdout.splitlines()
        values = [lines[i + 1].strip() for i, line in enumerate(lines) if re.fullmatch(r"-+", line) and i + 1 < len(lines)]
        # cqlsh reports an error as "<file>:<line>:<message>" (perhaps more
        # lines) and goes on with the next statement.
        errors = []
        for line in p.stderr.splitlines():
            if m := re.match(re.escape(f.name) + r":\d+:(.*)", line):
                errors.append(m.group(1))
            elif errors:
                errors[-1] += "\n" + line
        results = []
        it = iter(values)
        for i, _ in enumerate(statements):
            value = next(it, None)
            if value == f"marker{i}":
                results.append(("", errors.pop(0) if errors else "no output"))
            else:
                assert next(it, None) == f"marker{i}", f"cqlsh output out of step at {statements[i]}:\n{p.stdout}\n{p.stderr}"
                results.append((value, None))
        return results

    return run


def _cast_single_value(column_name, to_type, row_id, table_name, test):
    """Run cast select"""
    return f"SELECT cast({column_name} as {to_type}) FROM {table_name} WHERE k={row_id}"


def _agg_cast_function(column_name, to_type, row_id, table_name, test):
    """Run DB aggregation select"""
    return f"SELECT {test}(cast({column_name} as {to_type})) FROM {table_name}"


def _cast_agg_function(column_name, to_type, row_id, table_name, test):
    """Run DB cast of aggregation select"""
    func = test.replace("cast_", "")
    return f"SELECT cast({func}({column_name}) as {to_type}) FROM {table_name}"


def _cast_cast_function(column_name, to_type, row_id, table_name, test):
    """Run DB cast of other cast select"""
    to_types = to_type.split("/")
    return f"SELECT cast(cast({column_name} as {to_types[0]}) as {to_types[1]}) FROM {table_name} where k={row_id}"


# TEST structure: {<test type>: [<function reference>, <test name>]}
TESTS = {
    "cast": [_cast_single_value, "CAST()"],
    "avg": [_agg_cast_function, "AVG(CAST())"],
    "sum": [_agg_cast_function, "SUM(CAST())"],
    "min": [_agg_cast_function, "MIN(CAST())"],
    "max": [_agg_cast_function, "MAX(CAST())"],
    "cast_avg": [_cast_agg_function, "CAST(AVG())"],
    "cast_sum": [_cast_agg_function, "CAST(SUM())"],
    "cast_min": [_cast_agg_function, "CAST(MIN())"],
    "cast_max": [_cast_agg_function, "CAST(MAX())"],
    "cast_cast": [_cast_cast_function, "CAST(CAST())"],
}


def _is_commented(exclude, ttype):
    return True if not exclude and ttype.startswith("#") else False


def _checks(from_type, rows, exclude, column_name, table_name, test_types=None, test_to=None):
    """The checks of the values of one type: (query, description, from type, to type, expected)"""
    for row_id, exp_results in enumerate(rows):
        for test, test_exp_result in exp_results[1].items():
            # If asked exclude tests with name started with "#"
            if exclude and test.startswith("#"):
                continue
            test_commented = _is_commented(exclude, test)
            test = test.replace("#", "")
            if (test_types and test in test_types) or not test_types:
                for to_type, exp_result in test_exp_result.items():
                    # If asked exclude tests with name started with "#"
                    if exclude and to_type.startswith("#"):
                        continue
                    type_commented = _is_commented(exclude, to_type)
                    to_type = to_type.replace("#", "")
                    if exp_result and (not test_to or (test_to and to_type in test_to)) and (exclude or (not exclude and (type_commented or test_commented))):
                        query = TESTS[test][0](column_name, to_type, row_id, table_name, test)
                        yield query, f"test=[{test}] failed casting from type [{from_type}] to type [{to_type}]\n\tquery=[{query}]", from_type, to_type, exp_result


def _check(cqlsh, checks, compare_error=False):
    checks = list(checks)
    failures = []
    for (query, description, from_type, to_type, exp_result), (actual_result, err) in zip(checks, cqlsh([c[0] for c in checks])):
        if exp_result == OUT_OF_RANGE:
            if not err or "out of range" not in err:
                failures.append(f"{description}: expected an out of range error, got [{actual_result}]" + (f" error [{err}]" if err else ""))
            continue
        actual_result = err if compare_error else actual_result
        if to_type in ["text", "varchar"] and from_type == "timestamp":
            # scylla omit zero milliseconds
            actual_result = actual_result.removesuffix(".000Z")
            exp_result = str(exp_result).removesuffix(".000Z")
        if to_type in ["decimal", "text", "varchar"]:
            # in cassandra decimal omits .0, so we compare without it
            # https://github.com/scylladb/scylladb/issues/3109
            actual_result = actual_result.removesuffix(".0")
            exp_result = str(exp_result).removesuffix(".0")
        if to_type in ["decimal"] and from_type in ["float", "double"]:
            try:
                ok = float(exp_result) == pytest.approx(float(actual_result), rel=1e-6)
            except ValueError:
                ok = False
        else:
            ok = actual_result == str(exp_result)
        if not ok:
            failures.append(f"{description}: expected [{exp_result}], got [{actual_result}]" + (f" error [{err}]" if err and not compare_error else ""))
    assert not failures, "\n".join(failures)


def _test_run(cql, cqlsh, keyspace, test_from, data_dict, test_to=None, test_types=None, exclude=True, compare_error=False):
    """Run the checks of the values of the types in test_from, in a table each"""
    for from_type in test_from:
        if from_type in data_dict:
            column_name = COLUMN_NAME_TEMPLATE.format(from_type)
            with new_test_table(cql, keyspace, f"k int PRIMARY KEY, {column_name} {from_type}") as table_name:
                for _id, value in enumerate(data_dict[from_type]):
                    cql.execute(f"INSERT INTO {table_name} (k, {column_name}) VALUES ({_id}, {value[0]})")
                _check(cqlsh, _checks(from_type, data_dict[from_type], exclude, column_name, table_name, test_types=test_types, test_to=test_to), compare_error=compare_error)


def _udt_test_run(cql, cqlsh, keyspace, test_from, data_dict):
    """Run the checks of the values of the types in a UDT's fields"""
    for fromt in test_from:
        fields = data_dict[fromt]
        column_name = COLUMN_NAME_TEMPLATE.format(fromt)
        with new_type(cql, keyspace, "(" + ", ".join(f"{COLUMN_NAME_TEMPLATE.format(t)} {t}" for t in fields) + ")") as type_name, \
             new_test_table(cql, keyspace, f"k int PRIMARY KEY, {column_name} frozen<{type_name}>") as table_name:
            for _id in range(max(len(h) for h in fields.values())):
                data = ", ".join(f"{COLUMN_NAME_TEMPLATE.format(t)}: {values[_id][0]}" for t, values in fields.items() if len(values) > _id)
                cql.execute(f"INSERT INTO {table_name} (k, {column_name}) VALUES ({_id}, {{{data}}})")
            checks = []
            for from_type in fields:
                udt_column_name = f"{column_name}.{COLUMN_NAME_TEMPLATE.format(from_type)}"
                checks += _checks(from_type, fields[from_type], True, udt_column_name, table_name)
            _check(cqlsh, checks)


@pytest.mark.xfail(reason="scylladb/scylladb#3108")
def test_cast_negative(cql, cqlsh, test_keyspace, scylla_only):
    """Function performs positive tests CAST scalar function for user-defined type"""
    _test_run(cql, cqlsh, test_keyspace, ["text", "date"], TestData.NEGATIVE_VALUES, compare_error=True)


def test_cast_udt(cql, cqlsh, test_keyspace, scylla_only):
    """Function performs positive tests CAST scalar function for user-defined type"""
    _udt_test_run(cql, cqlsh, test_keyspace, ["udt"], TestData.POSITIVE_VALUES)


@pytest.mark.parametrize("from_type", ["varint", "tinyint", "smallint", "int", "bigint", "float", "double", "decimal", "date", "time", "timestamp", "uuid", "boolean", "ascii", "inet", "timeuuid"])
def test_cast(cql, cqlsh, test_keyspace, from_type, scylla_only):
    """Function performs positive tests CAST scalar function for one type"""
    _test_run(cql, cqlsh, test_keyspace, [from_type], TestData.POSITIVE_VALUES)


@pytest.mark.xfail(reason="scylladb/scylladb#13601")
def test_avg_cast_from_varint_to_decimal(cql, cqlsh, test_keyspace, scylla_only):
    # To remove the case from issue test, go to TestData.POSITIVE_VALUES and remove comment simbol "#" from relavant type
    # seems like cassandra bug: https://issues.apache.org/jira/browse/CASSANDRA-18470
    _test_run(cql, cqlsh, test_keyspace, ["varint"], TestData.POSITIVE_VALUES, test_to=["decimal"], test_types=["avg"], exclude=False)


class TestData:
    # class CAST_DATA(object):
    # TODO: counters are not supported. should be added later
    # CAST_MAPPING structure:
    #  {<column type>: [<cast to type 1>, <cast to type 2>, <cast to type 3>...]}
    CAST_MAPPING = {
        "bigint": ["tinyint", "smallint", "int", "float", "double", "decimal", "varint", "text", "varchar"],
        "ascii": ["text", "varchar"],
        "uuid": ["text", "varchar"],
        "boolean": ["text", "varchar"],
        "inet": ["text", "varchar"],
        "time": ["text", "varchar"],
        "timestamp": ["date", "text", "varchar"],
        "timeuuid": ["timestamp", "date", "text", "varchar"],
        "date": ["timestamp"],
        "decimal": ["tinyint", "smallint", "int", "float", "double", "bigint", "varint", "text", "varchar"],
        "double": ["tinyint", "smallint", "int", "float", "decimal", "bigint", "varint", "text", "varchar"],
        "float": ["tinyint", "smallint", "int", "double", "decimal", "bigint", "varint", "text", "varchar"],
        "int": ["tinyint", "smallint", "float", "double", "decimal", "bigint", "varint", "text", "varchar"],
        "smallint": ["tinyint", "int", "float", "double", "decimal", "bigint", "varint", "text", "varchar"],
        "tinyint": ["smallint", "int", "float", "double", "decimal", "bigint", "varint", "text", "varchar"],
        "varint": ["tinyint", "smallint", "int", "float", "double", "decimal", "bigint", "text", "varchar"],
    }

    # <ANY>_VALUES structure:
    # { <from type name>: [ [<input value>, {<test type>: {<to type>: <CAST_MAPPING[<from type name>][0]>, <to type>: <CAST_MAPPING[<from type name>][1]>, .. },
    #                                       <test type>: {<to type>: <CAST_MAPPING[<from type name>][0]>, <to type>: <CAST_MAPPING[<from type name>][1]>, .. }
    #                                       }],
    #                       [<input value>, {<test type>: {<to type>: <CAST_MAPPING[<from type name>][0]>, <to type>: <CAST_MAPPING[<from type name>][1]>, .. },
    #                                       <test type>: {<to type>: <CAST_MAPPING[<from type name>][0]>, <to type>: <CAST_MAPPING[<from type name>][1]>, .. }
    #                                       }]
    #                     ]
    # }
    # TODO: avg(cast) to decimal - Cassandra returns error
    POSITIVE_VALUES = {
        "bigint": [
            [
                7657139128428828644,
                {
                    "cast": {
                        "tinyint": OUT_OF_RANGE,
                        "smallint": OUT_OF_RANGE,
                        "int": OUT_OF_RANGE,
                        "float": "7.6571e+18",
                        "double": "7.6571e+18",
                        "decimal": "7657139128428828644",
                        "varint": 7657139128428828644,
                        "text": "7657139128428828644",
                        "varchar": "7657139128428828644",
                    },
                    "avg": {"tinyint": OUT_OF_RANGE, "smallint": OUT_OF_RANGE, "int": OUT_OF_RANGE, "float": "2.5524e+18", "double": "2.5524e+18", "decimal": "2552379709476274357", "varint": 2552379709476274357, "text": None, "varchar": None},
                    "sum": {"tinyint": OUT_OF_RANGE, "smallint": OUT_OF_RANGE, "int": OUT_OF_RANGE, "float": "7.6571e+18", "double": "7.6571e+18", "decimal": "7657139128428823071", "varint": 7657139128428823071, "text": None, "varchar": None},
                    "min": {"tinyint": OUT_OF_RANGE, "smallint": OUT_OF_RANGE, "int": OUT_OF_RANGE, "float": -5698, "double": -5698, "decimal": -5698.0, "varint": -5698, "text": "-5698", "varchar": "-5698"},
                    "max": {
                        "tinyint": OUT_OF_RANGE,
                        "smallint": OUT_OF_RANGE,
                        "int": OUT_OF_RANGE,
                        "float": "7.6571e+18",
                        "double": "7.6571e+18",
                        "decimal": "7657139128428828644",
                        "varint": 7657139128428828644,
                        "text": "7657139128428828644",
                        "varchar": "7657139128428828644",
                    },
                },
            ],
            [125, {"cast": {"tinyint": 125, "smallint": 125, "int": 125, "float": 125, "double": 125, "decimal": 125.0, "varint": 125, "text": "125", "varchar": "125"}}],
            [-5698, {"cast": {"tinyint": OUT_OF_RANGE, "smallint": -5698, "int": -5698, "double": -5698, "decimal": -5698.0, "varint": -5698, "text": "-5698", "varchar": "-5698"}}],
        ],
        "boolean": [[True, {"cast": {"text": "true", "varchar": "true"}}], [False, {"cast": {"text": "false", "varchar": "false"}}]],
        "decimal": [
            [
                76571.345,
                {
                    "cast": {"tinyint": OUT_OF_RANGE, "smallint": OUT_OF_RANGE, "int": 76571, "float": 76571.34375, "double": 76571.345, "bigint": 76571, "varint": 76571, "text": "76571.345", "varchar": "76571.345"},
                    "avg": {"tinyint": OUT_OF_RANGE, "smallint": OUT_OF_RANGE, "int": 38158, "float": 38158.54297, "double": 38158.54401, "bigint": 38158, "varint": 38158, "text": None, "varchar": None},
                    "sum": {"tinyint": OUT_OF_RANGE, "smallint": OUT_OF_RANGE, "int": 76317, "float": 76317.08594, "double": 76317.08801, "bigint": 76317, "varint": 76317, "text": None, "varchar": None},
                    "min": {"tinyint": OUT_OF_RANGE, "smallint": OUT_OF_RANGE, "int": -254, "float": -254.25699, "double": -254.25699, "bigint": -254, "varint": -254, "text": "-254.256987", "varchar": "-254.256987"},
                    "max": {"tinyint": OUT_OF_RANGE, "smallint": OUT_OF_RANGE, "int": 76571, "float": 76571.34375, "double": 76571.345, "bigint": 76571, "varint": 76571, "text": "76571.345", "varchar": "76571.345"},
                    "cast_avg": {"tinyint": OUT_OF_RANGE, "smallint": OUT_OF_RANGE, "int": 38158, "float": "38158.54297", "double": "38158.54401", "bigint": 38158, "varint": 38158, "text": None, "varchar": None},
                    "cast_sum": {"tinyint": OUT_OF_RANGE, "smallint": OUT_OF_RANGE, "int": 76317, "float": "76317.08594", "double": "76317.08801", "bigint": 76317, "varint": 76317, "text": None, "varchar": None},
                    "cast_min": {"tinyint": OUT_OF_RANGE, "smallint": -254, "int": -254, "float": "-254.25699", "double": "-254.25699", "bigint": -254, "varint": -254, "text": "-254.256987", "varchar": "-254.256987"},
                    "cast_max": {"tinyint": OUT_OF_RANGE, "smallint": OUT_OF_RANGE, "int": 76571, "float": 76571.34375, "double": "76571.345", "bigint": 76571, "varint": 76571, "text": "76571.345", "varchar": "76571.345"},
                    "cast_cast": {"double/bigint": "76571"},
                },
            ],
            [-254.256987, {"cast": {"tinyint": OUT_OF_RANGE, "smallint": -254, "int": -254, "float": -254.25699, "double": -254.25699, "bigint": -254, "varint": -254, "text": "-254.256987", "varchar": "-254.256987"}}],
        ],
        "double": [
            [
                76571.345,
                {
                    "cast": {"tinyint": OUT_OF_RANGE, "smallint": OUT_OF_RANGE, "int": 76571, "float": 76571.34375, "decimal": "76571.345000000001", "bigint": 76571, "varint": 76571, "text": "76571.345", "varchar": "76571.345"},
                    "avg": {"tinyint": OUT_OF_RANGE, "smallint": OUT_OF_RANGE, "int": 38158, "float": 38158.54297, "decimal": "38158.54400650000050", "bigint": 38158, "varint": 38158, "text": None, "varchar": None},
                    "sum": {"tinyint": OUT_OF_RANGE, "smallint": OUT_OF_RANGE, "int": 76317, "float": 76317.08594, "decimal": "76317.08801300000099", "bigint": 76317, "varint": 76317, "text": None, "varchar": None},
                    "min": {"tinyint": OUT_OF_RANGE, "smallint": OUT_OF_RANGE, "int": -254, "float": -254.25699, "decimal": "-254.25698700000001", "bigint": -254, "varint": -254, "text": "-254.256987", "varchar": "-254.256987"},
                    "max": {"tinyint": OUT_OF_RANGE, "smallint": OUT_OF_RANGE, "int": 76571, "float": 76571.34375, "decimal": "76571.345000000001", "bigint": 76571, "varint": 76571, "text": "76571.345", "varchar": "76571.345"},
                },
            ],
            [-254.256987, {"cast": {"tinyint": OUT_OF_RANGE, "smallint": -254, "int": -254, "float": -254.25699, "decimal": "-254.25698700000001", "bigint": -254, "varint": -254, "text": "-254.256987", "varchar": "-254.256987"}}],
        ],
        "float": [
            [
                -564,
                {
                    "cast": {"tinyint": OUT_OF_RANGE, "smallint": -564, "int": -564, "double": -564, "decimal": "-564.0", "bigint": -564, "varint": -564, "text": "-564.0", "varchar": "-564.0"},
                    "avg": {"tinyint": OUT_OF_RANGE, "smallint": 1034, "int": 1034, "double": "1034.23883", "decimal": "1034.238824", "bigint": 1034, "varint": 1034, "text": None, "varchar": None},
                    "sum": {"tinyint": OUT_OF_RANGE, "smallint": 3102, "int": 3102, "double": "3102.71648", "decimal": "3102.716473", "bigint": 3102, "varint": 3102, "text": None, "varchar": None},
                    "min": {"tinyint": OUT_OF_RANGE, "smallint": -564, "int": -564, "double": -564, "decimal": "-564.0", "bigint": -564, "varint": -564, "text": "-564.0", "varchar": "-564.0"},
                    "max": {"tinyint": OUT_OF_RANGE, "smallint": 3543, "int": 3543, "double": "3543.45654", "decimal": "3543.45654", "bigint": 3543, "varint": 3543, "text": "3543.4565", "varchar": "3543.4565"},
                },
            ],
            [3543.4565429688, {"cast": {"tinyint": OUT_OF_RANGE, "smallint": 3543, "int": 3543, "double": 3543.45654, "decimal": "3543.45654", "bigint": 3543, "varint": 3543, "text": "3543.4565", "varchar": "3543.4565"}}],
            ["123.25993624886543132316548678", {"cast": {"tinyint": 123, "smallint": 123, "int": 123, "double": "123.25993", "decimal": "123.259933", "bigint": 123, "varint": 123, "text": "123.25993", "varchar": "123.25993"}}],
        ],
        "int": [
            [
                0,
                {
                    "cast": {"tinyint": 0, "smallint": 0, "float": 0, "double": 0, "decimal": "0.0", "bigint": 0, "varint": 0, "text": "0", "varchar": "0"},
                    "avg": {"tinyint": OUT_OF_RANGE, "smallint": -2044, "float": -2044.40002, "double": -2044.4, "decimal": "-2044.4", "bigint": -2044, "varint": -2044, "text": None, "varchar": None},
                    "sum": {"tinyint": OUT_OF_RANGE, "smallint": -10222, "float": -10222, "double": -10222, "decimal": "-10222.0", "bigint": -10222, "varint": -10222, "text": None, "varchar": None},
                    "min": {"tinyint": OUT_OF_RANGE, "smallint": -32767, "float": -32767, "double": -32767, "decimal": "-32767.0", "bigint": -32767, "varint": -32767, "text": "-12876", "varchar": "-12876"},
                    "max": {"tinyint": OUT_OF_RANGE, "smallint": 32767, "float": 32767, "double": 32767, "decimal": "32767.0", "bigint": 32767, "varint": 32767, "text": "32767", "varchar": "32767"},
                },
            ],
            [32767, {"cast": {"tinyint": OUT_OF_RANGE, "smallint": 32767, "float": 32767, "double": 32767, "decimal": "32767.0", "bigint": 32767, "varint": 32767, "text": "32767", "varchar": "32767"}}],
            [-32767, {"cast": {"tinyint": OUT_OF_RANGE, "smallint": -32767, "float": -32767, "double": -32767, "decimal": "-32767.0", "bigint": -32767, "varint": -32767, "text": "-32767", "varchar": "-32767"}}],
            [-12876, {}],
            [2654, {}],
        ],
        "smallint": [
            [
                32767,
                {
                    "cast": {"tinyint": OUT_OF_RANGE, "int": 32767, "float": 32767, "double": 32767, "decimal": "32767.0", "bigint": 32767, "varint": 32767, "text": "32767", "varchar": "32767"},
                    "avg": {"tinyint": OUT_OF_RANGE, "int": -2555, "float": -2555.5, "double": -2555.5, "decimal": "-2555.5", "bigint": -2555, "varint": -2555, "text": None, "varchar": None},
                    "sum": {"tinyint": OUT_OF_RANGE, "int": -10222, "float": -10222, "double": -10222, "decimal": "-10222.0", "bigint": -10222, "varint": -10222, "text": None, "varchar": None},
                    "min": {"tinyint": OUT_OF_RANGE, "int": -32767, "float": -32767, "double": -32767, "decimal": "-32767.0", "bigint": -32767, "varint": -32767, "text": "-12876", "varchar": "-12876"},
                    "max": {"tinyint": OUT_OF_RANGE, "int": 32767, "float": 32767, "double": 32767, "decimal": "32767.0", "bigint": 32767, "varint": 32767, "text": "32767", "varchar": "32767"},
                },
            ],
            [-32767, {"cast": {"tinyint": OUT_OF_RANGE, "int": -32767, "float": -32767, "double": -32767, "decimal": "-32767.0", "bigint": -32767, "varint": -32767, "text": "-32767", "varchar": "-32767"}}],
            [-12876, {"cast": {"tinyint": OUT_OF_RANGE, "int": -12876, "float": -12876, "double": -12876, "decimal": "-12876.0", "bigint": -12876, "varint": -12876, "text": "-12876", "varchar": "-12876"}}],
            [2654, {"cast": {"tinyint": OUT_OF_RANGE, "int": 2654, "float": 2654, "double": 2654, "decimal": "2654.0", "bigint": 2654, "varint": 2654, "text": "2654", "varchar": "2654"}}],
        ],
        "tinyint": [
            [
                127,
                {
                    "cast": {"smallint": 127, "int": 127, "float": 127, "double": 127, "decimal": "127.0", "bigint": 127, "varint": 127, "text": "127", "varchar": "127"},
                    "avg": {"smallint": 11, "int": 11, "float": 11.33333, "double": 11.33333, "decimal": "11.3", "bigint": 11, "varint": 11, "text": None, "varchar": None},
                    "sum": {"smallint": 34, "int": 34, "float": 34, "double": 34, "decimal": "34.0", "bigint": 34, "varint": 34, "text": None, "varchar": None},
                    "min": {"smallint": -128, "int": -128, "float": -128, "double": -128, "decimal": "-128.0", "bigint": -128, "varint": -128, "text": "-128", "varchar": "-128"},
                    "max": {"smallint": 127, "int": 127, "float": 127, "double": 127, "decimal": "127.0", "bigint": 127, "varint": 127, "text": "35", "varchar": "35"},
                },
            ],
            [-128, {"cast": {"smallint": -128, "int": -128, "float": -128, "double": -128, "decimal": "-128.0", "bigint": -128, "varint": -128, "text": "-128", "varchar": "-128"}}],
            [35, {"cast": {"smallint": 35, "int": 35, "float": 35, "double": 35, "decimal": "35.0", "bigint": 35, "varint": 35, "text": "35", "varchar": "35"}}],
        ],
        "varint": [
            [
                32767456456456456456545678943512357658768763546575675,
                {
                    "cast": {
                        "tinyint": OUT_OF_RANGE,
                        "smallint": OUT_OF_RANGE,
                        "int": OUT_OF_RANGE,
                        "float": OUT_OF_RANGE,
                        "double": 3.2767e52,
                        "decimal": "32767456456456456456545678943512357658768763546575675",
                        "bigint": OUT_OF_RANGE,
                        "text": "32767456456456456456545678943512357658768763546575675",
                        "varchar": "32767456456456456456545678943512357658768763546575675",
                    },
                    "avg": {
                        "tinyint": OUT_OF_RANGE,
                        "smallint": OUT_OF_RANGE,
                        "int": OUT_OF_RANGE,
                        "float": OUT_OF_RANGE,
                        "double": "1419.33333",
                        "#decimal": "-2110054581",  # commented cause cassandra is rounding the result
                        "bigint": OUT_OF_RANGE,
                        "text": None,
                        "varchar": None,
                    },
                    "sum": {"tinyint": OUT_OF_RANGE, "smallint": OUT_OF_RANGE, "int": OUT_OF_RANGE, "float": OUT_OF_RANGE, "double": 4258, "decimal": "-6330163741", "bigint": OUT_OF_RANGE, "text": None, "varchar": None},
                    "min": {
                        "tinyint": OUT_OF_RANGE,
                        "smallint": OUT_OF_RANGE,
                        "int": OUT_OF_RANGE,
                        "float": OUT_OF_RANGE,
                        "double": -3.2767e52,
                        "decimal": "-32767456456456456456545678943512357658768769876743674",
                        "bigint": OUT_OF_RANGE,
                        "text": "-32767456456456456456545678943512357658768769876743674",
                        "varchar": "-32767456456456456456545678943512357658768769876743674",
                    },
                    "max": {
                        "tinyint": OUT_OF_RANGE,
                        "smallint": OUT_OF_RANGE,
                        "int": OUT_OF_RANGE,
                        "float": OUT_OF_RANGE,
                        "double": 3.2767e52,
                        "decimal": "32767456456456456456545678943512357658768763546575675",
                        "bigint": OUT_OF_RANGE,
                        "text": "4258",
                        "varchar": "4258",
                    },
                    "cast_avg": {"tinyint": OUT_OF_RANGE, "smallint": OUT_OF_RANGE, "int": -2110054580, "float": "-2.1101e+09", "double": "-2.1101e+09", "decimal": "-2110054580", "bigint": -2110054580, "text": None, "varchar": None},
                    "cast_sum": {"tinyint": OUT_OF_RANGE, "smallint": OUT_OF_RANGE, "int": OUT_OF_RANGE, "float": "-6.3302e+09", "double": "-6.3302e+09", "decimal": "-6330163741", "bigint": -6330163741, "text": None, "varchar": None},
                    "cast_min": {
                        "tinyint": OUT_OF_RANGE,
                        "smallint": OUT_OF_RANGE,
                        "int": OUT_OF_RANGE,
                        "float": OUT_OF_RANGE,
                        "double": "-3.2767e+52",
                        "decimal": "-32767456456456456456545678943512357658768769876743674",
                        "bigint": OUT_OF_RANGE,
                        "text": "-32767456456456456456545678943512357658768769876743674",
                        "varchar": "-32767456456456456456545678943512357658768769876743674",
                    },
                    "cast_max": {
                        "tinyint": OUT_OF_RANGE,
                        "smallint": OUT_OF_RANGE,
                        "int": OUT_OF_RANGE,
                        "float": OUT_OF_RANGE,
                        "double": "3.2767e+52",
                        "decimal": "32767456456456456456545678943512357658768763546575675",
                        "bigint": OUT_OF_RANGE,
                        "text": "32767456456456456456545678943512357658768763546575675",
                        "varchar": "32767456456456456456545678943512357658768763546575675",
                    },
                },
            ],
            [
                -32767456456456456456545678943512357658768769876743674,
                {
                    "cast": {
                        "tinyint": OUT_OF_RANGE,
                        "smallint": OUT_OF_RANGE,
                        "int": OUT_OF_RANGE,
                        "float": OUT_OF_RANGE,
                        "double": -3.2767e52,
                        "decimal": "-32767456456456456456545678943512357658768769876743674",
                        "bigint": OUT_OF_RANGE,
                        "text": "-32767456456456456456545678943512357658768769876743674",
                        "varchar": "-32767456456456456456545678943512357658768769876743674",
                    }
                },
            ],
            [4258, {"cast": {"tinyint": OUT_OF_RANGE, "smallint": 4258, "int": 4258, "float": 4258, "double": 4258, "decimal": "4258.0", "bigint": 4258, "text": "4258", "varchar": "4258"}}],
        ],
        "ascii": [
            ["'045asciitext'", {"cast": {"text": "045asciitext", "varchar": "045asciitext"}, "min": {"text": "045asciitext", "varchar": "045asciitext"}, "max": {"text": "abcdefj", "varchar": "abcdefj"}}],
            ["'abcdefj'", {"cast": {"text": "abcdefj", "varchar": "abcdefj"}}],
        ],
        "date": [
            ["'2017-11-25'", {"cast": {"timestamp": "2017-11-25 00:00:00.000000+0000"}, "min": {"timestamp": "1900-01-01 00:00:00.000000+0000"}, "max": {"timestamp": "2458-12-13 00:00:00.000000+0000"}}],
            ["'1900-01-01'", {"cast": {"timestamp": "1900-01-01 00:00:00.000000+0000"}}],
            ["'2458-12-13'", {"cast": {"timestamp": "2458-12-13 00:00:00.000000+0000"}}],
        ],
        "time": [
            [
                "'13:07:45.089'",
                {"cast": {"text": "13:07:45.089000000", "varchar": "13:07:45.089000000"}, "min": {"text": "00:45:25.123000000", "varchar": "00:45:25.123000000"}, "max": {"text": "13:07:45.089000000", "varchar": "13:07:45.089000000"}},
            ],
            ["'00:45:25.123'", {"cast": {"text": "00:45:25.123000000", "varchar": "00:45:25.123000000"}}],
            ["'10:14:36.0002312'", {"cast": {"text": "10:14:36.000231200", "varchar": "10:14:36.000231200"}}],
        ],
        "timestamp": [
            [
                "'2017-12-27 11:57:42+0000'",
                {
                    "cast": {"date": "2017-12-27", "text": "2017-12-27T11:57:42.000Z", "varchar": "2017-12-27T11:57:42.000Z"},
                    "min": {"date": "1900-01-01", "text": "1900-01-01T00:00:00.000Z", "varchar": "1900-01-01T00:00:00.000Z"},
                    "max": {"date": "2018-02-01", "text": "2018-02-01T01:00:42.000Z", "varchar": "2018-02-01T01:00:42.000Z"},
                },
            ],
            [
                "'2017-12-27 11:57:42+0100'",  # timezone offset
                {
                    "cast": {"date": "2017-12-27", "text": "2017-12-27T10:57:42.000Z", "varchar": "2017-12-27T10:57:42.000Z"},
                },
            ],
            [
                "'2017-12-27 11:57:42.500+0100'",  # with milliseconds, scylla is returning with 6 digit vs. cassandra with 3 digits
                {
                    "cast": {"date": "2017-12-27", "text": "2017-12-27T10:57:42.500Z", "varchar": "2017-12-27T10:57:42.500Z"},
                },
            ],
            ["'2018-02-01 01:00:42+0000'", {"cast": {"date": "2018-02-01", "text": "2018-02-01T01:00:42.000Z", "varchar": "2018-02-01T01:00:42.000Z"}}],
            ["'1900-01-01 00:00:00+0000'", {"cast": {"date": "1900-01-01", "text": "1900-01-01T00:00:00.000Z", "varchar": "1900-01-01T00:00:00.000Z"}}],
        ],
        "timeuuid": [
            [
                "minTimeuuid('2018-01-01 17:01:07+0000')",
                {
                    "cast": {"timestamp": "2018-01-01 17:01:07.000000+0000", "date": "2018-01-01", "text": "5bb48b80-ef15-11e7-8080-808080808080", "varchar": "5bb48b80-ef15-11e7-8080-808080808080"},
                    "min": {"timestamp": "1900-01-01 00:00:00.000000+0000", "date": "1900-01-01", "text": "3c230000-a32f-1163-8080-808080808080", "varchar": "3c230000-a32f-1163-8080-808080808080"},
                    "max": {"timestamp": "2018-01-01 17:01:07.000000+0000", "date": "2018-01-01", "text": "7a73e600-eb01-11e7-8080-808080808080", "varchar": "7a73e600-eb01-11e7-8080-808080808080"},
                },
            ],
            ["minTimeuuid('2017-12-27 12:28:44+0000')", {"cast": {"timestamp": "2017-12-27 12:28:44.000000+0000", "date": "2017-12-27", "text": "7a73e600-eb01-11e7-8080-808080808080", "varchar": "7a73e600-eb01-11e7-8080-808080808080"}}],
            ["minTimeuuid('1900-01-01 00:00:00+0000')", {"cast": {"timestamp": "1900-01-01 00:00:00.000000+0000", "date": "1900-01-01", "text": "3c230000-a32f-1163-8080-808080808080", "varchar": "3c230000-a32f-1163-8080-808080808080"}}],
        ],
        "uuid": [
            [
                "de5cba0d-41a2-4f39-8834-35130d8b5d86",
                {
                    "cast": {"text": "de5cba0d-41a2-4f39-8834-35130d8b5d86", "varchar": "de5cba0d-41a2-4f39-8834-35130d8b5d86"},
                    "min": {"text": "de5cba0d-41a2-4f39-8834-35130d8b5d86", "varchar": "de5cba0d-41a2-4f39-8834-35130d8b5d86"},
                    "max": {"text": "fa80080c-a4c5-46d6-afe4-5e184fec35ae", "varchar": "fa80080c-a4c5-46d6-afe4-5e184fec35ae"},
                },
            ],
            ["f5f576e7-efb6-41b5-804e-fa3205548137", {"cast": {"text": "f5f576e7-efb6-41b5-804e-fa3205548137", "varchar": "f5f576e7-efb6-41b5-804e-fa3205548137"}}],
            ["fa80080c-a4c5-46d6-afe4-5e184fec35ae", {"cast": {"text": "fa80080c-a4c5-46d6-afe4-5e184fec35ae", "varchar": "fa80080c-a4c5-46d6-afe4-5e184fec35ae"}}],
        ],
        "inet": [
            ["'205.118.5.3'", {"cast": {"text": "205.118.5.3", "varchar": "205.118.5.3"}, "min": {"text": "192.168.122.255", "varchar": "192.168.122.255"}, "max": {"text": "205.118.5.3", "varchar": "205.118.5.3"}}],
            ["'192.168.2.1'", {"cast": {"text": "192.168.2.1", "varchar": "192.168.2.1"}}],
            ["'192.168.122.255'", {"cast": {"text": "192.168.122.255", "varchar": "192.168.122.255"}}],
        ],
        "udt": {
            "bigint": [
                [
                    4561313648948,
                    {
                        "cast": {"decimal": "4561313648948.0", "text": "4561313648948"},
                        "avg": {"decimal": "2280656551235.0", "text": None},
                        "sum": {"decimal": "4561313102470.0", "text": None},
                        "min": {"decimal": "-546478.0", "text": "-546478"},
                        "max": {"decimal": "4561313648948.0", "text": "4561313648948"},
                        "cast_avg": {"decimal": "2280656551235.0", "text": "2280656551235"},
                        "cast_sum": {"decimal": "4561313102470.0", "text": "4561313102470"},
                        "cast_min": {"decimal": "-546478.0", "text": "-546478"},
                        "cast_max": {"decimal": "4561313648948.0", "text": "4561313648948"},
                        "cast_cast": {"decimal/float": "4.5613e+12", "smallint/text": OUT_OF_RANGE},
                    },
                ],
                [-546478, {"cast": {"decimal": "-546478.0", "text": "-546478"}}],
            ],
            "float": [[55646.234556984, {"cast": {"int": 55646, "double": "55646.23438"}}]],
            "timestamp": [["'2017-12-27 11:57:42+0000'", {"cast": {"date": "2017-12-27", "text": "2017-12-27T11:57:42.000Z"}}]],
        },
    }

    NEGATIVE_VALUES = {
        "text": [
            [
                "jhkhkjh",
                {
                    "cast": {
                        "int": 'InvalidRequest: Error from server: code=2200 [Invalid query] message="Invalid call to function system.castAsInt, none of its type signatures match (known type signatures: system.castAsInt : (smallint) -> int, system.castAsInt : (decimal) -> int, system.castAsInt : (varint) -> int, system.castAsInt : (float) -> int, system.castAsInt : (tinyint) -> int, system.castAsInt : (counter) -> int, system.castAsInt : (bigint) -> int, system.castAsInt : (double) -> int)"',
                        "uuid": 'InvalidRequest: Error from server: code=2200 [Invalid query] message="c cannot be cast to uuid"',
                        "ascii": 'InvalidRequest: Error from server: code=2200 [Invalid query] message="Invalid call to function system.castAsAscii, none of its type signatures match (known type signatures: system.castAsAscii : (boolean) -> ascii, system.castAsAscii : (smallint) -> ascii, system.castAsAscii : (decimal) -> ascii, system.castAsAscii : (varint) -> ascii, system.castAsAscii : (timestamp) -> ascii, system.castAsAscii : (date) -> ascii, system.castAsAscii : (int) -> ascii, system.castAsAscii : (time) -> ascii, system.castAsAscii : (float) -> ascii, system.castAsAscii : (timeuuid) -> ascii, system.castAsAscii : (uuid) -> ascii, system.castAsAscii : (tinyint) -> ascii, system.castAsAscii : (inet) -> ascii, system.castAsAscii : (counter) -> ascii, system.castAsAscii : (bigint) -> ascii, system.castAsAscii : (double) -> ascii)"',
                    },
                    "avg": {
                        "int": 'InvalidRequest: Error from server: code=2200 [Invalid query] message="Invalid call to function system.castAsInt, none of its type signatures match (known type signatures: system.castAsInt : (smallint) -> int, system.castAsInt : (decimal) -> int, system.castAsInt : (varint) -> int, system.castAsInt : (float) -> int, system.castAsInt : (tinyint) -> int, system.castAsInt : (counter) -> int, system.castAsInt : (bigint) -> int, system.castAsInt : (double) -> int)"'
                    },
                    "sum": {
                        "float": 'InvalidRequest: Error from server: code=2200 [Invalid query] message="Invalid call to function system.castAsFloat, none of its type signatures match (known type signatures: system.castAsFloat : (varint) -> float, system.castAsFloat : (int) -> float, system.castAsFloat : (tinyint) -> float, system.castAsFloat : (counter) -> float, system.castAsFloat : (bigint) -> float, system.castAsFloat : (double) -> float, system.castAsFloat : (smallint) -> float, system.castAsFloat : (decimal) -> float)"'
                    },
                    "min": {"inet": 'InvalidRequest: Error from server: code=2200 [Invalid query] message="c cannot be cast to inet"'},
                    "max": {"uuid": 'InvalidRequest: Error from server: code=2200 [Invalid query] message="c cannot be cast to uuid"'},
                },
            ]
        ],
        "date": [
            "2015-12-23",
            {
                "cast": {
                    "int": 'InvalidRequest: Error from server: code=2200 [Invalid query] message="Invalid call to function system.castAsInt, none of its type signatures match (known type signatures: system.castAsInt : (smallint) -> int, system.castAsInt : (decimal) -> int, system.castAsInt : (varint) -> int, system.castAsInt : (float) -> int, system.castAsInt : (tinyint) -> int, system.castAsInt : (counter) -> int, system.castAsInt : (bigint) -> int, system.castAsInt : (double) -> int)"'
                }
            },
        ],
    }
