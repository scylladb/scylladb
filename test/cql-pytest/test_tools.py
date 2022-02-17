# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for the tools hosted by scylla
#############################################################################

import glob
import json
import nodetool
import os
import pytest
import subprocess
import util


def simple_no_clustering_table(cql, keyspace):
    table = util.unique_name()
    schema = f"CREATE TABLE {keyspace}.{table} (pk int PRIMARY KEY , v int)"

    cql.execute(schema)

    for pk in range(0, 10):
        cql.execute(f"INSERT INTO {keyspace}.{table} (pk, v) VALUES ({pk}, 0)")

    nodetool.flush(cql, f"{keyspace}.{table}")

    return table, schema


def simple_clustering_table(cql, keyspace):
    table = util.unique_name()
    schema = f"CREATE TABLE {keyspace}.{table} (pk int, ck int, v int, PRIMARY KEY (pk, ck))"

    cql.execute(schema)

    for pk in range(0, 10):
        for ck in range(0, 10):
            cql.execute(f"INSERT INTO {keyspace}.{table} (pk, ck, v) VALUES ({pk}, {ck}, 0)")

    nodetool.flush(cql, f"{keyspace}.{table}")

    return table, schema


def clustering_table_with_collection(cql, keyspace):
    table = util.unique_name()
    schema = f"CREATE TABLE {keyspace}.{table} (pk int, ck int, v map<int, text>, PRIMARY KEY (pk, ck))"

    cql.execute(schema)

    for pk in range(0, 10):
        for ck in range(0, 10):
            map_vals = {f"{p}: '{c}'" for p in range(0, pk) for c in range(0, ck)}
            map_str = ", ".join(map_vals)
            cql.execute(f"INSERT INTO {keyspace}.{table} (pk, ck, v) VALUES ({pk}, {ck}, {{{map_str}}})")

    nodetool.flush(cql, f"{keyspace}.{table}")

    return table, schema


def clustering_table_with_udt(cql, keyspace):
    table = util.unique_name()
    create_type_schema = f"CREATE TYPE {keyspace}.type1 (f1 int, f2 text)"
    create_table_schema = f" CREATE TABLE {keyspace}.{table} (pk int, ck int, v type1, PRIMARY KEY (pk, ck))"

    cql.execute(create_type_schema)
    cql.execute(create_table_schema)

    for pk in range(0, 10):
        for ck in range(0, 10):
            cql.execute(f"INSERT INTO {keyspace}.{table} (pk, ck, v) VALUES ({pk}, {ck}, {{f1: 100, f2: 'asd'}})")

    nodetool.flush(cql, f"{keyspace}.{table}")

    return table, "; ".join((create_type_schema, create_table_schema))


def table_with_counters(cql, keyspace):
    table = util.unique_name()
    schema = f"CREATE TABLE {keyspace}.{table} (pk int PRIMARY KEY, v counter)"

    cql.execute(schema)

    for pk in range(0, 10):
        for c in range(0, 4):
            cql.execute(f"UPDATE {keyspace}.{table} SET v = v + 1 WHERE pk = {pk};")

    nodetool.flush(cql, f"{keyspace}.{table}")

    return table, schema


@pytest.fixture(scope="module", params=[
        simple_no_clustering_table,
        simple_clustering_table,
        clustering_table_with_collection,
        clustering_table_with_udt,
        table_with_counters,
])
def scylla_sstable(request, tmp_path_factory, cql, test_keyspace, scylla_only):
    workdir = request.config.getoption('workdir')
    scylla_path = request.config.getoption('scylla_path')
    if not workdir or not scylla_path:
        pytest.skip('Cannot run tool tests: workdir and/or scylla_path not provided')

    table, schema = request.param(cql, test_keyspace)

    schema_file = os.path.join(tmp_path_factory.getbasetemp(), "schema.cql")
    with open(schema_file, "w") as f:
        f.write(schema)

    sstables = glob.glob(os.path.join(workdir, 'data', test_keyspace, table + '-*', '*-Data.db'))

    try:
        yield (scylla_path, schema_file, sstables)
    finally:
        cql.execute(f"DROP TABLE {test_keyspace}.{table}")


def one_sstable(sstables):
    return [sstables[0]]


def all_sstables(sstables):
    return sstables


@pytest.mark.parametrize("what", ["index", "compression-info", "summary", "statistics", "scylla-metadata"])
@pytest.mark.parametrize("which_sstables", [one_sstable, all_sstables])
def test_scylla_sstable_dump(scylla_sstable, what, which_sstables):
    (scylla_path, schema_file, sstables) = scylla_sstable

    out = subprocess.check_output([scylla_path, "sstable", f"dump-{what}", "--schema-file", schema_file] + which_sstables(sstables))

    print(out)

    assert out
    assert json.loads(out)


@pytest.mark.parametrize("merge", [True, False])
@pytest.mark.parametrize("output_format", ["text", "json"])
def test_scylla_sstable_dump_merge(scylla_sstable, merge, output_format):
    (scylla_path, schema_file, sstables) = scylla_sstable

    args = [scylla_path, "sstable", "dump-data", "--schema-file", schema_file, "--output-format", output_format]
    if merge:
        args.append("--merge")
    out = subprocess.check_output(args + sstables)

    print(out)

    assert out
    if output_format == "json":
        assert json.loads(out)
