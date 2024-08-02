# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for the tools hosted by scylla
#############################################################################

import contextlib
import glob
import itertools
import functools
import json
import nodetool
import os
import pathlib
import pytest
import subprocess
import tempfile
import random
import re
import shutil
import util
from typing import Iterable, Type, Union


def simple_no_clustering_table(cql, keyspace):
    table = util.unique_name()
    schema = f"CREATE TABLE {keyspace}.{table} (pk int PRIMARY KEY, v int) WITH compaction = {{'class': 'NullCompactionStrategy'}}"

    cql.execute(schema)

    for pk in range(0, 10):
        x = random.randrange(0, 4)
        if x == 0:
            # partition tombstone
            cql.execute(f"DELETE FROM {keyspace}.{table} WHERE pk = {pk}")
        else:
            # live row
            cql.execute(f"INSERT INTO {keyspace}.{table} (pk, v) VALUES ({pk}, 0)")

        if pk == 5:
            nodetool.flush(cql, f"{keyspace}.{table}")

    nodetool.flush(cql, f"{keyspace}.{table}")

    return table, schema


def simple_clustering_table(cql, keyspace):
    table = util.unique_name()
    schema = f"CREATE TABLE {keyspace}.{table} (pk1 int, pk2 int, ck1 int, ck2 int, v int, s int STATIC, PRIMARY KEY ((pk1, pk2), ck1, ck2)) WITH compaction = {{'class': 'NullCompactionStrategy'}}"

    cql.execute(schema)

    for pk in range(0, 10):
        for ck in range(0, 10):
            x = random.randrange(0, 8)
            if x == 0:
                # ttl
                cql.execute(f"INSERT INTO {keyspace}.{table} (pk1, pk2, ck1, ck2, v) VALUES ({pk}, {pk}, {ck}, {ck}, 0) USING TTL 6000")
            elif x == 1:
                # row tombstone
                cql.execute(f"DELETE FROM {keyspace}.{table} WHERE pk1 = {pk} AND pk2 = {pk} AND ck1 = {ck} AND ck2 = {ck}")
            elif x == 2:
                # cell tombstone
                cql.execute(f"DELETE v FROM {keyspace}.{table} WHERE pk1 = {pk} AND pk2 = {pk} AND ck1 = {ck} AND ck2 = {ck}")
            elif x == 3:
                # range tombstone
                l = ck * 10
                u = ck * 11
                cql.execute(f"DELETE FROM {keyspace}.{table} WHERE pk1 = {pk} AND pk2 = {pk} AND ck1 > {l} AND ck1 < {u}")
            else:
                # live row
                cql.execute(f"INSERT INTO {keyspace}.{table} (pk1, pk2, ck1, ck2, v) VALUES ({pk}, {pk}, {ck}, {ck}, 0)")

        if pk == 5:
            cql.execute(f"UPDATE {keyspace}.{table} SET s = 10 WHERE pk1 = {pk} AND pk2 = {pk}")
            nodetool.flush(cql, f"{keyspace}.{table}")

    nodetool.flush(cql, f"{keyspace}.{table}")

    return table, schema


def clustering_table_with_collection(cql, keyspace):
    table = util.unique_name()
    schema = f"CREATE TABLE {keyspace}.{table} (pk int, ck int, v1 map<int, text>, v2 set<int>, v3 list<int>, PRIMARY KEY (pk, ck)) WITH compaction = {{'class': 'NullCompactionStrategy'}}"

    cql.execute(schema)

    for pk in range(0, 10):
        for ck in range(0, 10):
            map_vals = {f"{p}: '{c}'" for p in range(0, pk) for c in range(0, ck)}
            map_str = ", ".join(map_vals)
            set_list_vals = list(range(0, pk))
            set_list_str = ", ".join(map(str, set_list_vals))
            cql.execute(f"INSERT INTO {keyspace}.{table} (pk, ck, v1, v2, v3) VALUES ({pk}, {ck}, {{{map_str}}}, {{{set_list_str}}}, [{set_list_str}])")
        if pk == 5:
            nodetool.flush(cql, f"{keyspace}.{table}")

    nodetool.flush(cql, f"{keyspace}.{table}")

    return table, schema


def clustering_table_with_udt(cql, keyspace):
    table = util.unique_name()
    create_type_schema = f"CREATE TYPE IF NOT EXISTS {keyspace}.type1 (f1 int, f2 text)"
    create_table_schema = f" CREATE TABLE {keyspace}.{table} (pk int, ck int, v type1, PRIMARY KEY (pk, ck)) WITH compaction = {{'class': 'NullCompactionStrategy'}}"

    cql.execute(create_type_schema)
    cql.execute(create_table_schema)

    for pk in range(0, 10):
        for ck in range(0, 10):
            cql.execute(f"INSERT INTO {keyspace}.{table} (pk, ck, v) VALUES ({pk}, {ck}, {{f1: 100, f2: 'asd'}})")
        if pk == 5:
            nodetool.flush(cql, f"{keyspace}.{table}")

    nodetool.flush(cql, f"{keyspace}.{table}")

    return table, "; ".join((create_type_schema, create_table_schema))


def table_with_counters(cql, keyspace):
    table = util.unique_name()
    schema = f"CREATE TABLE {keyspace}.{table} (pk int PRIMARY KEY, v counter) WITH compaction = {{'class': 'NullCompactionStrategy'}}"

    cql.execute(schema)

    for pk in range(0, 10):
        for c in range(0, 4):
            cql.execute(f"UPDATE {keyspace}.{table} SET v = v + 1 WHERE pk = {pk};")
        if pk == 5:
            nodetool.flush(cql, f"{keyspace}.{table}")

    nodetool.flush(cql, f"{keyspace}.{table}")

    return table, schema


@contextlib.contextmanager
def scylla_sstable(table_factory, cql, ks, data_dir):
    table, schema = table_factory(cql, ks)

    schema_file = os.path.join(data_dir, "..", "test_tools_schema.cql")
    with open(schema_file, "w") as f:
        f.write(schema)

    sstables = glob.glob(os.path.join(data_dir, ks, table + '-*', '*-Data.db'))

    try:
        yield (schema_file, sstables)
    finally:
        cql.execute(f"DROP TABLE {ks}.{table}")
        os.unlink(schema_file)


def one_sstable(sstables):
    assert len(sstables) > 1
    return [sstables[0]]


def all_sstables(sstables):
    assert len(sstables) > 1
    return sstables


@pytest.mark.parametrize("what", ["index", "compression-info", "summary", "statistics", "scylla-metadata"])
@pytest.mark.parametrize("which_sstables", [one_sstable, all_sstables])
def test_scylla_sstable_dump_component(cql, test_keyspace, scylla_path, scylla_data_dir, what, which_sstables):
    with scylla_sstable(simple_clustering_table, cql, test_keyspace, scylla_data_dir) as (schema_file, sstables):
        out = subprocess.check_output([scylla_path, "sstable", f"dump-{what}", "--schema-file", schema_file] + which_sstables(sstables))

    print(out)

    assert out
    assert json.loads(out)


@pytest.mark.parametrize("table_factory", [
        simple_no_clustering_table,
        simple_clustering_table,
        clustering_table_with_collection,
        clustering_table_with_udt,
        table_with_counters,
])
@pytest.mark.parametrize("merge", [True, False])
@pytest.mark.parametrize("output_format", ["text", "json"])
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def test_scylla_sstable_dump_data(cql, test_keyspace, scylla_path, scylla_data_dir, table_factory, merge, output_format):
    with scylla_sstable(simple_clustering_table, cql, test_keyspace, scylla_data_dir) as (schema_file, sstables):
        args = [scylla_path, "sstable", "dump-data", "--schema-file", schema_file, "--output-format", output_format]
        if merge:
            args.append("--merge")
        out = subprocess.check_output(args + sstables)

    print(out)

    assert out
    if output_format == "json":
        assert json.loads(out)


@pytest.mark.parametrize("table_factory", [
        simple_no_clustering_table,
        simple_clustering_table,
])
def test_scylla_sstable_write(cql, test_keyspace, scylla_path, scylla_data_dir, table_factory):
    with scylla_sstable(table_factory, cql, test_keyspace, scylla_data_dir) as (schema_file, sstables):
        with tempfile.TemporaryDirectory() as tmp_dir:
            dump_common_args = [scylla_path, "sstable", "dump-data", "--schema-file", schema_file, "--output-format", "json", "--merge"]
            generation = util.unique_key_int()

            original_out = subprocess.check_output(dump_common_args + sstables)
            original_json = json.loads(original_out)["sstables"]["anonymous"]

            input_file = os.path.join(tmp_dir, 'input.json')

            with open(input_file, 'w') as f:
                json.dump(original_json, f)

            subprocess.check_call([scylla_path, "sstable", "write", "--schema-file", schema_file, "--input-file", input_file, "--output-dir", tmp_dir, "--generation", str(generation), '--logger-log-level', 'scylla-sstable=trace'])

            sstable_file = os.path.join(tmp_dir, f"me-{generation}-big-Data.db")

            actual_out = subprocess.check_output(dump_common_args + [sstable_file])
            actual_json = json.loads(actual_out)["sstables"]["anonymous"]

            assert actual_json == original_json


def script_consume_test_table_factory(cql, keyspace):
    table = util.unique_name()
    schema = f"CREATE TABLE {keyspace}.{table} (pk int, ck int, v int, s int STATIC, PRIMARY KEY (pk, ck)) WITH compaction = {{'class': 'NullCompactionStrategy'}}"

    cql.execute(schema)

    partitions = 4

    for sst in range(0, 2):
        for pk in range(sst * partitions, (sst + 1) * partitions):
            # static row
            cql.execute(f"UPDATE {keyspace}.{table} SET s = 10 WHERE pk = {pk}")
            # range tombstone
            cql.execute(f"DELETE FROM {keyspace}.{table} WHERE pk = {pk} AND ck >= 0 AND ck <= 4")
            # 2 rows
            for ck in range(0, 4):
                cql.execute(f"INSERT INTO {keyspace}.{table} (pk, ck, v) VALUES ({pk}, {ck}, 0)")

        nodetool.flush(cql, f"{keyspace}.{table}")

    return table, schema


def test_scylla_sstable_script_consume_sstable(cql, test_keyspace, scylla_path, scylla_data_dir):
    script_file = os.path.join(scylla_data_dir, "..", "test_scylla_sstable_script_consume_sstable.lua")

    script = """
wr = Scylla.new_json_writer()
i = 0

function arg(args, arg)
    ret = nil
    wr:key(arg)
    if args[arg] then
        ret = tonumber(args[arg])
        wr:int(ret)
    else
        wr:null()
    end
    return ret
end

function basename(path)
    s, e = string.find(string.reverse(path), '/', 1, true)
    return string.sub(path, #path - s + 2)
end

function consume_stream_start(args)
    wr:start_object()
    start_sst = arg(args, "start_sst")
    end_sst = arg(args, "end_sst")
    wr:key("content")
    wr:start_array()
end

function consume_sstable_start(sst)
    wrote_ps = false
    i = i + 1
    if i == start_sst then
        return false
    end
    wr:string(basename(sst.filename))
end

function consume_partition_start(ps)
    if not wrote_ps then
        wr:string("ps")
        wrote_ps = true
    end
end

function consume_sstable_end()
    if i == end_sst then
        return false
    end
end

function consume_stream_end()
    wr:end_array()
    wr:end_object()
end
"""
    with open(script_file, 'w') as f:
        f.write(script)

    with scylla_sstable(script_consume_test_table_factory, cql, test_keyspace, scylla_data_dir) as (schema_file, sstables):
        sst1 = os.path.basename(sstables[0])
        sst2 = os.path.basename(sstables[1])
        def run_scenario(script_args, expected):
            print(f"Scenario: '{script_args}'\n")
            if script_args:
                script_args = ["--script-arg", script_args]
            else:
                script_args = []
            script_args = [scylla_path, "sstable", "script", "--schema-file", schema_file, "--script-file", script_file] + script_args + sstables[0:2]
            res = json.loads(subprocess.check_output(script_args))
            assert res == expected

        run_scenario("", {'start_sst': None, 'end_sst': None, 'content': [sst1, "ps", sst2, "ps"]})
        run_scenario("start_sst=1", {'start_sst': 1, 'end_sst': None, 'content': [sst2, "ps"]})
        run_scenario("start_sst=2", {'start_sst': 2, 'end_sst': None, 'content': [sst1, "ps"]})
        run_scenario("start_sst=1:end_sst=1", {'start_sst': 1, 'end_sst': 1, 'content': []})
        run_scenario("start_sst=2:end_sst=2", {'start_sst': 2, 'end_sst': 2, 'content': [sst1, "ps"]})
        run_scenario("end_sst=1", {'start_sst': None, 'end_sst': 1, 'content': [sst1, "ps"]})
        run_scenario("end_sst=2", {'start_sst': None, 'end_sst': 2, 'content': [sst1, "ps", sst2, "ps"]})


def test_scylla_sstable_script_slice(cql, test_keyspace, scylla_path, scylla_data_dir):
    class bound:
        @staticmethod
        def unpack_value(value):
            if isinstance(value, tuple):
                return value
            else:
                return None, value

        def __init__(self, value, weight):
            self.token, self.value = self.unpack_value(value)
            self.weight = weight

        def tri_cmp(self, value):
            if self.token is None and self.value is None:
                assert(self.weight)
                return -self.weight
            token, value = self.unpack_value(value)
            if token is None:
                res = 0
            else:
                res = int(token) - int(self.token)
            if res == 0 and not value is None and not self.value is None :
                res = int(value) - int(self.value)
            return res if res else -self.weight

        def get_value(self, lookup_table, is_start):
            if self.token is None and self.value is None:
                return '-inf' if is_start else '+inf'
            if self.value is None:
                return "t{}".format(int(self.token))
            return lookup_table[self.value]

        @staticmethod
        def before(value):
            return bound(value, -1)

        @staticmethod
        def at(value):
            return bound(value, 0)

        @staticmethod
        def after(value):
            return bound(value, 1)


    class interval:
        def __init__(self, start_bound, end_bound):
            self.start = start_bound
            self.end = end_bound

        def contains(self, value):
            return self.start.tri_cmp(value) >= 0 and self.end.tri_cmp(value) <= 0


    def summarize_dump(dump):
        summary = []
        for partition in list(dump["sstables"].items())[0][1]:
            partition_summary = {"pk": partition["key"]["value"], "token": partition["key"]["token"], "frags": []}
            if "static_row" in partition:
                partition_summary["frags"].append(("sr", None))
            for clustering_fragment in partition.get("clustering_elements", []):
                type_str = "cr" if clustering_fragment["type"] == "clustering-row" else "rtc"
                partition_summary["frags"].append((type_str, clustering_fragment["key"]["value"]))
            summary.append(partition_summary)
        return summary

    def filter_summary(summary, partition_ranges, clustering_ranges):
        if not partition_ranges:
            return summary
        filtered_summary = []
        for partition in summary:
            if any(map(lambda x: interval.contains(x, (partition["token"], partition["pk"])), partition_ranges)):
                filtered_summary.append({"pk": partition["pk"], "token": partition["token"], "frags": []})
                for (t, k) in partition["frags"]:
                    if t == "rtc" or k is None or not clustering_ranges or any(map(lambda x: interval.contains(x, k), clustering_ranges)):
                        filtered_summary[-1]["frags"].append((t, k))

        return filtered_summary

    def serialize_ranges(prefix, ranges, lookup_table):
        serialized_ranges = []
        i = 0
        for r in ranges:
            s = r.start.get_value(lookup_table, True)
            e = r.end.get_value(lookup_table, False)
            serialized_ranges.append("{}{}={}{},{}{}".format(
                prefix,
                i,
                "(" if s == '-inf' or r.start.weight > 0 else "[",
                s,
                e,
                ")" if e == '+inf' or r.end.weight < 0 else "]"))
            i = i + 1
        return serialized_ranges

    scripts_path = os.path.realpath(os.path.join(__file__, '../../../tools/scylla-sstable-scripts'))
    script_file = os.path.join(scripts_path, 'slice.lua')

    with scylla_sstable(script_consume_test_table_factory, cql, test_keyspace, scylla_data_dir) as (schema_file, sstables):
        reference_summary = summarize_dump(json.loads(subprocess.check_output([scylla_path, "sstable", "dump-data", "--schema-file", schema_file, "--merge"] + sstables)))

        # same order as in dump
        pks = [(p["token"], p["pk"]) for p in reference_summary]
        cks = set()
        for p in reference_summary:
            for t, ck in p["frags"]:
                if not ck is None:
                    cks.add(ck)
        cks = sorted(list(cks))
        serialized_pk_lookup = {pk: subprocess.check_output([scylla_path, "types", "serialize", "--full-compound", "-t", "Int32Type", "--", pk]).strip().decode() for t, pk in pks}
        serialized_ck_lookup = {ck: subprocess.check_output([scylla_path, "types", "serialize", "--prefix-compound", "-t", "Int32Type", "--", ck]).strip().decode() for ck in cks}

        script_common_args = [scylla_path, "sstable", "script", "--schema-file", schema_file, "--merge", "--script-file", script_file]

        def run_scenario(scenario, partition_ranges, clustering_ranges):
            print(f"running scenario {scenario}")
            script_args = serialize_ranges("pr", partition_ranges, serialized_pk_lookup) + serialize_ranges("cr", clustering_ranges, serialized_ck_lookup)
            if script_args:
                script_args = ["--script-arg"] + [":".join(script_args)]
            print(f"script_args={script_args}")
            expected = filter_summary(reference_summary, partition_ranges, clustering_ranges)
            out = subprocess.check_output(script_common_args + script_args + sstables)
            summary = summarize_dump(json.loads(out))
            assert summary == expected

        run_scenario("no args", [], [])
        run_scenario("full range", [interval(bound.before(None), bound.after(None))], [])
        run_scenario("(pks[0], +inf)", [interval(bound.after(pks[0]), bound.after(None))], [])
        run_scenario("(-inf, pks[-3]]", [interval(bound.before(None), bound.after(pks[-3]))], [])
        run_scenario("[pks[2], pks[-2]]", [interval(bound.before(pks[2]), bound.after(pks[-2]))], [])
        run_scenario("[pks[0], pks[1]], [pks[2], pks[3]]", [interval(bound.before(pks[1]), bound.after(pks[2])), interval(bound.before(pks[3]), bound.after(pks[4]))], [])
        run_scenario("[t:pks[2], t:pks[-2]]", [interval(bound.before((pks[2][0], None)), bound.after((pks[-2][0], None)))], [])
        run_scenario("full pk range | [-inf, cks[2]]", [interval(bound.before(None), bound.after(None))], [interval(bound.before(None), bound.after(cks[2]))])
        run_scenario("[pks[0], pks[1]] | (cks[0], cks[1]], (cks[2], +inf)", [interval(bound.before(pks[1]), bound.after(pks[2]))],
                     [interval(bound.after(cks[0]), bound.after(cks[1])), interval(bound.after(cks[2]), bound.after(None))])


@pytest.mark.parametrize("table_factory", [
        simple_no_clustering_table,
        simple_clustering_table,
        clustering_table_with_collection,
        clustering_table_with_udt,
        table_with_counters,
])
def test_scylla_sstable_script(cql, test_keyspace, scylla_path, scylla_data_dir, table_factory, has_tablets):
    if has_tablets and table_factory == table_with_counters: # issue #18180
        return
    scripts_path = os.path.realpath(os.path.join(__file__, '../../../tools/scylla-sstable-scripts'))
    slice_script_path = os.path.join(scripts_path, 'slice.lua')
    dump_script_path = os.path.join(scripts_path, 'dump.lua')
    with scylla_sstable(table_factory, cql, test_keyspace, scylla_data_dir) as (schema_file, sstables):
        dump_common_args = [scylla_path, "sstable", "dump-data", "--schema-file", schema_file, "--output-format", "json"]
        script_common_args = [scylla_path, "sstable", "script", "--schema-file", schema_file]

        # without --merge
        cxx_json = json.loads(subprocess.check_output(dump_common_args + sstables))
        dump_lua_json = json.loads(subprocess.check_output(script_common_args + ["--script-file", dump_script_path] + sstables))
        slice_lua_json = json.loads(subprocess.check_output(script_common_args + ["--script-file", slice_script_path] + sstables))

        assert dump_lua_json == cxx_json
        assert slice_lua_json == cxx_json

        # with --merge
        cxx_json = json.loads(subprocess.check_output(dump_common_args + ["--merge"] + sstables))
        dump_lua_json = json.loads(subprocess.check_output(script_common_args + ["--merge", "--script-file", dump_script_path] + sstables))
        slice_lua_json = json.loads(subprocess.check_output(script_common_args + ["--merge", "--script-file", slice_script_path] + sstables))

        assert dump_lua_json == cxx_json
        assert slice_lua_json == cxx_json


class TestScyllaSsstableSchemaLoadingBase:

    def check(self, scylla_path, extra_args, sstable, dump_reference, cwd=None, env=None):
        dump_common_args = [scylla_path, "sstable", "dump-data", "--output-format", "json", "--logger-log-level", "scylla-sstable=debug:schema_loader=trace"]
        dump = json.loads(subprocess.check_output(dump_common_args + extra_args + [sstable], cwd=cwd, env=env))["sstables"]
        dump = list(dump.values())[0]
        assert dump == dump_reference

    def check_fail(self, scylla_path, extra_args, sstable, error_msg=None, cwd=None):
        common_args = [scylla_path, "sstable", "dump-data", "--logger-log-level", "scylla-sstable=debug:schema_loader=trace"]
        res = subprocess.run(common_args + extra_args + [sstable], capture_output=True, text=True, cwd=cwd, env={})
        print(res.stderr)
        if error_msg is None:
            error_msg = "Failed to autodetect and load schema, try again with --logger-log-level scylla-sstable=debug to learn more or provide the schema source manually"
        assert res.stderr.split('\n')[-2] == error_msg
        assert res.returncode != 0

    def copy_sstable_to_external_dir(self, system_scylla_local_sstable_prepared, temp_workdir):
        table_data_dir, sstable_filename = os.path.split(system_scylla_local_sstable_prepared)
        sstable_glob = "-".join(sstable_filename.split("-")[:-1]) + "*"
        sstable_components = glob.glob(os.path.join(table_data_dir, sstable_glob))

        for c in sstable_components:
            shutil.copy(c, temp_workdir)

        return glob.glob(os.path.join(temp_workdir, "*-Data.db"))[0]


@contextlib.contextmanager
def _prepare_sstable(cql, scylla_data_dir, table, write_fun=None):
    """ Prepares the table for the needs of the schema loading tests.

    Namely:
    * Disable auto-compaction for the system-schema keyspace and table's keyspace.
    * Flushes said keyspaces.
    * Locates an sstable belonging to the table and returns it.
    """
    keyspace_name, table_name = table.split(".")
    with nodetool.no_autocompaction_context(cql, keyspace_name, "system_schema"):
        if write_fun is not None:
            write_fun()

        # Need to flush system keyspaces whose sstables we want to meddle
        # with, to make sure they are actually on disk.
        nodetool.flush_keyspace(cql, "system_schema")
        nodetool.flush_keyspace(cql, keyspace_name)

        sstables = glob.glob(os.path.join(scylla_data_dir, keyspace_name, table_name + "-*", "*-Data.db"))
        yield sstables[0]


@pytest.fixture(scope="class")
def system_scylla_local_sstable_prepared(cql, scylla_data_dir):
    with _prepare_sstable(cql, scylla_data_dir, "system.scylla_local") as sst:
        yield sst


@pytest.fixture(scope="class")
def system_scylla_local_schema_file():
    """ Prepares a schema.cql with the schema of system.scylla_local. """
    with tempfile.NamedTemporaryFile("w+t") as f:
        f.write("CREATE TABLE system.scylla_local (key text PRIMARY KEY, value text)")
        f.flush()
        yield f.name


@pytest.fixture(scope="class")
def scylla_home_dir(scylla_data_dir):
    """ Create a temporary directory structure to be used as SCYLLA_HOME.

    The top-level directory contains a conf dir, which contains a scylla.yaml,
    which has the workdir configuration item set.
    The top level directory can be used as SCYLLA_HOME, while the conf dir can be
    used as SCYLLA_CONF environment variables respectively, allowing scylla sstable
    tool to locate the work-directory of the node.
    """
    with tempfile.TemporaryDirectory() as scylla_home:
        conf_dir = os.path.join(scylla_home, "conf")
        os.mkdir(conf_dir)

        scylla_yaml_file = os.path.join(conf_dir, "scylla.yaml")
        with open(scylla_yaml_file, "w") as f:
            f.write(f"workdir: {os.path.split(scylla_data_dir)[0]}")

        yield scylla_home


def _produce_reference_dump(scylla_path, schema_args, sstable):
    """ Produce a json dump, to be used as a reference, of the specified sstable. """
    dump_reference = subprocess.check_output([
        scylla_path,
        "sstable",
        "dump-data",
        "--output-format", "json",
        "--logger-log-level", "scylla-sstable=debug",
        ] + schema_args + [sstable])
    dump_reference = json.loads(dump_reference)["sstables"]
    return list(dump_reference.values())[0]


@pytest.fixture(scope="class")
def system_scylla_local_reference_dump(scylla_path, system_scylla_local_sstable_prepared):
    return _produce_reference_dump(scylla_path, ["--system-schema", "--keyspace", "system", "--table", "scylla_local"],
                                   system_scylla_local_sstable_prepared)


class TestScyllaSsstableSchemaLoading(TestScyllaSsstableSchemaLoadingBase):
    """ Test class containing all the schema loader tests.

    Helps in providing a natural scope of all the specialized fixtures shared by
    these tests.
    """
    keyspace = "system"
    table = "scylla_local"

    def test_table_dir_system_schema(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump):
        self.check(
                scylla_path,
                ["--system-schema", "--keyspace", self.keyspace, "--table", self.table],
                system_scylla_local_sstable_prepared,
                system_scylla_local_reference_dump)

    def test_table_dir_system_schema_deduced_keyspace_table(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump):
        self.check(
                scylla_path,
                ["--system-schema"],
                system_scylla_local_sstable_prepared,
                system_scylla_local_reference_dump)

    def test_table_dir_schema_file(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, system_scylla_local_schema_file):
        self.check(
                scylla_path,
                ["--schema-file", system_scylla_local_schema_file],
                system_scylla_local_sstable_prepared,
                system_scylla_local_reference_dump)

    def test_table_dir_data_dir(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, scylla_data_dir):
        self.check(
                scylla_path,
                ["--scylla-data-dir", scylla_data_dir, "--keyspace", self.keyspace, "--table", self.table],
                system_scylla_local_sstable_prepared,
                system_scylla_local_reference_dump)

    def test_table_dir_data_dir_deduced_keyspace_table(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, scylla_data_dir):
        self.check(
                scylla_path,
                ["--scylla-data-dir", scylla_data_dir],
                system_scylla_local_sstable_prepared,
                system_scylla_local_reference_dump)

    def test_table_dir_scylla_yaml(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, scylla_home_dir):
        scylla_yaml_file = os.path.join(scylla_home_dir, "conf", "scylla.yaml")
        self.check(
                scylla_path,
                ["--scylla-yaml-file", scylla_yaml_file, "--keyspace", self.keyspace, "--table", self.table],
                system_scylla_local_sstable_prepared,
                system_scylla_local_reference_dump)

    def test_table_dir_scylla_yaml_deduced_keyspace_table(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, scylla_home_dir):
        scylla_yaml_file = os.path.join(scylla_home_dir, "conf", "scylla.yaml")
        self.check(
                scylla_path,
                ["--scylla-yaml-file", scylla_yaml_file],
                system_scylla_local_sstable_prepared,
                system_scylla_local_reference_dump)

    def test_external_dir_system_schema(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, temp_workdir):
        ext_sstable = self.copy_sstable_to_external_dir(system_scylla_local_sstable_prepared, temp_workdir)
        self.check(
                scylla_path,
                ["--system-schema", "--keyspace", self.keyspace, "--table", self.table],
                ext_sstable,
                system_scylla_local_reference_dump)

    def test_external_dir_schema_file(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, temp_workdir, system_scylla_local_schema_file):
        ext_sstable = self.copy_sstable_to_external_dir(system_scylla_local_sstable_prepared, temp_workdir)
        self.check(
                scylla_path,
                ["--schema-file", system_scylla_local_schema_file],
                ext_sstable,
                system_scylla_local_reference_dump)


    def test_external_dir_data_dir(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, temp_workdir, scylla_data_dir):
        ext_sstable = self.copy_sstable_to_external_dir(system_scylla_local_sstable_prepared, temp_workdir)
        self.check(
                scylla_path,
                ["--scylla-data-dir", scylla_data_dir, "--keyspace", self.keyspace, "--table", self.table],
                ext_sstable,
                system_scylla_local_reference_dump)

    def test_external_dir_scylla_yaml(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, temp_workdir, scylla_home_dir):
        ext_sstable = self.copy_sstable_to_external_dir(system_scylla_local_sstable_prepared, temp_workdir)
        scylla_yaml_file = os.path.join(scylla_home_dir, "conf", "scylla.yaml")
        self.check(
                scylla_path,
                ["--scylla-yaml-file", scylla_yaml_file, "--keyspace", self.keyspace, "--table", self.table],
                ext_sstable,
                system_scylla_local_reference_dump)

    def test_external_dir_autodetect_schema_file(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, temp_workdir, system_scylla_local_schema_file):
        ext_sstable = self.copy_sstable_to_external_dir(system_scylla_local_sstable_prepared, temp_workdir)
        shutil.copy(system_scylla_local_schema_file, os.path.join(temp_workdir, "schema.cql"))
        self.check(
                scylla_path,
                [],
                ext_sstable,
                system_scylla_local_reference_dump,
                cwd=temp_workdir)

    def test_external_dir_autodetect_conf_dir(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, temp_workdir, scylla_home_dir):
        ext_sstable = self.copy_sstable_to_external_dir(system_scylla_local_sstable_prepared, temp_workdir)
        self.check(
                scylla_path,
                ["--keyspace", self.keyspace, "--table", self.table],
                ext_sstable,
                system_scylla_local_reference_dump,
                cwd=scylla_home_dir)

    def test_external_dir_autodetect_conf_dir_conf_env(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, temp_workdir, scylla_home_dir):
        ext_sstable = self.copy_sstable_to_external_dir(system_scylla_local_sstable_prepared, temp_workdir)
        conf_dir = os.path.join(scylla_home_dir, "conf")
        self.check(
                scylla_path,
                ["--keyspace", self.keyspace, "--table", self.table],
                ext_sstable,
                system_scylla_local_reference_dump,
                env={"SCYLLA_CONF": conf_dir})

    def test_external_dir_autodetect_conf_dir_home_env(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, temp_workdir, scylla_home_dir):
        ext_sstable = self.copy_sstable_to_external_dir(system_scylla_local_sstable_prepared, temp_workdir)
        self.check(
                scylla_path,
                ["--keyspace", self.keyspace, "--table", self.table],
                ext_sstable,
                system_scylla_local_reference_dump,
                env={"SCYLLA_HOME": scylla_home_dir})

    def test_external_dir_autodetect_sstable_serialization_header_keyspace_table(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, temp_workdir):
        ext_sstable = self.copy_sstable_to_external_dir(system_scylla_local_sstable_prepared, temp_workdir)
        # It is important to use a controlled workdir, so scylla-sstable doesn't accidentally pick up a scylla.yaml.
        self.check(
                scylla_path,
                ["--keyspace", self.keyspace, "--table", self.table],
                ext_sstable,
                system_scylla_local_reference_dump,
                cwd=temp_workdir)

    def test_external_dir_autodetect_sstable_serialization_header(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, temp_workdir):
        ext_sstable = self.copy_sstable_to_external_dir(system_scylla_local_sstable_prepared, temp_workdir)
        # It is important to use a controlled workdir, so scylla-sstable doesn't accidentally pick up a scylla.yaml.
        self.check(
                scylla_path,
                [],
                ext_sstable,
                system_scylla_local_reference_dump,
                cwd=temp_workdir)

    def test_fail_nonexistent_keyspace(self, scylla_path, system_scylla_local_sstable_prepared, temp_workdir, scylla_home_dir):
        ext_sstable = self.copy_sstable_to_external_dir(system_scylla_local_sstable_prepared, temp_workdir)
        scylla_yaml_file = os.path.join(scylla_home_dir, "conf", "scylla.yaml")
        self.check_fail(
                scylla_path,
                ["--scylla-yaml-file", scylla_yaml_file, "--keyspace", "non-existent-keyspace", "--table", self.table],
                ext_sstable,
                error_msg="error processing arguments: could not load schema via schema-tables: std::runtime_error (Failed to find non-existent-keyspace.scylla_local in schema tables)")

    def test_fail_nonexistent_table(self, scylla_path, system_scylla_local_sstable_prepared, temp_workdir, scylla_home_dir):
        ext_sstable = self.copy_sstable_to_external_dir(system_scylla_local_sstable_prepared, temp_workdir)
        scylla_yaml_file = os.path.join(scylla_home_dir, "conf", "scylla.yaml")
        self.check_fail(
                scylla_path,
                ["--scylla-yaml-file", scylla_yaml_file, "--keyspace", self.keyspace, "--table", "non-existent-table"],
                ext_sstable,
                error_msg="error processing arguments: could not load schema via schema-tables: std::runtime_error (Failed to find system.non-existent-table in schema tables)")


@pytest.fixture(scope="class")
def schema_test_base_table(cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, "pk int, v1 text, v2 text, PRIMARY KEY (pk)") as table:
        yield table


@pytest.fixture(scope="class")
def schema_test_mv(cql, schema_test_base_table):
    with util.new_materialized_view(cql, schema_test_base_table,
                                    '*', 'v1, pk', 'v1 is not null and pk is not null') as mv:
        yield mv


@pytest.fixture(scope="class")
def schema_test_si(cql, schema_test_base_table):
    keyspace, base_table = schema_test_base_table.split(".")
    si_name = f"{base_table}_by_v2"
    with util.new_secondary_index(cql, schema_test_base_table, "v2", name=si_name) as si:
        yield si + "_index"


@pytest.fixture(scope="class")
def schema_test_mv_sstable_prepared(cql, test_keyspace, schema_test_base_table, schema_test_mv, scylla_data_dir):
    def write():
        cql.execute(f"INSERT INTO {schema_test_base_table} (pk, v1, v2) VALUES (0, 'v1-0', 'v2-0')")
        cql.execute(f"INSERT INTO {schema_test_base_table} (pk, v1, v2) VALUES (1, 'v1-1', 'v2-1')")
        cql.execute(f"INSERT INTO {schema_test_base_table} (pk, v1, v2) VALUES (2, 'v1-1', 'v2-2')")

    with _prepare_sstable(cql, scylla_data_dir, schema_test_mv, write) as sst:
        yield sst


@pytest.fixture(scope="class")
def schema_test_si_sstable_prepared(cql, test_keyspace, schema_test_base_table, schema_test_si, scylla_data_dir):
    def write():
        cql.execute(f"INSERT INTO {schema_test_base_table} (pk, v1, v2) VALUES (0, 'v1-0', 'v2-0')")
        cql.execute(f"INSERT INTO {schema_test_base_table} (pk, v1, v2) VALUES (1, 'v1-1', 'v2-1')")
        cql.execute(f"INSERT INTO {schema_test_base_table} (pk, v1, v2) VALUES (2, 'v1-1', 'v2-2')")

    with _prepare_sstable(cql, scylla_data_dir, schema_test_si, write) as sst:
        yield sst


@pytest.fixture(scope="class")
def schema_test_mv_schema_file(schema_test_base_table, schema_test_mv):
    """ Prepares a schema.cql with the schema of the view, matching that in the `mv_sstable_prepared` fixture. """
    with tempfile.NamedTemporaryFile("w+t") as f:
        f.write(f"CREATE TABLE {schema_test_base_table} (pk int, v1 text, v2 text, PRIMARY KEY (pk));")
        f.write(f"CREATE MATERIALIZED VIEW {schema_test_mv} AS")
        f.write(f"    SELECT * FROM {schema_test_base_table} WHERE v1 IS NOT NULL AND pk IS NOT NULL")
        f.write("     PRIMARY KEY (v1, pk);")
        f.flush()
        yield f.name


@pytest.fixture(scope="class")
def schema_test_si_schema_file(schema_test_base_table, schema_test_si):
    """ Prepares a schema.cql with the schema of the index, matching that in the `si_sstable_prepared` fixture. """
    keyspace, base_table = schema_test_base_table.split(".")
    with tempfile.NamedTemporaryFile("w+t") as f:
        f.write(f"CREATE TABLE {schema_test_base_table} (pk int, v1 text, v2 text, PRIMARY KEY (pk));")
        f.write(f"CREATE INDEX {base_table}_by_v2 ON {schema_test_base_table}(v2);")
        f.flush()
        yield f.name


@pytest.fixture(scope="class")
def schema_test_mv_reference_dump(scylla_path, schema_test_mv, schema_test_mv_sstable_prepared):
    with tempfile.NamedTemporaryFile("w+t") as f:
        f.write(f"CREATE TABLE {schema_test_mv} (v1 text, pk int, v2 text, PRIMARY KEY (v1, pk))")
        f.flush()
        return _produce_reference_dump(scylla_path, ["--schema-file", f.name], schema_test_mv_sstable_prepared)


@pytest.fixture(scope="class")
def schema_test_si_reference_dump(scylla_path, schema_test_si, schema_test_si_sstable_prepared):
    with tempfile.NamedTemporaryFile("w+t") as f:
        f.write(f"CREATE TABLE {schema_test_si} (v2 text, idx_token bigint, pk int, PRIMARY KEY (v2, idx_token, pk))")
        f.flush()
        return _produce_reference_dump(scylla_path, ["--schema-file", f.name], schema_test_si_sstable_prepared)


class TestScyllaSsstableViewSchemaLoading(TestScyllaSsstableSchemaLoadingBase):
    """ Test class containing schema-loading tests for materialized views and indexes.

    Similar to TestScyllaSsstableSchemaLoading, but focuses on testing that
    materialized view and index schemas can be loaded with all methods.
    Not focusing on exhaustively testing data directory discovery, that is
    already tested by TestScyllaSsstableSchemaLoading.
    """

    def test_mv_table_dir_schema_file(self, scylla_path, schema_test_mv_sstable_prepared,
                                      schema_test_mv_reference_dump, schema_test_mv_schema_file):
        self.check(
                scylla_path,
                ["--schema-file", schema_test_mv_schema_file],
                schema_test_mv_sstable_prepared,
                schema_test_mv_reference_dump)

    def test_mv_external_dir_schema_file(self, scylla_path, schema_test_mv_sstable_prepared,
                                         schema_test_mv_reference_dump, schema_test_mv_schema_file, temp_workdir):
        ext_sstable = self.copy_sstable_to_external_dir(schema_test_mv_sstable_prepared, temp_workdir)
        self.check(
                scylla_path,
                ["--schema-file", schema_test_mv_schema_file],
                ext_sstable,
                schema_test_mv_reference_dump)

    def test_mv_table_dir_autodeduced(self, scylla_path, schema_test_mv, schema_test_mv_sstable_prepared,
                                      schema_test_mv_reference_dump, scylla_home_dir):
        self.check(
                scylla_path,
                [],
                schema_test_mv_sstable_prepared,
                schema_test_mv_reference_dump)

    def test_mv_table_dir_scylla_yaml(self, scylla_path, schema_test_mv, schema_test_mv_sstable_prepared,
                                      schema_test_mv_reference_dump, scylla_home_dir):
        scylla_yaml_file = os.path.join(scylla_home_dir, "conf", "scylla.yaml")
        keyspace, table = schema_test_mv.split(".")
        self.check(
                scylla_path,
                ["--scylla-yaml-file", scylla_yaml_file, "--keyspace", keyspace, "--table", table],
                schema_test_mv_sstable_prepared,
                schema_test_mv_reference_dump)

    def test_mv_external_dir_scylla_yaml(self, scylla_path, schema_test_mv, schema_test_mv_sstable_prepared,
                                         schema_test_mv_reference_dump, scylla_home_dir, temp_workdir):
        ext_sstable = self.copy_sstable_to_external_dir(schema_test_mv_sstable_prepared, temp_workdir)
        scylla_yaml_file = os.path.join(scylla_home_dir, "conf", "scylla.yaml")
        keyspace, table = schema_test_mv.split(".")
        self.check(
                scylla_path,
                ["--scylla-yaml-file", scylla_yaml_file, "--keyspace", keyspace, "--table", table],
                ext_sstable,
                schema_test_mv_reference_dump)

    def test_si_table_dir_schema_file(self, scylla_path, schema_test_si_sstable_prepared,
                                      schema_test_si_reference_dump, schema_test_si_schema_file):
        self.check(
                scylla_path,
                ["--schema-file", schema_test_si_schema_file],
                schema_test_si_sstable_prepared,
                schema_test_si_reference_dump)

    def test_si_external_dir_schema_file(self, scylla_path, schema_test_si_sstable_prepared,
                                         schema_test_si_reference_dump, schema_test_si_schema_file, temp_workdir):
        ext_sstable = self.copy_sstable_to_external_dir(schema_test_si_sstable_prepared, temp_workdir)
        self.check(
                scylla_path,
                ["--schema-file", schema_test_si_schema_file],
                ext_sstable,
                schema_test_si_reference_dump)

    def test_si_table_dir_autodeduced(self, scylla_path, schema_test_si, schema_test_si_sstable_prepared,
                                      schema_test_si_reference_dump, scylla_home_dir):
        self.check(
                scylla_path,
                [],
                schema_test_si_sstable_prepared,
                schema_test_si_reference_dump)

    def test_si_table_dir_scylla_yaml(self, scylla_path, schema_test_si, schema_test_si_sstable_prepared,
                                      schema_test_si_reference_dump, scylla_home_dir):
        scylla_yaml_file = os.path.join(scylla_home_dir, "conf", "scylla.yaml")
        keyspace, table = schema_test_si.split(".")
        self.check(
                scylla_path,
                ["--scylla-yaml-file", scylla_yaml_file, "--keyspace", keyspace, "--table", table],
                schema_test_si_sstable_prepared,
                schema_test_si_reference_dump)

    def test_si_external_dir_scylla_yaml(self, scylla_path, schema_test_si, schema_test_si_sstable_prepared,
                                         schema_test_si_reference_dump, scylla_home_dir, temp_workdir):
        ext_sstable = self.copy_sstable_to_external_dir(schema_test_si_sstable_prepared, temp_workdir)
        scylla_yaml_file = os.path.join(scylla_home_dir, "conf", "scylla.yaml")
        keyspace, table = schema_test_si.split(".")
        self.check(
                scylla_path,
                ["--scylla-yaml-file", scylla_yaml_file, "--keyspace", keyspace, "--table", table],
                ext_sstable,
                schema_test_si_reference_dump)


@pytest.fixture(scope="module")
def scrub_workdir():
    """A root temporary directory to be shared by all the scrub tests"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.fixture(scope="module")
def scrub_schema_file(scrub_workdir):
    """Create a schema.cql for the scrub tests"""
    fname = os.path.join(scrub_workdir, "schema.cql")
    with open(fname, "w") as f:
        f.write("CREATE TABLE ks.tbl (pk int, ck int, v text, PRIMARY KEY (pk, ck))")
        f.flush()
    yield fname


@pytest.fixture(scope="module")
def scrub_good_sstable(scylla_path, scrub_workdir, scrub_schema_file):
    """A good sstable used by the scrub tests."""
    with tempfile.TemporaryDirectory(prefix="good-sstable", dir=scrub_workdir) as tmp_dir:
        sst_json_path = os.path.join(tmp_dir, "sst.json")
        with open(sst_json_path, "w") as f:
            sst_json = [
                {
                    "key": { "raw": "0004000000c8" },
                    "clustering_elements": [
                        { "type": "clustering-row", "key": { "raw": "000400000001" }, "columns": { "v": { "is_live": True, "type": "regular", "timestamp": 1686815362417553, "value": "vv" } } }
                    ]
                }
            ]
            json.dump(sst_json, f)
        subprocess.check_call([scylla_path, "sstable", "write", "--schema-file", scrub_schema_file, "--output-dir", tmp_dir, "--generation", "1", "--input-file", sst_json_path])
        ssts = glob.glob(os.path.join(tmp_dir, "*-Data.db"))
        assert len(ssts) == 1
        yield ssts[0]


@pytest.fixture(scope="module")
def scrub_bad_sstable(scylla_path, scrub_workdir, scrub_schema_file):
    """A bad sstable (out-of-order rows) used by the scrub tests."""
    with tempfile.TemporaryDirectory(prefix="bad-sstable", dir=scrub_workdir) as tmp_dir:
        sst_json_path = os.path.join(tmp_dir, "sst.json")
        with open(sst_json_path, "w") as f:
            # rows are out-of-order
            sst_json = [
                {
                    "key": { "raw": "0004000000c8" },
                    "clustering_elements": [
                        { "type": "clustering-row", "key": { "raw": "000400000002" }, "columns": { "v": { "is_live": True, "type": "regular", "timestamp": 1686815362417553, "value": "vv" } } },
                        { "type": "clustering-row", "key": { "raw": "000400000001" }, "columns": { "v": { "is_live": True, "type": "regular", "timestamp": 1686815362417553, "value": "vv" } } }
                    ]
                }
            ]
            json.dump(sst_json, f)
        subprocess.check_call([scylla_path, "sstable", "write", "--schema-file", scrub_schema_file, "--output-dir", tmp_dir, "--generation", "1", "--input-file", sst_json_path, "--validation-level", "none"])
        ssts = glob.glob(os.path.join(tmp_dir, "*-Data.db"))
        assert len(ssts) == 1
        yield ssts[0]


def subprocess_check_error(args, pattern):
    """Invoke scubprocess.run() with the provided args and check that it fails with stderr matching the provided pattern."""
    res = subprocess.run(args, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert res.returncode != 0
    err = res.stderr.decode('utf-8')
    assert re.search(pattern, err) is not None


def check_scrub_output_dir(sst_dir, num_sstables):
    assert len(glob.glob(os.path.join(sst_dir, "*-Data.db"))) == num_sstables


def test_scrub_no_sstables(scylla_path, scrub_schema_file):
    subprocess_check_error([scylla_path, "sstable", "scrub", "--schema-file", scrub_schema_file, "--scrub-mode", "validate"], "error processing arguments: no sstables specified on the command line")


def test_scrub_missing_scrub_mode_cli_arg(scylla_path, scrub_workdir, scrub_schema_file, scrub_good_sstable):
    subprocess_check_error([scylla_path, "sstable", "scrub", "--schema-file", scrub_schema_file, scrub_good_sstable], "error processing arguments: missing mandatory command-line argument --scrub-mode")


def test_scrub_output_dir(scylla_path, scrub_workdir, scrub_schema_file, scrub_good_sstable):
    with tempfile.TemporaryDirectory(prefix="test_scrub_output_dir", dir=scrub_workdir) as tmp_dir:
        # Empty output directory is accepted.
        subprocess.check_call([scylla_path, "sstable", "scrub", "--schema-file", scrub_schema_file, "--scrub-mode", "abort", "--output-dir", tmp_dir, scrub_good_sstable])

    with tempfile.TemporaryDirectory(prefix="test_scrub_output_dir", dir=scrub_workdir) as tmp_dir:
        with open(os.path.join(tmp_dir, "dummy.txt"), "w") as f:
            f.write("dummy")
            f.flush()

        # Non-empty output directory is rejected.
        subprocess_check_error([scylla_path, "sstable", "scrub", "--schema-file", scrub_schema_file, "--scrub-mode", "abort", "--output-dir", tmp_dir, scrub_good_sstable], "error processing arguments: output-directory is not empty, pass --unsafe-accept-nonempty-output-dir if you are sure you want to write into this directory\n")

        # Validate doesn't write output sstables, so it doesn't care if output dir is non-empty.
        subprocess.check_call([scylla_path, "sstable", "scrub", "--schema-file", scrub_schema_file, "--scrub-mode", "validate", "--output-dir", tmp_dir, scrub_good_sstable])

        # Check that overriding with --unsafe-accept-nonempty-output-dir works.
        subprocess.check_call([scylla_path, "sstable", "scrub", "--schema-file", scrub_schema_file, "--scrub-mode", "abort", "--output-dir", tmp_dir, "--unsafe-accept-nonempty-output-dir", scrub_good_sstable])


def test_scrub_output_dir_sstable_clash(scylla_path, scrub_workdir, scrub_schema_file, scrub_good_sstable):
    with tempfile.TemporaryDirectory(prefix="test_scrub_output_dir_sstable_clash", dir=scrub_workdir) as tmp_dir:
        subprocess.check_call([scylla_path, "sstable", "scrub", "--schema-file", scrub_schema_file, "--scrub-mode", "abort", "--output-dir", tmp_dir, "--unsafe-accept-nonempty-output-dir", scrub_good_sstable])
        check_scrub_output_dir(tmp_dir, 1)
        subprocess_check_error([scylla_path, "sstable", "scrub", "--schema-file", scrub_schema_file, "--scrub-mode", "abort", "--output-dir", tmp_dir, "--unsafe-accept-nonempty-output-dir", scrub_good_sstable], "cannot create output sstable .*, file already exists")


def test_scrub_abort_mode(scylla_path, scrub_workdir, scrub_schema_file, scrub_good_sstable, scrub_bad_sstable):
    with tempfile.TemporaryDirectory(prefix="test_scrub_abort_mode", dir=scrub_workdir) as tmp_dir:
        subprocess.check_call([scylla_path, "sstable", "scrub", "--schema-file", scrub_schema_file, "--scrub-mode", "abort", "--output-dir", tmp_dir, scrub_good_sstable])
        check_scrub_output_dir(tmp_dir, 1)

    with tempfile.TemporaryDirectory(prefix="test_scrub_abort_mode", dir=scrub_workdir) as tmp_dir:
        subprocess_check_error([scylla_path, "sstable", "scrub", "--schema-file", scrub_schema_file, "--scrub-mode", "abort", "--output-dir", tmp_dir, scrub_bad_sstable], "compaction_aborted_exception \\(Compaction for ks/tbl was aborted due to: scrub compaction found invalid data\\)")
        check_scrub_output_dir(tmp_dir, 0)


def test_scrub_skip_mode(scylla_path, scrub_workdir, scrub_schema_file, scrub_good_sstable, scrub_bad_sstable):
    with tempfile.TemporaryDirectory(prefix="test_scrub_skip_mode", dir=scrub_workdir) as tmp_dir:
        subprocess.check_call([scylla_path, "sstable", "scrub", "--schema-file", scrub_schema_file, "--scrub-mode", "skip", "--output-dir", tmp_dir, scrub_good_sstable])
        check_scrub_output_dir(tmp_dir, 1)

    with tempfile.TemporaryDirectory(prefix="test_scrub_skip_mode", dir=scrub_workdir) as tmp_dir:
        subprocess.check_call([scylla_path, "sstable", "scrub", "--schema-file", scrub_schema_file, "--scrub-mode", "skip", "--output-dir", tmp_dir, scrub_bad_sstable])
        check_scrub_output_dir(tmp_dir, 1)


def test_scrub_segregate_mode(scylla_path, scrub_workdir, scrub_schema_file, scrub_good_sstable, scrub_bad_sstable):
    with tempfile.TemporaryDirectory(prefix="test_scrub_segregate_mode", dir=scrub_workdir) as tmp_dir:
        subprocess.check_call([scylla_path, "sstable", "scrub", "--schema-file", scrub_schema_file, "--scrub-mode", "segregate", "--output-dir", tmp_dir, scrub_good_sstable])
        check_scrub_output_dir(tmp_dir, 1)

    with tempfile.TemporaryDirectory(prefix="test_scrub_segregate_mode", dir=scrub_workdir) as tmp_dir:
        subprocess.check_call([scylla_path, "sstable", "scrub", "--schema-file", scrub_schema_file, "--scrub-mode", "segregate", "--output-dir", tmp_dir, scrub_bad_sstable])
        check_scrub_output_dir(tmp_dir, 2)


def test_scrub_validate_mode(scylla_path, scrub_workdir, scrub_schema_file, scrub_good_sstable, scrub_bad_sstable):
    with tempfile.TemporaryDirectory(prefix="test_scrub_validate_mode", dir=scrub_workdir) as tmp_dir:
        subprocess.check_call([scylla_path, "sstable", "scrub", "--schema-file", scrub_schema_file, "--scrub-mode", "validate", "--output-dir", tmp_dir, scrub_good_sstable])
        check_scrub_output_dir(tmp_dir, 0)

        subprocess.check_call([scylla_path, "sstable", "scrub", "--schema-file", scrub_schema_file, "--scrub-mode", "validate", "--output-dir", tmp_dir, scrub_bad_sstable])
        check_scrub_output_dir(tmp_dir, 0)

        # Check that validate did not move the bad sstable into qurantine
        assert os.path.exists(scrub_bad_sstable)


def _to_cql3_type(t: Type) -> str:
    # map from Python type to Cassandra type, only a small subset is supported
    py_to_cql3_type = {int: "Int32Type",
                       str: "UTF8Type",
                       bool: "BooleanType"}
    return py_to_cql3_type[t]


KeyType = Union[int, str, bool]


def _serialize_value(scylla_path: str, value: KeyType) -> str:
    return subprocess.check_output([scylla_path,
                                    "types", "serialize", "--full-compound",
                                    "-t", _to_cql3_type(type(value)),
                                    "--",
                                    str(value)]).strip().decode()


@functools.cache
def _shard_of_values(scylla_path: str, shards: int, *values: list[KeyType]) -> int:
    args = [scylla_path, "types", "shardof",
            "--full-compound",
            "--shards", str(shards)]
    for value in values:
        args.extend(['-t', _to_cql3_type(type(value))])
    serialized = ''.join(_serialize_value(scylla_path, v) for v in values)
    args.extend(['--', serialized])
    output = subprocess.check_output(args).strip().decode()
    # the output looks like:
    # (file_instance, 2021-03-27, c61a3321-0459-41c3-8e56-75255feb0196): token: -5043005771368701888, shard: 1
    shard = output.rsplit(':', 1)[-1]
    return int(shard)


def _generate_key_for_shard(scylla_path: str, shards: int, shard_id: int) -> Iterable[int]:
    # this only works with the table with a single integer pk. if we want to
    # be more general, we could use a randomized generator to enumerate all
    # possible pk combinations.
    for pk in itertools.count(start=0, step=1):
        if _shard_of_values(scylla_path, shards, pk) == shard_id:
            yield pk


def _simple_table_with_keys(cql, keyspace: str, keys: Iterable[int]) -> tuple[str, str]:
    table = util.unique_name()
    schema = (f"CREATE TABLE {keyspace}.{table} (pk int PRIMARY KEY, v int) "
              "WITH compaction = {'class': 'NullCompactionStrategy'}")
    cql.execute(schema)

    for pk in keys:
        cql.execute(f"INSERT INTO {keyspace}.{table} (pk, v) VALUES ({pk}, 0)")
    nodetool.flush(cql, f"{keyspace}.{table}")

    return table, schema


def test_scylla_sstable_shard_of_vnodes(cql, test_keyspace_vnodes, scylla_path, scylla_data_dir) -> None:
    # cql-pytest/run.py::run_scylla_cmd() passes "--smp 2" to scylla, so we
    # need to be consistent with it to get the correct sstable-shard mapping
    scylla_option_smp = 2
    shards = scylla_option_smp
    num_keys = 1
    for shard_id in range(shards):
        all_keys_for_shard = _generate_key_for_shard(scylla_path, shards, shard_id)
        keys = itertools.islice(all_keys_for_shard, num_keys)
        table_factory = functools.partial(_simple_table_with_keys, keys=keys)
        with scylla_sstable(table_factory, cql, test_keyspace_vnodes, scylla_data_dir) as (schema_file, sstables):
            out = subprocess.check_output([scylla_path,
                                           "sstable", "shard-of",
                                           "--vnodes",
                                           "--schema-file", schema_file,
                                           "--shards", str(shards)] +
                                          sstables)
            # all sstables contains the rows with the keys deliberately
            # created for specified shard
            sstables_json = json.loads(out)['sstables']
            expected_json = [shard_id]
            for actual_json in sstables_json.values():
                assert actual_json == expected_json


def test_scylla_sstable_shard_of_tablets(cql, test_keyspace_tablets, scylla_path, scylla_data_dir) -> None:
    # the token for 0 is mapped to shard 0, 142 is mapped to shard 1
    shard_to_key = {0: 0, 1: 142}
    for shard_id, key in shard_to_key.items():
        table_factory = functools.partial(_simple_table_with_keys, keys=[key])
        with scylla_sstable(table_factory, cql, test_keyspace_tablets, scylla_data_dir) as (schema_file, sstables):
            with nodetool.no_autocompaction_context(cql, "system.tablets"):
                nodetool.flush_keyspace(cql, "system")
                out = subprocess.check_output([scylla_path,
                                               "sstable", "shard-of",
                                               "--tablets",
                                               "--schema-file", schema_file] +
                                              sstables)
                sstables_json = json.loads(out)['sstables']
                for replica_sets in sstables_json.values():
                    for replica_set in replica_sets:
                        actual_shard = replica_set['shard']
                        assert actual_shard == shard_id


def test_scylla_sstable_no_args(scylla_path):
    res = subprocess.run([scylla_path, "sstable"], capture_output=True, text=True)

    assert res.stdout == ""
    assert res.stderr == """\
Usage: scylla sstable OPERATION [OPTIONS] ...
Try `scylla sstable --help` for more information.
"""


def test_scylla_sstable_bad_scylla_yaml(cql, test_keyspace, scylla_path, scylla_data_dir):
    """ scylla-sstable should not choke on deprecated/unrecognized/etc options in scylla.yaml
    It should just log a debug-level log and proceed with reading it.
    This test checks that the config is successfully read, even if there are errors.
    Reproduces: https://github.com/scylladb/scylladb/issues/16538
    """
    with scylla_sstable(simple_clustering_table, cql, test_keyspace, scylla_data_dir) as (schema_file, sstables):
        with tempfile.NamedTemporaryFile("w+t") as scylla_yaml:
            scylla_yaml.write("foo: bar")
            scylla_yaml.flush()
            res = subprocess.run([scylla_path,
                                  "sstable", "dump-data",
                                  "--scylla-yaml", scylla_yaml.name,
                                  "--schema-file", schema_file,
                                  "--logger-log-level", "scylla-sstable=debug"]
                                 + sstables,
                                 text=True, stderr=subprocess.PIPE)
            assert res.returncode == 0
            print(res.stderr)  # when the test fails, it helps to see what the actual output is
            stderr_lines = res.stderr.split('\n')
            for expected_msg in (
                    "error processing configuration item: Unknown option : foo",
                    "Successfully read scylla.yaml from"):
                assert any(map(lambda stderr_line: expected_msg in stderr_line, stderr_lines))


def test_scylla_sstable_format_version(cql, test_keyspace, scylla_data_dir):
    # Reproduces https://github.com/scylladb/scylladb/issues/16551
    #
    # an sstable component filename looks like:
    #  me-3g8w_00qf_4pbog2i7h2c7am0uoe-big-Data.db
    sstable_re = re.compile(r"""(?P<version>la|m[cde])- # the sstable version
                                (?P<id>[^-]+)-          # sstable identifier
                                (?P<format>\w+)-        # format: 'big'
                                (?P<component>.*)       # component: e.g., 'Data'""", re.X)
    with scylla_sstable(simple_clustering_table, cql, test_keyspace, scylla_data_dir) as (_, sstables):
        for fn in sstables:
            stem = pathlib.Path(fn).stem
            matched = sstable_re.match(stem)
            assert matched is not None, f"unmatched sstable component path: {fn}"
            sstable_version = matched["version"]
            # "me" is specified by sstables_manager::_format, so new sstables
            # created by a scylla instance are always persisted with "me" sstable
            # format, unless the "sstable_format" setting persisted in
            # "system.scylla_local" system tables has a different setting. but in a
            # new installation of scylla, this setting does not exist.
            assert sstable_version == "me", f"unexpected sstable format: {sstable_version}"
