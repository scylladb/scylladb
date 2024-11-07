# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for sstable validation.
#
# Namely, detecting discrepancies between index and data files.
# These tests produce pairs of sstables, that differ only slightly, mixes the
# index/data from the two of them, then runs `sstable validate` expecting it to
# find the discrepancies.
#############################################################################


import glob
import json
import os
import pytest
import shutil
import subprocess
import tempfile


@pytest.fixture(scope="module")
def schema1_file():
    """Create a schema.cql for the schema1"""
    with tempfile.NamedTemporaryFile("w+t") as f:
        f.write("CREATE TABLE ks.tbl (pk int, ck int, v text, PRIMARY KEY (pk, ck))")
        f.flush()
        yield f.name


@pytest.fixture(scope="module")
def schema2_file():
    """Create a schema.cql for the schema2"""
    with tempfile.NamedTemporaryFile("w+t") as f:
        f.write("CREATE TABLE ks.tbl (pk int, ck int, v text, s text STATIC, PRIMARY KEY (pk, ck))")
        f.flush()
        yield f.name


@pytest.fixture(scope="module")
def large_rows():
    """Create a set of large rows"""
    rows = []
    v_1k = 'v' * 1024
    for ck in range(128):
        ck_raw = "0004{:08x}".format(ck)
        rows.append({ "type": "clustering-row", "key": { "raw": ck_raw }, "columns": { "v": { "is_live": True, "type": "regular", "timestamp": 1680263186359882, "value": v_1k } } })
    return rows


def find_component(generation, component_type, sst_dir):
    comps = glob.glob(os.path.join(sst_dir, f"*-{str(generation)}-big-{component_type}.db"))
    assert len(comps) == 1
    return comps[0]


@pytest.fixture(scope="module")
def sstable_cache(scylla_path):
    """Content-addressable sstable cache"""
    class cache:
        def __init__(self, scylla_path, workdir):
            self._scylla_path = scylla_path
            self._in_json = os.path.join(workdir, "input.json")
            self._store_dir = os.path.join(workdir, "sst_store_dir")
            self._cache = {}
            self._next_generation = 0

            os.mkdir(self._store_dir)

        @property
        def dir(self):
            return self._store_dir

        def get_generation(self, sst, schema_file):
            json_str = json.dumps(sst)
            generation = self._cache.get(json_str)
            if not generation is None:
                return generation
            with open(self._in_json, "w") as f:
                f.write(json_str)
            generation = self._next_generation
            self._cache[json_str] = generation
            self._next_generation = self._next_generation + 1
            subprocess.check_call([self._scylla_path, "sstable", "write", "--schema-file", schema_file, "--output-dir", self._store_dir, "--generation", str(generation), "--input-file", self._in_json])
            return generation

        def copy_sstable_to(self, generation, target_dir):
            for f in glob.glob(os.path.join(self._store_dir, f"*-{str(generation)}-big-*.*")):
                shutil.copy(f, target_dir)

    with tempfile.TemporaryDirectory() as workdir:
        yield cache(scylla_path, workdir)


def validate_mixed_sstable_pair(ssta, sstb, scylla_path, sst_cache, sst_work_dir, schema_file, error_message):
    """Validate an sstable, created by mixing the index and data from two different sstables.

    Check that the validation has the expected result (`error_message`).
    """
    generation_a = sst_cache.get_generation(ssta, schema_file)
    generation_b = sst_cache.get_generation(sstb, schema_file)
    shutil.rmtree(sst_work_dir)
    os.mkdir(sst_work_dir)
    sst_cache.copy_sstable_to(generation_a, sst_work_dir)
    shutil.copyfile(find_component(generation_b, "Index", sst_cache.dir), find_component(generation_a, "Index", sst_work_dir))

    res = subprocess.run([scylla_path, "sstable", "validate", "--schema-file", schema_file, find_component(generation_a, "Data", sst_work_dir)],
                         check=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    out = res.stdout.decode('utf-8')
    err = res.stderr.decode('utf-8')
    sstables = json.loads(out)['sstables']
    valid = next(iter(sstables.values()))['valid']
    if error_message:
        assert not valid
        assert error_message in err
    else:
        print(err)
        assert valid


def make_partition(pk, ck, v):
    pks = [ "000400000001", "000400000000", "000400000002", "000400000003" ]
    r = { "type": "clustering-row", "key": { "raw": "0004{:08x}".format(ck) }, "columns": { "v": { "is_live": True, "type": "regular", "timestamp": 1680263186359882, "value": v } } }
    return { "key": { "raw": pks[pk] }, "clustering_elements": [ r ] }


def make_large_partition(pk, rows, with_static_row = False):
    pks = [ "000400000001", "000400000000", "000400000002", "000400000003" ]
    if with_static_row:
        return {
             "key": { "raw": pks[pk] },
             "static_row": { "s": { "is_live": True, "type": "regular", "timestamp": 1680263186359882, "value": "s" * 128 } },
             "clustering_elements": rows
             }
    else:
        return { "key": { "raw": pks[pk] }, "clustering_elements": rows }


def test_scylla_sstable_validate_ok1(cql, scylla_path, temp_workdir, schema1_file, sstable_cache):
    validate_mixed_sstable_pair(
            [make_partition(0, 0, 'v')],
            [make_partition(0, 0, 'v')],
            scylla_path,
            sstable_cache,
            temp_workdir,
            schema_file = schema1_file,
            error_message = "")


def test_scylla_sstable_validate_ok2(cql, scylla_path, temp_workdir, schema1_file, sstable_cache):
    validate_mixed_sstable_pair(
            [make_partition(0, 0, 'v'), make_partition(1, 0, 'v'), make_partition(2, 0, 'v')],
            [make_partition(0, 0, 'v'), make_partition(1, 0, 'v'), make_partition(2, 0, 'v')],
            scylla_path,
            sstable_cache,
            temp_workdir,
            schema_file = schema1_file,
            error_message = "")


def test_scylla_sstable_validate_mismatching_partition(cql, scylla_path, temp_workdir, schema1_file, sstable_cache):
    validate_mixed_sstable_pair(
            [make_partition(0, 0, 'v')],
            [make_partition(1, 0, 'v')],
            scylla_path,
            sstable_cache,
            temp_workdir,
            schema_file = schema1_file,
            error_message = "mismatching index/data: partition mismatch")


def test_scylla_sstable_validate_mismatching_position(cql, scylla_path, temp_workdir, schema1_file, sstable_cache):
    validate_mixed_sstable_pair(
            [make_partition(0, 0, 'v'), make_partition(2, 0, 'v')],
            [make_partition(0, 0, 'vv'), make_partition(2, 0, 'v')],
            scylla_path,
            sstable_cache,
            temp_workdir,
            schema_file = schema1_file,
            error_message = "mismatching index/data: position mismatch")


def test_scylla_sstable_validate_index_ends_before_data(cql, scylla_path, temp_workdir, schema1_file, sstable_cache):
    validate_mixed_sstable_pair(
            [make_partition(0, 0, 'v'), make_partition(2, 0, 'v')],
            [make_partition(0, 0, 'v')],
            scylla_path,
            sstable_cache,
            temp_workdir,
            schema_file = schema1_file,
            error_message = "mismatching index/data: index is at EOF, but data file has more data")


@pytest.mark.xfail(reason="index's EOF definition depends on data file size")
def test_scylla_sstable_validate_index_ends_before_data1(cql, scylla_path, temp_workdir, schema1_file, sstable_cache):
    validate_mixed_sstable_pair(
            [make_partition(0, 0, 'v')],
            [make_partition(0, 0, 'vvvvvvvvvvvvvvvvvvvvvv'), make_partition(2, 0, 'v')],
            scylla_path,
            sstable_cache,
            temp_workdir,
            schema_file = schema1_file,
            error_message = "mismatching index/data: data is at EOF, but index has more data")


@pytest.mark.xfail(reason="index's EOF definition depends on data file size")
def test_scylla_sstable_validate_index_ends_before_data2(cql, scylla_path, temp_workdir, schema1_file, sstable_cache):
    validate_mixed_sstable_pair(
            [make_partition(0, 0, 'v')],
            [make_partition(0, 0, 'v'), make_partition(2, 0, 'v')],
            scylla_path,
            sstable_cache,
            temp_workdir,
            schema_file = schema1_file,
            error_message = "mismatching index/data: data is at EOF, but index has more data")

def test_scylla_sstable_validate_large_rows_ok1(cql, scylla_path, temp_workdir, schema1_file, sstable_cache, large_rows):
    validate_mixed_sstable_pair(
            [make_large_partition(0, large_rows)],
            [make_large_partition(0, large_rows)],
            scylla_path,
            sstable_cache,
            temp_workdir,
            schema_file = schema1_file,
            error_message = "")

def test_scylla_sstable_validate_large_rows_ok2(cql, scylla_path, temp_workdir, schema2_file, sstable_cache, large_rows):
    validate_mixed_sstable_pair(
            [make_large_partition(0, large_rows)],
            [make_large_partition(0, large_rows)],
            scylla_path,
            sstable_cache,
            temp_workdir,
            schema_file = schema2_file,
            error_message = "")

def test_scylla_sstable_validate_large_rows_ok3(cql, scylla_path, temp_workdir, schema2_file, sstable_cache, large_rows):
    validate_mixed_sstable_pair(
            [make_large_partition(0, large_rows, True)],
            [make_large_partition(0, large_rows, True)],
            scylla_path,
            sstable_cache,
            temp_workdir,
            schema_file = schema2_file,
            error_message = "")


def test_scylla_sstable_validate_large_rows_mismatching_position1(cql, scylla_path, temp_workdir, schema1_file, sstable_cache, large_rows):
    row_0x = { "type": "clustering-row", "key": { "raw": "000400000000" }, "columns": { "v": { "is_live": True, "type": "regular", "timestamp": 1680263186359882, "value": "v" } } }
    validate_mixed_sstable_pair(
            [make_large_partition(0, large_rows)],
            [make_large_partition(0, [row_0x] + large_rows[1:])],
            scylla_path,
            sstable_cache,
            temp_workdir,
            schema_file = schema1_file,
            error_message = "mismatching index/data: position mismatch: promoted index:")


def test_scylla_sstable_validate_large_rows_mismatching_position2(cql, scylla_path, temp_workdir, schema2_file, sstable_cache, large_rows):
    validate_mixed_sstable_pair(
            [make_large_partition(0, large_rows, True)],
            [make_large_partition(0, large_rows)],
            scylla_path,
            sstable_cache,
            temp_workdir,
            schema_file = schema2_file,
            error_message = "mismatching index/data: position mismatch: promoted index:")


def test_scylla_sstable_validate_large_rows_mismatching_position3(cql, scylla_path, temp_workdir, schema2_file, sstable_cache, large_rows):
    validate_mixed_sstable_pair(
            [make_large_partition(0, large_rows)],
            [make_large_partition(0, large_rows, True)],
            scylla_path,
            sstable_cache,
            temp_workdir,
            schema_file = schema2_file,
            error_message = "mismatching index/data: position mismatch: promoted index:")

def test_scylla_sstable_validate_large_rows_mismathing_row(cql, scylla_path, temp_workdir, schema1_file, sstable_cache, large_rows):
    validate_mixed_sstable_pair(
            [make_large_partition(0, [large_rows[1]] + large_rows[4:])],
            [make_large_partition(0, [large_rows[2]] + large_rows[4:])],
            scylla_path,
            sstable_cache,
            temp_workdir,
            schema_file = schema1_file,
            error_message = "mismatching index/data: clustering element")

def test_scylla_sstable_validate_large_rows_end_of_pi_not_end_of_rows(cql, scylla_path, temp_workdir, schema1_file, sstable_cache, large_rows):
    row_0x = { "type": "clustering-row", "key": { "raw": "000400000000" }, "columns": { "v": { "is_live": True, "type": "regular", "timestamp": 1680263186359882, "value": "v" } } }
    validate_mixed_sstable_pair(
            [make_large_partition(0, [row_0x]), make_partition(2, 0, 'v')],
            [make_large_partition(0, large_rows), make_partition(2, 0, 'v')],
            scylla_path,
            sstable_cache,
            temp_workdir,
            schema_file = schema1_file,
            error_message = "mismatching index/data: promoted index has more blocks, but it is end of partition")

def test_scylla_sstable_validate_large_rows_end_of_rows_not_end_of_pi(cql, scylla_path, temp_workdir, schema1_file, sstable_cache, large_rows):
    validate_mixed_sstable_pair(
            [make_large_partition(0, large_rows), make_partition(2, 0, 'v')],
            [make_large_partition(0, large_rows[:96]), make_partition(2, 0, 'v')],
            scylla_path,
            sstable_cache,
            temp_workdir,
            schema_file = schema1_file,
            error_message = "mismatching index/data: promoted index has no more blocks, but partition")
