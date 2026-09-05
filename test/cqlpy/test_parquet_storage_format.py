# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for the `storage_format` table property, which selects the encoding of
# an SSTable's Data component: 'sstable' (the native row format, the default),
# 'parquet' (columnar, every SSTable) or 'hybrid'. See
# docs/dev/parquet-storage-format.md.
#
# Everything here is driven through CQL against a running node, which is the
# layer a user actually touches. The C++ unit tests build SSTables directly and
# so never exercise DDL parsing, schema-table persistence, DESCRIBE, or the
# flush path's format choice.
#
# TWO THINGS THAT MAKE TESTS IN THIS AREA SILENTLY VACUOUS. Both have already
# cost real time on this project, so they are worth stating before the tests:
#
# 1. A CQL `INSERT` that *binds* NULL writes a cell **tombstone**. Omitting the
#    column, or binding `UNSET_VALUE`, writes **nothing at all**. So "insert a
#    row with a missing value" and "delete a cell" are the same operation
#    unless you are deliberate about which one you wrote. Absent, live and dead
#    are three distinct on-disk states, and at CQL level absent and dead are
#    only distinguishable by whether they *shadow* an older value -- a plain
#    SELECT returns None for both. `test_absent_null_and_unset_are_distinct`
#    below is built entirely around that distinction.
#
# 2. Tombstones are purged by any compaction on this setup. A table created
#    without an explicit `tombstone_gc` gets mode `repair`, and at RF=1 (which
#    is what the cqlpy keyspace uses) that short-circuits `gc_before` to *now*,
#    so `gc_grace_seconds` is never consulted and every dead cell is dropped.
#    A "delete then inspect" test then finds nothing and looks green. Every
#    table below that cares about a tombstone therefore sets
#    `tombstone_gc = {'mode': 'timeout'}`; see TOMBSTONE_GC.
#
# A third, subtler one specific to *format* tests: a read served from the
# memtable or the row cache never touches the SSTable, so it proves nothing
# about Parquet. Every test here flushes and then reads with `BYPASS CACHE`,
# and asserts the flushed files are actually `pq`. That pairing is what makes
# these format tests rather than CQL tests -- without the on-disk assertion
# they would pass just as well if the flush had written the native format.
#############################################################################

import glob
import os
import time

import pytest
from cassandra.protocol import ConfigurationException
from cassandra.query import UNSET_VALUE

from . import nodetool
from .util import new_test_table, unique_name

# All tests in this file are Scylla-only: `storage_format` is not a Cassandra
# table property.
@pytest.fixture(scope="module", autouse=True)
def parquet_scylla_only(scylla_only):
    pass


# See trap 2 in the file header. Without this, a dead cell written by any test
# below is purgeable the moment it is written, and an assertion that the
# deletion survived a flush would pass for the wrong reason.
TOMBSTONE_GC = "tombstone_gc = {'mode': 'timeout'}"

TWCS = ("compaction = {'class': "
        "'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'}")
ICS = "compaction = {'class': 'IncrementalCompactionStrategy'}"


def _table_dirs(data_dir, keyspace, table_name):
    """The on-disk directories for one table. Normally exactly one; a table
    name recycled by new_test_table() within a run could in principle leave a
    second, and silently globbing both would make the format assertion below
    depend on a stale directory, so callers assert the count."""
    return glob.glob(os.path.join(data_dir, keyspace, f"{table_name}-*"))


def sstable_components(data_dir, table):
    """Every SSTable component file belonging to `table` ("keyspace.name"),
    as bare basenames. Excludes anything under snapshots/ or a still-being-
    written SSTable (one whose TOC is a .tmp)."""
    keyspace, table_name = table.split('.')
    dirs = _table_dirs(data_dir, keyspace, table_name)
    assert len(dirs) == 1, \
        f"expected exactly one data directory for {table}, found {dirs}"
    names = [os.path.basename(p)
             for p in glob.glob(os.path.join(dirs[0], "*.*"))]
    # A component whose TOC.txt.tmp still exists belongs to an SSTable that is
    # mid-write; including it would make the assertions racy.
    def written(name):
        prefix = "-".join(name.split("-")[:-1])
        return not os.path.exists(os.path.join(dirs[0], prefix + "-TOC.txt.tmp"))
    return [n for n in names if written(n)]


def sstable_versions(data_dir, table):
    """The set of SSTable version prefixes present for `table`, read off the
    Data component filenames. SSTable filenames are
    `<version>-<generation>-<format>-<component>`, e.g. `me-3-big-Data.db` for
    the native format and `pq-3-big-Data.db` for Parquet."""
    data = [n for n in sstable_components(data_dir, table)
            if n.endswith("-Data.db")]
    assert data, f"no Data.db files found for {table}; did the flush happen?"
    return {n.split("-", 1)[0] for n in data}


def assert_parquet_on_disk(data_dir, table):
    """The load-bearing format assertion: every flushed SSTable of this table
    is Parquet. Paired with a value assertion, this is what distinguishes a
    format test from a plain CQL test."""
    versions = sstable_versions(data_dir, table)
    assert versions == {"pq"}, \
        f"expected only Parquet SSTables for {table}, got versions {versions}"


def assert_native_on_disk(data_dir, table):
    versions = sstable_versions(data_dir, table)
    assert "pq" not in versions, \
        f"expected no Parquet SSTables for {table}, got versions {versions}"


def scylla_tables_storage_format(cql, table):
    """The persisted value of the property, or None if the table never set it.
    Note the asymmetry documented in test_storage_format_persisted_vs_absent:
    a table that never mentioned the property stores no cell at all, while one
    that explicitly set 'sstable' stores 'sstable'."""
    keyspace, table_name = table.split('.')
    return cql.execute(
        "SELECT storage_format FROM system_schema.scylla_tables "
        f"WHERE keyspace_name = '{keyspace}' AND table_name = '{table_name}'"
    ).one().storage_format


def describe(cql, table):
    return cql.execute(f"DESCRIBE TABLE {table}").one().create_statement


###########################################################################
# DDL: the property round-trips through CREATE, the schema tables, DESCRIBE
# and ALTER.
###########################################################################

# The property as written by CREATE TABLE must be readable back from
# system_schema.scylla_tables, for each of the three legal values.
@pytest.mark.parametrize("fmt", ["sstable", "parquet", "hybrid"])
def test_storage_format_create_round_trips(cql, test_keyspace, fmt):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        f" WITH storage_format = '{fmt}'") as table:
        assert scylla_tables_storage_format(cql, table) == fmt


# DESCRIBE shows the property whenever it was explicitly set -- INCLUDING at the
# 'sstable' default. Changed 2026-08-23: it used to be suppressed at the default,
# which made storage_format the only property in the CREATE statement whose
# absence was ambiguous. Every other property prints at its default
# (bloom_filter_fp_chance = 0.01, default_time_to_live = 0, crc_check_chance = 1),
# so a missing storage_format could mean "explicitly sstable" or "server does not
# know this property" -- and for a feature whose entire subject is which storage
# format a table uses, that is the last thing that should have to be guessed.
def test_storage_format_describe_shows_every_explicit_value(cql, test_keyspace):
    for fmt in ["parquet", "hybrid", "sstable"]:
        with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                            f" WITH storage_format = '{fmt}'") as table:
            assert f"storage_format = '{fmt}'" in describe(cql, table)


# A table that never mentioned the property is distinguishable from one that
# explicitly asked for the default: the former stores no cell (NULL), the latter
# stores 'sstable'. As of 2026-08-23 DESCRIBE reflects that distinction too --
# has_storage_format() is storage_format.has_value(), so the never-set case still
# prints nothing while an explicit 'sstable' prints the line. Previously DESCRIBE
# hid both and the distinction was only visible in system_schema.scylla_tables.
def test_storage_format_persisted_vs_absent(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        assert scylla_tables_storage_format(cql, table) is None
        # And the never-set case must stay absent from DESCRIBE, or the two states
        # become indistinguishable again in the opposite direction.
        assert "storage_format" not in describe(cql, table)

    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        " WITH storage_format = 'sstable'") as table:
        assert scylla_tables_storage_format(cql, table) == 'sstable'
        # Explicitly-chosen default now prints, which is the whole point of the
        # 2026-08-23 change: "I asked for sstable" is visible, not inferred.
        assert "storage_format = 'sstable'" in describe(cql, table)


# An unrecognised value must be rejected at DDL time with a message naming the
# legal values -- not silently ignored, and not accepted and then discovered at
# flush time. The error is a ConfigurationException (CONFIG_ERROR), not an
# InvalidRequest; that distinction is part of the contract a driver sees, so it
# is pinned here.
@pytest.mark.parametrize("bad", ["", "PARQUET", "Parquet", "parq", "sstables",
                                 "columnar", "hybrid ", "0"])
def test_storage_format_invalid_value_rejected(cql, test_keyspace, bad):
    with pytest.raises(ConfigurationException,
                       match=f"Invalid value '{bad}' for 'storage_format'; "
                             "expected one of: sstable, parquet, hybrid"):
        with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                            f" WITH storage_format = '{bad}'"):
            pass


# The same rejection must apply on the ALTER path, which is a separate
# statement class even though it shares cf_prop_defs. A table left with a
# bogus format by ALTER would be unopenable.
def test_storage_format_invalid_value_rejected_by_alter(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        with pytest.raises(ConfigurationException,
                           match="Invalid value 'nonesuch' for 'storage_format'; "
                                 "expected one of: sstable, parquet, hybrid"):
            cql.execute(f"ALTER TABLE {table} WITH storage_format = 'nonesuch'")
        # The failed ALTER must not have changed anything.
        assert scylla_tables_storage_format(cql, table) is None


# The property survives ALTER in every direction, including back to the
# default. Converting back is the direction most likely to be forgotten.
def test_storage_format_survives_alter(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        assert scylla_tables_storage_format(cql, table) is None

        cql.execute(f"ALTER TABLE {table} WITH storage_format = 'parquet'")
        assert scylla_tables_storage_format(cql, table) == 'parquet'
        assert "storage_format = 'parquet'" in describe(cql, table)

        cql.execute(f"ALTER TABLE {table} WITH storage_format = 'hybrid'")
        assert scylla_tables_storage_format(cql, table) == 'hybrid'
        assert "storage_format = 'hybrid'" in describe(cql, table)

        # Back to the default. The cell is now explicitly 'sstable' rather than
        # NULL -- the ALTER is recorded -- and since 2026-08-23 DESCRIBE says so.
        # An ALTER back to the default used to become invisible, so a table that
        # had been parquet and was reverted looked identical to one that was never
        # anything else.
        cql.execute(f"ALTER TABLE {table} WITH storage_format = 'sstable'")
        assert scylla_tables_storage_format(cql, table) == 'sstable'
        assert "storage_format = 'sstable'" in describe(cql, table)


# DESCRIBE has to be reproducible: the statement it prints must recreate a
# table with the same storage_format. This catches a DESCRIBE that prints the
# property in a place the parser will not accept it, which asserting on a
# substring alone would not.
@pytest.mark.parametrize("fmt", ["parquet", "hybrid"])
def test_storage_format_describe_is_reproducible(cql, test_keyspace, fmt):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        f" WITH storage_format = '{fmt}'") as table:
        stmt = describe(cql, table)
        copy = f"{test_keyspace}.{unique_name()}"
        cql.execute(stmt.replace(table, copy))
        try:
            assert scylla_tables_storage_format(cql, copy) == fmt
        finally:
            cql.execute(f"DROP TABLE {copy}")


###########################################################################
# The flush path's format choice.
###########################################################################

# The point of the property: a 'parquet' table's flushed SSTables are Parquet.
# Also pins the component set, because `pq` diverges from the native m-family
# in one way that is easy to regress -- it always writes CRC.db and never
# CompressionInfo.db, whatever the table's compression setting says.
def test_parquet_flush_writes_pq_components(cql, test_keyspace, scylla_data_dir):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        " WITH storage_format = 'parquet'") as table:
        for i in range(10):
            cql.execute(f"INSERT INTO {table} (pk, v) VALUES ({i}, {i * 10})")
        nodetool.flush(cql, table)

        components = sstable_components(scylla_data_dir, table)
        assert_parquet_on_disk(scylla_data_dir, table)

        # Every component of a pq SSTable carries the pq- version prefix; the
        # format field is still "big".
        for name in components:
            assert name.startswith("pq-"), \
                f"unexpected non-Parquet component {name}"
            assert name.split("-")[2] == "big", \
                f"unexpected format field in {name}"

        suffixes = {n.split("-", 3)[3] for n in components}
        assert "Data.db" in suffixes
        assert "TOC.txt" in suffixes
        assert "CRC.db" in suffixes, \
            "pq always writes CRC.db, whatever the compression setting"
        assert "CompressionInfo.db" not in suffixes, \
            "pq never writes CompressionInfo.db"


# A default table must not accidentally start writing Parquet. This is the
# control for every assertion above: without it, a build in which *everything*
# flushed as pq would pass the whole file.
def test_default_flush_writes_native(cql, test_keyspace, scylla_data_dir):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 1)")
        nodetool.flush(cql, table)
        assert_native_on_disk(scylla_data_dir, table)


# 'hybrid' means "native in the upper tiers, columnar in the bottom tier" --
# except under TWCS, where every SSTable is bottom-tier by construction and so
# hybrid resolves to Parquet unconditionally, including for flushes. Under any
# other strategy a *flush* is native, and which format a later compaction
# picks is a data-dependent policy decision (it weighs a measured size gain),
# so this test deliberately asserts only the flush.
def test_hybrid_flush_format_depends_on_strategy(cql, test_keyspace,
                                                 scylla_data_dir):
    with new_test_table(cql, test_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)",
                        f" WITH storage_format = 'hybrid' AND {TWCS}") as table:
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (1, 1, 1)")
        nodetool.flush(cql, table)
        assert_parquet_on_disk(scylla_data_dir, table)

    with new_test_table(cql, test_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)",
                        f" WITH storage_format = 'hybrid' AND {ICS}") as table:
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (1, 1, 1)")
        nodetool.flush(cql, table)
        assert_native_on_disk(scylla_data_dir, table)


# ALTERing an existing table to 'parquet' must change what subsequent flushes
# write. The DDL round-trip tests above would all pass if the property were
# stored and then never consulted by the write path.
def test_alter_to_parquet_changes_flush_format(cql, test_keyspace,
                                               scylla_data_dir):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 1)")
        nodetool.flush(cql, table)
        assert_native_on_disk(scylla_data_dir, table)

        cql.execute(f"ALTER TABLE {table} WITH storage_format = 'parquet'")
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (2, 2)")
        nodetool.flush(cql, table)
        # Now format-mixed: the old native SSTable is untouched, the new one is
        # Parquet, and a read has to merge across both.
        assert sstable_versions(scylla_data_dir, table) >= {"pq"}
        rows = {(r.pk, r.v) for r in
                cql.execute(f"SELECT pk, v FROM {table} BYPASS CACHE")}
        assert rows == {(1, 1), (2, 2)}


###########################################################################
# DML correctness on a Parquet table, read back through the Parquet file.
#
# Each test writes, flushes, asserts the SSTables are pq, and reads with
# BYPASS CACHE so the answer comes from the file rather than the row cache.
###########################################################################

def parquet_table(cql, keyspace, schema, extra=""):
    props = f" WITH storage_format = 'parquet' AND {TOMBSTONE_GC}"
    if extra:
        props += f" AND {extra}"
    return new_test_table(cql, keyspace, schema, props)


def flushed_read(cql, data_dir, table, query):
    """Flush, assert the file is Parquet, and return the rows read from it."""
    nodetool.flush(cql, table)
    assert_parquet_on_disk(data_dir, table)
    return list(cql.execute(f"{query} BYPASS CACHE"))


# insert, overwrite, cell update and delete, each observed through the file
# rather than the memtable.
def test_parquet_insert_update_delete(cql, test_keyspace, scylla_data_dir):
    with parquet_table(cql, test_keyspace,
                       "pk int, ck int, v1 int, v2 text, PRIMARY KEY (pk, ck)") as table:
        cql.execute(f"INSERT INTO {table} (pk, ck, v1, v2) VALUES (1, 1, 10, 'a')")
        cql.execute(f"INSERT INTO {table} (pk, ck, v1, v2) VALUES (1, 2, 20, 'b')")
        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT pk, ck, v1, v2 FROM {table}")
        assert {(r.ck, r.v1, r.v2) for r in rows} == {(1, 10, 'a'), (2, 20, 'b')}

        # UPDATE one cell; the sibling column must be untouched.
        cql.execute(f"UPDATE {table} SET v1 = 99 WHERE pk = 1 AND ck = 1")
        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT pk, ck, v1, v2 FROM {table} WHERE pk = 1 AND ck = 1")
        assert (rows[0].v1, rows[0].v2) == (99, 'a')

        # Delete a single cell. The row survives, because ck=1 still has v2.
        cql.execute(f"DELETE v1 FROM {table} WHERE pk = 1 AND ck = 1")
        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT pk, ck, v1, v2 FROM {table} WHERE pk = 1 AND ck = 1")
        assert len(rows) == 1
        assert rows[0].v1 is None and rows[0].v2 == 'a'

        # Delete a whole row.
        cql.execute(f"DELETE FROM {table} WHERE pk = 1 AND ck = 2")
        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT pk, ck FROM {table} WHERE pk = 1")
        assert [r.ck for r in rows] == [1]


# Trap 1 from the file header, as a test. Absent, dead and unset are three
# different writes, and the only way to tell them apart through CQL is whether
# they shadow an older live value -- a plain SELECT returns None for both
# absent-over-nothing and dead.
#
# The flush in the middle is what gives this test its teeth, and it is not
# optional. If both writes land in the same memtable they are merged *before*
# anything reaches an SSTable, so the file holds a single already-resolved cell
# and a reader that wrongly treated "dead" as "absent" would still answer None
# -- there would be nothing left for the tombstone to shadow. Only with the
# live value in one SSTable and the tombstone in another does the read have to
# merge them, and only then does collapsing dead into absent resurrect the old
# value. (That collapse is a real, previously-shipped class of bug; see the
# "Three states, and they have to stay three" comment in
# sstables/parquet/schema_mapping.cc.)
def test_absent_null_and_unset_are_distinct(cql, test_keyspace, scylla_data_dir):
    with parquet_table(cql, test_keyspace,
                       "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table:
        for pk in (1, 2, 3):
            cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES ({pk}, 1, 5) "
                        "USING TIMESTAMP 1000")
        nodetool.flush(cql, table)

        # pk=1: omit v entirely. Writes a row marker and no v cell, so the
        # older live value is *not* shadowed.
        cql.execute(f"INSERT INTO {table} (pk, ck) VALUES (1, 1) "
                    "USING TIMESTAMP 2000")

        # pk=2: bind NULL. Writes a cell tombstone, which *does* shadow it.
        stmt = cql.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?) "
                           "USING TIMESTAMP 2000")
        cql.execute(stmt, (2, 1, None))

        # pk=3: bind UNSET_VALUE. Writes nothing for v, like the omission.
        cql.execute(stmt, (3, 1, UNSET_VALUE))

        nodetool.flush(cql, table)
        assert_parquet_on_disk(scylla_data_dir, table)

        # Both SSTables must still be there: if they had been compacted into
        # one, the merge this test depends on would already have happened
        # (and, at RF=1 with the default tombstone_gc, the tombstone would
        # have been purged outright -- see trap 2). TOMBSTONE_GC and the
        # absence of any compact call are what keep them separate.
        data_files = [n for n in sstable_components(scylla_data_dir, table)
                      if n.endswith("-Data.db")]
        assert len(data_files) >= 2, \
            ("the live value and the tombstone must be in different SSTables "
             f"for this test to mean anything, found {data_files}")

        rows = list(cql.execute(f"SELECT pk, v FROM {table} BYPASS CACHE"))
        got = {r.pk: r.v for r in rows}
        assert got[1] == 5, "omitting a column must not overwrite the old value"
        assert got[2] is None, \
            "binding NULL must write a cell tombstone that shadows the older value"
        assert got[3] == 5, "UNSET_VALUE must not overwrite the old value"


# A range tombstone covers a contiguous span of clustering keys and is a
# distinct on-disk object from a row or cell tombstone.
def test_parquet_range_tombstone(cql, test_keyspace, scylla_data_dir):
    with parquet_table(cql, test_keyspace,
                       "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table:
        for ck in range(10):
            cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (1, {ck}, {ck})")
        cql.execute(f"DELETE FROM {table} WHERE pk = 1 AND ck >= 3 AND ck < 7")

        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT ck FROM {table} WHERE pk = 1")
        assert [r.ck for r in rows] == [0, 1, 2, 7, 8, 9]

        # An open-ended range on the other side, to exercise a bound rather
        # than a closed interval.
        cql.execute(f"DELETE FROM {table} WHERE pk = 1 AND ck >= 8")
        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT ck FROM {table} WHERE pk = 1")
        assert [r.ck for r in rows] == [0, 1, 2, 7]


def test_parquet_partition_delete(cql, test_keyspace, scylla_data_dir):
    with parquet_table(cql, test_keyspace,
                       "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table:
        for pk in (1, 2):
            for ck in range(3):
                cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES ({pk}, {ck}, {ck})")
        cql.execute(f"DELETE FROM {table} WHERE pk = 1")

        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT pk, ck FROM {table}")
        assert {(r.pk, r.ck) for r in rows} == {(2, 0), (2, 1), (2, 2)}


# A static column lives once per partition rather than per row, and is stored
# in its own place in the file. It must survive a flush, be updatable
# independently of the clustered rows, and be deletable on its own -- and a
# partition holding only a live static column and no rows is its own shape.
def test_parquet_static_columns(cql, test_keyspace, scylla_data_dir):
    with parquet_table(cql, test_keyspace,
                       "pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck)") as table:
        cql.execute(f"INSERT INTO {table} (pk, ck, s, v) VALUES (1, 1, 100, 10)")
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (1, 2, 20)")
        # A partition with a static column and no clustered rows at all.
        cql.execute(f"INSERT INTO {table} (pk, s) VALUES (2, 200)")

        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT pk, ck, s, v FROM {table}")
        assert {(r.pk, r.ck, r.s, r.v) for r in rows} == {
            (1, 1, 100, 10), (1, 2, 100, 20), (2, None, 200, None)}

        # Updating the static column changes it for every row of the partition.
        cql.execute(f"UPDATE {table} SET s = 111 WHERE pk = 1")
        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT ck, s FROM {table} WHERE pk = 1")
        assert {(r.ck, r.s) for r in rows} == {(1, 111), (2, 111)}

        # Deleting the static column leaves the clustered rows alone.
        cql.execute(f"DELETE s FROM {table} WHERE pk = 1")
        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT ck, s, v FROM {table} WHERE pk = 1")
        assert {(r.ck, r.s, r.v) for r in rows} == {(1, None, 10), (2, None, 20)}


# A table with no clustering key at all: every partition is a single row, and
# the file has no clustering columns to encode.
def test_parquet_no_clustering_key(cql, test_keyspace, scylla_data_dir):
    with parquet_table(cql, test_keyspace,
                       "pk int PRIMARY KEY, v1 int, v2 text") as table:
        for i in range(5):
            cql.execute(f"INSERT INTO {table} (pk, v1, v2) VALUES ({i}, {i}, '{i}')")
        cql.execute(f"DELETE FROM {table} WHERE pk = 3")
        cql.execute(f"DELETE v1 FROM {table} WHERE pk = 4")

        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT pk, v1, v2 FROM {table}")
        assert {(r.pk, r.v1, r.v2) for r in rows} == {
            (0, 0, '0'), (1, 1, '1'), (2, 2, '2'), (4, None, '4')}


# Frozen collections are single opaque cells, so they behave like any other
# value; the non-frozen cases below are the interesting ones.
def test_parquet_frozen_collections(cql, test_keyspace, scylla_data_dir):
    with parquet_table(cql, test_keyspace,
                       "pk int PRIMARY KEY, fm frozen<map<text, int>>, "
                       "fs frozen<set<int>>, fl frozen<list<int>>") as table:
        cql.execute(f"INSERT INTO {table} (pk, fm, fs, fl) VALUES "
                    "(1, {'a': 1, 'b': 2}, {1, 2, 3}, [1, 2, 3])")
        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT * FROM {table} WHERE pk = 1")
        assert rows[0].fm == {'a': 1, 'b': 2}
        assert rows[0].fs == {1, 2, 3}
        assert rows[0].fl == [1, 2, 3]

        # Overwriting a frozen collection replaces it wholesale.
        cql.execute(f"UPDATE {table} SET fm = {{'c': 3}} WHERE pk = 1")
        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT fm FROM {table} WHERE pk = 1")
        assert rows[0].fm == {'c': 3}

        # Deleting one frozen collection cell leaves the siblings.
        cql.execute(f"DELETE fs FROM {table} WHERE pk = 1")
        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT fm, fs, fl FROM {table} WHERE pk = 1")
        assert rows[0].fs is None
        assert rows[0].fm == {'c': 3} and rows[0].fl == [1, 2, 3]


# Non-frozen collections are stored as one cell per element, with a collection
# tombstone for whole-collection overwrites. Both the per-element delete and
# the whole-collection delete are distinct on-disk shapes, and both are
# exercised here.
def test_parquet_nonfrozen_map(cql, test_keyspace, scylla_data_dir):
    with parquet_table(cql, test_keyspace,
                       "pk int PRIMARY KEY, m map<text, int>") as table:
        cql.execute(f"INSERT INTO {table} (pk, m) VALUES (1, {{'a': 1, 'b': 2}})")
        cql.execute(f"UPDATE {table} SET m = m + {{'c': 3}} WHERE pk = 1")
        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT m FROM {table} WHERE pk = 1")
        assert rows[0].m == {'a': 1, 'b': 2, 'c': 3}

        # Per-element delete.
        cql.execute(f"DELETE m['b'] FROM {table} WHERE pk = 1")
        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT m FROM {table} WHERE pk = 1")
        assert rows[0].m == {'a': 1, 'c': 3}

        # Whole-collection assignment: a collection tombstone plus new cells.
        cql.execute(f"UPDATE {table} SET m = {{'z': 26}} WHERE pk = 1")
        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT m FROM {table} WHERE pk = 1")
        assert rows[0].m == {'z': 26}

        # Whole-collection delete.
        cql.execute(f"DELETE m FROM {table} WHERE pk = 1")
        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT pk, m FROM {table} WHERE pk = 1")
        assert rows[0].m is None


def test_parquet_nonfrozen_set(cql, test_keyspace, scylla_data_dir):
    with parquet_table(cql, test_keyspace,
                       "pk int PRIMARY KEY, s set<int>") as table:
        cql.execute(f"INSERT INTO {table} (pk, s) VALUES (1, {{1, 2, 3}})")
        cql.execute(f"UPDATE {table} SET s = s + {{4}} WHERE pk = 1")
        cql.execute(f"UPDATE {table} SET s = s - {{2}} WHERE pk = 1")
        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT s FROM {table} WHERE pk = 1")
        assert rows[0].s == {1, 3, 4}

        cql.execute(f"DELETE s FROM {table} WHERE pk = 1")
        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT pk, s FROM {table} WHERE pk = 1")
        assert rows[0].s is None


def test_parquet_nonfrozen_list(cql, test_keyspace, scylla_data_dir):
    with parquet_table(cql, test_keyspace,
                       "pk int PRIMARY KEY, l list<int>") as table:
        cql.execute(f"INSERT INTO {table} (pk, l) VALUES (1, [1, 2, 3])")
        cql.execute(f"UPDATE {table} SET l = l + [4] WHERE pk = 1")
        cql.execute(f"UPDATE {table} SET l = [0] + l WHERE pk = 1")
        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT l FROM {table} WHERE pk = 1")
        assert rows[0].l == [0, 1, 2, 3, 4]

        # Positional element delete.
        cql.execute(f"DELETE l[0] FROM {table} WHERE pk = 1")
        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT l FROM {table} WHERE pk = 1")
        assert rows[0].l == [1, 2, 3, 4]

        cql.execute(f"DELETE l FROM {table} WHERE pk = 1")
        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT pk, l FROM {table} WHERE pk = 1")
        assert rows[0].l is None


# Counters are stored differently from ordinary cells (they carry a shard-wise
# accumulator, not a value), and a counter table cannot hold non-counter
# columns.
def test_parquet_counters(cql, test_keyspace, scylla_data_dir):
    with parquet_table(cql, test_keyspace,
                       "pk int, ck int, c counter, PRIMARY KEY (pk, ck)") as table:
        cql.execute(f"UPDATE {table} SET c = c + 5 WHERE pk = 1 AND ck = 1")
        cql.execute(f"UPDATE {table} SET c = c + 7 WHERE pk = 1 AND ck = 1")
        cql.execute(f"UPDATE {table} SET c = c - 2 WHERE pk = 1 AND ck = 2")
        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT ck, c FROM {table} WHERE pk = 1")
        assert {(r.ck, r.c) for r in rows} == {(1, 12), (2, -2)}

        # An increment applied after the flush must merge with what is already
        # in the Parquet file, not replace it.
        cql.execute(f"UPDATE {table} SET c = c + 1 WHERE pk = 1 AND ck = 1")
        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT ck, c FROM {table} WHERE pk = 1 AND ck = 1")
        assert rows[0].c == 13


# TTL is stored per cell as an expiry time alongside the write time. A cell
# that has not yet expired must read back with its ttl intact; one that has
# expired must read as absent, and must stay absent through a flush.
def test_parquet_ttl_and_expiry(cql, test_keyspace, scylla_data_dir):
    with parquet_table(cql, test_keyspace,
                       "pk int, ck int, v int, w int, PRIMARY KEY (pk, ck)") as table:
        # A long TTL, to assert the expiry survives the round trip.
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (1, 1, 10) USING TTL 3600")
        # No TTL on w, so a single row mixes a expiring and a permanent cell.
        cql.execute(f"UPDATE {table} SET w = 20 WHERE pk = 1 AND ck = 1")

        rows = flushed_read(cql, scylla_data_dir, table,
                            f"SELECT v, w, TTL(v) AS tv, TTL(w) AS tw FROM {table} "
                            f"WHERE pk = 1 AND ck = 1")
        assert rows[0].v == 10 and rows[0].w == 20
        assert rows[0].tv is not None and 0 < rows[0].tv <= 3600
        assert rows[0].tw is None, "w was written without a TTL"

        # A short TTL that we then wait out. The expired cell must be gone
        # from the answer even though it is still present in the file.
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (1, 2, 99) USING TTL 1")
        nodetool.flush(cql, table)
        assert_parquet_on_disk(scylla_data_dir, table)
        deadline = time.time() + 30
        while time.time() < deadline:
            rows = list(cql.execute(
                f"SELECT v FROM {table} WHERE pk = 1 AND ck = 2 BYPASS CACHE"))
            if not rows or rows[0].v is None:
                break
            time.sleep(0.5)
        else:
            pytest.fail("cell with TTL 1 never expired")

        # The un-expired sibling is unaffected.
        rows = list(cql.execute(
            f"SELECT v FROM {table} WHERE pk = 1 AND ck = 1 BYPASS CACHE"))
        assert rows[0].v == 10
