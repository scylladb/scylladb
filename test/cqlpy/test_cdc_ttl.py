# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests of the CDC 'ttl' option, ported from scylla-dtest cdc_ttl_test.py,
# one test per dtest: the "cdc$ttl" column of the log, how log rows and base
# rows expire, each by its own TTL, and altering the option. As in the
# dtests, ten rows in two partitions are written with INSERT or UPDATE, as
# unprepared statements with the values formatted into them, into text or
# list columns, with the dtests' TTLs. Expiration is waited for by polling
# instead of sleeping for the TTL.

import re
import time

import pytest

from .util import new_test_table, run_cqlsh

# CDC log operation codes, see cdc::operation.
PREIMAGE, UPDATE, INSERT, POSTIMAGE = 0, 1, 2, 9

def build_cdc_properties(preimage_enable, postimage_enable, cdc_ttl):
    statement = "cdc={'enabled': true"
    if preimage_enable:
        statement += ", 'preimage': true"
    if postimage_enable:
        statement += ", 'postimage': true"
    return statement + f", 'ttl': {cdc_ttl} }}"


def create_table(cql, keyspace, preimage_enable, postimage_enable, cdc_ttl, value_type):
    schema = f"pkey bigint, ckey int, cval1 {value_type}, cval2 {value_type}, PRIMARY KEY (pkey, ckey)"
    return new_test_table(cql, keyspace, schema, " WITH " + build_cdc_properties(preimage_enable, postimage_enable, cdc_ttl))


def populate_base_table(cql, table, operation, value_type, updating_column="cval1", ttl=None):
    """The dtest's populate_base_table(): INSERT or UPDATE of the column of
    rows (i % 2, i) for i in 0..9, with a text or list value."""
    for i in range(10):
        if operation == INSERT:
            value = [f"text{i}", f"text-{i}"] if "list" in value_type else f"text{i}"
            stm = f"INSERT INTO {table} (pkey, ckey, {updating_column}) VALUES (%(pkey)s, %(ckey)s, %({updating_column})s)"
            if ttl:
                stm += f" USING TTL {ttl}"
        else:
            value = [f"new_text{i}", f"new_text{i + 1}"] if "list" in value_type else f"new_text{i}"
            using_ttl = f"USING TTL {ttl}" if ttl else ""
            stm = f"UPDATE {table} {using_ttl} SET {updating_column} = %({updating_column})s WHERE pkey = %(pkey)s and ckey = %(ckey)s"
        cql.execute(stm, {"pkey": i % 2, "ckey": i, updating_column: value})


def get_base_rows(cql, table):
    return list(cql.execute(f"SELECT * FROM {table}"))


def get_log_rows(cql, table):
    return list(cql.execute(f"SELECT * FROM {table}_scylla_cdc_log"))


def wait_until(cond, timeout=120):
    deadline = time.time() + timeout
    while not cond():
        assert time.time() < deadline, "Timed out waiting for TTL expiration"
        time.sleep(0.1)


def verify_log_row_ttl(log_rows, expected_ttl):
    for row in log_rows:
        if row.cdc_operation not in [PREIMAGE, POSTIMAGE] and (not row.cdc_deleted_cval1 and not row.cdc_deleted_cval2):
            assert row.cdc_ttl == expected_ttl, row


def check_cdc_ttl_column_value_in_cdc_log_table(cql, keyspace, preimage_enable=False, postimage_enable=False, base_ttl=None, value_type="text"):
    """Writes with the base TTL `base_ttl` (INSERTs of both columns, then
    UPDATEs of them) and checks "cdc$ttl" of the delta rows with a value."""
    with create_table(cql, keyspace, preimage_enable, postimage_enable, 30, value_type) as table:
        # The log has only first records (no preimage if enabled).
        populate_base_table(cql, table, INSERT, value_type, updating_column="cval1", ttl=base_ttl)
        populate_base_table(cql, table, INSERT, value_type, updating_column="cval2", ttl=base_ttl)
        verify_log_row_ttl(get_log_rows(cql, table), base_ttl)
        # The log has all rows (preimage if enabled).
        populate_base_table(cql, table, UPDATE, value_type, updating_column="cval1", ttl=base_ttl)
        populate_base_table(cql, table, UPDATE, value_type, updating_column="cval2", ttl=base_ttl)
        verify_log_row_ttl(get_log_rows(cql, table), base_ttl)


def check_rows_cleared_after_ttl_expired_for_operation(cql, keyspace, operation=INSERT, value_type="text", cdc_ttl=10, base_ttl=None,
                                                       preimage_enable=False, postimage_enable=False):
    """Writes the rows with the base TTL `base_ttl` (if any) to a table with
    CDC TTL `cdc_ttl`, and checks that the log and base rows expire each by its own TTL: when
    the base rows expire first, the log is unchanged; when the log expires
    first, the base table is unchanged."""
    with create_table(cql, keyspace, preimage_enable, postimage_enable, cdc_ttl, value_type) as table:
        populate_base_table(cql, table, operation, value_type, updating_column="cval1", ttl=base_ttl)
        base_table_rows = get_base_rows(cql, table)
        log_table_rows = get_log_rows(cql, table)
        assert len(base_table_rows) > 0
        assert len(log_table_rows) > 0

        if base_ttl and cdc_ttl > base_ttl:
            # The base rows expire, the log rows stay and no log rows are added.
            wait_until(lambda: get_base_rows(cql, table) == [])
            assert get_log_rows(cql, table) == log_table_rows
        elif base_ttl and cdc_ttl < base_ttl:
            # The log rows expire, the base rows stay.
            wait_until(lambda: get_log_rows(cql, table) == [])
            assert get_base_rows(cql, table) == base_table_rows

        wait_until(lambda: get_log_rows(cql, table) == [] and (not base_ttl or get_base_rows(cql, table) == []))
        if base_ttl:
            assert get_base_rows(cql, table) == []
        else:
            assert get_base_rows(cql, table) == base_table_rows
        assert get_log_rows(cql, table) == []


def verify_cdc_ttl_configured(cql, table, expected_cdc_ttl):
    """Checks the table's cdc 'ttl' in cqlsh's DESC KEYSPACE output."""
    result = run_cqlsh(cql, f"desc keyspace {table.split('.')[0]}")
    assert result.returncode == 0, result.stderr
    create = [s for s in result.stdout.split(";") if f"CREATE TABLE {table} (" in s]
    assert len(create) == 1, result.stdout
    matched = re.search(r"cdc\s?=\s?{.*'ttl':\s+'(?P<ttl>[\d]+?)'", create[0], flags=re.MULTILINE)
    found_cdc_ttl = int(matched.group("ttl")) if matched else None
    assert found_cdc_ttl == expected_cdc_ttl


def check_clear_log_rows_according_altered_cdc_ttl(cql, keyspace, operation=INSERT, preimage_enable=False, postimage_enable=False, cdc_ttl=10, value_type="text"):
    """Writes rows, alters the CDC TTL to twice `cdc_ttl`, writes the other
    column, checks that the log rows written before the ALTER expire first,
    then the rest, and at the end checks the altered TTL in cqlsh's DESCRIBE
    output."""
    new_cdc_ttl = cdc_ttl * 2
    with create_table(cql, keyspace, preimage_enable, postimage_enable, cdc_ttl, value_type) as table:
        populate_base_table(cql, table, operation, value_type, updating_column="cval1")
        log_rows_with_first_ttl = get_log_rows(cql, table)
        assert len(log_rows_with_first_ttl) > 0

        cql.execute(f"ALTER TABLE {table} WITH {build_cdc_properties(preimage_enable, postimage_enable, new_cdc_ttl)};")

        populate_base_table(cql, table, operation, value_type, updating_column="cval2")
        log_rows_with_old_new_ttl = get_log_rows(cql, table)
        assert len(log_rows_with_old_new_ttl) > len(log_rows_with_first_ttl)

        # The rows with the old TTL expire, the rest were there before.
        wait_until(lambda: not any(row in log_rows_with_first_ttl for row in get_log_rows(cql, table)))
        not_expired_log_rows = get_log_rows(cql, table)
        assert not_expired_log_rows
        for row in not_expired_log_rows:
            assert row in log_rows_with_old_new_ttl

        wait_until(lambda: get_log_rows(cql, table) == [])

        # cqlsh is slow; keep it out of every TTL window
        verify_cdc_ttl_configured(cql, table, new_cdc_ttl)


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_cdc_ttl_column_value_for_native_type.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_cdc_ttl_column_value_for_native_type(scylla_only, cql, test_keyspace_vnodes):
    check_cdc_ttl_column_value_in_cdc_log_table(cql, test_keyspace_vnodes, base_ttl=30)


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_cdc_ttl_column_value_for_native_type_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_cdc_ttl_column_value_for_native_type_preimage_postimage(scylla_only, cql, test_keyspace_vnodes):
    check_cdc_ttl_column_value_in_cdc_log_table(cql, test_keyspace_vnodes, preimage_enable=True, postimage_enable=True, base_ttl=30)


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_cd_cttl_column_value_for_collection_type.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_cd_cttl_column_value_for_collection_type(scylla_only, cql, test_keyspace_vnodes):
    check_cdc_ttl_column_value_in_cdc_log_table(cql, test_keyspace_vnodes, base_ttl=30, value_type="text")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_cdc_ttl_column_value_for_collection_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_cdc_ttl_column_value_for_collection_preimage_postimage(scylla_only, cql, test_keyspace_vnodes):
    check_cdc_ttl_column_value_in_cdc_log_table(cql, test_keyspace_vnodes, preimage_enable=True, postimage_enable=True, base_ttl=30, value_type="list<text>")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_log_table_cleared_after_cdc_ttl_expired_for_insert_native_type.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_log_table_cleared_after_cdc_ttl_expired_for_insert_native_type(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=INSERT, cdc_ttl=10, value_type="text")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_log_table_cleared_after_cdc_ttl_expired_for_insert_native_type_with_preimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_log_table_cleared_after_cdc_ttl_expired_for_insert_native_type_with_preimage(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=INSERT, preimage_enable=True, cdc_ttl=10, value_type="varchar")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_log_table_cleared_after_cdc_ttl_expired_for_insert_native_type_native_type_with_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_log_table_cleared_after_cdc_ttl_expired_for_insert_native_type_native_type_with_postimage(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=INSERT, postimage_enable=True, cdc_ttl=10, value_type="ascii")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_log_table_cleared_after_cdc_ttl_expired_for_insert_native_type_with_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_log_table_cleared_after_cdc_ttl_expired_for_insert_native_type_with_preimage_postimage(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=INSERT, preimage_enable=True, postimage_enable=True, cdc_ttl=10, value_type="ascii")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_log_table_cleared_after_cdc_ttl_expired_for_update_native_type.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_log_table_cleared_after_cdc_ttl_expired_for_update_native_type(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=UPDATE, cdc_ttl=10, value_type="text")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_log_table_cleared_after_cdc_ttl_expired_for_update_native_type_with_preimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_log_table_cleared_after_cdc_ttl_expired_for_update_native_type_with_preimage(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=UPDATE, preimage_enable=True, cdc_ttl=10, value_type="varchar")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_log_table_cleared_after_cdc_ttl_expired_for_update_native_type_with_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_log_table_cleared_after_cdc_ttl_expired_for_update_native_type_with_postimage(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=UPDATE, postimage_enable=True, cdc_ttl=10, value_type="ascii")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_log_table_cleared_after_cdc_ttl_expired_for_update_native_type_with_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_log_table_cleared_after_cdc_ttl_expired_for_update_native_type_with_preimage_postimage(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=UPDATE, preimage_enable=True, postimage_enable=True, cdc_ttl=10, value_type="ascii")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_log_table_cleared_after_cdc_ttl_expired_for_insert_collection.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_log_table_cleared_after_cdc_ttl_expired_for_insert_collection(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=INSERT, cdc_ttl=10, value_type="list<varchar>")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_log_table_cleared_after_cdc_ttl_expired_for_insert_collection_preimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_log_table_cleared_after_cdc_ttl_expired_for_insert_collection_preimage(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=INSERT, preimage_enable=True, cdc_ttl=10, value_type="list<text>")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_log_table_cleared_after_cdc_ttl_expired_for_insert_collection_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_log_table_cleared_after_cdc_ttl_expired_for_insert_collection_postimage(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=INSERT, postimage_enable=True, cdc_ttl=10, value_type="list<ascii>")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_log_table_cleared_after_cdc_ttl_expired_for_insert_collection_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_log_table_cleared_after_cdc_ttl_expired_for_insert_collection_preimage_postimage(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=INSERT, preimage_enable=True, postimage_enable=True, cdc_ttl=10, value_type="list<text>")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_log_table_cleared_after_cdc_ttl_expired_for_update_collection.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_log_table_cleared_after_cdc_ttl_expired_for_update_collection(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=UPDATE, cdc_ttl=10, value_type="list<varchar>")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_log_table_cleared_after_cdc_ttl_expired_for_update_collection_preimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_log_table_cleared_after_cdc_ttl_expired_for_update_collection_preimage(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=UPDATE, preimage_enable=True, cdc_ttl=10, value_type="list<text>")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_log_table_cleared_after_cdc_ttl_expired_for_update_collection_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_log_table_cleared_after_cdc_ttl_expired_for_update_collection_postimage(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=UPDATE, postimage_enable=True, cdc_ttl=10, value_type="list<ascii>")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_log_table_cleared_after_cdc_ttl_expired_for_update_collection_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_log_table_cleared_after_cdc_ttl_expired_for_update_collection_preimage_postimage(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=UPDATE, preimage_enable=True, postimage_enable=True, cdc_ttl=10, value_type="list<text>")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_base_and_log_tables_cleared_after_ttl_expired_for_insert_native_type.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_base_and_log_tables_cleared_after_ttl_expired_for_insert_native_type(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=INSERT, cdc_ttl=10, base_ttl=10, value_type="text")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_base_and_log_tables_cleared_after_ttl_expired_for_insert_native_type_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_base_and_log_tables_cleared_after_ttl_expired_for_insert_native_type_preimage_postimage(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=INSERT, preimage_enable=True, postimage_enable=True, cdc_ttl=10, base_ttl=10, value_type="text")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_base_and_log_tables_cleared_after_ttl_expired_for_update_native_type.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_base_and_log_tables_cleared_after_ttl_expired_for_update_native_type(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=UPDATE, cdc_ttl=10, base_ttl=10, value_type="text")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_base_and_log_tables_cleared_after_ttl_expired_for_update_native_type_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_base_and_log_tables_cleared_after_ttl_expired_for_update_native_type_preimage_postimage(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=UPDATE, preimage_enable=True, postimage_enable=True, cdc_ttl=10, base_ttl=10, value_type="text")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_base_and_log_tables_cleared_after_ttl_expired_for_insert_collection.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_base_and_log_tables_cleared_after_ttl_expired_for_insert_collection(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=INSERT, cdc_ttl=10, base_ttl=10, value_type="list<text>")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_base_and_log_tables_cleared_after_ttl_expired_for_insert_collection_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_base_and_log_tables_cleared_after_ttl_expired_for_insert_collection_preimage_postimage(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=INSERT, preimage_enable=True, postimage_enable=True, cdc_ttl=10, base_ttl=10, value_type="list<text>")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_base_and_log_tables_cleared_after_ttl_expired_for_update_collection.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_base_and_log_tables_cleared_after_ttl_expired_for_update_collection(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=UPDATE, cdc_ttl=10, base_ttl=10, value_type="list<text>")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_base_and_log_tables_cleared_after_ttl_expired_for_update_collection_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_base_and_log_tables_cleared_after_ttl_expired_for_update_collection_preimage_postimage(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=UPDATE, preimage_enable=True, postimage_enable=True, cdc_ttl=10, base_ttl=10, value_type="list<text>")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_clear_base_table_by_expired_ttl_and_not_affect_log_table_for_insert_native_types.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_clear_base_table_by_expired_ttl_and_not_affect_log_table_for_insert_native_types(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, cdc_ttl=10, base_ttl=5, value_type="text")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_clear_base_table_by_expired_ttl_and_not_affect_log_table_for_insert_native_types_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_clear_base_table_by_expired_ttl_and_not_affect_log_table_for_insert_native_types_preimage_postimage(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, preimage_enable=True, postimage_enable=True, cdc_ttl=10, base_ttl=5, value_type="text")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_clear_base_table_by_expired_ttl_and__not_affect_log_table_for_update_collection.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_clear_base_table_by_expired_ttl_and__not_affect_log_table_for_update_collection(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=UPDATE, cdc_ttl=10, base_ttl=5, value_type="text")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_clear_base_table_by_expired_ttl_and__not_affect_log_table_for_update_collection_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_clear_base_table_by_expired_ttl_and__not_affect_log_table_for_update_collection_preimage_postimage(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, operation=UPDATE, preimage_enable=True, postimage_enable=True, cdc_ttl=10, base_ttl=5, value_type="text")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_clear_log_table_by_ttl_and_not_affect_base_table_for_insert_native_type.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_clear_log_table_by_ttl_and_not_affect_base_table_for_insert_native_type(scylla_only, cql, test_keyspace_vnodes):
    check_rows_cleared_after_ttl_expired_for_operation(cql, test_keyspace_vnodes, preimage_enable=True, postimage_enable=True, cdc_ttl=10, base_ttl=15, value_type="text")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_rows_cleared_in_log_table_according_set_cdc_ttl_for_insert_native_type.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_rows_cleared_in_log_table_according_set_cdc_ttl_for_insert_native_type(scylla_only, cql, test_keyspace_vnodes):
    check_clear_log_rows_according_altered_cdc_ttl(cql, test_keyspace_vnodes, operation=INSERT, cdc_ttl=10, value_type="varchar")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_rows_cleared_in_log_table_according_set_cdc_ttl_for_insert_native_type_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_rows_cleared_in_log_table_according_set_cdc_ttl_for_insert_native_type_preimage_postimage(scylla_only, cql, test_keyspace_vnodes):
    check_clear_log_rows_according_altered_cdc_ttl(cql, test_keyspace_vnodes, operation=INSERT, cdc_ttl=10, value_type="varchar", preimage_enable=True, postimage_enable=True)


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_rows_cleared_in_log_table_according_set_cdc_ttl_for_update_native_type.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_rows_cleared_in_log_table_according_set_cdc_ttl_for_update_native_type(scylla_only, cql, test_keyspace_vnodes):
    check_clear_log_rows_according_altered_cdc_ttl(cql, test_keyspace_vnodes, operation=UPDATE, cdc_ttl=10, value_type="varchar")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_rows_cleared_in_log_table_according_set_cdc_ttl_for_update_native_type_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_rows_cleared_in_log_table_according_set_cdc_ttl_for_update_native_type_preimage_postimage(scylla_only, cql, test_keyspace_vnodes):
    check_clear_log_rows_according_altered_cdc_ttl(cql, test_keyspace_vnodes, operation=UPDATE, cdc_ttl=10, value_type="varchar", preimage_enable=True, postimage_enable=True)


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_rows_cleared_in_log_table_according_set_cdc_ttl_for_insert_collection.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_rows_cleared_in_log_table_according_set_cdc_ttl_for_insert_collection(scylla_only, cql, test_keyspace_vnodes):
    check_clear_log_rows_according_altered_cdc_ttl(cql, test_keyspace_vnodes, operation=INSERT, cdc_ttl=10, value_type="list<varchar>")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_rows_cleared_in_log_table_according_set_cdc_ttl_for_insert_collection_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_rows_cleared_in_log_table_according_set_cdc_ttl_for_insert_collection_preimage_postimage(scylla_only, cql, test_keyspace_vnodes):
    check_clear_log_rows_according_altered_cdc_ttl(cql, test_keyspace_vnodes, operation=INSERT, cdc_ttl=10, value_type="list<varchar>", preimage_enable=True, postimage_enable=True)


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_rows_cleared_in_log_table_according_set_cdc_ttl_for_update_collection.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_rows_cleared_in_log_table_according_set_cdc_ttl_for_update_collection(scylla_only, cql, test_keyspace_vnodes):
    check_clear_log_rows_according_altered_cdc_ttl(cql, test_keyspace_vnodes, operation=UPDATE, cdc_ttl=10, value_type="list<varchar>")


# Ported from scylla-dtest cdc_ttl_test.py::TestCDCTTLFunctionality::test_rows_cleared_in_log_table_according_set_cdc_ttl_for_update_collection_preimage_postimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_rows_cleared_in_log_table_according_set_cdc_ttl_for_update_collection_preimage_postimage(scylla_only, cql, test_keyspace_vnodes):
    check_clear_log_rows_according_altered_cdc_ttl(cql, test_keyspace_vnodes, operation=UPDATE, cdc_ttl=10, value_type="list<varchar>", preimage_enable=True, postimage_enable=True)
