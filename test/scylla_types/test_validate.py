#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#


def test_validate_valid(scylla_types):
    res = scylla_types("validate", "-t", "Int32Type", "b34b62d4")
    assert res.stdout == "b34b62d4: VALID - -1286905132\n"


def test_validate_invalid(scylla_types):
    res = scylla_types("validate", "-t", "Int32Type", "000001")
    assert res.stdout.startswith("000001: INVALID - ")
    assert "got 3 bytes" in res.stdout


def test_validate_multiple_values(scylla_types):
    """Each value is validated on its own, an invalid value doesn't affect the others."""
    res = scylla_types("validate", "-t", "Int32Type", "00000001", "000001", "00000002")
    lines = res.stdout.splitlines()
    assert len(lines) == 3
    assert lines[0] == "00000001: VALID - 1"
    assert lines[1].startswith("000001: INVALID - ")
    assert lines[2] == "00000002: VALID - 2"


def test_validate_prefix_compound(scylla_types):
    res = scylla_types("validate", "--prefix-compound", "-t", "Int32Type", "-t", "UTF8Type", "000400000001")
    assert res.stdout == "000400000001: VALID - (1)\n"


def test_validate_full_compound(scylla_types):
    res = scylla_types("validate", "--full-compound", "-t", "Int32Type", "-t", "UTF8Type", "0004000000010003616263", "0004000000")
    lines = res.stdout.splitlines()
    # The error of the invalid value comes with a backtrace, which spans
    # multiple lines in debug builds, so don't check the number of lines.
    assert len(lines) >= 2
    assert lines[0] == "0004000000010003616263: VALID - (1, abc)"
    assert lines[1].startswith("0004000000: INVALID - ")


def test_validate_legacy_composite(scylla_types):
    res = scylla_types("validate", "--legacy-composite", "-t", "Int32Type", "-t", "UTF8Type",
                       "00040000000100000361626300", "000400000001000003616200", "0004000000010003616263")
    lines = res.stdout.splitlines()
    assert len(lines) == 3
    assert lines[0] == "00040000000100000361626300: VALID - (1, abc)"
    # Truncated value.
    assert lines[1].startswith("000400000001000003616200: INVALID - ")
    # Value in scylla's in-memory format, missing the end-of-component bytes.
    assert lines[2].startswith("0004000000010003616263: INVALID - ")
