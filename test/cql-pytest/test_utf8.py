
# -*- coding: utf-8 -*-
# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for finer points of UTF-8 support. The issue of *invalid* UTF-8 input
# is tested in a separate test file - test_validation.py
#############################################################################

import pytest
import unicodedata
from cassandra.protocol import SyntaxException, AlreadyExists, InvalidRequest, ConfigurationException, ReadFailure
from util import unique_name, unique_key_string


@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (k text, c text, primary key (k, c))")
    yield table
    cql.execute("DROP TABLE " + table)

# Demonstrate that Scylla, like Cassandra, does NOT support the notion of
# "Unicode equivalence" (a.k.a. Unicode normalization). Consider the Spanish
# letter ñ - it can be represented by a single Unicode character 00F1, but
# can also be represented as a 006E (lowercase "n") followed by a 0303
# ("combining tilde"). But if you write one of these representations, and
# then look up the other, Scylla will not find the item. So Scylla does
# not support unicode equivalence.
# See https://en.wikipedia.org/wiki/Unicode_equivalence for more information
# on the issue of Unicode equivalence.
def test_unicode_equivalence(cql, table1):
    u1 = "\u00F1"        # Spanish ñ as one character
    u2 = "\u006E\u0303"  # Two characters: n followed by combining tilde.
    # Confirm that u1 and u2 are different Unicode strings, but are
    # equivalent, i.e., have the same normalized value:
    assert u1 != u2
    assert unicodedata.normalize('NFC', u1) == unicodedata.normalize('NFC', u2)

    insert = cql.prepare(f"INSERT INTO {table1} (k, c) VALUES (?, ?)")
    search = cql.prepare(f"SELECT k, c FROM {table1} WHERE k=? and c=?")
    s = unique_key_string()
    # Test that writing u1 as a *clustering key* and looking up u2 will not
    # work.
    cql.execute(insert, [s, u1])
    assert len(list(cql.execute(search, [s, u1]))) == 1
    assert len(list(cql.execute(search, [s, u2]))) == 0
    # Test that writing u1 as a *partition key* and looking up u2 will not
    # work.
    cql.execute(insert, [u1, s])
    assert len(list(cql.execute(search, [u1, s]))) == 1
    assert len(list(cql.execute(search, [u2, s]))) == 0

# Demonstrate that the LIKE operation is also not aware of Unicode
# equivalence: a 'n%' pattern can match one representation of ñ but not
# another. This is a Scylla-only test, because the LIKE operator doesn't
# exist in Cassandra.
def test_unicode_equivalence_like(scylla_only, cql, table1):
    u1 = "\u00F1"        # Spanish ñ as one character
    u2 = "\u006E\u0303"  # Two characters: n followed by combining tilde.
    # Confirm that u1 and u2 are different Unicode strings, but are
    # equivalent, i.e., have the same normalized value:
    assert u1 != u2
    assert unicodedata.normalize('NFC', u1) == unicodedata.normalize('NFC', u2)

    insert = cql.prepare(f"INSERT INTO {table1} (k, c) VALUES (?, ?)")
    search = cql.prepare(f"SELECT k, c FROM {table1} WHERE k=? AND c LIKE ? ALLOW FILTERING")
    s = unique_key_string()
    # u1 does not match the pattern 'n%':
    cql.execute(insert, [s, u1])
    assert set(cql.execute(search, [s, 'n%'])) == set()
    # u1 matches the pattern '_' (a single character though not a single byte)
    assert set(cql.execute(search, [s, '_'])) == set([(s, u1)])
    # but u2 does match 'n%', but not '_':
    cql.execute(insert, [s, u2])
    assert set(cql.execute(search, [s, 'n%'])) == set([(s, u2)])
    assert set(cql.execute(search, [s, '_'])) == set([(s, u1)])
