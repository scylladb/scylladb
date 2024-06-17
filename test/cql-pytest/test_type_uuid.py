# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Test involving the "uuid" column type.
#
# Note that although time uuids (uuid with version 1) are an example of a
# uuid value, the "uuid" type and "timeuuid" are different column types,
# and each type should be tested separately.
# timeuuid is a separate type and may
#############################################################################

from util import new_test_table, unique_key_int

import pytest
import uuid

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, u uuid, primary key (p, u)") as table:
        yield table

# Check how UUID values are sorted - as discussed in issue #15561.
# It turns out they are *not* sorted simply in lexicographical order.
# Our implementation, std::strong_ordering operator()(const uuid_type_impl&)
# in types.cc, implements the following ordering:
#   * The version nibble (higher nibble of the 7th byte) is compared
#     first - higher version is considered higher UUID.
#   * If the version is 1 (time uuid), it is compared like timeuuid
#   * If the version is anything else, it is compared using unsigned
#     bytewise comparison.
#   * All UUID values shorter than the expected 128 bits are considered
#     to have the same minimal position. This is bad, but it can't happen
#     (you can't get CQL to insert incomplete UUID values).
#
# In the tests below we check these cases by inserting different uuids
# as clustering key and seeing how they are sorted. These tests will prevent
# this sort order from ever regressing (such a regression can make old
# data unreadable!), and also will allow us to check that the order we
# chose (outlined above) is identical to the one chosen by Cassandra.

# UUIDs of *different* versions are ordered by the version number, not the
# highest or lowest bytes:
def test_uuid_order_versions(cql, table1):
    p = unique_key_int()
    v0 = "018ad550-b25d-09d0-7e90-ea5438411dc7"
    v1 = "028ad550-b25d-19d0-7e90-ea5438411dc7"
    v2 = "018ad550-b227-2108-2871-99a6c9bdfe96"
    v3 = "008ad550-b227-3108-2871-99a6c9bdfe96"
    v4 = "008ad550-b227-4108-2871-99a6c9bd0096"
    v5 = "108ad550-b227-5108-2871-99a6c9bd0096"
    v6 = "107ad350-b227-6108-2871-99a6c9bd0096"
    v7 = "308ad850-b227-7108-2871-99a6c9bd0036"
    for u in [v3, v5, v0, v7, v1, v4, v2, v6]:
        cql.execute(f"INSERT INTO {table1} (p, u) VALUES ({p}, {u})")
    assert [uuid.UUID(v0), uuid.UUID(v1), uuid.UUID(v2), uuid.UUID(v3),
            uuid.UUID(v4), uuid.UUID(v5), uuid.UUID(v6), uuid.UUID(v7)] ==  \
        [x.u for x in cql.execute(f"SELECT u FROM {table1} where p={p}")]

# UUIDs with version 1 are ordered among themselves by timeuuid order.
# This is NOT bytewise ordering
def test_uuid_order_version1(cql, table1):
    p = unique_key_int()
    u0 = "00000000-0000-1000-0000-000000000000"
    u1 = "70000000-0000-1000-0000-000000000000"
    u2 = "00700000-0000-1000-0000-000000000000"
    u3 = "00007000-0000-1000-0000-000000000000"
    u4 = "00000070-0000-1000-0000-000000000000"
    u5 = "00000000-7000-1000-0000-000000000000"
    u6 = "00000000-0070-1000-0000-000000000000"
    u7 = "00000000-0000-1070-0000-000000000000"
    u8 = "00000000-0000-1000-7000-000000000000"
    u9 = "00000000-0000-1000-0070-000000000000"
    ua = "00000000-0000-1000-0000-700000000000"
    ub = "00000000-0000-1000-0000-007000000000"
    uc = "00000000-0000-1000-0000-000070000000"
    ud = "00000000-0000-1000-0000-000000700000"
    ue = "00000000-0000-1000-0000-000000007000"
    uf = "00000000-0000-1000-0000-000000000070"
    for u in [u0,u1,u2,u3,u4,u5,u6,u7,u8,u9,ua,ub,uc,ud,ue,uf]:
        cql.execute(f"INSERT INTO {table1} (p, u) VALUES ({p}, {u})")
    # The order we expect for the above time (version 1) uuids is very
    # non-obvious, but we can check that it is correct on Scylla and
    # Cassandra and never changes.
    assert [
        uuid.UUID(u0),
        uuid.UUID(uf),
        uuid.UUID(ue),
        uuid.UUID(ud),
        uuid.UUID(uc),
        uuid.UUID(ub),
        uuid.UUID(ua),
        uuid.UUID(u9),
        uuid.UUID(u8),
        uuid.UUID(u4),
        uuid.UUID(u3),
        uuid.UUID(u2),
        uuid.UUID(u1),
        uuid.UUID(u6),
        uuid.UUID(u5),
        uuid.UUID(u7),
    ] == [x.u for x in cql.execute(f"SELECT u FROM {table1} where p={p}")]

# UUIDs with version 7 (or anything other than 1) are ordered among themselves
# in unsigned bytewise order, not in the bizarre ordering of version 1 above:
def test_uuid_order_version7(cql, table1):
    p = unique_key_int()
    u0 = "00000000-0000-7000-0000-000000000000"
    u1 = "f0000000-0000-7000-0000-000000000000"
    u2 = "70000000-0000-7000-0000-000000000000"
    u3 = "50000000-0000-7000-0000-000000000000"
    u4 = "00000070-0000-7000-0000-000000000000"
    u5 = "00000000-7000-7000-0000-000000000000"
    u6 = "00000000-0070-7000-0000-000000000000"
    u7 = "00000000-0000-7070-0000-000000000000"
    u8 = "00000000-0000-7000-7000-000000000000"
    u9 = "00000000-0000-7000-0070-000000000000"
    ua = "00000000-0000-7000-0000-700000000000"
    ub = "00000000-0000-7000-0000-007000000000"
    uc = "00000000-0000-7000-0000-000070000000"
    ud = "00000000-0000-7000-0000-000000700000"
    ue = "00000000-0000-7000-0000-000000007000"
    uf = "00000000-0000-7000-0000-000000000070"
    for u in [u0,u1,u2,u3,u4,u5,u6,u7,u8,u9,ua,ub,uc,ud,ue,uf]:
        cql.execute(f"INSERT INTO {table1} (p, u) VALUES ({p}, {u})")
    # The order we expect is unsigned bytewise comparison, first byte
    # is most significant (big-endian) so u1 is last in ascending sort.
    assert [
        uuid.UUID(u0),
        uuid.UUID(uf),
        uuid.UUID(ue),
        uuid.UUID(ud),
        uuid.UUID(uc),
        uuid.UUID(ub),
        uuid.UUID(ua),
        uuid.UUID(u9),
        uuid.UUID(u8),
        uuid.UUID(u7),
        uuid.UUID(u6),
        uuid.UUID(u5),
        uuid.UUID(u4),
        uuid.UUID(u3),
        uuid.UUID(u2),
        uuid.UUID(u1),
    ] == [x.u for x in cql.execute(f"SELECT u FROM {table1} where p={p}")]
