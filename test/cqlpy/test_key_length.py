# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Although regular column values are only limited by 31 bits (2GB),
# in Cassandra and Scylla keys are limited to 16 bits - 65535 bytes.
#
# This file contains several tests that check various cases of this
# key length limit - what exactly does this limit include (compound keys?
# clustering column name?) and how attempts to insert or select with
# oversized keys are handled (naturally, we want a clear error, not some
# sort of hang or crash).
#
# There are also cases - namely materialized views and secondary index -
# where a regular column of potentially 2GB size is used as a key, and
# needs to be limited to 64KB. We have tests for this (see issue #8627)
# in test_secondary_index.py
#############################################################################

import pytest
from util import new_test_table, random_string, unique_key_string, unique_key_int
import nodetool
from cassandra.protocol import InvalidRequest
from cassandra.util import SortedSet


@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p text, c text, v int, PRIMARY KEY (p, c)") as table:
        yield table

# Trying to insert a 65KB (clearly over 64KB) value to a clustering key or
# partition key should fail, with a clean InvalidRequest - not some internal
# server error or worse.
# Reproduces #12247
@pytest.mark.xfail(reason="Issue #12247")
def test_insert_65k_pk(cql, table1):
    stmt = cql.prepare(f'INSERT INTO {table1} (p, c) VALUES (?,?)')
    # Cassandra writes: "Key length of 66560 is longer than maximum of 65535"
    with pytest.raises(InvalidRequest, match='Key length'):
        cql.execute(stmt, ['x'*(65*1024), 'hello'])

# Reproduces #12247
@pytest.mark.xfail(reason="Issue #12247")
def test_insert_65k_ck(cql, table1):
    stmt = cql.prepare(f'INSERT INTO {table1} (p, c) VALUES (?,?)')
    # Cassandra writes: "Key length of 66560 is longer than maximum of 65535"
    with pytest.raises(InvalidRequest, match='Key length'):
        cql.execute(stmt, ['hello', 'x'*(65*1024)])

# Trying to read with a 65KB (clearly over 64KB) clustering key or partition
# key should fail, with a clean InvalidRequest - not some internal server
# error or worse.
# Reproduces #10366.
@pytest.mark.xfail(reason="Issue #10366")
def test_where_65k_pk(cql, table1):
    stmt = cql.prepare(f'SELECT * FROM {table1} WHERE p = ?')
    # Cassandra writes: "Key length of 66560 is longer than maximum of 65535"
    with pytest.raises(InvalidRequest, match='Key length'):
        cql.execute(stmt, ['x'*(65*1024)])

# While "WHERE p = ?" gives an InvalidRequest error on an oversized key (as
# the previous test checks), in Cassandra the clustering-key version
# "WHERE c = ?" doesn't throw, and just silently returns no matches.
# I don't want to enshrine this inconsistent behavior, and the following
# test allows this case to either throw a clean InvalidRequest or silently
# return no match as Cassandra does. In any case, returning some ugly internal
# error (as happened in Scylla) is a bug.
# Reproduces #10366.
@pytest.mark.xfail(reason="Issue #10366")
def test_where_65k_ck(cql, table1):
    stmt = cql.prepare(f'SELECT * FROM {table1} WHERE p = ? AND c = ?')
    try:
        assert [] == list(cql.execute(stmt, ['dog', 'x'*(65*1024)]))
    except InvalidRequest:
        # Acceptable as an alternative to Cassandra's silently returning
        # nothing (see explanation above)
        pass

# What is the exact size limit of the key? It turns out that Cassandra allows
# the partition key length to reach all the way to 65535 (2^16-1), and the
# limit also applies for clustering key length (here, we are talking about
# the case of a single-component key). The following two tests check that
# such a 65535-byte partition and clustering key (respectively) can be
# written and then correctly read back.
# Scylla shouldn't impose its own smaller size limit - but in issue #16772,
# it did (the limit was 65533).

@pytest.mark.xfail(reason="Issue #16772")
def test_insert_65535_pk(cql, table1):
    stmt = cql.prepare(f'INSERT INTO {table1} (p, c) VALUES (?,?)')
    p = random_string(length=65535)
    c = unique_key_string()  # not particularly long
    cql.execute(stmt, [p, c])
    stmt = cql.prepare(f'SELECT p,c FROM {table1} WHERE p=?')
    assert list(cql.execute(stmt, [p])) == [(p, c)]

@pytest.mark.xfail(reason="Issue #16772")
def test_insert_65535_ck(cql, table1):
    stmt = cql.prepare(f'INSERT INTO {table1} (p, c) VALUES (?,?)')
    p = unique_key_string()  # not particularly long
    c = random_string(length=65535)
    cql.execute(stmt, [p, c])
    stmt = cql.prepare(f'SELECT p,c FROM {table1} WHERE p=?')
    assert list(cql.execute(stmt, [p])) == [(p, c)]

# Same as test_insert_65535_ck() but also set a regular column. Scylla's
# implementation docs/architecture/sstable/sstable2/sstable-interpretation.rst
# encodes an cell's key to contain the clustering key *value* as the
# first component, and the regular column *name* as the second component.
# This should work - Scylla shouldn't try to limit the sum of these lengths
# or something.
@pytest.mark.xfail(reason="Issue #16772")
def test_insert_65535_ck_with_regular_column(cql, table1):
    stmt = cql.prepare(f'INSERT INTO {table1} (p, c, v) VALUES (?,?,?)')
    p = unique_key_string()  # not particularly long
    c = random_string(length=65535)
    v = unique_key_int()
    cql.execute(stmt, [p, c, v])
    stmt = cql.prepare(f'SELECT p,c,v FROM {table1} WHERE p=?')
    assert list(cql.execute(stmt, [p])) == [(p, c, v)]

# The following are tests for compound partition key and composite clustering
# key - i.e., keys containing multiple columns.
# Individual components cannot be too long, but there are also limits on
# the total size, and the compound format has some overheads which limits
# the maximum size, and we want to check all of this below.

@pytest.fixture(scope="module")
def table2(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p1 text, p2 text, c1 text, c2 text, PRIMARY KEY ((p1, p2), c1, c2)") as table:
        yield table

# As we checked above for single-component keys, a 65 KB key is definitely
# oversized and no key component can be this big.
# Cassandra also has a bug (see CASSANDRA-19270) in the compound pk case -
# instead of the expected InvalidRequest error, it generates an internal
# server error (i.e., NoHostAvailable) with the message "'H' format requires
# 0 <= number <= 65535".
@pytest.mark.xfail(reason="Issue #12247")
def test_insert_65k_pk_compound(cql, table2, cassandra_bug):
    stmt = cql.prepare(f'INSERT INTO {table2} (p1, p2, c1, c2) VALUES (?,?,?,?)')
    big = 'x'*(65*1024)
    with pytest.raises(InvalidRequest, match='Key length'):
        cql.execute(stmt, [big, 'dog', 'cat', 'mouse'])
    with pytest.raises(InvalidRequest, match='Key length'):
        cql.execute(stmt, ['dog', big, 'cat', 'mouse'])

@pytest.mark.xfail(reason="Issue #12247")
def test_insert_65k_ck_composite(cql, table2):
    stmt = cql.prepare(f'INSERT INTO {table2} (p1, p2, c1, c2) VALUES (?,?,?,?)')
    big = 'x'*(65*1024)
    with pytest.raises(InvalidRequest, match='Key length'):
        cql.execute(stmt, ['dog', 'cat', big, 'mouse'])
    with pytest.raises(InvalidRequest, match='Key length'):
        cql.execute(stmt, ['dog', 'cat', 'mouse', big])

# The above test_insert_65535_pk tried to reach the limit 65535 bytes in the
# case of a single-component partition key, and checked that see we can
# actually use a string of that size as a key. This test tries to reach the
# limit with a *compound* partition key of two components. Unsurprisingly
# (because of how partition keys are serialized), we see that what is limited
# is the total size the compound partition key, not individual components.
# In Scylla, we can reach 65535-4 bytes of keys, apparently two extra bytes
# (lengths?) are used per component, but in Cassandra the limit we can reach
# with two key components is 65535-6, perhaps there's an additional count
# of components? 
# 
# of components?). It's not at all clear that this is
# correct (see issue #16772) but we'll enshirine it for the moment because
# Cassandra is even more broken in this case :-(
def test_insert_total_compound_pk_ok(cql, table2):
    stmt = cql.prepare(f'INSERT INTO {table2} (p1, p2, c1, c2) VALUES (?,?,?,?)')
    # A total compound pk of size 65535-6 will work (in Scylla, even -4 works)
    length = 65535 - 6
    p1 = random_string(length=100)
    p2 = random_string(length=(length-len(p1)))
    c1 = unique_key_string()  # not particularly long
    c2 = unique_key_string()  # not particularly long
    cql.execute(stmt, [p1, p2, c1, c2])
    stmt = cql.prepare(f'SELECT * FROM {table2} WHERE p1=? AND p2=?')
    assert list(cql.execute(stmt, [p1, p2])) == [(p1, p2, c1, c2)]

@pytest.mark.xfail(reason="Issue #12247")
def test_insert_total_compound_pk_err(cql, table2):
    stmt = cql.prepare(f'INSERT INTO {table2} (p1, p2, c1, c2) VALUES (?,?,?,?)')
    # A total compound pk of size 65536 is definitely too long
    length = 65536
    p1 = random_string(length=length//2)
    p2 = p1
    c1 = unique_key_string()  # not particularly long
    c2 = unique_key_string()  # not particularly long
    # In issue #12247, Scylla returned an internal server error instead of
    # a clean InvalidRequest here.
    with pytest.raises(InvalidRequest, match='Key length'):
        cql.execute(stmt, [p1, p2, c1, c2])

# The case of a composite clustering key (clustering key composed of multiple
# columns) is similar to the case we saw above of compound partition key -
# again the *total* size of the key is limited, not the size of individual
# components. Strangely, this limit wasn't enforced like that in Cassandra 4 -
# it only limited individual components, not the sum - but it was changed in
# Cassandra 5 as an incidental part of adding a Vector type,
# https://github.com/apache/cassandra/commit/ae537abc6494564d7254a2126465522d86b44c1e
# Scylla always limited the total size (see discussion in issue #16772).
#
# As in the the compound partition key case above, also here there is a
# slight difference on what is counted towards this limit: In Scylla two
# components can only reach 65535-4 (i.e., a 2-byte length for each component)
# but in Cassandra the sum of the two components can reach all the way to
# 65535. I suspect this might even be a Cassandra bug (if there's a reason
# why the sum is limited, it should include everything, including length
# bytes).
def test_insert_total_composite_ck_ok(cql, table2):
    stmt = cql.prepare(f'INSERT INTO {table2} (p1, p2, c1, c2) VALUES (?,?,?,?)')
    # A total composite ck of size 65535-4 will work (in Cassandra 5, even
    # 65535 works, I suspect this might be a bug)
    length = 65535 - 4
    c1 = random_string(length=100)
    c2 = random_string(length=(length-len(c1)))
    p1 = unique_key_string()  # not particularly long
    p2 = unique_key_string()  # not particularly long
    cql.execute(stmt, [p1, p2, c1, c2])
    stmt = cql.prepare(f'SELECT * FROM {table2} WHERE p1=? AND p2=?')
    assert list(cql.execute(stmt, [p1, p2])) == [(p1, p2, c1, c2)]

@pytest.mark.xfail(reason="Issue #12247")
def test_insert_total_composite_ck_err(cql, table2):
    stmt = cql.prepare(f'INSERT INTO {table2} (p1, p2, c1, c2) VALUES (?,?,?,?)')
    # A total composite ck of size 65536 is definitely too long
    length = 65536
    c1 = random_string(length=length//2)
    c2 = c1
    p1 = unique_key_string()  # not particularly long
    p2 = unique_key_string()  # not particularly long
    # In issue #12247, Scylla returned an internal server error instead of
    # a clean InvalidRequest here.
    with pytest.raises(InvalidRequest, match='Key length'):
        cql.execute(stmt, [p1, p2, c1, c2])

# The following tests check length limits on parts of collections.
# These tests belong in this file, i.e., are related to the limit on key
# length, because in some cases collection keys are represented internally
# in the same way as clustering keys - see
# docs/architecture/sstable/sstable2/sstable-interpretation.rst.

@pytest.fixture(scope="module")
def table3(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, set_column set<text>, list_column list<text>, map_column map<text,text>") as table:
        yield table

# Ever since CASSANDRA-10374 was fixed in 2015, set values are no longer
# limited to 64KB - and even though internally they behave like column names,
# which in the past were limited to 64KB, now it should work beyond 64KB.
# Reproduces issue #3017 where Scylla did the worst of both worlds: We allowed
# oversized set values, but then crashed on compaction.
# Note that because of #7745, this test needs to use prepared statements
# for the writes.
def test_set_65k(cql, table3):
    stmt = cql.prepare(f'UPDATE {table3} SET set_column = set_column + ? WHERE p = ?')
    p = unique_key_int()
    val = random_string(length=65*1024)
    cql.execute(stmt, [{val}, p])
    results = list(cql.execute(f'SELECT p, set_column FROM {table3} WHERE p={p}'))
    assert results == [(p, SortedSet([val]))]
    # Reproduces issue #3017: After the above code worked well, and correctly
    # supported the more-than-64KB set value, if we now write it to an sstable
    # the compact() will crash Scylla.
    nodetool.flush(cql, table3)
    nodetool.compact(cql, table3)

# For lists, it's even more obvious that the size of the value is not
# limited to 64KB, because it's *not* represented as a column name.
def test_list_65k(cql, table3):
    stmt = cql.prepare(f'UPDATE {table3} SET list_column = list_column + ? WHERE p = ?')
    p = unique_key_int()
    val = random_string(length=65*1024)
    cql.execute(stmt, [[val], p])
    results = list(cql.execute(f'SELECT p, list_column FROM {table3} WHERE p={p}'))
    assert results == [(p, [val])]

# Map keys behave like set values, similar to clustering keys, so it's
# important to check that they really allow more than 64KB, even with
# flush and compact.
# Reproduces #3017 where Scylla did the worst of both worlds: It allowed
# oversized map keys, but then crashed on compaction.
def test_map_65k(cql, table3):
    stmt = cql.prepare(f'UPDATE {table3} SET map_column[?] = ? WHERE p = ?')
    p = unique_key_int()
    key = random_string(length=65*1024)
    val = random_string(length=65*1024)
    cql.execute(stmt, [key, val, p])
    results = list(cql.execute(f'SELECT p, map_column FROM {table3} WHERE p={p}'))
    assert results == [(p, {key: val})]
    # Reproduces issue #3017: After the above code worked well, and correctly
    # supported the more-than-64KB map key, if we now write it to an sstable
    # the compact() will crash Scylla.
    nodetool.flush(cql, table3)
    nodetool.compact(cql, table3)
