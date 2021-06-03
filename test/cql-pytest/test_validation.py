# -*- coding: utf-8 -*-
# Copyright 2020-present ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

#############################################################################
# Tests for the type validation in CQL. For example, it should not be
# possible to insert a non-ASCII string into a column of type "ascii", or an
# invalid UTF-8 string into a column of type "text".
#############################################################################

import pytest
import re
import random
from cassandra.protocol import SyntaxException, AlreadyExists, InvalidRequest, ConfigurationException, ReadFailure
from util import unique_name

@pytest.fixture(scope="session")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (k int primary key, a ascii, t text)")
    yield table
    cql.execute("DROP TABLE " + table)

#############################################################################
# The following tests verify that inserting an invalid UTF-8 string into a
# "text" column is forbidden. There are multiple ways in which we can try to
# inject an invalid UTF-8 into a request, and each of them exercises a
# different code path so should check all of them below.

# Examples of invalid UTF-8 strings, with comments on why they are invalid.
# Note that currently, Scylla's UTF-8 parser is stricter than Cassandra's,
# and rejects the following cases which Cassandra does *not* reject:
# 1. \xC0\x80 as another non-minimal representation of null (other non-
#    minimal encodings are rejected as expected)
# 2. Characters beyond the official Unicode range.
# 3. UTF-16 surrogates (which are not valid UTF-8).
bad_utf8 = [
    # Non-minimal representations (in this case of 0x00) are not valid UTF-8
    b'\xC0\x80',           # NOTE: not recognized invalid by Cassandra
    b'\xE0\x80\x80',
    b'\xF0\x80\x80\x80',
    # 0x80-0xBF are continuation bytes - can never be the first byte
    b'\x80',
    b'\xBF',
    # 0xC0-0xDF indicate the first byte of a 2-byte sequence. The next byte
    # must be a continuation byte (0x80-0xBF).
    # Above we also checked that \xC0\x80 is also invalid (non-minal
    # representation), but more generally any sequence starting in C0 or
    # C1 will be non-minimal (they can only encode characters between
    # 00-7F), so the characters C0 or C1 can never appear in valid UTF8.
    b'\xC0',
    b'\xC0\x7F',
    b'\xC0\xC0',
    b'\xC0\x81',
    b'\xC0\xBE',
    b'\xC1\x81',
    b'\xC1\xBE',
    b'\xC2',
    b'\xC2\x7F',
    b'\xC2\xC0',
    # 0xE0-0xEF indicate the first byte of a 3-byte sequence. The next two
    # bytes must be continuation bytes (0x80-0xBF).
    b'\xE0',
    b'\xE0\xA0',
    b'\xE0\xA0\x79',
    b'\xE0\xA0\xC0',
    # 0xF0-0xF4 indicate the first byte of a 4-byte sequence. Note that 0xF4
    # is the last possible byte in this range because unicode ends at 0x10FFFF.
    # The next three bytes must be continuation bytes (0x80-0xBF).
    b'\xF0',
    b'\xF0\x90',
    b'\xF0\x90\x81',
    b'\xF0\x90\x81\x79',
    b'\xF0\x90\x81\xC0',
    # Actually, the unicode range ends in the middle of 0xF4.
    b'\xF4\x90\x80\x80',     # NOTE: not recognized invalid by Cassandra
    # 0xF5-0xFF cannot be the first byte because of Unicode's range as
    # explained above. These bytes can't appear inside any UTF8.
    b'\xF5',
    b'\xF5\x81',
    b'\xF5\x81\x81',
    #b'\xF5\x81\x81\x81',
    # UTF-16 surrogates are not valid UTF-8
    b'\xED\xA0\x80', # NOTE: not recognized invalid by Cassandra
    b'\xED\xAF\xBF', # NOTE: not recognized invalid by Cassandra
    b'\xED\xB0\x80', # NOTE: not recognized invalid by Cassandra
    b'\xED\xBF\xBF', # NOTE: not recognized invalid by Cassandra
]
# Some examples of good UTF-8 strings, as byte strings. It is important that
# all the tests below check that good UTF-8 works, not just that bad UTF-8
# fails. That confirms that the tests can actually tell apart good and bad
# results - and aren't just buggy and always fail! (we had such a bug in the
# first version of this test...)
good_utf8 = [
    # ASCII
    b'hello',
    # Null is fine
    b'\x00',
    # Some Hebrew :-)
    'שלום'.encode('utf-8'),
    # 0xC0-0xDF indicate the first byte of a 2-byte sequence. The next byte
    # must be a continuation byte (0x80-0xBF). As explained above, because of
    # minimal encoding requirement, 0xC0 and 0xC1 aren't actually legal, but
    # 0xC2 is:
    b'\xC2\x80',
    b'\xC2\xBF',
    # 0xE0-0xEF indicate the first byte of a 3-byte sequence. The next two
    # bytes must be continuation bytes (0x80-0xBF), but again for the encoding
    # to be minimal, when the first byte is the first possible one (E0) the
    # second byte needs to be A0 or above.
    b'\xE0\xA0\x80',
    b'\xE0\xA0\xBF',
    b'\xE0\xBF\x80',
    b'\xE0\xBF\xBF',
    b'\xEF\x80\x80',
    b'\xEF\x80\xBF',
    b'\xEF\xBF\x80',
    b'\xEF\xBF\xBF',
    # 0xF0-0xF4 indicate the first byte of a 4-byte sequence.
    # The next three bytes must be continuation bytes (0x80-0xBF).
    # Again, because of minimal encoding, the earlier sequences with these
    # bytes aren't actually allowed.
    b'\xF0\x90\x80\x80', # the lowest sequence allowed because of minimalism
    b'\xF0\x90\x80\xBF',
    b'\xF0\x90\xBF\x80',
    b'\xF0\x90\xBF\xBF',
    b'\xF0\xBF\x80\x80',
    b'\xF0\xBF\x80\xBF',
    b'\xF0\xBF\xBF\x80',
    b'\xF0\xBF\xBF\xBF',
    b'\xF4\x80\x80\x80',
    b'\xF4\x80\x80\xBF',
    b'\xF4\x80\xBF\x80',
    b'\xF4\x80\xBF\xBF',
    b'\xF4\x8F\xBF\xBF', # the highest allowed sequence because of unicode range
]

# 1. We can pass a string using the blob representation of its bytes (0x...)
#    and the builtin blobAsText function. This function converts the blob into
#    a string assuming it has UTF-8 encoding, and should complain when it's
#    invalid. The error Cassandra and Scylla print in this case looks like
#    "In call to function blobastext [or system.blobastext], value 0xc0 is
#    not a valid binary representation for type text".
# Note that currently, Scylla's UTF-8 parser is stricter than Cassandra's
# (see comment above listing the relevant cases), so this test, as all tests
# using the bad_utf8 array, will fail on Cassandra.
def test_validation_utf8_as_blob(scylla_only, cql, table1):
    cmd = "INSERT INTO {} (k, t) VALUES (1, blobAsText(0x{}))"
    for b in good_utf8:
        print(b)
        cql.execute(cmd.format(table1, b.hex()))
        # verify that the successfully-written value can be read correctly
        results = list(cql.execute(f"SELECT k, t FROM {table1} WHERE k=1"))
        assert len(results) == 1
        assert results[0].k == 1 and results[0].t.encode('utf-8') == b
    for b in bad_utf8:
        print(b)
        with pytest.raises(InvalidRequest, match='not a valid binary representation for type text'):
            cql.execute(cmd.format(table1, b.hex()))

# 2. We can pass the string as a bound argument to a prepared  statement.
#    Convincing Python to put an invalid UTF-8 here is not trivial, because
#    the driver outputs strings, which are not supposed to be able to contain
#    invalid UTF-8. We use a rather funky workaround here use a wrapped
#    version ("surrogateescape") of bad UTF-8, and monkey-patch the driver to
#    unescape it when converting it back to bytes.
# Note that currently, Scylla's UTF-8 parser is stricter than Cassandra's
# (see comment above listing the relevant cases), so this test, as all tests
# using the bad_utf8 array, will fail on Cassandra.
def test_validation_utf8_bound_column(scylla_only, cql, table1):
    import cassandra.cqltypes
    orig_serialize = cassandra.cqltypes.UTF8Type.serialize
    def myserialize(ustr, protocol_version):
        return ustr.encode('utf-8', errors='surrogateescape')
    cassandra.cqltypes.UTF8Type.serialize = myserialize
    try:
        stmt = cql.prepare(f'INSERT INTO {table1} (k, t) VALUES (1, ?)')
        for b in good_utf8:
            print(b)
            cql.execute(stmt, [b.decode()])
            results = list(cql.execute(f"SELECT k, t FROM {table1} WHERE k=1"))
            assert len(results) == 1
            assert results[0].k == 1 and results[0].t.encode('utf-8') == b
        for b in bad_utf8:
            print(b)
            # Scylla prints "Exception while binding column t: marshaling error:
            # Validation failed - non-UTF8 character in a UTF8 string, at byte
            # offset 0". Cassandra prints "String didn't validate.". The only
            # thing in common is the word 'validat' :-)
            with pytest.raises(InvalidRequest, match=re.compile('validat', re.IGNORECASE)):
                cql.execute(stmt, [b.decode(errors='surrogateescape')])
    finally:
        cassandra.cqltypes.UTF8Type.serialize = orig_serialize

# 3. We can also insert the bad UTF-8 as part of the request string itself.
#    This will make the entire request string invalid UTF-8, not just the
#    value to be inserted, so Scylla and Cassandra should complain that the
#    entire request is bad - not just the inserted value.
# FIXME: this test is INCOMPLETE! It's very hard to get Python to output an
# illegal UTF-8 string in this case, and I gave up. Unlike the prepared-
# statement case above where it was easy to find and monkey-patch the
# function responsible for converting the string to bytes, in this case it
# was harder to find this function and I gave up.
def test_validation_utf8_query(cql, table1):
    for b in good_utf8:
        s = b.decode('utf-8')
        print(s)
        cql.execute(f"INSERT INTO {table1} (k, t) VALUES (1, '{s}')")
        results = list(cql.execute(f"SELECT k, t FROM {table1} WHERE k=1"))
        assert len(results) == 1
        assert results[0].k == 1 and results[0].t == s
    # FIXME: Need to figure out the appropriate monkey-patching or other
    # trick to make the following work (i.e., pass the invalid string to
    # the server, and let the server - not the driver - fail.
    # for b in bad_utf8:
    #  print(b)
    #  cql.execute("INSERT INTO {} (k, t) VALUES (1, '{}')".format(table1, b.decode(errors='surrogateescape')))

# 4. The invalid UTF-8 can be the result of a user-defined function in Lua,
#    which can easily produce invalid UTF-8. This is a Scylla-only test,
#    because Cassandra does not have user-defined functions in Lua.
# Notes:
# * This test doesn't try to insert data like other tests - the UTF-8
#   conversion attempt is done during a select.
def test_validation_utf8_from_lua(scylla_only, cql, test_keyspace, table1):
    # Create one row that the Lua functions below will run on
    cql.execute(f"INSERT INTO {table1} (k, a) VALUES (1, 'hello')")
    # This test is significantly slower than the rest, because we run the
    # CREATE FUNCTION operation separately for each tested string, and it is
    # very slow. So we only try a random sample of the good and bad strings.
    # TODO: can we have a faster Lua test, which uses a single function only
    # with different parameters, instead of multiple functions?
    for b in random.sample(good_utf8, 3):
        fname = unique_name()
        # translate byte 0xAB into the string "\xAB"
        b_lua = ''.join([('\\x%02x' % c) for c in b])
        print(b_lua)
        cql.execute(f"CREATE FUNCTION {test_keyspace}.{fname}(k int) CALLED ON NULL INPUT RETURNS text LANGUAGE Lua AS 'return \"{b_lua}\"';")
        results = list(cql.execute(f"SELECT {fname}(k) FROM {table1} WHERE k=1"))
        assert len(results) == 1
        assert len(results[0]) == 1
        assert results[0][0].encode('utf-8') == b
        cql.execute(f"DROP FUNCTION {test_keyspace}.{fname}")
    for b in random.sample(bad_utf8, 3):
        fname = unique_name()
        b_lua = ''.join([('\\x%02x' % c) for c in b])
        print(b_lua)
        cql.execute(f"CREATE FUNCTION {test_keyspace}.{fname}(k int) CALLED ON NULL INPUT RETURNS text LANGUAGE Lua AS 'return \"{b_lua}\"';")
        with pytest.raises(InvalidRequest, match='value is not valid utf8'):
            cql.execute(f"SELECT {fname}(k) FROM {table1} WHERE k=1")
        cql.execute(f"DROP FUNCTION {test_keyspace}.{fname}")

#############################################################################
# The following tests verify that inserting a non-ASCII string into an
# "ascii" column should be forbidden. There are multiple ways in which we
# can try to inject non-ASCII into a request, and each of them exercises
# a different code path so should check all of them below.

# Examples of non-ASCII and ASCII byte strings:
bad_ascii = [
    'שלום',
]
good_ascii = [
    'hello',
    # A null is considered valid ASCII
    '\x00',
]

# 1. We can pass a string using the blob representation of its bytes (0x...)
#    and the builtin blobAsAscii function. This function converts the blob into
#    a string assuming it has ASCII encoding, and should complain when it's
#    invalid. The error Cassandra and Scylla print in this case looks like
#    "In call to function blobastext [or system.blobastext], value 0xc0 is
#    not a valid binary representation for type ascii".
def test_validation_ascii_as_blob(cql, table1):
    cmd = "INSERT INTO {} (k, t) VALUES (1, blobAsAscii(0x{}))"
    for s in good_ascii:
        print(s)
        cql.execute(cmd.format(table1, s.encode().hex()))
        results = list(cql.execute(f"SELECT k, t FROM {table1} WHERE k=1"))
        assert len(results) == 1
        assert results[0].k == 1 and results[0].t == s
    for s in bad_ascii:
        print(s)
        with pytest.raises(InvalidRequest, match='not a valid binary representation for type ascii'):
            cql.execute(cmd.format(table1, s.encode().hex()))

# 2. We can pass the string as a bound argument to a prepared statement.
#    Again, a non-ASCII one should produce an error
def test_validation_ascii_bound_column(cql, table1):
    # Unfortunately, the Python CQL driver checks the ASCII encoding itself
    # in cassandra.cqltypes.AsciiType.serialize, so we need to monkey-patch
    # this function to avoid the client-side checking.
    import cassandra.cqltypes
    orig_serialize = cassandra.cqltypes.AsciiType.serialize
    def myserialize(ustr, protocol_version):
        # The original implementation has encode('ascii') here
        return ustr.encode('utf-8')
    cassandra.cqltypes.AsciiType.serialize = myserialize
    try:
        stmt = cql.prepare(f'INSERT INTO {table1} (k, a) VALUES (1, ?)')
        for s in good_ascii:
            print(s)
            cql.execute(stmt, [s])
            results = list(cql.execute(f"SELECT k, a FROM {table1} WHERE k=1"))
            assert len(results) == 1
            assert results[0].k == 1 and results[0].a == s
        for s in bad_ascii:
            print(s)
            # Scylla prints "Exception while binding column t: marshaling error:
            # Validation failed - non-ASCII character in an ASCII string".
            # Cassandra prints "Invalid byte for ascii: -41". The only thing
            # in common is the word 'ascii' in a different case...
            with pytest.raises(InvalidRequest, match=re.compile('ascii', re.IGNORECASE)):
                cql.execute(stmt, [s])
    finally:
        cassandra.cqltypes.AsciiType.serialize = orig_serialize

# 3. Insert the non-ASCII string as an integral part of the request string
#    itself. The request itself is valid (it just needs to be UTF-8), but
#    the non-ASCII insertion should be refused.
# Reproduces issue #5421.
def test_validation_ascii_query(cql, table1):
    for s in good_ascii:
        print(s)
        cql.execute(f"INSERT INTO {table1} (k, a) VALUES (1, '{s}')")
        results = list(cql.execute(f"SELECT k, a FROM {table1} WHERE k=1"))
        assert len(results) == 1
        assert results[0].k == 1 and results[0].a == s
    for s in bad_ascii:
        print(s)
        # Scylla prints "marshaling error: Value not compatible with type
        # org.apache.cassandra.db.marshal.AsciiType: '...'". Cassandra prints
        # "Invalid ASCII character in string literal". The only thing in common
        # is the word "ascii"...
        with pytest.raises(InvalidRequest, match=re.compile('ascii', re.IGNORECASE)):
            cql.execute(f"INSERT INTO {table1} (k, a) VALUES (1, '{s}')")

# 4. The invalid ASCII can be the result of a user-defined function in Lua,
#    which can easily produce invalid ASCII. This is a Scylla-only test,
#    because Cassandra does not have user-defined functions in Lua.
def test_validation_ascii_from_lua(scylla_only, cql, test_keyspace, table1):
    # Create one row that the Lua function below will run on
    cql.execute(f"INSERT INTO {table1} (k, a) VALUES (1, 'hello')")
    fname = unique_name()
    cql.execute(f"CREATE FUNCTION {test_keyspace}.{fname}(k int) CALLED ON NULL INPUT RETURNS ascii LANGUAGE Lua AS 'return \"שלום\"';")
    with pytest.raises(InvalidRequest, match='value is not valid ascii'):
        cql.execute(f"SELECT {fname}(k) FROM {table1} WHERE k=1")
    cql.execute(f"DROP FUNCTION {test_keyspace}.{fname}")
    cql.execute(f"CREATE FUNCTION {test_keyspace}.{fname}(k int) CALLED ON NULL INPUT RETURNS ascii LANGUAGE Lua AS 'return \"hello\"';")
    results = list(cql.execute(f"SELECT {fname}(k) FROM {table1} WHERE k=1"))
    assert len(results) == 1
    assert len(results[0]) == 1
    assert results[0][0] == 'hello'
