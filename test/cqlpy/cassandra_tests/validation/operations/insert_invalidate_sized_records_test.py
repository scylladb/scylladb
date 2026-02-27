# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 1737efb050e1da9576d47287ebc6f1cc3073a8a0
#
# The original Apache Cassandra license:
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

from ...porting import *

LARGE_BLOB = b'x' * (2**16)
MEDIUM_BLOB = b'x' * (2**15 + 9)

@pytest.mark.xfail(reason="issue #12247")
def testsingleValuePk(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a blob PRIMARY KEY)") as table:
        # reproduces #12247:
        with pytest.raises(InvalidRequest, match=f'Key length of {len(LARGE_BLOB)} is longer than maximum of 65535'):
             execute(cql, table, "INSERT INTO %s (a) VALUES (?)", LARGE_BLOB)

        # null / empty checks
        with pytest.raises(InvalidRequest, match='Invalid null value.* for column a'):
            execute(cql, table, "INSERT INTO %s (a) VALUES (?)", None)
        with pytest.raises(InvalidRequest, match='Key may not be empty'):
            execute(cql, table, "INSERT INTO %s (a) VALUES (?)", b'')

@pytest.mark.xfail(reason="issue #12247")
# Currently fails on Cassandra due to CASSANDRA-19270
def testcompositeValuePk(cql, test_keyspace, cassandra_bug):
    with create_table(cql, test_keyspace, "(a blob, b blob, PRIMARY KEY ((a, b)))") as table:
        # sum of columns is too large
        with pytest.raises(InvalidRequest, match=f'Key length of {len(MEDIUM_BLOB)*2} is longer than maximum of 65535'):
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", MEDIUM_BLOB, MEDIUM_BLOB)

        # single column is too large
        # Fails on Cassandra for an unknown reason, see CASSANDRA-19270
        with pytest.raises(InvalidRequest, match=f'Key length of {len(MEDIUM_BLOB)+len(LARGE_BLOB)} is longer than maximum of 65535'):
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", MEDIUM_BLOB, LARGE_BLOB)

        # null / empty checks
        # this is an inconsistent behavior... null is blocked by org.apache.cassandra.db.MultiCBuilder.OneClusteringBuilder.addElementToAll
        # but this does not count empty as null, and doesn't check for this case...  We have a requirement in cqlsh that empty is allowed when
        # user opts-in to allow it (NULL='-'), so we will find that null is blocked, but empty is allowed!
        # Fails on Cassandra for an unknown reason, see CASSANDRA-19270
        with pytest.raises(InvalidRequest, match='Invalid null value.* for column a'):
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", None, None)
        # Fails on Cassandra for an unknown reason, see CASSANDRA-19270
        with pytest.raises(InvalidRequest, match='Invalid null value.* for column b'):
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", MEDIUM_BLOB, None)
        # Fails on Cassandra for an unknown reason, see CASSANDRA-19270
        with pytest.raises(InvalidRequest, match='Invalid null value.* for column a'):
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", None, MEDIUM_BLOB)

        # empty is allowed when composite partition columns...
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", b'', b'')
        execute(cql, table, "TRUNCATE %s")

        execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", MEDIUM_BLOB, b'')
        execute(cql, table, "TRUNCATE %s")

        execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", b'', MEDIUM_BLOB)

@pytest.mark.xfail(reason="issue #12247")
def testsingleValueClustering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a blob, b blob, PRIMARY KEY (a, b))") as table:
        # reproduces #12247:
        with pytest.raises(InvalidRequest, match=f'Key length of {len(LARGE_BLOB)} is longer than maximum of 65535'):
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", MEDIUM_BLOB, LARGE_BLOB)

        # null / empty checks
        with pytest.raises(InvalidRequest, match='Invalid null value.* for column b'):
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", MEDIUM_BLOB, None)

        # org.apache.cassandra.db.MultiCBuilder.OneClusteringBuilder.addElementToAll defines "null" differently than most of the code
        # most of the code defines null as:
        #   value == null || accessor.isEmpty(value)
        # but the code defines null as
        #   value == null
        # In CASSANDRA-18504 a new isNull method was added to the type, as blob and text both "should" allow empty, but this scattered null logic doesn't allow...
        # For backwards compatability reasons, need to keep empty support
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", MEDIUM_BLOB, b'')

@pytest.mark.xfail(reason="issue #12247")
def testcompositeValueClustering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a blob, b blob, c blob, PRIMARY KEY (a, b, c))") as table:
        # sum of columns is too large
        # reproduces #12247:
        with pytest.raises(InvalidRequest, match=f'Key length of {len(MEDIUM_BLOB)*2} is longer than maximum of 65535'):
            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", MEDIUM_BLOB, MEDIUM_BLOB, MEDIUM_BLOB)

        # single column is too large
        # the logic prints the total clustering size and not the single column's size that was too large
        # reproduces #12247:
        with pytest.raises(InvalidRequest, match=f'Key length of {len(MEDIUM_BLOB)+len(LARGE_BLOB)} is longer than maximum of 65535'):
            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", MEDIUM_BLOB, MEDIUM_BLOB, LARGE_BLOB)

@pytest.mark.xfail(reason="issue #8627")
def testsingleValueIndex(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a blob, b blob, PRIMARY KEY (a))") as table:
        execute(cql, table, "CREATE INDEX single_value_index ON %s (b)")
        with pytest.raises(InvalidRequest, match=re.escape(f'Cannot index value of size {len(LARGE_BLOB)} for index single_value_index on {table}(b) (maximum allowed size=65535)')):
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", MEDIUM_BLOB, LARGE_BLOB)
