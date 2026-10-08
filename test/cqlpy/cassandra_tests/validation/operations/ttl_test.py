# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1


from ...porting import *
from ....util import is_scylla, is_cassandra_older_than
from cassandra.protocol import ConfigurationException
from test.pylib.skip_types import skip_env

MAX_TTL = 20 * 365 * 24 * 60 * 60 # 20 years in seconds

def testTTLPerRequestLimit(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, i int)") as table:
        # insert with low TTL should not be denied
        execute(cql, table, "INSERT INTO %s (k, i) VALUES (1, 1) USING TTL ?", 10)

        assert_invalid_message(cql, table, "ttl is too large.", "INSERT INTO %s (k, i) VALUES (1, 1) USING TTL ?", MAX_TTL + 1)

        assert_invalid_message(cql, table, "A TTL must be greater or equal to 0", "INSERT INTO %s (k, i) VALUES (1, 1) USING TTL ?", -1)

        execute(cql, table, "TRUNCATE %s")

        # insert with low TTL should not be denied
        execute(cql, table, "UPDATE %s USING TTL ? SET i = 1 WHERE k = 2", 5)

        assert_invalid_message(cql, table, "ttl is too large.", "UPDATE %s USING TTL ? SET i = 1 WHERE k = 2", MAX_TTL + 1)

        assert_invalid_message(cql, table, "A TTL must be greater or equal to 0", "UPDATE %s USING TTL ? SET i = 1 WHERE k = 2", -1)
