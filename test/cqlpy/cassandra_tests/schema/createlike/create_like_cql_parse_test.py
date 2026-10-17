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

# Incorrect use of create table like cql
unSupportedCqls = [
        "CREATE TABLE ta (a int primary key, b int) LIKE tb", # useless column information
        "CREATE TABLE ta (a int primary key, b int MASKED WITH DEFAULT) LIKE tb",
        "CREATE TABLE IF NOT EXISTS LIKE tb", # missing target table
        "CREATE TABLE IF NOT EXISTS LIKE tb WITH compression = { 'enabled' : 'false'}",
        "CREATE TABLE ta IF NOT EXISTS LIKE ", # missing source table
        "CREATE TABLE ta LIKE WITH id = '123-111'" # id is not supported
]

# The original Java test only parses each statement, without executing it.
# The closest we can do through CQL is to prepare the statement - which
# parses it without executing it.
def testUnsupportedCqlParse(cql):
    for stmt in unSupportedCqls:
        with pytest.raises(SyntaxException):
            cql.prepare(stmt)
