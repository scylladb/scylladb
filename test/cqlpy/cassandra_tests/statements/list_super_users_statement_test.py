# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from ..porting import *
from ...util import new_user, new_session
import time
from cassandra.protocol import Unauthorized

# The Java test sets Cassandra's roles, permissions and credentials caches'
# validity to 0, and also invalidates the roles cache (through Cassandra's
# internal Roles.cache) after granting roles. We can't do this through CQL,
# so after granting roles we retry a check until it succeeds or a timeout
# passes, in case the old roles are still cached.
def eventually_rows(fun, expected, timeout_s=10):
    deadline = time.time() + timeout_s
    while True:
        rows = sorted(r[0] for r in fun())
        if rows == sorted(expected) or time.time() > deadline:
            assert rows == sorted(expected)
            return
        time.sleep(0.1)

def listSuperusers(session):
    return session.execute("list superusers")

# The tests testNoRoles, testGetAllRolesReturnsNull and
# testListSuperUserStatementToString were not translated, because they
# test Cassandra's internal ListSuperUsersStatement and Roles classes
# directly, through Java APIs.

# Reproduces SCYLLADB-5190 (LIST SUPERUSERS statement).
@pytest.mark.xfail(reason="SCYLLADB-5190")
def testAcquiredSuperUsers(cql, new_to_cassandra_6):
    # (useSuperUser() - the cql fixture is the superuser "cassandra")
    assert_rows(listSuperusers(cql), row("cassandra"))

    with new_user(cql) as role1, new_user(cql) as role11, new_user(cql) as role2:
        assert_rows(listSuperusers(cql), row("cassandra"))

        cql.execute(f"grant cassandra to {role1}")
        cql.execute(f"grant {role1} to {role11}")
        eventually_rows(lambda: listSuperusers(cql), ["cassandra", role1, role11])

        with new_session(cql, role1) as session:
            eventually_rows(lambda: listSuperusers(session), ["cassandra", role1, role11])

        with new_session(cql, role11) as session:
            eventually_rows(lambda: listSuperusers(session), ["cassandra", role1, role11])

# Reproduces SCYLLADB-5190 (LIST SUPERUSERS statement).
@pytest.mark.xfail(reason="SCYLLADB-5190")
def testNonSuperUserDescribePermission(cql, new_to_cassandra_6):
    with new_user(cql) as nonsuper:
        with new_session(cql, nonsuper) as session:
            with pytest.raises(Unauthorized, match="You are not authorized to view superuser details"):
                listSuperusers(session)

        cql.execute(f"GRANT DESCRIBE ON ALL ROLES to {nonsuper}")

        with new_session(cql, nonsuper) as session:
            # verify list command returned non-empty results
            deadline = time.time() + 10
            while True:
                try:
                    result = list(listSuperusers(session))
                    break
                except Unauthorized:
                    # the old permissions may still be cached
                    if time.time() > deadline:
                        raise
                    time.sleep(0.1)
            assert result
