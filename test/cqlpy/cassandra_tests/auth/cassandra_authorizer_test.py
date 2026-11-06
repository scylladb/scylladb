# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# This is a translation of CassandraAuthorizerTest.java from Cassandra's
# test/unit/org/apache/cassandra/auth directory.

import re
from ..porting import *
from ...util import unique_name
from .create_and_alter_role_test import use_user, dropped_roles

PASSWORD = "secret"

# Cassandra's assertInvalidMessageNet(), which accepts any exception type.
def assertInvalidMessageNet(session, message, query):
    with pytest.raises(Exception, match=re.escape(message)):
        session.execute(query)

# The Java test uses the fixed role names "parent", "child" and "other".
# We use unique names, to not collide with other tests.
def role_names():
    suffix = unique_name()
    return ("parent_" + suffix, "child_" + suffix, "other_" + suffix)

# Reproduces SCYLLADB-5232: Scylla doesn't have the DESCRIBE permission on a
# specific role (added in Cassandra 4.1, CASSANDRA-16902), so the creator of
# a role doesn't get it, and can't list the role's permissions.
@pytest.mark.xfail(reason="SCYLLADB-5232")
def testListPermissionsOfChildByParent(cql):
    PARENT, CHILD, OTHER = role_names()
    with dropped_roles(cql, PARENT, CHILD, OTHER):
        # create parent role by super user
        cql.execute("CREATE ROLE %s WITH login=true AND password='%s'" % (PARENT, PASSWORD))
        cql.execute("GRANT CREATE ON ALL ROLES TO %s" % PARENT)
        assertRows(cql.execute("LIST ALL PERMISSIONS OF %s" % PARENT),
                   row(PARENT, PARENT, "<all roles>", "CREATE"))

        # create other role by super user
        cql.execute("CREATE ROLE %s WITH login=true AND password='%s'" % (OTHER, PASSWORD))
        assertRows(cql.execute("LIST ALL PERMISSIONS OF %s" % OTHER))

        with use_user(cql, PARENT, PASSWORD) as session:
            # create child role by parent
            session.execute("CREATE ROLE %s WITH login = true AND password='%s'" % (CHILD, PASSWORD))

            # list permissions by parent
            # The order of the permissions in the result isn't defined, and
            # Cassandra and Scylla list them in different orders, so we
            # ignore the order.
            assertRowsIgnoringOrder(session.execute("LIST ALL PERMISSIONS OF %s" % PARENT),
                       row(PARENT, PARENT, "<all roles>", "CREATE"),
                       row(PARENT, PARENT, "<role %s>" % CHILD, "ALTER"),
                       row(PARENT, PARENT, "<role %s>" % CHILD, "DROP"),
                       row(PARENT, PARENT, "<role %s>" % CHILD, "AUTHORIZE"),
                       row(PARENT, PARENT, "<role %s>" % CHILD, "DESCRIBE"))
            assertRows(session.execute("LIST ALL PERMISSIONS OF %s" % CHILD))
            assertInvalidMessageNet(session, "You are not authorized to view %s's permissions" % OTHER,
                                    "LIST ALL PERMISSIONS OF %s" % OTHER)

        with use_user(cql, CHILD, PASSWORD) as session:
            # list permissions by child
            assertInvalidMessageNet(session, "You are not authorized to view %s's permissions" % PARENT,
                                    "LIST ALL PERMISSIONS OF %s" % PARENT)
            assertRows(session.execute("LIST ALL PERMISSIONS OF %s" % CHILD))
            assertInvalidMessageNet(session, "You are not authorized to view %s's permissions" % OTHER,
                                    "LIST ALL PERMISSIONS OF %s" % OTHER)

            # try to create role by child
            assertInvalidMessageNet(session, "User %s does not have sufficient privileges to perform the requested operation" % CHILD,
                                    "CREATE ROLE %s WITH login=true AND password='%s'" % ("nope", PASSWORD))

        with use_user(cql, PARENT, PASSWORD) as session:
            # alter child's role by parent
            session.execute("ALTER ROLE %s WITH login = false" % CHILD)
            session.execute("DROP ROLE %s" % CHILD)

# Reproduces SCYLLADB-5233: LIST ROLES OF and LIST ALL PERMISSIONS OF tell a
# user who isn't authorized to view a role whether it exists. Cassandra fixed
# this only recently (CASSANDRA-21560), after the releases we test, so the
# test is also marked cassandra_bug.
@pytest.mark.xfail(reason="SCYLLADB-5233")
def testListDoesNotLeakRoleExistenceToUnauthorizedUsers(cql, cassandra_bug):
    _, CHILD, OTHER = role_names()
    nonexistent_role = "nonexistent_role_" + unique_name()
    with dropped_roles(cql, CHILD, OTHER):
        # A low-privilege login role with no DESCRIBE on the root roles resource, plus an unrelated
        # role it is not authorized to view.
        cql.execute("CREATE ROLE %s WITH login=true AND password='%s'" % (CHILD, PASSWORD))
        cql.execute("CREATE ROLE %s WITH login=true AND password='%s'" % (OTHER, PASSWORD))

        # An authorized caller (superuser) still gets the friendly existence error for a missing role.
        assertInvalidMessageNet(cql, "doesn't exist", "LIST ROLES OF " + nonexistent_role)
        assertInvalidMessageNet(cql, "doesn't exist", "LIST ALL PERMISSIONS OF " + nonexistent_role)

        with use_user(cql, CHILD, PASSWORD) as session:
            # Unauthorized caller: a non-existent role and an existing-but-unviewable role must be
            # indistinguishable - both "not authorized", never "doesn't exist".
            assertInvalidMessageNet(session, "You are not authorized to view roles granted to " + nonexistent_role,
                                    "LIST ROLES OF " + nonexistent_role)
            assertInvalidMessageNet(session, "You are not authorized to view roles granted to %s" % OTHER,
                                    "LIST ROLES OF %s" % OTHER)

            assertInvalidMessageNet(session, "You are not authorized to view %s's permissions" % nonexistent_role,
                                    "LIST ALL PERMISSIONS OF " + nonexistent_role)
            assertInvalidMessageNet(session, "You are not authorized to view %s's permissions" % OTHER,
                                    "LIST ALL PERMISSIONS OF %s" % OTHER)
