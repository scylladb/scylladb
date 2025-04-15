#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.pylib.suite.unit import UnitTestSuite
from test.pylib.suite.boost import BoostTest


class LdapTestSuite(UnitTestSuite):
    """TestSuite for ldap unit tests"""

    async def create_test(self, shortname, casename, suite, args):
        test = LdapTest(self.next_id((shortname, self.suite_key)), shortname, suite, args)
        self.tests.append(test)

    def junit_tests(self):
        """Ldap tests produce an own XML output, so are not included in a junit report"""
        return []


class LdapTest(BoostTest):
    """A unit test which can produce its own XML output, and needs an ldap server"""

    def __init__(self, test_no, shortname, args, suite):
        super().__init__(test_no, shortname, args, suite, None, False, None)
