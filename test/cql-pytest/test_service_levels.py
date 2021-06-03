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
# Tests for the service levels infrastructure. Service levels can be attached
# to roles in order to apply various role-specific parameters, like timeouts.
#############################################################################

from contextlib import contextmanager
from util import unique_name, new_test_table

from cassandra.protocol import InvalidRequest, ReadTimeout
from cassandra.util import Duration

import pytest

@contextmanager
def new_service_level(cql):
    try:
        sl = f"sl_{unique_name()}"
        cql.execute(f"CREATE SERVICE LEVEL {sl}")
        cql.execute(f"ATTACH SERVICE LEVEL {sl} TO {cql.cluster.auth_provider.username}")
        yield sl
    finally:
        cql.execute(f"DETACH SERVICE LEVEL FROM {cql.cluster.auth_provider.username}")
        cql.execute(f"DROP SERVICE LEVEL IF EXISTS {sl}")

# Test that setting service level timeouts correctly sets the timeout parameter
def test_set_service_level_timeouts(scylla_only, cql):
    with new_service_level(cql) as sl:
        cql.execute(f"ALTER SERVICE LEVEL {sl} WITH timeout = 575ms")
        res = cql.execute(f"LIST SERVICE LEVEL {sl}")
        assert res.one().timeout == Duration(0, 0, 575000000)
        cql.execute(f"ALTER SERVICE LEVEL {sl} WITH timeout = 2h")
        res = cql.execute(f"LIST SERVICE LEVEL {sl}")
        assert res.one().timeout == Duration(0, 0, 2*60*60*10**9)
        cql.execute(f"ALTER SERVICE LEVEL {sl} WITH timeout = null")
        res = cql.execute(f"LIST SERVICE LEVEL {sl}")
        assert not res.one().timeout

# Test that incorrect service level timeout values result in an error
def test_validate_service_level_timeouts(scylla_only, cql):
    with new_service_level(cql) as sl:
        for incorrect in ['1ns', '-5s','writetime', '1second', '10d', '5y', '7', '0']:
            print(f"Checking {incorrect}")
            with pytest.raises(Exception):
                cql.execute(f"ALTER SERVICE LEVEL {sl} WITH timeout = {incorrect}")

# Test that the service level is correctly attached to the user's role
def test_attached_service_level(scylla_only, cql):
    with new_service_level(cql) as sl:
        res_one = cql.execute(f"LIST ATTACHED SERVICE LEVEL OF {cql.cluster.auth_provider.username}").one()
        assert res_one.role == cql.cluster.auth_provider.username and res_one.service_level == sl
        res_one = cql.execute(f"LIST ALL ATTACHED SERVICE LEVELS").one()
        assert res_one.role == cql.cluster.auth_provider.username and res_one.service_level == sl

# Test that declaring service level workload types is possible
def test_set_workload_type(scylla_only, cql):
    with new_service_level(cql) as sl:
        res = cql.execute(f"LIST SERVICE LEVEL {sl}")
        assert not res.one().workload_type
        for wt in ['interactive', 'batch']:
            cql.execute(f"ALTER SERVICE LEVEL {sl} WITH workload_type = '{wt}'")
            res = cql.execute(f"LIST SERVICE LEVEL {sl}")
            assert res.one().workload_type == wt

# Test that workload type input is validated
def test_set_invalid_workload_types(scylla_only, cql):
    with new_service_level(cql) as sl:
        for incorrect in ['', 'i', 'b', 'dog', 'x'*256]:
            print(f"Checking {incorrect}")
            with pytest.raises(Exception):
                cql.execute(f"ALTER SERVICE LEVEL {sl} WITH workload_type = '{incorrect}'")

# Test that resetting an already set workload type by assigning NULL to it works fine
def test_reset_workload_type(scylla_only, cql):
    with new_service_level(cql) as sl:
        cql.execute(f"ALTER SERVICE LEVEL {sl} WITH workload_type = 'interactive'")
        res = cql.execute(f"LIST SERVICE LEVEL {sl}")
        assert res.one().workload_type == 'interactive'
        cql.execute(f"ALTER SERVICE LEVEL {sl} WITH workload_type = null")
        res = cql.execute(f"LIST SERVICE LEVEL {sl}")
        assert not res.one().workload_type