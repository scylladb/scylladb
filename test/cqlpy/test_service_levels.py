# -*- coding: utf-8 -*-
# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#############################################################################
# Tests for the service levels infrastructure. Service levels can be attached
# to roles in order to apply various role-specific parameters, like timeouts.
#############################################################################

from contextlib import contextmanager, ExitStack
from .util import unique_name, new_test_table, new_user
from .rest_api import scylla_inject_error

from cassandra.protocol import InvalidRequest, ReadTimeout
from cassandra.util import Duration

import pytest
import time

@contextmanager
def new_service_level(cql, timeout=None, workload_type=None, shares=None, role=None):
    params = ""
    if timeout or workload_type or shares:
        params = "WITH "
        first = True

        if timeout:
            if first:
                first = False
            else:
                params += "AND "
            params += f"timeout = {timeout} "
        if workload_type:
            if first:
                first = False
            else:
                params += "AND "
            params += f"workload_type = '{workload_type}' "
        if shares:
            if first:
                first = False
            else:
                params += "AND "
            params += f"shares = {shares} "

    attach_to = role if role else cql.cluster.auth_provider.username

    try:
        sl = f"sl_{unique_name()}"
        cql.execute(f"CREATE SERVICE LEVEL {sl} {params}")
        cql.execute(f"ATTACH SERVICE LEVEL {sl} TO {attach_to}")
        yield sl
    finally:
        cql.execute(f"DETACH SERVICE LEVEL FROM {attach_to}")
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

def test_list_effective_service_level(scylla_only, cql):
    sl1 = "sl1"
    sl2 = "sl2"
    timeout = "10s"
    workload_type = "batch"

    with new_user(cql, "r1") as r1:
        with new_user(cql, "r2") as r2:
            with new_service_level(cql, timeout=timeout, role=r1) as sl1:
                with new_service_level(cql, workload_type=workload_type, role=r2) as sl2:
                    cql.execute(f"GRANT {r2} TO {r1}")

                    list_r1 = cql.execute(f"LIST EFFECTIVE SERVICE LEVEL OF {r1}")
                    for row in list_r1:
                        if row.service_level_option == "timeout":
                            assert row.effective_service_level == sl1
                            assert row.value == "10s"
                        if row.service_level_option == "workload_type":
                            assert row.effective_service_level == sl2
                            assert row.value == "batch"

                    list_r2 = cql.execute(f"LIST EFFECTIVE SERVICE LEVEL OF {r2}")
                    for row in list_r2:
                        if row.service_level_option == "timeout":
                            assert row.effective_service_level == sl2
                            assert row.value == None
                        if row.service_level_option == "workload_type":
                            assert row.effective_service_level == sl2
                            assert row.value == "batch"

def test_list_effective_service_level_shares(scylla_only, cql):
    sl1 = "sl1"
    sl2 = "sl2"
    shares1 = 500
    shares2 = 200

    with new_user(cql, "r1") as r1:
        with new_user(cql, "r2") as r2:
            with new_service_level(cql, shares=shares1, role=r1) as sl1:
                with new_service_level(cql, shares=shares2, role=r2) as sl2:
                    cql.execute(f"GRANT {r2} TO {r1}")

                    list_r1 = cql.execute(f"LIST EFFECTIVE SERVICE LEVEL OF {r1}")
                    for row in list_r1:
                        if row.service_level_option == "shares":
                            assert row.effective_service_level == sl2
                            assert row.value == f"{shares2}"
                    list_r2 = cql.execute(f"LIST EFFECTIVE SERVICE LEVEL OF {r2}")
                    for row in list_r2:
                        if row.service_level_option == "shares":
                            assert row.effective_service_level == sl2
                            assert row.value == f"{shares2}"

def test_list_effective_service_level_without_attached(scylla_only, cql):
    with new_user(cql) as role:
        with pytest.raises(InvalidRequest, match=f"Role {role} doesn't have assigned any service level"):
            cql.execute(f"LIST EFFECTIVE SERVICE LEVEL OF {role}")

# Scylla Enterprise limits the number of service levels to a small number (8 including 1 default service level).
# This test verifies that attempting to create more service levels than that results in an InvalidRequest error
# and doesn't silently succeed. 
# The test also has a regression check if a user can create exactly 7 service levels.
# In case you are adding a new internal scheduling group and this test failed, you should increase `SCHEDULING_GROUPS_COUNT`
#
# Reproduces enterprise issue #4481.
# Reproduces enterprise issue #5014.
def test_scheduling_groups_limit(scylla_only, cql):
    sl_count = 100
    created_count = 0

    with pytest.raises(InvalidRequest, match="Can't create service level - no more scheduling groups exist"):
        with ExitStack() as stack:
            for i in range(sl_count):
                stack.enter_context(new_service_level(cql))
                created_count = created_count + 1

    assert created_count > 0
    assert created_count == 7 # regression check

def test_default_shares_in_listings(scylla_only, cql):
    with scylla_inject_error(cql, "create_service_levels_without_default_shares", one_shot=False), \
        new_user(cql) as role:
        with new_service_level(cql, role=role) as sl:
            list_effective = cql.execute(f"LIST EFFECTIVE SERVICE LEVEL OF {role}")
            shares_info = [row for row in list_effective if row.service_level_option == "shares"][0]
            assert shares_info.value == "1000"
            assert shares_info.effective_service_level == sl

            list_sl = cql.execute(f"LIST SERVICE LEVEL {sl}").one()
            assert list_sl.shares == 1000
