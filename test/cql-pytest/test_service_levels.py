# -*- coding: utf-8 -*-
# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for the service levels infrastructure. Service levels can be attached
# to roles in order to apply various role-specific parameters, like timeouts.
#############################################################################

from contextlib import contextmanager
from util import unique_name, new_test_table, new_user

from cassandra.protocol import InvalidRequest, ReadTimeout
from cassandra.util import Duration

import pytest
import time

@contextmanager
def new_service_level(cql, timeout=None, workload_type=None, role=None):
    params = ""
    if timeout and workload_type:
        params = f"WITH timeout = {timeout} AND workload_type = '{workload_type}'"
    elif timeout:
        params = f"WITH timeout = {timeout}"
    elif workload_type:
        params = f"WITH workload_type = '{workload_type}'"

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
