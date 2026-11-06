# -*- coding: utf-8 -*-
# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for the service levels infrastructure. Service levels can be attached
# to roles in order to apply various role-specific parameters, like timeouts.
#############################################################################

from contextlib import contextmanager, ExitStack
from .util import unique_name, new_user, new_session, new_test_table, config_value_context
from .rest_api import scylla_inject_error

from cassandra import ReadTimeout
from cassandra.protocol import InvalidRequest
from cassandra.util import Duration

import pytest

# MAX_USER_SERVICE_LEVELS represents the maximal number of service levels that users can create.
# The value is documentented in `docs/features/workload-prioritization.rst`.
# It's used for regression testing of user service level limit in this and other files.
MAX_USER_SERVICE_LEVELS = 8

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
    # Check the error messages of some of the incorrect values, when they
    # are used in CREATE SERVICE LEVEL.
    # Translated from scylla-dtest's sla_per_user_test.py::TestSLTimeoutsNegative::test_invalid_timeout_values.
    for incorrect, message in [('1d', 'Timeout values cannot be expressed in days/months'),
                               ('2mo', 'Timeout values cannot be expressed in days/months'),
                               ('1000ns', 'Timeout values must be expressed in millisecond granularity'),
                               ('-1200ms', 'Timeout values must be nonnegative')]:
        sl = f"sl_{unique_name()}"
        try:
            with pytest.raises(InvalidRequest, match=message):
                cql.execute(f"CREATE SERVICE_LEVEL {sl} WITH timeout = {incorrect}")
        finally:
            cql.execute(f"DROP SERVICE_LEVEL IF EXISTS {sl}")

# Test that the service level is correctly attached to the user's role
def test_attached_service_level(scylla_only, cql):
    with new_service_level(cql) as sl:
        res_one = cql.execute(f"LIST ATTACHED SERVICE LEVEL OF {cql.cluster.auth_provider.username}").one()
        assert res_one.role == cql.cluster.auth_provider.username and res_one.service_level == sl
        res_one = cql.execute(f"LIST ALL ATTACHED SERVICE LEVELS").one()
        assert res_one.role == cql.cluster.auth_provider.username and res_one.service_level == sl

def test_list_effective_service_level(scylla_only, cql):
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

# ScyllaDB limits the number of service levels to a small number (10 including 1 default and 1 driver service level).
# This test verifies that attempting to create more service levels than that results in an InvalidRequest error
# and doesn't silently succeed. 
# The test also has a regression check if a user can create exactly 8 service levels.
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
    assert created_count == MAX_USER_SERVICE_LEVELS # regression check

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

# Verify that we cannot manipulate the default service level
# and that the messages Scylla returns are informative.
def test_manipulating_default_service_level(cql, scylla_only):
    default_sl = "default"

    with new_user(cql) as role:
        # Creation.
        create_err = f"The default service level, {default_sl}, already exists and cannot be created"
        with pytest.raises(InvalidRequest, match=create_err):
            cql.execute(f"CREATE SERVICE LEVEL {default_sl} WITH SHARES = 100")

        # Alteration.
        alter_err = f"The default service level, {default_sl}, cannot be altered"
        with pytest.raises(InvalidRequest, match=alter_err):
            cql.execute(f"ALTER SERVICE LEVEL {default_sl} WITH SHARES = 200")

        # Attachment.
        attach_err = f"The default service level, {default_sl}, cannot be attached to a role. " \
                      "If you want to detach an attached service level, use the DETACH SERVICE " \
                      "LEVEL statement"
        with pytest.raises(InvalidRequest, match=attach_err):
            cql.execute(f"ATTACH SERVICE LEVEL {default_sl} TO {role}")

        # Dropping.
        drop_err = f"The default service level, {default_sl}, cannot be dropped"
        with pytest.raises(InvalidRequest, match=drop_err):
            cql.execute(f"DROP SERVICE LEVEL {default_sl}")

# Verify that we can manipulate service levels whose names are similar
# to the name of the default service level, but they're not the same.
# Sanity check.
def test_manipulating_fake_default_service_level(cql, scylla_only):
    # Service levels are case-sensitive (if used with quotation marks).
    fake_default_sl = '"DeFaUlT"'

    with new_user(cql) as role:
        try:
            # Creation.
            cql.execute(f"CREATE SERVICE LEVEL {fake_default_sl} WITH SHARES = 100")

            # Alteration.
            cql.execute(f"ALTER SERVICE LEVEL {fake_default_sl} WITH SHARES = 200")

            # Attachment.
            cql.execute(f"ATTACH SERVICE LEVEL {fake_default_sl} TO {role}")

            # Dropping.
            cql.execute(f"DROP SERVICE LEVEL {fake_default_sl}")
        finally:
            cql.execute(f"DROP SERVICE LEVEL IF EXISTS {fake_default_sl}")

#############################################################################
# The following tests were translated from scylla-dtest's sla_per_user_test.py.
# Like the dtests, they use the SERVICE_LEVEL syntax (with an underscore),
# which Scylla accepts as well as SERVICE LEVEL, and quoted service level
# names. They attach service levels to roles created with CREATE ROLE and to
# users created with the older CREATE USER.
#############################################################################

# Create a service level with a quoted name - lower-case, or with upper-case
# letters (so it is case-sensitive) - with the given options.
@contextmanager
def new_quoted_service_level(cql, options='', upper_case=True):
    name = f'"{"Sl" if upper_case else "sl"}_{unique_name()}"'
    cql.execute(f'CREATE SERVICE_LEVEL IF NOT EXISTS {name} {options}')
    try:
        yield name
    finally:
        cql.execute(f'DROP SERVICE_LEVEL IF EXISTS {name}')

# Create a role (with CREATE ROLE) or a user (with CREATE USER), with the
# given options.
@contextmanager
def new_entity(cql, kind, options=''):
    name = unique_name()
    cql.execute(f'CREATE {kind} {name} {options}')
    try:
        yield name
    finally:
        cql.execute(f'DROP {kind} IF EXISTS {name}')

# Return the given service level's row in LIST SERVICE_LEVEL, and check that
# LIST ALL SERVICE_LEVELS lists the same attributes (it also has a column
# with the percentage of all the service levels' shares).
def list_service_level(cql, sl):
    row = cql.execute(f'LIST SERVICE_LEVEL {sl}').one()
    assert row.service_level == sl.strip('"')
    def attributes(r):
        return (r.service_level, r.timeout, r.workload_type, r.shares)
    assert [attributes(r) for r in cql.execute('LIST ALL SERVICE_LEVELS') if r.service_level == row.service_level] == [attributes(row)]
    return row

def attached_service_levels(cql, entity):
    return [r.service_level for r in cql.execute(f'LIST ATTACHED SERVICE_LEVEL OF {entity}')]

entity_kinds = pytest.mark.parametrize('kind', ['ROLE', 'USER'])

# Test creating a service level with a lower-case or a case-sensitive name,
# and listing it.
# Translated from scylla-dtest's sla_per_user_test.py::TestSLA::test_sla.
@pytest.mark.parametrize('upper_case', [False, True], ids=['lower_case', 'upper_case'])
def test_create_and_list_service_level(scylla_only, cql, upper_case):
    with new_quoted_service_level(cql, 'WITH shares = 100', upper_case) as sl:
        assert list_service_level(cql, sl).shares == 100

# Test attaching a service level to a role or a user, with or without a
# password and login.
# Translated from scylla-dtest's sla_per_user_test.py::TestSLA::test_sla_attached_to_entity.
@pytest.mark.parametrize('kind,options', [('ROLE', ''), ('USER', ''), ('ROLE', "WITH PASSWORD = 'auth' AND LOGIN = true"), ('USER', "WITH PASSWORD ''")],
                         ids=['role', 'user', 'role_with_login', 'user_with_password'])
def test_attach_service_level_to_entity(scylla_only, cql, kind, options):
    with new_quoted_service_level(cql, 'WITH shares = 100') as sl, new_entity(cql, kind, options) as entity:
        cql.execute(f'ATTACH SERVICE_LEVEL {sl} TO {entity}')
        assert list_service_level(cql, sl).shares == 100
        assert attached_service_levels(cql, entity) == [sl.strip('"')]

# Test that a role or a user with an attached service level can log in, and
# write and read data.
# Translated from scylla-dtest's sla_per_user_test.py::TestSLA::test_sla_usage,
# which wrote 10 rows with cassandra-stress.
@entity_kinds
def test_service_level_entity_can_log_in(scylla_only, cql, test_keyspace, kind):
    with new_quoted_service_level(cql, 'WITH shares = 100') as sl, new_test_table(cql, test_keyspace, 'p int PRIMARY KEY, v int') as table:
        name = unique_name()
        options = f"WITH PASSWORD = '{name}' AND LOGIN = true AND SUPERUSER = true" if kind == 'ROLE' else f"WITH PASSWORD '{name}' SUPERUSER"
        cql.execute(f'CREATE {kind} {name} {options}')
        try:
            cql.execute(f'ATTACH SERVICE_LEVEL {sl} TO {name}')
            assert attached_service_levels(cql, name) == [sl.strip('"')]
            with new_session(cql, name) as session:
                session.execute(f'INSERT INTO {table} (p, v) VALUES (1, 2)')
                assert list(session.execute(f'SELECT * FROM {table}')) == [(1, 2)]
        finally:
            cql.execute(f'DROP {kind} IF EXISTS {name}')

# Test that a service level created without shares has the default 1000
# shares, also when listed as attached.
# Translated from scylla-dtest's sla_per_user_test.py::TestSLA::test_sla_no_shares.
def test_service_level_default_shares(scylla_only, cql):
    with new_quoted_service_level(cql) as sl, new_entity(cql, 'ROLE') as role:
        cql.execute(f'ATTACH SERVICE_LEVEL {sl} TO {role}')
        assert list_service_level(cql, sl).shares == 1000
        assert attached_service_levels(cql, role) == [sl.strip('"')]

# Test replacing the service level attached to a role or user, by detaching
# it and attaching another service level.
# Translated from scylla-dtest's sla_per_user_test.py::TestSLA::test_replace_sla.
@entity_kinds
def test_replace_attached_service_level(scylla_only, cql, kind):
    with new_quoted_service_level(cql, 'WITH shares = 50') as sl1, new_quoted_service_level(cql, 'WITH shares = 300') as sl2, \
         new_entity(cql, kind) as entity:
        cql.execute(f'ATTACH SERVICE_LEVEL {sl1} TO {entity}')
        assert attached_service_levels(cql, entity) == [sl1.strip('"')]
        cql.execute(f'DETACH SERVICE_LEVEL FROM {entity}')
        cql.execute(f'ATTACH SERVICE_LEVEL {sl2} TO {entity}')
        assert attached_service_levels(cql, entity) == [sl2.strip('"')]
        assert list_service_level(cql, sl1).shares == 50
        assert list_service_level(cql, sl2).shares == 300

# Test that attaching a second service level to a role or user replaces the
# first one.
# Translated from scylla-dtest's sla_per_user_test.py::TestSLA::test_attach_2_slas_to_role.
@entity_kinds
def test_attach_second_service_level(scylla_only, cql, kind):
    with new_quoted_service_level(cql, 'WITH shares = 100') as sl1, new_quoted_service_level(cql, 'WITH shares = 200') as sl2, \
         new_entity(cql, kind) as entity:
        cql.execute(f'ATTACH SERVICE_LEVEL {sl1} TO {entity}')
        cql.execute(f'ATTACH SERVICE_LEVEL {sl2} TO {entity}')
        assert attached_service_levels(cql, entity) == [sl2.strip('"')]
        assert list_service_level(cql, sl1).shares == 100
        assert list_service_level(cql, sl2).shares == 200

# Test that changing the shares of a service level attached to a role or
# user changes them in its listing.
# Translated from scylla-dtest's sla_per_user_test.py::TestSLA::test_update_assigned_sla_service_shares.
@entity_kinds
def test_alter_attached_service_level_shares(scylla_only, cql, kind):
    with new_quoted_service_level(cql) as sl, new_entity(cql, kind) as entity:
        cql.execute(f'ATTACH SERVICE_LEVEL {sl} TO {entity}')
        assert list_service_level(cql, sl).shares == 1000
        cql.execute(f'ALTER SERVICE_LEVEL {sl} WITH shares = 500')
        assert list_service_level(cql, sl).shares == 500
        assert attached_service_levels(cql, entity) == [sl.strip('"')]

# Test that dropping the service level attached to a role or user detaches
# it, and another service level can then be attached.
# Translated from scylla-dtest's sla_per_user_test.py::TestSLA::test_drop_sl_and_attach_new_to_role.
@entity_kinds
def test_drop_attached_service_level(scylla_only, cql, kind):
    with new_quoted_service_level(cql, 'WITH shares = 200') as sl2, new_entity(cql, kind) as entity:
        with new_quoted_service_level(cql, 'WITH shares = 100') as sl1:
            cql.execute(f'ATTACH SERVICE_LEVEL {sl1} TO {entity}')
            assert attached_service_levels(cql, entity) == [sl1.strip('"')]
        assert attached_service_levels(cql, entity) == []
        cql.execute(f'ATTACH SERVICE_LEVEL {sl2} TO {entity}')
        assert attached_service_levels(cql, entity) == [sl2.strip('"')]

# Test creating a service level with valid timeouts, which are listed as
# given, and that a role with this service level can read.
# Translated from scylla-dtest's sla_per_user_test.py::TestSLATimeouts::test_timeout_valid_values,
# which read 1000 rows.
@pytest.mark.parametrize('timeout,nanoseconds', [('1000000ms', 1000000 * 10**6), ('5h', 5 * 3600 * 10**9),
                                                 ('23h2m2s2ms', ((23 * 60 + 2) * 60 + 2) * 10**9 + 2 * 10**6)],
                         ids=['1000000ms', '5h', '23h2m2s2ms'])
def test_service_level_valid_timeouts(scylla_only, cql, test_keyspace, timeout, nanoseconds):
    with new_quoted_service_level(cql, f'WITH timeout = {timeout}') as sl, new_user(cql) as role, \
         new_test_table(cql, test_keyspace, 'p int PRIMARY KEY, v int') as table:
        assert list_service_level(cql, sl).timeout == Duration(0, 0, nanoseconds)
        cql.execute(f'ATTACH SERVICE_LEVEL {sl} TO {role}')
        cql.execute(f'INSERT INTO {table} (p, v) VALUES (1, 2)')
        cql.execute(f'GRANT SELECT ON {table} TO {role}')
        with new_session(cql, role) as session:
            assert list(session.execute(f'SELECT * FROM {table}')) == [(1, 2)]

# Test the priority of the request timeouts: A service level's timeout takes
# precedence over the server's configuration, and a query's USING TIMEOUT
# takes precedence over the service level's timeout.
# Translated from scylla-dtest's sla_per_user_test.py::TestSLATimeouts::test_sla_timeout_priority,
# which was skipped because of issue #10285, and restarted the node to change
# the configuration. As explained in that issue, a 0ms timeout may not time
# out a request which finishes before the timeout timer's granularity (about
# 10ms) - so this test passes or fails depending on how fast the read is, and
# its xfail isn't strict.
@pytest.mark.xfail(reason="#10285", strict=False)
@pytest.mark.parametrize('case', ['service_level_wins_over_configuration', 'query_timeout_wins_over_service_level'])
def test_service_level_timeout_priority(scylla_only, cql, test_keyspace, case):
    with new_quoted_service_level(cql, 'WITH timeout = 100ms') as sl, new_user(cql) as role, \
         new_test_table(cql, test_keyspace, 'p int PRIMARY KEY, v int') as table:
        insert = cql.prepare(f'INSERT INTO {table} (p, v) VALUES (?, ?)')
        for p in range(100):
            cql.execute(insert, [p, p])
        cql.execute(f'ATTACH SERVICE_LEVEL {sl} TO {role}')
        cql.execute(f'GRANT SELECT ON {table} TO {role}')
        with new_session(cql, role) as session:
            assert len(list(session.execute(f'SELECT * FROM {table}'))) == 100
        if case == 'service_level_wins_over_configuration':
            cql.execute(f'ALTER SERVICE_LEVEL {sl} WITH timeout = 0ms')
            with config_value_context(cql, 'range_request_timeout_in_ms', '100'):
                with new_session(cql, role) as session:
                    with pytest.raises(ReadTimeout):
                        session.execute(f'SELECT * FROM {table}')
        else:
            with new_session(cql, role) as session:
                with pytest.raises(ReadTimeout):
                    session.execute(f'SELECT * FROM {table} USING TIMEOUT 0ms')
