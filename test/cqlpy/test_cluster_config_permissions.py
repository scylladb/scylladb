# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for the CONFIGURE permission, which authorizes setting cluster config
# options: GRANT CONFIGURE [(option, ...)] ON <scope target> TO <role>.
#############################################################################

import pytest
from contextlib import contextmanager, ExitStack
from cassandra.protocol import SyntaxException, InvalidRequest, Unauthorized
from .test_permissions import eventually_authorized, eventually_unauthorized
from .util import new_user, new_session, new_test_keyspace, new_cql, unique_name

pytestmark = pytest.mark.usefixtures("scylla_only")

KS_OPTS = "WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }"
OPT = "auto_repair_enabled"
OTHER_OPT = "auto_repair_threshold_in_seconds"
# Registered for the CLUSTER scope only.
CLUSTER_ONLY_OPT = "tablets_per_shard_goal"


def allowed(session, stmt):
    eventually_authorized(lambda: session.execute(stmt))


def refused(session, stmt):
    eventually_unauthorized(lambda: session.execute(stmt))


@contextmanager
def user_session(cql):
    with new_user(cql) as user:
        with new_session(cql, user) as session:
            yield user, session


@contextmanager
def cluster_option_cleanup(cql, *options):
    try:
        yield
    finally:
        for option in options:
            cql.execute(f"ALTER CLUSTER WITH {option} = NULL")


def permissions_of(cql, user):
    return sorted((row.resource, row.permission) for row in cql.execute(f"LIST ALL PERMISSIONS OF {user}"))


def local_topology(cql):
    row = cql.execute("SELECT data_center, rack, host_id FROM system.local").one()
    return row.data_center, row.rack, row.host_id


def test_data_permissions_do_not_cover_restricted_options(cql):
    """ALL on a keyspace, or having created a table, does not allow setting a restricted option."""
    with new_test_keyspace(cql, KS_OPTS) as ks, user_session(cql) as (user, session):
        cql.execute(f"GRANT ALL ON KEYSPACE {ks} TO {user}")
        assert (f"<keyspace {ks}>", "CONFIGURE") not in permissions_of(cql, user)
        allowed(session, f"CREATE TABLE {ks}.t (pk int PRIMARY KEY)")
        assert (f"<table {ks}.t>", "CONFIGURE") not in permissions_of(cql, user)
        allowed(session, f"ALTER TABLE {ks}.t WITH comment = 'x'")
        refused(session, f"ALTER TABLE {ks}.t WITH {OPT} = true")
        refused(session, f"ALTER TABLE {ks}.t WITH comment = 'y' AND {OPT} = true")
        refused(session, f"ALTER KEYSPACE {ks} WITH {OPT} = true")
        refused(session, f"CREATE TABLE {ks}.t2 (pk int PRIMARY KEY) WITH {OPT} = true")
        refused(session, f"ALTER CLUSTER WITH {OPT} = true")
        # The creator has AUTHORIZE on the table but holds no CONFIGURE to hand out.
        refused(session, f"GRANT CONFIGURE ON TABLE {ks}.t TO {user}")
        refused(session, f"GRANT CONFIGURE ({OPT}) ON TABLE {ks}.t TO {user}")


def test_option_grant_on_keyspace(cql):
    """A grant narrowed to one option on a keyspace covers that option in the keyspace and its tables only."""
    with new_test_keyspace(cql, KS_OPTS) as ks, new_test_keyspace(cql, KS_OPTS) as other_ks, user_session(cql) as (user, session):
        cql.execute(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY)")
        cql.execute(f"CREATE TABLE {other_ks}.t (pk int PRIMARY KEY)")
        cql.execute(f"GRANT CONFIGURE ({OPT}) ON KEYSPACE {ks} TO {user}")
        assert permissions_of(cql, user) == [(f"<keyspace {ks}>", f"CONFIGURE ({OPT})")]

        allowed(session, f"ALTER KEYSPACE {ks} WITH {OPT} = true")
        allowed(session, f"ALTER TABLE {ks}.t WITH {OPT} = false")
        allowed(session, f"ALTER TABLE {ks}.t WITH {OPT} = NULL")
        refused(session, f"ALTER TABLE {ks}.t WITH {OTHER_OPT} = 60")
        refused(session, f"ALTER TABLE {ks}.t WITH {OPT} = true AND {OTHER_OPT} = 60")
        refused(session, f"ALTER TABLE {other_ks}.t WITH {OPT} = true")
        refused(session, f"ALTER KEYSPACE {other_ks} WITH {OPT} = true")
        refused(session, f"ALTER CLUSTER WITH {OPT} = true")
        # CONFIGURE is not ALTER.
        refused(session, f"ALTER TABLE {ks}.t WITH comment = 'x'")

        cql.execute(f"REVOKE CONFIGURE ({OPT}) ON KEYSPACE {ks} FROM {user}")
        assert permissions_of(cql, user) == []
        refused(session, f"ALTER TABLE {ks}.t WITH {OPT} = true")


def test_all_options_grant_and_revoke(cql):
    """CONFIGURE without an option list covers every option; revoking it also removes per-option grants."""
    with new_test_keyspace(cql, KS_OPTS) as ks, user_session(cql) as (user, session):
        cql.execute(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY)")
        cql.execute(f"GRANT CONFIGURE ON KEYSPACE {ks} TO {user}")
        cql.execute(f"GRANT CONFIGURE ({OPT}, {OTHER_OPT}) ON KEYSPACE {ks} TO {user}")
        assert permissions_of(cql, user) == [
            (f"<keyspace {ks}>", "CONFIGURE"),
            (f"<keyspace {ks}>", f"CONFIGURE ({OPT})"),
            (f"<keyspace {ks}>", f"CONFIGURE ({OTHER_OPT})"),
        ]
        allowed(session, f"ALTER TABLE {ks}.t WITH {OPT} = true AND {OTHER_OPT} = 60")
        # CREATE TABLE needs its own permission besides CONFIGURE for the option.
        refused(session, f"CREATE TABLE {ks}.t2 (pk int PRIMARY KEY) WITH {OPT} = true")
        cql.execute(f"GRANT CREATE ON KEYSPACE {ks} TO {user}")
        allowed(session, f"CREATE TABLE {ks}.t2 (pk int PRIMARY KEY) WITH {OPT} = true")
        cql.execute(f"REVOKE CREATE ON KEYSPACE {ks} FROM {user}")

        cql.execute(f"REVOKE CONFIGURE ON KEYSPACE {ks} FROM {user}")
        assert [p for p in permissions_of(cql, user) if p[0] == f"<keyspace {ks}>"] == []
        refused(session, f"ALTER TABLE {ks}.t WITH {OPT} = false")


def test_grant_on_cluster_and_all_keyspaces(cql):
    """CLUSTER is above ALL KEYSPACES in the scope chain; ALL KEYSPACES does not cover ALTER CLUSTER."""
    with new_test_keyspace(cql, KS_OPTS) as ks, cluster_option_cleanup(cql, OPT, OTHER_OPT), ExitStack() as stack:
        cql.execute(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY)")
        operator, operator_session = stack.enter_context(user_session(cql))
        wide, wide_session = stack.enter_context(user_session(cql))

        cql.execute(f"GRANT CONFIGURE ({OPT}) ON ALL SCOPES TO {operator}")
        assert permissions_of(cql, operator) == [("<all scopes>", f"CONFIGURE ({OPT})")]
        allowed(operator_session, f"ALTER CLUSTER WITH {OPT} = true")
        allowed(operator_session, f"ALTER KEYSPACE {ks} WITH {OPT} = true")
        allowed(operator_session, f"ALTER TABLE {ks}.t WITH {OPT} = true")
        refused(operator_session, f"ALTER CLUSTER WITH {OTHER_OPT} = 60")
        refused(operator_session, f"ALTER TABLE {ks}.t WITH {OTHER_OPT} = 60")

        cql.execute(f"GRANT CONFIGURE ON ALL KEYSPACES TO {wide}")
        allowed(wide_session, f"ALTER KEYSPACE {ks} WITH {OTHER_OPT} = 60")
        allowed(wide_session, f"ALTER TABLE {ks}.t WITH {OTHER_OPT} = 60")
        refused(wide_session, f"ALTER CLUSTER WITH {OTHER_OPT} = 60")

        # A recursive listing on a table includes the broader scopes up to the cluster.
        listed = {(row.role, row.resource, row.permission) for row in cql.execute(f"LIST CONFIGURE ON TABLE {ks}.t")}
        assert (operator, "<all scopes>", f"CONFIGURE ({OPT})") in listed
        assert (wide, "<all keyspaces>", "CONFIGURE") in listed


def test_grant_through_role(cql):
    """A CONFIGURE grant is inherited through a granted role."""
    with new_test_keyspace(cql, KS_OPTS) as ks, user_session(cql) as (user, session), new_user(cql) as role:
        cql.execute(f"GRANT CONFIGURE ({OPT}) ON KEYSPACE {ks} TO {role}")
        refused(session, f"ALTER KEYSPACE {ks} WITH {OPT} = true")
        cql.execute(f"GRANT {role} TO {user}")
        allowed(session, f"ALTER KEYSPACE {ks} WITH {OPT} = true")


def test_delegation(cql):
    """A role with AUTHORIZE can grant the options it holds, and nothing else."""
    with new_test_keyspace(cql, KS_OPTS) as ks, ExitStack() as stack:
        lead, lead_session = stack.enter_context(user_session(cql))
        member, member_session = stack.enter_context(user_session(cql))
        cql.execute(f"GRANT CONFIGURE ({OPT}) ON ALL SCOPES TO {lead}")
        refused(lead_session, f"GRANT CONFIGURE ({OPT}) ON KEYSPACE {ks} TO {member}")
        cql.execute(f"GRANT AUTHORIZE ON ALL SCOPES TO {lead}")
        allowed(lead_session, f"GRANT CONFIGURE ({OPT}) ON KEYSPACE {ks} TO {member}")
        refused(lead_session, f"GRANT CONFIGURE ({OTHER_OPT}) ON KEYSPACE {ks} TO {member}")
        refused(lead_session, f"GRANT CONFIGURE ON KEYSPACE {ks} TO {member}")
        allowed(member_session, f"ALTER KEYSPACE {ks} WITH {OPT} = true")


def test_grant_validation(cql):
    with new_test_keyspace(cql, KS_OPTS) as ks, new_user(cql) as user:
        with pytest.raises(InvalidRequest, match="Unknown cluster config option"):
            cql.execute(f"GRANT CONFIGURE (no_such_option) ON KEYSPACE {ks} TO {user}")
        with pytest.raises(InvalidRequest, match="cannot be set on"):
            cql.execute(f"GRANT CONFIGURE ({CLUSTER_ONLY_OPT}) ON KEYSPACE {ks} TO {user}")
        with pytest.raises(SyntaxException, match="Only the CONFIGURE permission"):
            cql.execute(f"GRANT SELECT ({OPT}) ON KEYSPACE {ks} TO {user}")
        with pytest.raises((InvalidRequest, SyntaxException), match="support.*permissions"):
            cql.execute(f"GRANT CONFIGURE ON ALL ROLES TO {user}")
        with pytest.raises((InvalidRequest, SyntaxException), match="support.*permissions"):
            cql.execute(f"GRANT SELECT ON ALL SCOPES TO {user}")
        with pytest.raises(InvalidRequest, match="doesn't exist"):
            cql.execute(f"GRANT CONFIGURE ON DATACENTER no_such_dc TO {user}")
        cql.execute(f"GRANT CONFIGURE ({CLUSTER_ONLY_OPT}) ON ALL SCOPES TO {user}")
        assert permissions_of(cql, user) == [("<all scopes>", f"CONFIGURE ({CLUSTER_ONLY_OPT})")]


def test_drop_revokes_option_grants(cql):
    """Dropping a table or a keyspace removes the grants on it, so a re-created one starts without any."""
    with new_test_keyspace(cql, KS_OPTS) as ks, user_session(cql) as (user, session):
        cql.execute(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY)")
        cql.execute(f"GRANT CONFIGURE ({OPT}) ON TABLE {ks}.t TO {user}")
        cql.execute(f"GRANT CONFIGURE ON TABLE {ks}.t TO {user}")
        allowed(session, f"ALTER TABLE {ks}.t WITH {OPT} = true")
        cql.execute(f"DROP TABLE {ks}.t")
        assert permissions_of(cql, user) == []
        cql.execute(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY)")
        refused(session, f"ALTER TABLE {ks}.t WITH {OPT} = true")

    ks = unique_name()
    with new_user(cql) as user:
        cql.execute(f"CREATE KEYSPACE {ks} {KS_OPTS}")
        try:
            cql.execute(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY)")
            cql.execute(f"GRANT CONFIGURE ({OPT}) ON KEYSPACE {ks} TO {user}")
            cql.execute(f"GRANT CONFIGURE ({OPT}) ON TABLE {ks}.t TO {user}")
        finally:
            cql.execute(f"DROP KEYSPACE {ks}")
        assert permissions_of(cql, user) == []


def test_topology_targets(cql):
    """Grants on a datacenter, a rack and a node, and the node -> rack -> datacenter -> cluster chain.

    No option is registered for these scopes, so an authorized ALTER fails later, on the
    scope check, with InvalidRequest; an unauthorized one fails with Unauthorized."""
    dc, rack, host_id = local_topology(cql)

    def authorized_then_rejected(session, stmt):
        def run():
            with pytest.raises(InvalidRequest, match="does not support this scope"):
                session.execute(stmt)
        eventually_authorized(run)

    alter_dc = f'ALTER DATACENTER "{dc}" WITH {CLUSTER_ONLY_OPT} = 200'
    alter_rack = f'ALTER RACK "{dc}" "{rack}" WITH {CLUSTER_ONLY_OPT} = 200'
    alter_node = f'ALTER NODE {host_id} WITH {CLUSTER_ONLY_OPT} = 200'

    with ExitStack() as stack:
        dc_ops, dc_session = stack.enter_context(user_session(cql))
        rack_ops, rack_session = stack.enter_context(user_session(cql))
        node_ops, node_session = stack.enter_context(user_session(cql))

        for session in (dc_session, rack_session, node_session):
            for stmt in (alter_dc, alter_rack, alter_node):
                refused(session, stmt)

        cql.execute(f'GRANT CONFIGURE ON DATACENTER "{dc}" TO {dc_ops}')
        cql.execute(f'GRANT CONFIGURE ON RACK "{dc}" "{rack}" TO {rack_ops}')
        cql.execute(f'GRANT CONFIGURE ON NODE {host_id} TO {node_ops}')
        assert permissions_of(cql, dc_ops) == [(f"<datacenter {dc}>", "CONFIGURE")]
        assert permissions_of(cql, rack_ops) == [(f"<rack {dc} {rack}>", "CONFIGURE")]
        assert permissions_of(cql, node_ops) == [(f"<node {host_id}>", "CONFIGURE")]

        for stmt in (alter_dc, alter_rack, alter_node):
            authorized_then_rejected(dc_session, stmt)
        refused(rack_session, alter_dc)
        for stmt in (alter_rack, alter_node):
            authorized_then_rejected(rack_session, stmt)
        refused(node_session, alter_dc)
        refused(node_session, alter_rack)
        authorized_then_rejected(node_session, alter_node)
        for session in (dc_session, rack_session, node_session):
            refused(session, f"ALTER CLUSTER WITH {CLUSTER_ONLY_OPT} = 200")

        # An option that no scope at or below the target supports cannot be granted there.
        with pytest.raises(InvalidRequest, match="cannot be set on"):
            cql.execute(f'GRANT CONFIGURE ({OPT}) ON DATACENTER "{dc}" TO {dc_ops}')
        with pytest.raises(InvalidRequest, match="doesn't exist"):
            cql.execute(f'GRANT CONFIGURE ON RACK "{dc}" no_such_rack TO {rack_ops}')
        with pytest.raises(InvalidRequest, match="doesn't exist"):
            cql.execute(f"GRANT CONFIGURE ON NODE 00000000-0000-0000-0000-000000000001 TO {node_ops}")

        cql.execute(f'REVOKE CONFIGURE ON NODE {host_id} FROM {node_ops}')
        assert permissions_of(cql, node_ops) == []
        refused(node_session, alter_node)


def test_describe_emits_configure_grants(cql):
    """DESCRIBE SCHEMA WITH INTERNALS emits the grants in a form that can be replayed."""
    dc, _, _ = local_topology(cql)
    with new_test_keyspace(cql, KS_OPTS) as ks, new_user(cql) as user:
        grants = [
            f"GRANT CONFIGURE ({OPT}) ON KEYSPACE {ks} TO {user};",
            f"GRANT CONFIGURE ON ALL SCOPES TO {user};",
            f"GRANT CONFIGURE ON DATACENTER {dc} TO {user};",
        ]
        for grant in grants:
            cql.execute(grant)
        # A grant on a node names a host ID that no other cluster has; it is not part of the dump.
        _, _, host_id = local_topology(cql)
        cql.execute(f"GRANT CONFIGURE ON NODE {host_id} TO {user}")
        described = {row.create_statement for row in cql.execute("DESCRIBE SCHEMA WITH INTERNALS")}
        for grant in grants:
            assert grant in described
        assert not any(" ON NODE " in statement for statement in described)
        cql.execute(f"REVOKE CONFIGURE ON NODE {host_id} FROM {user}")
        for grant in grants:
            cql.execute(grant.replace("GRANT", "REVOKE").replace(" TO ", " FROM "))
        assert permissions_of(cql, user) == []
        for grant in grants:
            cql.execute(grant)
        assert len(permissions_of(cql, user)) == 3


def test_table_named_cluster(cql):
    """CLUSTER is not a resource keyword: a table of that name is granted on as before."""
    with new_test_keyspace(cql, KS_OPTS) as ks, new_user(cql) as user:
        cql.execute(f"CREATE TABLE {ks}.cluster (pk int PRIMARY KEY)")
        cql.execute(f"GRANT SELECT ON {ks}.cluster TO {user}")
        with new_cql(cql) as ks_cql:
            ks_cql.execute(f"USE {ks}")
            ks_cql.execute(f"GRANT ALTER ON cluster TO {user}")
        assert permissions_of(cql, user) == [(f"<table {ks}.cluster>", "ALTER"), (f"<table {ks}.cluster>", "SELECT")]


def test_revoke_all_revokes_configure(cql):
    """GRANT ALL does not grant CONFIGURE, but REVOKE ALL leaves no CONFIGURE grant behind."""
    with new_test_keyspace(cql, KS_OPTS) as ks, user_session(cql) as (user, session):
        cql.execute(f"GRANT ALL ON KEYSPACE {ks} TO {user}")
        cql.execute(f"GRANT CONFIGURE ON KEYSPACE {ks} TO {user}")
        cql.execute(f"GRANT CONFIGURE ({OPT}) ON KEYSPACE {ks} TO {user}")
        allowed(session, f"ALTER KEYSPACE {ks} WITH {OPT} = true")
        cql.execute(f"REVOKE ALL ON KEYSPACE {ks} FROM {user}")
        assert permissions_of(cql, user) == []
        refused(session, f"ALTER KEYSPACE {ks} WITH {OPT} = true")


def test_revoke_all_needs_configure_to_revoke_it(cql):
    """A role with AUTHORIZE but without CONFIGURE cannot take away another role's CONFIGURE with REVOKE ALL."""
    with new_test_keyspace(cql, KS_OPTS) as ks, ExitStack() as stack:
        tenant, tenant_session = stack.enter_context(user_session(cql))
        operator, operator_session = stack.enter_context(user_session(cql))
        cql.execute(f"GRANT ALL ON KEYSPACE {ks} TO {tenant}")
        cql.execute(f"GRANT SELECT ON KEYSPACE {ks} TO {operator}")
        cql.execute(f"GRANT CONFIGURE ({OPT}) ON KEYSPACE {ks} TO {operator}")
        refused(tenant_session, f"REVOKE CONFIGURE ON KEYSPACE {ks} FROM {operator}")
        allowed(tenant_session, f"REVOKE ALL ON KEYSPACE {ks} FROM {operator}")
        assert permissions_of(cql, operator) == [(f"<keyspace {ks}>", f"CONFIGURE ({OPT})")]
        allowed(operator_session, f"ALTER KEYSPACE {ks} WITH {OPT} = true")


def test_configure_does_not_open_internal_tables(cql):
    """A statement that only sets options still goes through the rules that protect system tables."""
    with user_session(cql) as (user, session):
        cql.execute(f"GRANT CONFIGURE ON ALL KEYSPACES TO {user}")
        with pytest.raises(Unauthorized, match="not user-modifiable"):
            session.execute(f"ALTER TABLE system.local WITH {OPT} = true")
        with pytest.raises(Unauthorized, match="Cannot ALTER"):
            session.execute(f"ALTER TABLE system_distributed.cdc_generation_timestamps WITH {OPT} = true")
    with pytest.raises(Unauthorized, match="Cannot ALTER"):
        cql.execute(f"ALTER TABLE system_distributed.cdc_generation_timestamps WITH {OPT} = true")


def test_node_grant_ignores_uuid_case(cql):
    """A host ID typed in upper case names the same node."""
    _, _, host_id = local_topology(cql)
    with user_session(cql) as (user, session):
        cql.execute(f"GRANT CONFIGURE ON NODE {str(host_id).upper()} TO {user}")
        assert permissions_of(cql, user) == [(f"<node {host_id}>", "CONFIGURE")]
        def run():
            with pytest.raises(InvalidRequest, match="does not support this scope"):
                session.execute(f"ALTER NODE {host_id} WITH {CLUSTER_ONLY_OPT} = 200")
        eventually_authorized(run)
        cql.execute(f"REVOKE CONFIGURE ON NODE {host_id} FROM {user}")
        assert permissions_of(cql, user) == []


def test_role_named_like_a_config_option_part(cql):
    """A role named like the stored name part of a config option does not break listing permissions."""
    with new_user(cql) as user:
        cql.execute('CREATE ROLE "@config:admin"')
        try:
            cql.execute(f'GRANT ALTER ON ROLE "@config:admin" TO {user}')
            assert permissions_of(cql, user) == [("<role @config:admin>", "ALTER")]
            assert any('"@config:admin"' in row.create_statement for row in cql.execute("DESCRIBE SCHEMA WITH INTERNALS"))
        finally:
            cql.execute('DROP ROLE "@config:admin"')


def test_revoke_what_cannot_be_granted(cql):
    """REVOKE accepts an option that is not registered and a topology target that does not exist,
    so that a grant can be removed after its option or its target is gone."""
    with new_test_keyspace(cql, KS_OPTS) as ks, new_user(cql) as user:
        cql.execute(f"GRANT CONFIGURE ({OPT}) ON KEYSPACE {ks} TO {user}")
        cql.execute(f"REVOKE CONFIGURE (no_such_option) ON KEYSPACE {ks} FROM {user}")
        cql.execute(f"REVOKE CONFIGURE ON DATACENTER no_such_dc FROM {user}")
        cql.execute(f"REVOKE CONFIGURE ON RACK no_such_dc no_such_rack FROM {user}")
        cql.execute(f"REVOKE CONFIGURE ON NODE 00000000-0000-0000-0000-000000000001 FROM {user}")
        assert permissions_of(cql, user) == [(f"<keyspace {ks}>", f"CONFIGURE ({OPT})")]


def test_list_configure_with_options(cql):
    """LIST CONFIGURE (option) shows the grants that cover the option."""
    with new_test_keyspace(cql, KS_OPTS) as ks, new_user(cql) as user:
        cql.execute(f"GRANT CONFIGURE ({OPT}) ON KEYSPACE {ks} TO {user}")
        cql.execute(f"GRANT CONFIGURE ({OTHER_OPT}) ON KEYSPACE {ks} TO {user}")
        cql.execute(f"GRANT CONFIGURE ON ALL KEYSPACES TO {user}")
        listed = sorted((row.resource, row.permission) for row in cql.execute(f"LIST CONFIGURE ({OPT}) OF {user}"))
        assert listed == [("<all keyspaces>", "CONFIGURE"), (f"<keyspace {ks}>", f"CONFIGURE ({OPT})")]
        with pytest.raises(SyntaxException, match="Only the CONFIGURE permission"):
            cql.execute(f"LIST SELECT ({OPT}) OF {user}")


def test_unknown_names_are_refused_before_validation(cql):
    """A role without grants gets Unauthorized for an unknown option or datacenter, as for a known one."""
    with user_session(cql) as (user, session):
        refused(session, "ALTER CLUSTER WITH no_such_option = 1")
        refused(session, f"ALTER DATACENTER no_such_dc WITH {CLUSTER_ONLY_OPT} = 200")
        refused(session, f"ALTER RACK no_such_dc no_such_rack WITH {CLUSTER_ONLY_OPT} = 200")
