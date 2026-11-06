#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest

from test.cluster.lwt.test_lwt_destructive_ddl import MUTATION_WRITE_TIMEOUT, case_template, prepare
from test.cluster.util import stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test."""
    yield
    await stop_servers_gracefully(manager)


# Ported from scylla-dtest lwt_destructive_ddl_test.py::TestLwtDestructiveDDL::test_revoke_permissions.
# Scylla doesn't crash while a role running LWT statements on a table (in
# four sessions) has all its permissions on the table revoked and granted
# back, over and over, by the default superuser (in four other sessions),
# for 30 s (90 s in debug mode); both must succeed at times. The
# permissions cache is almost disabled, so the changes take effect.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.check_nodes_for_errors
async def test_revoke_permissions(manager: ScyllaClusterManager, build_mode: str):
    manager.ignore_log_patterns += [MUTATION_WRITE_TIMEOUT]
    servers, session = await prepare(manager, setup_auth=True, user="cassandra", password="cassandra")

    lwt_user = "jon"
    lwt_pass = "snow"
    # The default superuser runs the DDL statements.
    ddl_user = "cassandra"
    ddl_pass = "cassandra"

    session.execute(f"""
            CREATE ROLE {lwt_user}
            WITH password='{lwt_pass}'
                AND superuser=false AND login=true""")

    revoke_permissions_stmt = session.prepare(f"REVOKE ALL ON TABLE ks.test FROM {lwt_user}")
    grant_permissions_stmt = session.prepare(f"GRANT ALL ON TABLE ks.test TO {lwt_user}")

    def revoke_permissions_fn(session):
        """REVOKE + GRANT PERMISSION ON TABLE"""
        session.execute(revoke_permissions_stmt)
        session.execute(grant_permissions_stmt)

    try:
        await case_template(manager, servers, session, revoke_permissions_fn, build_mode, ddl_user=ddl_user, ddl_pass=ddl_pass,
                            lwt_user=lwt_user, lwt_pass=lwt_pass)
    finally:
        session.cluster.shutdown()
