import logging

import pytest
import requests
from ccmlib.node import Status
from ccmlib.utils.version import ComparableScyllaVersion

from dtest_class import create_cf, create_ks, get_ip_from_node, read_barrier, wait_for
from tools.cluster_topology import generate_cluster_topology
from upgrade_test import (
    UpgradeTester,
    upgrade_matrix_full_path,
)

logger = logging.getLogger(__name__)


def wait_for_complete_connection_pool(client, node):
    # cluster.connect() waits only for the first connection of a pool, the driver opens the
    # per-shard ones in the background, so without this wait they can still be in sl:driver
    # once the warm-up queries are done.
    def pool_is_complete():
        # shard_aware_stats() maps every host to {"shards_count": N, "connected": M}, but returns
        # None for a non-shard-aware cluster and raises IndexError until the first pool exists.
        try:
            stats = client.cluster.shard_aware_stats() or {}
        except IndexError:
            stats = {}
        logger.debug(f"Shard aware stats: {stats}")
        return len(stats) > 0 and all(host["connected"] == host["shards_count"] for host in stats.values())

    wait_for(pool_is_complete, timeout=60, text=f"Waiting for a connection to every shard of node {node.name}")


def query_runs_in_scheduling_group(client, sg):
    result = client.execute("SELECT * FROM ks.cf", trace=True)
    trace = result.get_query_trace(max_wait_sec=60)
    semaphore_events = 0

    for e in trace.events:
        # count_connections reports the new scheduling group before queries actually run
        # under it: the connection switches only after the request that triggered the
        # switch finishes, and the driver picks a random connection of the pool for a
        # statement without a routing key. Retry instead of failing.
        if sg not in e.thread_name:
            logger.debug(f"Query on {e.source} was executed under {e.thread_name}, not under {sg}")
            return False

        # Verify reader concurrency semaphore name
        if "[reader concurrency semaphore" in e.description:
            semaphore_events += 1
            assert f"[reader concurrency semaphore {sg}]" in e.description, f"Query on {e.source} was not executed with semaphore for {sg} scheduling group"

    # A read query must go through the reader concurrency semaphore, so keep retrying if the
    # trace did not capture it yet instead of passing without verifying it.
    return semaphore_events > 0


@pytest.mark.dtest_full
class TestWorkloadPrioritizationUpgrade(UpgradeTester):
    __test__ = True
    _multiprocess_can_split_ = False
    upgrade_path = upgrade_matrix_full_path
    init_version = upgrade_path[0]

    roles = ["role1", "role2", "role3"]

    @property
    def admin_connection_kwargs(self):
        return {"user": "cassandra", "password": "cassandra"}

    def connect_sessions(self):
        return [self.patient_exclusive_cql_connection(node, **self.admin_connection_kwargs) for node in self.cluster.nodelist()]

    def create_roles(self, session):
        for role in self.roles:
            session.execute(f"CREATE ROLE {role} WITH login=true AND password='{role}' AND SUPERUSER=true")

    def connect_clients(self):
        # A new connection starts in the sl:driver scheduling group and only switches to the
        # role's service level once the server processes a user request on it. Use exclusive
        # connections (one per node) and run warm-up queries on each so every pool connection
        # is classified under the role's scheduling group before it gets validated.
        clients = {}
        for role in self.roles:
            clients[role] = []
            for node in self.cluster.nodelist():
                client = self.patient_exclusive_cql_connection(node, user=role, password=role)
                wait_for_complete_connection_pool(client, node)
                for _ in range(20):
                    client.execute("SELECT * FROM ks.cf")
                clients[role].append(client)
        return clients

    @staticmethod
    def role_to_sl_name(role_name):
        return f"sl{role_name[4]}"

    def check_shares_column_added(self, cql, table):
        desc = cql.execute(f"DESCRIBE TABLE {table}").one()
        return "shares int" in desc.create_statement

    def check_workload_prioritization_enabled(self, cql):
        features = cql.execute("SELECT value FROM system.scylla_local WHERE key='enabled_features'").one()
        return "WORKLOAD_PRIORITIZATION" in features.value

    def validate_connections_scheduling_groups(self, nodes):
        def role_in_scheduling_group(node, role, sl_name):
            # Sample result of /service_levels/count_connections:
            # {'sl:test_sl': {'test_role': 3}, 'sl:default': {'cassandra': 3}}
            try:
                response = requests.get(f"http://{get_ip_from_node(node=node)}:{node.api_port}/service_levels/count_connections", timeout=5)
                response.raise_for_status()
                sg_connections_map = response.json()
            except (requests.RequestException, ValueError):
                return False
            return role in sg_connections_map.get(f"sl:{sl_name}", {})

        for node in nodes:
            for role in self.roles:
                sl_name = self.role_to_sl_name(role)
                wait_for(
                    lambda node=node, role=role, sl_name=sl_name: role_in_scheduling_group(node, role, sl_name),
                    timeout=60,
                    text=f"Waiting for role {role} connections to switch to sl:{sl_name} scheduling group on node {node.name}",
                )

    def validate_connections_semaphore(self, clients):
        for role, role_clients in clients.items():
            sg = f"sl:{self.role_to_sl_name(role)}"
            for client in role_clients:
                wait_for(
                    lambda client=client, sg=sg: query_runs_in_scheduling_group(client, sg),
                    timeout=60,
                    text=f"Waiting for a query of {role} to run under {sg} scheduling group",
                )

    @pytest.mark.next_gating
    def test_workload_prioritization_after_upgrade(self, dtest_config):
        self.clone_upgrade_path(dtest_config)
        config = {
            "authenticator": "org.apache.cassandra.auth.PasswordAuthenticator",
            "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer",
            "role_manager": "org.apache.cassandra.auth.CassandraRoleManager",
            "service_levels_interval_ms": 500,
        }
        cluster_topology = generate_cluster_topology(rack_num=3)
        self.init_cluster(cluster_topology, additional_config=config, skip_session=True)
        sessions = self.connect_sessions()

        create_ks(session=sessions[0], name="ks", rf=1)
        create_cf(session=sessions[0], name="cf")

        self.create_roles(sessions[0])
        sessions[0].execute("CREATE SERVICE LEVEL sl1")
        sessions[0].execute("CREATE SERVICE LEVEL sl2")
        sessions[0].execute("ATTACH SERVICE LEVEL sl1 TO role1")
        sessions[0].execute("ATTACH SERVICE LEVEL sl2 TO role2")

        raft_topology_enabled = self.is_consistent_topology_changes_enabled(sessions[0])
        service_level_table = "system.service_levels_v2" if raft_topology_enabled else "system_distributed.service_levels"

        nodes = self.cluster.nodelist()
        for version in self.current_upgrade_path:
            for node in nodes:
                node.upgrade(upgrade_to_version=version)
                assert node.status == Status.UP
                logger.info(f"Node '{node.name}' was upgraded.")

            if not raft_topology_enabled and ComparableScyllaVersion(nodes[0].node_scylla_version) >= ComparableScyllaVersion("2026.1-dev"):
                logger.info("Enable raft topology if it's not enabled yet")
                self.wait_upgrade_schema_on_raft_finished(nodes, connection_kwargs=self.admin_connection_kwargs)
                self.enable_raft_topology(nodes, connection_kwargs=self.admin_connection_kwargs)
                self.wait_for_sl_v2(nodes, connection_kwargs=self.admin_connection_kwargs)
                raft_topology_enabled = True
                service_level_table = "system.service_levels_v2"

        sessions = self.connect_sessions()
        wait_for(lambda: self.check_shares_column_added(sessions[0], service_level_table), timeout=60)
        wait_for(lambda: self.check_workload_prioritization_enabled(sessions[0]), timeout=60)
        marks = [node.mark_log() for node in nodes]

        sessions[0].execute("ALTER SERVICE LEVEL sl2 WITH shares = 400")
        sessions[0].execute("CREATE SERVICE LEVEL sl3 WITH shares = 500")
        sessions[0].execute("ATTACH SERVICE LEVEL sl3 TO role3")

        for node, session, mark in zip(nodes, sessions, marks):
            if raft_topology_enabled:
                read_barrier(session)
            node.watch_log_for('service level "sl3" was added.', from_mark=mark, timeout=30)

        clients = self.connect_clients()
        self.validate_connections_scheduling_groups(nodes)
        self.validate_connections_semaphore(clients)

        for role_clients in clients.values():
            for client in role_clients:
                client.shutdown()
        for session in sessions:
            session.shutdown()

        sessions[0].cluster.shutdown()
