from typing import NamedTuple, Optional

import pytest
import requests

from dtest_class import Tester, create_cf, create_ks, read_barrier
from tools.cluster import run_rest_api
from tools.rackdc import update_properties


class TabletReplicas(NamedTuple):
    last_token: int
    replicas: list[tuple[str, int]]


class MoveParams(NamedTuple):
    ks: str
    table: str
    src_host: str
    src_shard: int
    dst_host: str
    dst_shard: int
    token: int


def get_table_id(session, keyspace: str, table: str):
    rows = session.execute(f"select id from system_schema.tables where keyspace_name = '{keyspace}' and table_name = '{table}'")
    return rows[0].id


def get_all_tablet_replicas(session, keyspace_name: str, table_name: str) -> list[TabletReplicas]:
    """
    Retrieves the tablet distribution for a given table.
    This call is guaranteed to see all prior changes applied to group0 tables.

    :param server: server to query. Can be any live node.
    """
    # read_barrier is needed to ensure that local tablet metadata on the queried node
    # reflects the finalized tablet movement.
    read_barrier(session)

    table_id = get_table_id(session, keyspace_name, table_name)
    rows = session.execute(f"SELECT last_token, replicas FROM system.tablets where table_id = {table_id}")
    return [TabletReplicas(last_token=x.last_token, replicas=[(str(host), shard) for (host, shard) in x.replicas]) for x in rows]


def get_tablet_replicas(session, keyspace_name: str, table_name: str, token: int) -> list[tuple[str, int]]:
    """
    Gets tablet replicas of the tablet which owns a given token of a given table.
    This call is guaranteed to see all prior changes applied to group0 tables.

    :param server: server to query. Can be any live node.
    """
    rows = get_all_tablet_replicas(session, keyspace_name, table_name)
    for row in rows:
        if row.last_token >= token:
            return row.replicas
    return []


def move_tablet(node, params: MoveParams, force: bool = False, expected_status: int = 200):
    params = {"ks": params.ks, "table": params.table, "token": params.token, "src_host": params.src_host, "dst_host": params.dst_host, "src_shard": params.src_shard, "dst_shard": params.dst_shard, "force": "true" if force else "false"}
    res = requests.post(f"http://{node.address()}:{node.api_port}/storage_service/tablets/move", params=params)
    assert res.status_code == expected_status


def get_non_replica_node(nodes, replicas):
    no_replica_nodes = [node for node in nodes if node.hostid() not in [replica[0] for replica in replicas]]
    assert len(no_replica_nodes) > 0, "No non-replica nodes found"
    return no_replica_nodes[0]


@pytest.mark.required_features("tablets")
class TestTablets(Tester):
    def test_moving_tablets_different_dc(self):
        cluster = self.cluster
        cluster.populate([1, 1]).start()
        nodes = cluster.nodelist()

        ks_name = "ks"
        table_name = "cf"
        token = 0
        session = self.patient_exclusive_cql_connection(nodes[0])
        create_ks(session, ks_name, rf={"dc1": 1, "dc2": 0}, tablets=1)
        create_cf(session, table_name, columns={"c1": "text", "c2": "text"})

        replicas = get_tablet_replicas(session, ks_name, table_name, token)
        assert len(replicas) == 1, "Incorrect replicas number"

        no_replica_node = get_non_replica_node(nodes, replicas)
        move_tablet(nodes[1], MoveParams(ks_name, table_name, replicas[0][0], replicas[0][1], no_replica_node.hostid(), 0, token), force=False, expected_status=500)

        move_tablet(nodes[1], MoveParams(ks_name, table_name, replicas[0][0], replicas[0][1], no_replica_node.hostid(), 0, token), force=True)

    def test_moving_tablets_rack_availability(self):
        cluster = self.cluster
        cluster.populate([3])
        cluster.set_configuration_options(values={"endpoint_snitch": "org.apache.cassandra.locator.GossipingPropertyFileSnitch"})

        nodes = cluster.nodelist()
        update_properties(nodes=[nodes[0], nodes[1]], properties={"rack": "rc0"})
        update_properties(nodes=[nodes[2]], properties={"rack": "rc1"})

        cluster.start()
        nodes = cluster.nodelist()

        ks_name = "ks"
        table_name = "cf"
        token = 0
        session = self.patient_exclusive_cql_connection(nodes[0])
        create_ks(session, ks_name, rf=2, tablets=1)
        create_cf(session, table_name, columns={"c1": "text", "c2": "text"})

        replicas = get_tablet_replicas(session, ks_name, table_name, token)
        assert len(replicas) == 2, "Incorrect replicas number"

        no_replica_node = get_non_replica_node(nodes, replicas)
        assert nodes[2].hostid() != no_replica_node.hostid(), "Data distributed irregularly"
        rc1_replica = replicas[0] if replicas[0][0] == nodes[2].hostid() else replicas[1]

        move_tablet(nodes[1], MoveParams(ks_name, table_name, rc1_replica[0], rc1_replica[1], no_replica_node.hostid(), 0, token), force=False, expected_status=500)

        move_tablet(nodes[1], MoveParams(ks_name, table_name, rc1_replica[0], rc1_replica[1], no_replica_node.hostid(), 0, token), force=True)
