import logging
from typing import NamedTuple

from cassandra import ConsistencyLevel
from cassandra.cluster import ResultSet, Session
from cassandra.concurrent import execute_concurrent_with_args

from dtest_class import Tester, create_ks
from tools.cluster_topology import generate_cluster_topology

LOGGER = logging.getLogger(__name__)


class ConnectionArgs(NamedTuple):
    protocol_version: str = None
    user: bool = None
    password: str = None


class ClusterSetupArgs(NamedTuple):
    nodes: int = 3
    rf: int = 2
    start_rpc: bool = False
    use_cache: bool = False
    configuration_options: dict = {}


class CQLTester(Tester):
    def prepare(self, connection_args: ConnectionArgs, cluster_setup_args: ClusterSetupArgs, create_keyspace: bool = False) -> Session:
        cluster = self.cluster

        if cluster_setup_args.use_cache:
            cluster.set_configuration_options(values={"row_cache_size_in_mb": 100})

        cluster.set_configuration_options(values={"start_rpc": cluster_setup_args.start_rpc})

        if connection_args.user:
            config = {"authenticator": "org.apache.cassandra.auth.PasswordAuthenticator", "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer", "permissions_validity_in_ms": 0}
            cluster.set_configuration_options(values=config)

        if cluster_setup_args.configuration_options:
            LOGGER.debug("Setting cluster configuration_options: %s", cluster_setup_args.configuration_options)
            cluster.set_configuration_options(values=cluster_setup_args.configuration_options)

        if not cluster.nodelist():
            cluster.populate(generate_cluster_topology(rack_num=cluster_setup_args.nodes)).start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(cluster.nodelist()[0], protocol_version=connection_args.protocol_version, user=connection_args.user, password=connection_args.password)

        if create_keyspace:
            create_ks(session, "ks", cluster_setup_args.rf)

        return session

    @staticmethod
    def _create_schema(session: Session, cf_name: str, node_count: int, schema: str):
        session.execute(f"CREATE KEYSPACE ks WITH replication = {{ 'class':'NetworkTopologyStrategy', 'replication_factor': {node_count!s}}} AND DURABLE_WRITES = true")
        session.execute(f"CREATE TABLE {cf_name} {schema}")

    @staticmethod
    def _insert_data(session: Session, data: list, cf_name: str, columns: str):
        statement = session.prepare(f"INSERT INTO {cf_name} {columns} VALUES (?, ?, ?, ?)")
        statement.consistency_level = ConsistencyLevel.QUORUM
        execute_concurrent_with_args(session=session, statement=statement, parameters=data)

    @staticmethod
    def _create_index(session: Session, cf_name: str, indexed_col: str):
        create_index_cmd = f"CREATE INDEX ON {cf_name} ({indexed_col})"
        session.execute(create_index_cmd)

    @staticmethod
    def _validate_rows(query_result: ResultSet, expected_result: list[dict], expected_count: int):
        query_result_all = query_result.all()
        assert query_result_all
        assert len(query_result_all) == expected_count

        zipped = zip([item._asdict() for item in query_result_all], expected_result)

        for result, expected in zipped:
            for key, value in result.items():
                exprected_value = expected.get(key, "")
                assert exprected_value == value, f"Actual value: {expected} does not equal expected value: {value}"
