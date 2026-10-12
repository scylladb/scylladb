from cassandra import ConsistencyLevel
from cassandra.policies import DCAwareRoundRobinPolicy, RetryPolicy, TokenAwarePolicy
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.internal_types import ServerInfo
VNODES = {"tablets_mode_for_new_keyspaces": "disabled"}
class FlakyRetryPolicy(RetryPolicy):
    """The dtests' retry policy: timeouts are retried 5 times and
    Unavailable 3 times, on the same node."""
    max_retries = 5
    max_unavail_retries = 3

    def on_read_timeout(self, *args, **kwargs):
        return (self.RETRY, None) if kwargs["retry_num"] < self.max_retries else (self.RETHROW, None)

    def on_write_timeout(self, *args, **kwargs):
        return (self.RETRY, None) if kwargs["retry_num"] < self.max_retries else (self.RETHROW, None)

    def on_unavailable(self, *args, **kwargs):
        return (self.RETRY, None) if kwargs["retry_num"] < self.max_unavail_retries else (self.RETHROW, None)
def dtest_session(cluster, keyspace: str | None = None):
    session = cluster.connect()
    profile = session.get_execution_profile(EXEC_PROFILE_DEFAULT)
    profile.consistency_level = ConsistencyLevel.ONE
    profile.serial_consistency_level = None
    profile.retry_policy = FlakyRetryPolicy()
    if keyspace:
        session.set_keyspace(keyspace)
    return session
def patient_cql_connection(manager: ScyllaClusterManager, server: ServerInfo, keyspace: str | None = None):
    """The dtests' patient_cql_connection(): the driver's default token- and
    DC-aware policy, the node as contact point."""
    return dtest_session(manager.con_gen([server.ip_addr], load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy())), keyspace)
def create_ks(session, name: str, rf: int | dict[str, int]) -> None:
    """The dtests' create_ks(): NetworkTopologyStrategy, then USE it."""
    if isinstance(rf, int):
        replication = f"'class':'NetworkTopologyStrategy', 'replication_factor':{rf}"
    else:
        replication = "'class':'NetworkTopologyStrategy', " + ", ".join(f"'{dc}':{n}" for dc, n in rf.items())
    session.execute(f"CREATE KEYSPACE {name} WITH replication={{{replication}}}")
    session.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    session.execute(f"USE {name}")
