import re

from cassandra.cluster import Session, SimpleStatement
from ccmlib.scylla_cluster import ScyllaNode

from dtest_class import Tester, wait_for

UUID_REGEX = re.compile(r"([0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{12})")


class NoRaftTopologyCoordinatorNodeFoundError(Exception):
    """Raise if topology coordinator node was not found by host id"""


class TopologyCoordinatorFinder:
    def __init__(self, tester: Tester) -> None:
        self.tester: Tester = tester
        self.num_of_elections: int = 0

    def _parse_coordinator_election_history(self) -> list[str]:
        stm = SimpleStatement("select description from system.group0_history where key = 'history' and description LIKE 'Starting new topology coordinator%' ALLOW FILTERING;")
        session: Session = self.tester.patient_cql_connection(self.tester.cluster.nodelist())
        result = session.execute(stm)
        coordinators_ids = []
        for row in result:
            if match := UUID_REGEX.search(row.description):
                coordinators_ids.append(match.group(1))
        assert coordinators_ids, "No host ids were found in raft history"
        self.num_of_elections = len(coordinators_ids)
        return coordinators_ids

    def get_topology_coordinator_node(self) -> ScyllaNode:
        latest_elected_host_id = self._parse_coordinator_election_history()[0]
        for node in self.tester.cluster.nodelist():
            if node.hostid() == latest_elected_host_id:
                return node
        raise NoRaftTopologyCoordinatorNodeFoundError()

    def wait_topology_coordinator_elected(self, timeout: int = 10) -> None:
        current_num_of_elections = self.num_of_elections

        def check_election_history() -> bool:
            self._parse_coordinator_election_history()
            return self.num_of_elections > current_num_of_elections

        wait_for(check_election_history, text="Wait topology coordinator reelection...", timeout=timeout)


def get_raft_group_id(session: Session) -> str:
    result = session.execute("select value from system.scylla_local where key = 'raft_group0_id'").one()
    return result.value if result else ""


def get_raft_snapshot_id(session: Session) -> str:
    result = session.execute("select snapshot_id from system.raft limit 1").one()
    return result.snapshot_id if result else ""
