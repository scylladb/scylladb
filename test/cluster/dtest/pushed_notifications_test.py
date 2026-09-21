import logging
import time
from threading import Event

import pytest
from cassandra import ConsistencyLevel, ReadFailure, ReadTimeout
from cassandra.query import SimpleStatement
from filelock import FileLock
from packaging.version import Version

from dtest_class import Tester, create_cf, create_ks, get_ip_from_node
from tools.assertions import assert_invalid
from tools.cluster_topology import generate_cluster_topology
from tools.data import insert_c1c2

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def using_localhost(fixture_dtest_setup):
    """
    make sure tests using localhost are not running at the same time is pytest-xdist is used
    """
    logging.getLogger("filelock").setLevel(logging.INFO)
    with FileLock("/tmp/localhost"):
        yield


class NotificationWaiter:
    """
    A helper class for waiting for pushed notifications from
    Cassandra over the native protocol.
    """

    def __init__(self, tester, node, notification_types, keyspace=None):
        """
        `address` should be a ccmlib.node.Node instance
        `notification_types` should be a list of
        "TOPOLOGY_CHANGE", "STATUS_CHANGE", and "SCHEMA_CHANGE".
        """
        self.node = node
        self.address = node.network_interfaces["binary"][0]
        self.notification_types = notification_types
        self.keyspace = keyspace

        # get a single, new connection
        session = tester.patient_cql_connection(node)
        self.connection = session.cluster.connection_factory(self.address, is_control_connection=True)

        # coordinate with an Event
        self.event = Event()

        # the pushed notification
        self.notifications = []

        # register a callback for the notification type
        for notification_type in notification_types:
            self.connection.register_watcher(notification_type, self.handle_notification, register_timeout=5.0)

    def handle_notification(self, notification):
        """
        Called when a notification is pushed from Cassandra.
        """
        logger.debug(f"Source {self.address} sent {notification}")

        if self.keyspace and notification["keyspace"] and self.keyspace != notification["keyspace"]:
            return  # we are not interested in this schema change

        self.notifications.append(notification)
        self.event.set()

    def wait_for_notifications(self, timeout, num_notifications=1):
        """
        Waits up to `timeout` seconds for notifications from Cassandra. If
        passed `num_notifications`, stop waiting when that many notifications
        are observed.
        """

        deadline = time.time() + timeout
        while time.time() < deadline:
            self.event.wait(deadline - time.time())
            self.event.clear()
            if len(self.notifications) >= num_notifications:
                break

        return self.notifications

    def clear_notifications(self):
        self.notifications = []
        self.event.clear()

    def close(self):
        self.connection.close()


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestPushedNotifications(Tester):
    """
    Tests for pushed native protocol notification from Cassandra.
    """

    def test_restart_node(self, request: pytest.FixtureRequest):
        """
        @jira_ticket CASSANDRA-7816
        Restarting a node should generate exactly one DOWN and one UP notification
        """
        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()

        waiter = NotificationWaiter(self, node1, ["STATUS_CHANGE", "TOPOLOGY_CHANGE"])
        request.addfinalizer(lambda: waiter.close())
        # need to block for up to 2 notifications (NEW_NODE and UP) so that these notifications
        # don't confuse the state below.
        logger.debug("Waiting for unwanted notifications...")
        waiter.wait_for_notifications(timeout=30, num_notifications=2)
        waiter.clear_notifications()

        # On versions prior to 2.2, an additional NEW_NODE notification is sent when a node
        # is restarted. This bug was fixed in CASSANDRA-11038 (see also CASSANDRA-11360)
        version = self.cluster.cassandra_version()
        logger.debug(f"Version={version}")
        expected_notifications = 2 if version >= "2.2" else 3
        for i in range(5):
            logger.debug("Restarting second node...")
            node2.stop(wait_other_notice=True)
            node2.start(wait_other_notice=True)
            logger.debug(f"Waiting for notifications from {waiter.address}")
            notifications = waiter.wait_for_notifications(timeout=60.0, num_notifications=expected_notifications)
            assert expected_notifications, len(notifications) == notifications
            for notification in notifications:
                assert get_ip_from_node(node2) == notification["address"][0]
            assert "DOWN" == notifications[0]["change_type"]
            if version >= "2.2":
                assert "UP" == notifications[1]["change_type"]
            else:
                # pre 2.2, we'll receive both a NEW_NODE and an UP notification,
                # but the order is not guaranteed
                assert {"NEW_NODE", "UP"} == set([n["change_type"] for n in notifications[1:]])

            waiter.clear_notifications()

    def test_sleep_and_restart_node(self, request: pytest.FixtureRequest):
        """
        Sleep 120 seconds after cluster is ready, then restart the second node,
        check we get correct client notifications during restart
        """
        cluster = self.cluster
        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        cluster.start(wait_for_binary_proto=True)
        # Sleep 120 to wait the pending joined notification to be sent
        time.sleep(120)

        # register for notification with node1
        waiter = NotificationWaiter(self, node1, ["STATUS_CHANGE", "TOPOLOGY_CHANGE"])
        request.addfinalizer(lambda: waiter.close())
        # restart node 2
        logger.debug("Restarting second node...")
        node2.stop(wait_other_notice=True)
        node2.start(wait_other_notice=True)

        # check that node1 did not send UP or DOWN notification for node2
        logger.debug(f"Waiting for notifications from {waiter.address}")
        notifications = waiter.wait_for_notifications(timeout=30.0, num_notifications=2)
        assert 2 == len(notifications)
        for notification in notifications:
            assert node2.address() == notification["address"][0]
        assert "DOWN" == notifications[0]["change_type"]
        assert "UP" == notifications[1]["change_type"]

    @pytest.mark.usefixtures("using_localhost")
    def test_restart_node_localhost(self, request: pytest.FixtureRequest):
        """
        Test that we don't get client notifications when rpc_address is set to localhost Pre 4.0.
        Test that we get correct client notifications when rpc_address is set to localhost Post 4.0.
        @jira_ticket  CASSANDRA-10052
        @jira_ticket  CASSANDRA-15677

        Scylla doesn't support nodes with same IP, it's worth to test it.

        To set-up this test we override the rpc_address to "localhost" for all nodes, and
        therefore we must change the rpc port or else processes won't start.
        """
        cluster = self.cluster
        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        i = 0  # change 'rpc_address' from '127.0.0.x' to 'localhost' and diversify port numbers
        for node in cluster.nodelist():
            node.network_interfaces["binary"] = ("localhost", node.network_interfaces["binary"][1] + i)
            node.import_config_files()  # this regenerates the yaml file and sets 'rpc_address' to the 'binary' address
            # `native_shard_aware_transport_port' has default value (19042) in scylla.yaml,
            # so it's need to be unique in this test.
            node.set_configuration_options(values={"native_shard_aware_transport_port": node.network_interfaces["binary"][1] + 10000})
            i += 1
            logger.debug(node.show())

        def start_node(n):
            mark = n.mark_log()
            p = n.start()
            n.wait_for_binary_interface(process=p, from_mark=mark)

        start_node(node1)
        # register for notification with node1
        waiter = NotificationWaiter(self, node1, ["STATUS_CHANGE", "TOPOLOGY_CHANGE"])
        request.addfinalizer(lambda: waiter.close())
        start_node(node2)

        def wait_for_notifications(expected_notifications):
            logger.debug(f"Waiting for notifications {expected_notifications} from {waiter.address}")
            notifications = waiter.wait_for_notifications(timeout=30.0, num_notifications=len(expected_notifications))
            logger.debug(f"Received {len(notifications)} notifications: {notifications}")
            assert len(notifications) == len(expected_notifications)
            for notification in notifications:
                assert node2.address() == notification["address"][0]
            assert [n["change_type"] for n in notifications] == expected_notifications

        wait_for_notifications(["NEW_NODE", "UP"])

        # restart node 2
        logger.debug("Restarting second node...")
        node2.stop(wait_other_notice=True)
        node2.start(wait_other_notice=True)

        wait_for_notifications(["NEW_NODE", "UP", "DOWN", "UP"])

    def test_new_node_event_delay(self, request: pytest.FixtureRequest):
        """
        NEW_NODE event is delayed, otherwise cql client will connect Scylla server
        even the new node isn't ready.
        """
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1)
        cluster.populate(cluster_topology)
        node1, node2 = cluster.nodelist()

        node1.start(wait_for_binary_proto=True)

        # Register for notifications with node1
        waiter = NotificationWaiter(self, node1, ["STATUS_CHANGE", "TOPOLOGY_CHANGE"])
        request.addfinalizer(lambda: waiter.close())

        logger.debug("Start the second node, expect the NEW_NODE event is delayed until the cql server is ready")
        node2.start()

        logger.debug(f"Waiting for notifications from {waiter.address}")
        notifications = waiter.wait_for_notifications(timeout=30.0, num_notifications=1)

        # Try to connect the server when any notification is received
        session = self.cql_connection(node2)
        create_ks(session, "ks", 2)
        create_cf(session, "cf", columns={"c1": "text", "c2": "text"})
        insert_c1c2(session, keys=range(100))

        received_new_node_event = False
        for notification in notifications:
            assert node2.address() == notification["address"][0]
            if "NEW_NODE" == notification["change_type"]:
                received_new_node_event = True
        assert received_new_node_event, "NEW_NODE event isn't received"


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestVariousNotifications(Tester):
    """
    Tests for various notifications/messages from Cassandra.
    """

    @pytest.mark.skip("Scylla doesn't support `tombstone_failure_threshold', read railure won't be triggered")
    def test_tombstone_failure_threshold_message(self):
        """
        Ensure nodes return an error message in case of TombstoneOverwhelmingExceptions rather
        than dropping the request. A drop makes the coordinator waits for the specified
        read_request_timeout_in_ms.
        @jira_ticket CASSANDRA-7886
        """

        self.cluster.set_configuration_options(
            values={
                "tombstone_failure_threshold": 500,
                "read_request_timeout_in_ms": 30000,  # 30 seconds
                "range_request_timeout_in_ms": 40000,
            }
        )
        self.cluster.populate(3).start()
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)

        create_ks(session, "test", 3)
        session.execute("CREATE TABLE test ( id int, mytext text, col1 int, col2 int, col3 int, PRIMARY KEY (id, mytext) )")

        # Add data with tombstones
        values = map(lambda i: str(i), range(1000))
        for value in values:
            session.execute(SimpleStatement(f"insert into test (id, mytext, col1) values (1, '{value}', null) ", consistency_level=ConsistencyLevel.ALL))

        failure_msg = "Scanned over.* tombstones.* query aborted"
        self.ignore_log_patterns += [failure_msg]

        @pytest.mark.timeout(25)
        def read_failure_query():
            assert_invalid(
                session,
                SimpleStatement("select * from test where id in (1,2,3,4,5)", consistency_level=ConsistencyLevel.ALL),
                expected=ReadTimeout if Version(self.cluster.version()) < Version("3.0") else ReadFailure,
            )

        read_failure_query()

        failure = node1.grep_log(failure_msg) or node2.grep_log(failure_msg) or node3.grep_log(failure_msg)

        assert failure, "Cannot find tombstone failure threshold error in log after failed query"
        mark1 = node1.mark_log()
        mark2 = node2.mark_log()
        mark3 = node3.mark_log()

        @pytest.mark.timeout(35)
        def range_request_failure_query():
            assert_invalid(
                session,
                SimpleStatement("select * from test", consistency_level=ConsistencyLevel.ALL),
                expected=ReadTimeout if Version(self.cluster.version()) < Version("3.0") else ReadFailure,
            )

        range_request_failure_query()

        failure = node1.watch_log_for(failure_msg, from_mark=mark1, timeout=5) or node2.watch_log_for(failure_msg, from_mark=mark2, timeout=5) or node3.watch_log_for(failure_msg, from_mark=mark3, timeout=5)

        assert failure, "Cannot find tombstone failure threshold error in log after range_request_timeout_query"
