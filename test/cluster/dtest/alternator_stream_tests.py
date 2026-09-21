import logging
import random
import time
from pprint import pformat

import pytest
from deepdiff import DeepDiff

from alternator.utils import enums
from alternator_utils import (
    NUM_OF_ITEMS,
    TABLE_NAME,
    BaseAlternatorStream,
    StreamsTable,
)
from tools.cluster import new_node
from tools.marks import issue_open, unmark, with_feature
from tools.retrying import retrying

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
@pytest.mark.next_gating
@pytest.mark.skip_if(with_feature("tablets") & issue_open("#23838"))
class TestAlternatorStreams(BaseAlternatorStream):
    def test_verify_all_nodes_have_same_stream(self):
        num_of_items = NUM_OF_ITEMS
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1 = self.cluster.nodelist()[0]
        stream_arn = self.prefill_dynamodb_table(node=node1, stream_specification=enums.StreamSpecification.KEYS_ONLY.value, num_of_items=num_of_items)[0]
        expected_items = [{self._table_primary_key: item[self._table_primary_key]} for item in self.create_items()]

        for node in self.cluster.nodelist():
            responses = self.get_responses(node=node, stream_arn=stream_arn, num_of_requests=num_of_items)
            records = self.extract_data_from_responses(responses, event_names=frozenset(("INSERT",)))
            diff = self.compare_table_keys_only_data(expected_table_data=expected_items, table_data=records)
            assert not diff, f"The following keys are missing '{pformat(diff)}'"

    @pytest.mark.cluster_options(uuid_sstable_identifiers_enabled=False)
    def test_verify_stream_records_after_topology_changed(self):  # noqa: PLR0915
        """
        The tests verify the data after topology changes - Stream during maintenance operations that alter topology
         (ex. Decommission/stop/delete/add node).
        Create a test that checks there are no new events after reading multiple records from different nodes.
        """

        def _add_new_nodes(nodes_size):
            new_nodes = []
            for node_idx in range(cluster_size + 1, cluster_size + 1 + nodes_size):
                logger.info(f"Adding new node{node_idx} to cluster")
                node = new_node(self.cluster, bootstrap=True, data_center="dc1", rack=f"rack{((node_idx - 1) % cluster_size) + 1}")
                node.start(wait_for_binary_proto=True, wait_other_notice=True)
                self.wait_for_alternator(node=node)
                logger.info(f"The node{node_idx} was successfully added")
                new_nodes.append(node)
            return new_nodes

        def _verify_items(_node, _expected_table_data, _num_of_requests, event_names):
            logger.info(f'Verifying the new items exists in "{_node.name}" node, expecting "{_num_of_requests}" items')
            responses = self.get_responses(node=_node, stream_arn=stream_arn, num_of_requests=_num_of_requests)
            records = self.extract_data_from_responses(responses, event_names)
            diff = self.compare_table_keys_only_data(expected_table_data=_expected_table_data, table_data=records)
            assert not diff, f"The following keys are missing '{pformat(diff)}'"

        stream_specification = enums.StreamSpecification.KEYS_ONLY.value
        table_name = TABLE_NAME
        cluster_size = 3
        self.prepare_dynamodb_cluster(num_of_nodes=cluster_size)
        node1, node2, node3 = self.cluster.nodelist()

        logger.info(f'Pre setup - Creating "{table_name}" table via "{node1.name}" node with "{stream_specification}" stream key')
        self.create_table(node=node1, table_name=table_name, stream_specification=stream_specification)
        items = self.create_items(num_of_items=400)
        logger.info(f'Waiting until stream of "{table_name}" table be active')
        stream_arn = self.wait_for_active_stream(node=node1, table_name=table_name)[0]

        node4, node5 = _add_new_nodes(nodes_size=2)
        selected_node = node1
        expected_table_data = new_items = items[:100]
        logger.info(f'Step 1 - Adding "{len(new_items)}" new items from "{node1.name}" node')
        self.batch_write_actions(table_name=table_name, node=node1, new_items=new_items)
        _verify_items(_node=selected_node, _expected_table_data=expected_table_data, _num_of_requests=len(expected_table_data), event_names=frozenset(("INSERT",)))

        new_items = items[100:200]
        expected_table_data.extend(new_items)
        logger.info(f'Step 2 - Adding "{len(new_items)}" new items from "{node2.name}" node')
        self.batch_write_actions(table_name=table_name, node=node2, new_items=new_items)
        logger.info(f'Decommission the "{selected_node.name}" node')
        selected_node.decommission()
        _verify_items(_node=node4, _expected_table_data=expected_table_data, _num_of_requests=len(expected_table_data), event_names=frozenset(("INSERT",)))

        new_items = items[200:300]
        expected_table_data.extend(new_items)
        logger.info(f'Step 3 - Adding "{len(new_items)}" new items from "{node3.name}" node')
        self.batch_write_actions(table_name=table_name, node=node3, new_items=new_items)
        logger.info(f'Stopping the "{node4.name}" node')
        node4.stop(wait_other_notice=True)
        logger.info(f'Starting the "{node4.name}" node')
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)
        self.wait_for_alternator(node=node4)
        _verify_items(_node=node3, _expected_table_data=expected_table_data, _num_of_requests=len(expected_table_data), event_names=frozenset(("INSERT",)))

        new_items = items[300:400]
        expected_table_data.extend(new_items)
        logger.info(f'Step 4 - Adding 100 new items from "{node4.name}" node')
        self.batch_write_actions(table_name=table_name, node=node4, new_items=new_items)
        logger.info(f'Deleting the "{node4.name}" node')
        self.cluster.remove(node4, wait_other_notice=True)
        _verify_items(_node=node5, _expected_table_data=expected_table_data, _num_of_requests=len(expected_table_data), event_names=frozenset(("INSERT",)))

    @pytest.mark.next_gating
    def test_list_streams_limit_parameter(self):
        """
        Test the list_streams command limit parameter.
        See that when the response is large enough, it can be paged
        correctly according the 'limit' value.
        Test steps:
        1. create and enable streams for 20 tables.
        2. read variable size chunks of these tables streams in list_streams command via random node.
        3. where using and verifying different 'limit' values + its total output.
        """
        stream_specification = enums.StreamSpecification.KEYS_ONLY.value
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1 = self.cluster.nodelist()[0]
        table_names = [TABLE_NAME + str(idx) for idx in range(20)]
        for table_name in table_names:
            self.create_table(node=node1, table_name=table_name, stream_specification=stream_specification)
        tables_arns = [self.wait_for_active_stream(node=node1, table_name=table_name)[0] for table_name in table_names]
        total_limited_stream_responses = []
        last_evaluated_stream_arn = None
        for limit in [5, 7, 3, 1, 4]:  # slice the created 20 tables to various size chunks
            params = {"Limit": limit}
            if last_evaluated_stream_arn:
                params["ExclusiveStartStreamArn"] = last_evaluated_stream_arn
            dynamodb_api = self.get_dynamodb_api(node=random.choice(self.cluster.nodelist()))
            result = dynamodb_api.stream.list_streams(**params)
            assert len(result["Streams"]) == limit, f"Got unexpected number of streams [{len(result['Streams'])}] for [{limit}] requested!"
            total_limited_stream_responses += result["Streams"]
            last_evaluated_stream_arn = result["LastEvaluatedStreamArn"]
        assert sorted(tables_arns) == sorted([stream["StreamArn"] for stream in total_limited_stream_responses]), f"Got unexpected ARN values by list streams paged responses: {total_limited_stream_responses}"
        empty_streams_list = dynamodb_api.stream.list_streams(ExclusiveStartStreamArn=last_evaluated_stream_arn)["Streams"]
        assert len(empty_streams_list) == 0, f"Got unexpected list of Streams after the last evaluated Stream: {empty_streams_list}"

    @unmark.next_gating  # https://github.com/scylladb/scylladb/issues/15260
    def test_updated_shards_during_add_decommission_node(self):
        """
        Verify how open Streams shards react while the same node is repeatedly
        decommissioned, wiped, and re-bootstrapped into the same rack.

        A background thread repeatedly performs:
        1. Decommission the node.
        2. Wipe the node.
        3. Re-bootstrap the node into the same rack.
        4. Wait 5 seconds.
        5. Start the next decommission.

        The initial open-shard snapshot is taken after the first decommission starts.
        Each comparison then spans the following actions:
        1. Wait for the next re-bootstrap to start.
        2. Wait for the next decommission to start.
        3. Refresh the open shards.
        4. Compare them with the snapshot taken before the re-bootstrap.

        With vnodes, each topology change commits a new cluster-wide CDC generation.
        The comparison is retried for up to 7 iterations until the open shards change.

        With tablets, a new generation is produced only by a tablet resize.
        Re-bootstrapping the same node into the same rack restores the existing
        min_per_shard_tablet_count floor, while tablet merging is forbidden when
        Alternator Streams are enabled. Therefore, no resize is expected, and the
        comparison is run for 3 iterations, requiring the open shards to remain
        unchanged in every iteration.
        """
        self.fixture_dtest_setup.ignore_log_patterns += [r"storage_proxy - exception during mutation.*exceptions::mutation_write_timeout_exception"]

        tablets_enabled = "tablets" in self.scylla_features
        stream_specification = enums.StreamSpecification.KEYS_ONLY.value
        self.prepare_dynamodb_cluster(num_of_nodes=4, topo={"dc1": {"rack1": 1, "rack2": 1, "rack3": 2}})
        node1 = self.cluster.nodelist()[0]

        logger.info(f'Pre setup - Creating "{TABLE_NAME}" table via "{node1.name}" node with "{stream_specification}" stream key')
        stream_arn = self.prefill_dynamodb_table(node=node1, stream_specification=stream_specification)[0]
        self.run_write_stress(table_name=TABLE_NAME, node=node1, num_of_item=1000, ignore_errors=True)
        # Seed both with the current log position BEFORE the churn thread
        # starts, so the helpers can only match cycles enacted by this test:
        # node1's log already contains one "bootstrap: accept node" line per
        # node that joined during initial cluster setup, so grepping from the
        # beginning of the log would match those instead of a churn add-node.
        add_node_log_marks: dict = {node1.name: node1.mark_log()}
        decommission_log_marks: dict = {node1.name: node1.mark_log()}
        decommission_thread = self.run_decommission_add_node_thread()
        wait_for_running_decommission(node=node1, log_marks=decommission_log_marks)
        streams_table = StreamsTable(stream_arn=stream_arn, dynamodb_api=self.get_dynamodb_api(node=node1))

        def open_shards():
            return [shard for shard in streams_table.shards if streams_table.is_shard_open(shard)]

        def open_shards_after_topology_change():
            """Re-read the open shards once a full add-node/decommission cycle has been observed."""
            wait_for_running_add_node(node=node1, log_marks=add_node_log_marks)
            wait_for_running_decommission(node=node1, log_marks=decommission_log_marks)
            streams_table.update_shards()
            assert streams_table.count_open_shards(), "No open shards found"
            return open_shards()

        def open_shards_diff():
            current_open_shards = open_shards()
            return DeepDiff(current_open_shards, open_shards_after_topology_change(), ignore_order=True, ignore_numeric_type_changes=True)

        @retrying(num_attempts=7, sleep_time=1, allowed_exceptions=NoOpenShardsDiffError)
        def wait_for_open_shards_diff():
            if not open_shards_diff():
                raise NoOpenShardsDiffError

        def verify_open_shards_unchanged(num_of_cycles: int = 3):
            for cycle in range(num_of_cycles):
                diff = open_shards_diff()
                assert not diff, f"Open shards changed in cycle #{cycle + 1} without a tablet resize: {pformat(diff)}"

        if tablets_enabled:
            verify_open_shards_unchanged()
        else:
            wait_for_open_shards_diff()
        decommission_thread.join()

    @unmark.next_gating  # https://github.com/scylladb/scylladb/issues/15260
    @pytest.mark.skip_if(issue_open("jira:DTEST-200") | (with_feature("tablets") & issue_open("scylladb/scylla-dtest#7189")))
    def test_sequence_numbers_during_add_decommission_node(self):
        """
        Verify shards sequence numbers on topology changes.
        1) calculate monotonic increasing sequence numbers comparing the StartingSequenceNumber of old and new shards.
        2) calculate monotonic increasing sequence numbers comparing the EndingSequenceNumber of old shards
           to EndingSequenceNumber of new shards.
        """

        stream_specification = enums.StreamSpecification.KEYS_ONLY.value
        self.prepare_dynamodb_cluster(num_of_nodes=4, topo={"dc1": {"rack1": 1, "rack2": 1, "rack3": 2}})
        node1 = self.cluster.nodelist()[0]
        logger.info(f'Pre setup - Creating "{TABLE_NAME}" table via "{node1.name}" node with "{stream_specification}" stream key')
        stream_arn = self.prefill_dynamodb_table(node=node1, stream_specification=stream_specification)[0]
        times = 1000 if self.cluster.scylla_mode != "debug" else 20
        self.put_table_items(table_name=TABLE_NAME, node=node1, num_of_items=times)
        # See test_updated_shards_during_add_decommission_node: seed before
        # the churn thread starts.
        add_node_log_marks: dict = {node1.name: node1.mark_log()}
        decommission_log_marks: dict = {node1.name: node1.mark_log()}
        decommission_thread = self.run_decommission_add_node_thread()
        streams_table = StreamsTable(stream_arn=stream_arn, dynamodb_api=self.get_dynamodb_api(node=node1))

        for cycle in range(3):
            logger.info(f"Starting cycle #{cycle + 1}..")
            # Get original shards metadata
            original_start_sequence_numbers_set = streams_table.start_sequence_numbers_set
            original_start_sequence_numbers = streams_table.start_sequence_numbers_list
            max_original_start_sequence_number = max(original_start_sequence_numbers) if original_start_sequence_numbers else -1

            # Update table shards after topology change
            self.put_table_items(table_name=TABLE_NAME, node=node1, num_of_items=times)
            wait_for_running_add_node(node=node1, log_marks=add_node_log_marks)
            self.put_table_items(table_name=TABLE_NAME, node=node1, num_of_items=times)
            wait_for_running_decommission(node=node1, log_marks=decommission_log_marks)
            self.put_table_items(table_name=TABLE_NAME, node=node1, num_of_items=times)
            streams_table.update_shards()

            # Get updated shards metadata
            new_start_sequence_numbers = [seq_num for seq_num in streams_table.start_sequence_numbers_list if seq_num not in original_start_sequence_numbers]
            assert streams_table.start_sequence_numbers_set - original_start_sequence_numbers_set, "Start-sequence-numbers are not changed after topology changes!"

            # Verify new CDC/Streams Generation attribute of monotonic increasing sequence numbers.
            if new_start_sequence_numbers:
                min_new_start_sequence_numbers = min(new_start_sequence_numbers)
                assert min_new_start_sequence_numbers > max_original_start_sequence_number, "New Start sequence number is not greater than previous level one"

            # Verify EndingSequenceNumber is greater than StartingSequenceNumber
            closed_shards = [shard for shard in streams_table.shards if not streams_table.is_shard_open(shard)]
            for shard in closed_shards:
                assert int(shard["SequenceNumberRange"]["EndingSequenceNumber"]) > int(shard["SequenceNumberRange"]["StartingSequenceNumber"]), "EndingSequenceNumber is not greater than StartingSequenceNumber"

        decommission_thread.join()

    @unmark.next_gating  # https://github.com/scylladb/scylladb/issues/15260
    def test_added_node_gets_closed_shards(self):
        """
        test scenario:
            1. create a table with Streams.
            2. run alternator stress. (or multiple stresses to multiple nodes needed?)
            3. add new node to cluster.
            4. wait for new node bootstrap.
            5. run streams APIs queries, connecting to the new node.
            6. see that it returns some 'closed' shards with EndingSequenceNumber.
            https://github.com/scylladb/scylla/pull/8209#issuecomment-790625323
        """
        stream_specification = enums.StreamSpecification.KEYS_ONLY.value
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1 = self.cluster.nodelist()[0]

        logger.info(f'Pre setup - Creating "{TABLE_NAME}" table via "{node1.name}" node with "{stream_specification}" stream key')
        stream_arn = self.prefill_dynamodb_table(node=node1, stream_specification=stream_specification)[0]
        stress_thread = self.run_write_stress(table_name=TABLE_NAME, node=node1, num_of_item=1000, ignore_errors=True)
        logger.info(f"Adding new node to cluster")
        node4 = new_node(self.cluster, bootstrap=True)
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)
        self.wait_for_alternator(node=node4)
        logger.info(f"{node4.name} was successfully added")
        streams_table = StreamsTable(stream_arn=stream_arn, dynamodb_api=self.get_dynamodb_api(node=node4))

        # A shard is closed only once a newer CDC generation exists, and the two replication
        # strategies commit that generation at very different moments:
        # * vnodes - the bootstrap topology operation itself commits a new cluster-wide
        #   generation, so the closed shards are already there once node4 serves traffic.
        # * tablets - generations are per table and are committed only when a tablet resize
        #   finalizes. Bootstrapping node4 raises the rack's tablet count floor (this
        #   depends on cluster topology), then the load balancer emits a split decision,
        #   and the new generation lands only after every replica finished split-compacting.
        #   All of that runs asynchronously to the bootstrap, so the shards have to be polled.

        @retrying(num_attempts=90, sleep_time=2, allowed_exceptions=AssertionError, message="waiting for closed shards")
        def wait_for_closed_shards():
            streams_table.update_shards()
            closed_shards = [shard for shard in streams_table.shards if not streams_table.is_shard_open(shard)]
            assert closed_shards, f"New node {node4.name} has no closed shards"

        wait_for_closed_shards()
        stress_thread.join()


def wait_for_running_decommission(node, log_marks: dict, timeout: int = 1800):
    """Wait until a decommission cycle is detected on the cluster.

    Under Raft topology the decommission state machine transitions through
    all its phases in ~150ms, so the UL (Up/Leaving) gossip state cannot
    reliably be caught by `nodetool status` polling. Prefer detecting the
    raft_topology coordinator log line emitted at decommission start, and
    fall back to nodetool UL polling so the helper still works on gossip-
    based topologies that do not produce that log line.

    ``log_marks`` is a caller-owned dict (keyed by node name) that tracks
    the log position across repeated calls within the same test so each new
    decommission cycle is matched exactly once.  Callers should seed it with
    ``{node.name: node.mark_log()}`` before triggering any topology changes
    and pass the same instance on every call for that test: an unseeded dict
    makes the first call grep the log from its very beginning, which stays
    correct only for as long as nothing earlier in the node's lifetime logged
    a "start decommission" line.
    """
    from_mark = log_marks.get(node.name, 0)
    deadline = time.time() + timeout
    while time.time() < deadline:
        matches = node.grep_log("raft_topology - updating topology state: start decommission", from_mark=from_mark)
        if matches:
            log_marks[node.name] = node.mark_log()
            logger.debug(f"Detected raft-topology decommission start on {node.name}")
            return
        out, _err = node.nodetool("status", capture_output=True)
        logger.debug(f"nodetool status is: {out}")
        if "UL" in out:
            log_marks[node.name] = node.mark_log()
            return
        time.sleep(2)
    raise AssertionError(f"No decommission detected on {node.name} within {timeout}s (neither raft_topology start marker nor gossip UL state observed)")


def wait_for_running_add_node(node, log_marks: dict, timeout: int = 900):
    """Wait until a bootstrap (add-node) cycle is detected on the cluster.

    Under Raft topology the bootstrap state machine transitions through all
    its phases in well under a second, so the UJ (Up/Joining) gossip state
    cannot reliably be caught by `nodetool status` polling (mirrors the same
    race documented on `wait_for_running_decommission`). Prefer detecting the
    raft_topology coordinator log line emitted at bootstrap start, and fall
    back to nodetool UJ polling so the helper still works on gossip-based
    topologies that do not produce that log line.

    ``log_marks`` is a caller-owned dict (keyed by node name) that tracks the
    log position across repeated calls within the same test so each new
    bootstrap cycle is matched exactly once. Callers must seed it with
    ``{node.name: node.mark_log()}`` before any topology changes are
    triggered, and pass the same instance on every call for that test.
    Unlike decommissions, "bootstrap: accept node" lines already exist in
    the coordinator log from the initial cluster setup, so an unseeded dict
    (grepping from the start of the log) would match those setup-time lines
    and report an add-node that never ran.
    """
    from_mark = log_marks.get(node.name, 0)
    deadline = time.time() + timeout
    while time.time() < deadline:
        matches = node.grep_log("raft_topology - updating topology state: bootstrap: accept node", from_mark=from_mark)
        if matches:
            log_marks[node.name] = node.mark_log()
            logger.debug(f"Detected raft-topology bootstrap start on {node.name}")
            return
        out, _err = node.nodetool("status", capture_output=True)
        logger.debug(f"nodetool status is: {out}")
        if "UJ" in out:
            log_marks[node.name] = node.mark_log()
            return
        time.sleep(2)
    raise AssertionError(f"No add-node detected on {node.name} within {timeout}s (neither raft_topology bootstrap marker nor gossip UJ state observed)")


class NoOpenShardsDiffError(Exception):
    pass
