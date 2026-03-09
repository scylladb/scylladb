#
# Copyright (C) 2020-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import random
from pprint import pformat

import pytest
from alternator.utils import enums
from alternator_utils import (
    NUM_OF_ITEMS,
    TABLE_NAME,
    BaseAlternatorStream,
    StreamsTable,
)
from test.pylib.skip_types import skip_not_implemented
from tools.retrying import retrying
from tools.cluster import new_node

logger = logging.getLogger(__name__)


class TestAlternatorStreams(BaseAlternatorStream):
    @pytest.fixture(autouse=True)
    def skip_if_tablets(self):
        if self.dtest_config.tablets:
            skip_not_implemented("Alternator Streams not yet supported with tablets (issue #23838)")

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
                node = new_node(self.cluster, bootstrap=True)
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

    def test_updated_shards_during_add_decommission_node(self):
        """
        Verify Streams stays healthy across a one-shot decommission+add-node workflow.

        For vnode-based CDC we expect stream metadata to change after topology
        changes. For tablet-based CDC topology changes should not affect the
        number of streams, so we verify the open shard count is unchanged and
        matches the CDC log tablet count.
        """
        self.fixture_dtest_setup.ignore_log_patterns += [
            r"storage_proxy - exception during mutation.*exceptions::mutation_write_timeout_exception"
        ]

        stream_specification = enums.StreamSpecification.KEYS_ONLY.value
        self.prepare_dynamodb_cluster(num_of_nodes=4)
        node1 = self.cluster.nodelist()[0]

        logger.info(
            f'Pre setup - Creating "{TABLE_NAME}" table via "{node1.name}" '
            f'with "{stream_specification}" stream key'
        )
        stream_arn = self.prefill_dynamodb_table(
            node=node1,
            stream_specification=stream_specification,
        )[0]

        stress_thread = self.run_write_stress(
            table_name=TABLE_NAME,
            node=node1,
            num_of_item=1000,
            ignore_errors=True,
        )

        try:
            streams_table = StreamsTable(
                stream_arn=stream_arn,
                dynamodb_api=self.get_dynamodb_api(node=node1),
            )

            initial_open_shard_ids = set(streams_table.open_shard_ids_set)
            initial_shard_ids = set(streams_table.shard_ids_set)
            initial_open_shard_count = streams_table.count_open_shards()

            topology_result = self.run_decommission_add_node_once()

            if self.dtest_config.tablets:
                # With tablets, topology changes should NOT affect the number
                # of streams — only tablet splits/merges do.  Verify the open
                # shard count is unchanged and still matches the tablet count.
                cql_session = self.patient_cql_connection(node1)
                wait_for_stable_alternator_api(
                    test=self,
                    node=node1,
                    table_name=TABLE_NAME,
                    probes=2,
                )
                assert_open_shards_match_tablet_count(
                    cql_session=cql_session,
                    table_name=TABLE_NAME,
                    streams_table=streams_table,
                )
                streams_table.update_shards()
                post_open_shard_count = streams_table.count_open_shards()
                assert post_open_shard_count == initial_open_shard_count, (
                    f"Open shard count changed after topology operation "
                    f"({initial_open_shard_count} -> {post_open_shard_count}), "
                    f"but topology changes should not affect tablet-based streams"
                )
            else:
                logger.debug("Waiting for stream metadata to change after topology operation")
                check_vnode_streams_changed_after_topology(
                    test=self,
                    node=node1,
                    table_name=TABLE_NAME,
                    streams_table=streams_table,
                    initial_open_shard_ids=initial_open_shard_ids,
                    initial_shard_ids=initial_shard_ids,
                )
            wait_for_expected_topology_effect(
                test=self,
                removed_node_name=topology_result["removed_node_name"],
                added_node=topology_result["added_node"],
                table_name=TABLE_NAME,
            )
        finally:
            stress_thread.join()

    def test_sequence_numbers_during_add_decommission_node(self):
        """
        Verify streams/shards behavior on topology changes.

        For vnode-based CDC:
          1) new start sequence numbers eventually appear,
          2) new start sequence numbers are monotonically increasing,
          3) closed shards have EndingSequenceNumber > StartingSequenceNumber.

        For tablet-based CDC:
          topology changes should not affect the number of streams.
          Verify that open shard count remains stable and matches
          the CDC log tablet count after each topology round.
        """
        stream_specification = enums.StreamSpecification.KEYS_ONLY.value
        self.prepare_dynamodb_cluster(num_of_nodes=4)
        node1 = self.cluster.nodelist()[0]

        logger.info(
            f'Pre setup - Creating "{TABLE_NAME}" table via "{node1.name}" '
            f'with "{stream_specification}" stream key'
        )
        stream_arn = self.prefill_dynamodb_table(
            node=node1,
            stream_specification=stream_specification,
        )[0]

        times = 1000 if self.cluster.scylla_mode != "debug" else 20

        self.put_table_items(
            table_name=TABLE_NAME,
            node=node1,
            num_of_items=times,
        )

        streams_table = StreamsTable(
            stream_arn=stream_arn,
            dynamodb_api=self.get_dynamodb_api(node=node1),
        )

        rounds = 2

        for round_idx in range(rounds):
            logger.info("Starting topology round %s", round_idx + 1)

            streams_table.update_shards()
            original_start_sequence_numbers_set = set(streams_table.start_sequence_numbers_set)
            original_start_sequence_numbers = list(streams_table.start_sequence_numbers_list)
            max_original_start_sequence_number = (
                max(original_start_sequence_numbers)
                if original_start_sequence_numbers else -1
            )
            initial_open_shard_count = streams_table.count_open_shards()

            topology_result = self.run_decommission_add_node_once()

            if self.dtest_config.tablets:
                # With tablets, topology changes should NOT affect the number
                # of streams — only tablet splits/merges do.
                wait_for_stable_alternator_api(
                    test=self,
                    node=node1,
                    table_name=TABLE_NAME,
                    probes=2,
                )
                cql_session = self.patient_cql_connection(node1)
                assert_open_shards_match_tablet_count(
                    cql_session=cql_session,
                    table_name=TABLE_NAME,
                    streams_table=streams_table,
                )
                streams_table.update_shards()
                post_open_shard_count = streams_table.count_open_shards()
                assert post_open_shard_count == initial_open_shard_count, (
                    f"Open shard count changed after topology round {round_idx + 1} "
                    f"({initial_open_shard_count} -> {post_open_shard_count}), "
                    f"but topology changes should not affect tablet-based streams"
                )
            else:
                wait_for_vnode_sequence_change_after_topology(
                    test=self,
                    node=node1,
                    table_name=TABLE_NAME,
                    streams_table=streams_table,
                    old_start_sequence_numbers_set=original_start_sequence_numbers_set,
                    writes_per_probe=times,
                )

                new_start_sequence_numbers = [
                    seq_num
                    for seq_num in streams_table.start_sequence_numbers_list
                    if seq_num not in original_start_sequence_numbers
                ]

                assert new_start_sequence_numbers, \
                    f"No new start sequence numbers after topology round {round_idx + 1}"

                assert min(new_start_sequence_numbers) > max_original_start_sequence_number, \
                    f"New Start sequence number is not greater than previous one in round {round_idx + 1}"

                closed_shards = [
                    shard for shard in streams_table.shards
                    if not streams_table.is_shard_open(shard)
                ]
                assert closed_shards, \
                    f"No closed shards found after topology round {round_idx + 1}"

                for shard in closed_shards:
                    assert int(shard["SequenceNumberRange"]["EndingSequenceNumber"]) > int(
                        shard["SequenceNumberRange"]["StartingSequenceNumber"]
                    ), "EndingSequenceNumber is not greater than StartingSequenceNumber"

            wait_for_expected_topology_effect(
                test=self,
                removed_node_name=topology_result["removed_node_name"],
                added_node=topology_result["added_node"],
                table_name=TABLE_NAME,
            )

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

        https://github.com/scylladb/scylladb/issues/15260
        """
        stream_specification = enums.StreamSpecification.KEYS_ONLY.value
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1 = self.cluster.nodelist()[0]

        logger.info(f'Pre setup - Creating "{TABLE_NAME}" table via "{node1.name}" node with "{stream_specification}" stream key')
        stream_arn = self.prefill_dynamodb_table(node=node1, stream_specification=stream_specification)[0]
        stress_thread = self.run_write_stress(table_name=TABLE_NAME, node=node1, num_of_item=1000, ignore_errors=True)
        logger.info("Adding new node to cluster")
        node4 = new_node(self.cluster, bootstrap=True)
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)
        self.wait_for_alternator(node=node4)
        logger.info(f"{node4.name} was successfully added")
        streams_table = StreamsTable(stream_arn=stream_arn, dynamodb_api=self.get_dynamodb_api(node=node4))
        closed_shards = [shard for shard in streams_table.shards if not streams_table.is_shard_open(shard)]
        assert closed_shards, f"New node {node4.name} has no closed shards"
        stress_thread.join()


@retrying(num_attempts=60, sleep_time=2, allowed_exceptions=AssertionError)
def wait_for_expected_topology_effect(test, removed_node_name, added_node, table_name):
    """
    Verify the one-shot topology operation converged to the expected observable state:
    - removed node is not running anymore
    - added node is running and serves Alternator
    """
    nodes = test.cluster.nodelist()

    removed_nodes = [node for node in nodes if node.name == removed_node_name]
    if removed_nodes:
        assert not removed_nodes[0].is_running(), \
            f"Removed node {removed_node_name} is still running"

    assert added_node.is_running(), f"Added node {added_node.name} is not running"

    wait_for_stable_alternator_api(
        test=test,
        node=added_node,
        table_name=table_name,
        probes=2,
    )


@retrying(num_attempts=30, sleep_time=1, allowed_exceptions=AssertionError)
def wait_for_stable_alternator_api(test, node, table_name=None, probes=3):
    """
    Verify Alternator serves real API requests repeatedly, not just a single HTTP probe.
    """
    dynamodb_api = test.get_dynamodb_api(node=node)

    for _ in range(probes):
        test.wait_for_alternator(node=node, timeout=5)
        if table_name:
            response = dynamodb_api.client.describe_table(TableName=table_name)
            assert response["Table"]["TableStatus"] in ("ACTIVE", "UPDATING"), \
                f"Unexpected table status: {response['Table']['TableStatus']}"
        else:
            dynamodb_api.client.list_tables()


@retrying(num_attempts=30, sleep_time=1, allowed_exceptions=AssertionError)
def wait_for_vnode_sequence_change_after_topology(
    test,
    node,
    table_name,
    streams_table,
    old_start_sequence_numbers_set,
    writes_per_probe,
):
    test.put_table_items(
        table_name=table_name,
        node=node,
        num_of_items=writes_per_probe,
    )

    wait_for_stable_alternator_api(
        test=test,
        node=node,
        table_name=table_name,
        probes=2,
    )

    streams_table.update_shards()
    new_start_sequence_numbers_set = (
        streams_table.start_sequence_numbers_set - old_start_sequence_numbers_set
    )
    assert new_start_sequence_numbers_set, \
        "Start-sequence-numbers are not changed after topology operation"


@retrying(num_attempts=30, sleep_time=2, allowed_exceptions=AssertionError)
def assert_open_shards_match_tablet_count(
    cql_session,
    table_name: str,
    streams_table: StreamsTable,
) -> None:
    """
    Post-topology-change invariant check for tablet-aware Alternator Streams.

    With the tablets-based CDC implementation each tablet in the CDC log table
    (``<table_name>_scylla_cdc_log``) maps to exactly one active CDC stream,
    which in turn surfaces as a single *open* shard in the DynamoDB Streams API.

    Therefore, after topology convergence the following invariant must hold::

        open_shard_count == tablet_count(cdc_log_table)

    Only call this when tablets are enabled (caller's responsibility — see the
    ``if self.dtest_config.tablets`` guard at the call site).

    Decorated with ``@retrying`` so it keeps polling until the cluster's tablet
    metadata converges after a node operation (~60 s total).

    :param cql_session: Active cassandra-driver ``Session``.  Should be created
        **once** by the caller so the same connection is reused across retries.
    :param table_name: Alternator (DynamoDB) table name, e.g. ``"user_table"``.
    :param streams_table: :class:`StreamsTable` tracking the table's stream ARN.
    """
    ks_name = f"alternator_{table_name}"
    cdc_log_table = f"{table_name}_scylla_cdc_log"

    # Issue a read barrier so the queried node reflects the latest committed
    # (group-0-replicated) tablet metadata before we read system.tablets.
    cql_session.execute("DROP TABLE IF EXISTS nosuchkeyspace.nosuchtable")

    # Resolve the table_id for the CDC log table from system_schema.tables.
    schema_rows = cql_session.execute(
        f"SELECT id FROM system_schema.tables "
        f"WHERE keyspace_name = '{ks_name}' AND table_name = '{cdc_log_table}'"
    )
    schema_row = schema_rows.one()
    assert schema_row is not None, (
        f"CDC log table {ks_name}.{cdc_log_table} not found in system_schema.tables"
    )
    table_id = schema_row.id

    # Each row in system.tablets (per table_id partition) represents one tablet.
    cnt_rows = cql_session.execute(
        f"SELECT count(*) AS cnt FROM system.tablets WHERE table_id = {table_id}"
    )
    tablet_count = int(cnt_rows.one().cnt)
    assert tablet_count > 0, (
        f"No tablets found for {ks_name}.{cdc_log_table} in system.tablets"
    )

    # Refresh Streams API metadata and count the open (active) shards.
    # An open shard has no EndingSequenceNumber – it corresponds to a live CDC stream.
    streams_table.update_shards()
    open_shard_count = streams_table.count_open_shards()

    assert open_shard_count == tablet_count, (
        f"Open Streams shards ({open_shard_count}) != "
        f"tablet count ({tablet_count}) for {ks_name}.{cdc_log_table}. "
        f"Open shard IDs: {streams_table.open_shard_ids_set}"
    )
    logger.info(
        "Verified: %d open shards == %d tablets for %s.%s",
        open_shard_count,
        tablet_count,
        ks_name,
        cdc_log_table,
    )


@retrying(num_attempts=30, sleep_time=1, allowed_exceptions=AssertionError)
def check_vnode_streams_changed_after_topology(
    test,
    node,
    table_name,
    streams_table,
    initial_open_shard_ids,
    initial_shard_ids,
):
    wait_for_stable_alternator_api(
        test=test,
        node=node,
        table_name=table_name,
        probes=2,
    )

    streams_table.update_shards()
    open_shards_changed = streams_table.open_shard_ids_set != initial_open_shard_ids
    shard_set_changed = streams_table.shard_ids_set != initial_shard_ids

    assert open_shards_changed or shard_set_changed, \
        "Shard metadata did not change after topology operation"
