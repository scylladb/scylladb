import logging
import re
import time
from datetime import datetime, timedelta
from enum import IntEnum
from itertools import zip_longest
from typing import Union

from cassandra import ConsistencyLevel
from cassandra.cluster import SimpleStatement

from dtest_class import Tester, wait_for

logger = logging.getLogger(__name__)

CDC_GENERATIONS_TABLE = "system_distributed_everywhere.cdc_generation_descriptions_v2"
CDC_STREAMS_TABLE_VNODES = "system_distributed.cdc_streams_descriptions_v2"
CDC_TIMESTAMPS_TABLE = "system_distributed.cdc_generation_timestamps"
CDC_STREAMS_TABLE_TABLETS = "system.cdc_streams"
CDC_TIMESTAMPS_TABLE_TABLETS = "system.cdc_timestamps"

CDC_TESTER_TYPE = Union[Tester, "CDCInitializeHelper"]


def consistency_for_cdc_streams_query(current_cluster_size: int) -> ConsistencyLevel:
    if current_cluster_size == 1:
        return ConsistencyLevel.ONE
    else:
        return ConsistencyLevel.QUORUM


class CdcLogOperations(IntEnum):
    PREIMAGE = 0
    UPDATE = 1
    INSERT = 2
    ROW_DELETE = 3
    PARTITION_DELETE = 4
    RANGE_DELETE_START_INCLUSIVE = 5
    RANGE_DELETE_START_EXCLUSIVE = 6
    RANGE_DELETE_END_INCLUSIVE = 7
    RANGE_DELETE_END_EXCLUSIVE = 8
    POSTIMAGE = 9


class CDCInitializeHelper:
    def populate_sequentially(self: CDC_TESTER_TYPE, n, wait_other_notice=False):
        cluster = self.cluster
        logger.debug("Starting node 1")
        # We need to use populate() for the first node, because it writes
        # a configuration file that specifies the first node as a seed.
        # Unless we do that, the first node will try to communicate
        # with 127.0.0.1 - which is configured to be the default seed - and
        # might fail, because in some environments the first node might listen
        # for gossip on a different address.
        cluster.populate(1).start(wait_for_binary_proto=True, wait_other_notice=wait_other_notice)
        for i in range(2, n + 1):
            logger.debug(f"Starting node {i}")
            node = cluster.new_node(i, auto_bootstrap=True)
            node.start(wait_for_binary_proto=True, wait_other_notice=wait_other_notice)

    def wait_for_last_generation_to_be_active(self: CDC_TESTER_TYPE, session):
        cdc_descriptions = list(self.get_cdc_description_rows(session))
        assert len(cdc_descriptions) > 0, "No CDC generations"
        last_timestamp = max(desc.time for desc in cdc_descriptions)

        # Add one second to account for clock differences
        self.sleep_until(last_timestamp + timedelta(seconds=1))
        logger.debug(f"Current generation timestamp: {last_timestamp}")
        return last_timestamp

    def get_cdc_description_rows(self: CDC_TESTER_TYPE, session):
        cluster = self.cluster
        cl = consistency_for_cdc_streams_query(len(cluster.nodes))
        query = SimpleStatement(f"SELECT * FROM {CDC_STREAMS_TABLE_VNODES}", consistency_level=cl)
        return session.execute(query)

    def wait_for_metadata_update(self: CDC_TESTER_TYPE, session, cluster_size):
        # Cluster metadata is updated asynchronously, so we need to wait
        def check_metadata():
            ring = self.get_vnode_ring(session)
            logger.debug(f"Token ring length: {len(ring)}")
            return len(ring) == cluster_size * 256

        wait_for(check_metadata, timeout=60, text="Waiting until metadata is updated")

    def sleep_until(self: CDC_TESTER_TYPE, timestamp):
        secs = (timestamp - datetime.utcnow()).total_seconds()
        if secs > 0:
            logger.debug(f"Sleeping for {secs} seconds")
            time.sleep(secs)

    def get_vnode_ring(self, session):
        return list(session.cluster.metadata.token_map.ring)


class CDCTraceInfoMatcher:
    start_line = "CDC: Started generating mutations for log rows.*$"
    end_line = "CDC: Finished generating all log mutations.*$"
    # should be consistent with the way how dht::decorated_key is formatted in
    # scylladb
    key_template = r"{{key:\s*pk{{.*?}},\s*token:\s*{token_id}}}"

    def __init__(self, tokens, preimage=False, postimage=False, splitting=False):
        self.tokens = tokens
        self.preimage = preimage
        self.postimage = postimage
        self.splitting = splitting

    @property
    def preimage_pattern(self):
        if self.preimage or self.postimage:
            return f"CDC: Selecting preimage for {self.key_template}.*$"
        else:
            return f"CDC: Preimage not enabled for the table, not querying current value of {self.key_template}.*$"

    @property
    def generate_log_mutation_pattern(self):
        return f"CDC: Generating log mutations for {self.key_template}.*$"

    @property
    def splitting_pattern(self):
        if self.splitting:
            return f"CDC: Splitting {self.key_template}.*$"
        else:
            return f"CDC: No need to split {self.key_template}.*$"

    @property
    def number_log_mutation_pattern(self):
        return rf"CDC: Generated [\d]+ log mutations from {self.key_template}.*$"

    def get_raw_cdc_lines(self, output: str) -> list[str]:
        cdc_lines = [line.strip() for line in output.splitlines() if "CDC:" in line]
        return cdc_lines

    def verify_cdc_trace_info(self, output: str) -> None:
        cdc_trace_info_lines = self.get_raw_cdc_lines(output)
        assert re.match(self.start_line, cdc_trace_info_lines.pop(0)), "Start line for CDC tracing was not found"
        assert re.match(self.end_line, cdc_trace_info_lines.pop(-1)), "End line for CDC tracing was not found"

        # verify cdc trace info per token
        for token in self.tokens:
            cdc_lines_for_token = [line for line in cdc_trace_info_lines if str(token) in line]
            self._verify_trace_info_per_token(token, cdc_lines_for_token)
            # clean matched lines from cdc tracing info lines
            for line in cdc_lines_for_token:
                cdc_trace_info_lines.remove(line)

        # verify that cdc trace info doesn't contain unmatched lines
        assert len(cdc_trace_info_lines) == 0, f"Next strings were not matched {cdc_trace_info_lines}"

    def _verify_trace_info_per_token(self, token: int, lines: list[str]) -> None:
        patterns = [self.preimage_pattern, self.generate_log_mutation_pattern, self.splitting_pattern, self.number_log_mutation_pattern]

        for pattern, line in zip_longest(patterns, lines):
            # if new unexpected line will appeared in tracing, pattern will be none
            assert pattern, f"{line} is not matched any pattern"
            # if expected line will be missing in output,  pattern will not match it
            assert line, f"{pattern} doesn't match any line"
            # assert cdc line order and correctnes
            assert re.match(pattern.format(token_id=token), line), f"{pattern.format(token_id=token)} not matched {line}"


def mkident(s):
    s = re.sub(r"\s+", "", s)
    s = re.sub("[<>,]", "_", s)
    return re.sub("_+$", "", s)


def get_next_timestamp():
    return int(time.time() * 1000000) + 1000
