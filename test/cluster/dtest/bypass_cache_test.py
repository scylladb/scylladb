#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import logging
import random
import string
import time
from collections.abc import Callable

import pytest

from dtest_class import Tester, create_cf, create_ks, get_ip_from_node
from tools.data import create_c1c2_table, insert_c1c2
from tools.metrics import get_node_metrics

logger = logging.getLogger(__file__)
NUM_OF_QUERY_EXECUTIONS = 100


@pytest.mark.single_node
class TestBypassCache(Tester):
    """
    Test that will verify if the select statement will skip cache during its read
    Introduced by commit 2a371c2689d327a10f5888f39414ec66efedb093
    """

    METRIC_VALIDATORS = {
        "ignore": lambda before, after: "",
        "not_changed": lambda before, after: "" if before == after else "should not be changed",
        "increased_by_1": lambda before, after: "" if before + 1 == after else "should be increased by 1",
        "increased_by_at_least_1": lambda before, after: "" if before < after else "should be increased by at least 1",
    }

    def gen_less_than(self, value):
        return lambda before, after: "" if before + value >= after else f"changed by more than {value}"

    def gen_more_than(self, value):
        return lambda before, after: "" if before + value < after else f"changed by less than {value}"

    def prepare(  # noqa: PLR0913
        self,
        nodes=1,
        keyspace_name="bypass_cache",
        rf=1,
        options_dict=None,
        table_name="user_events",
        insert_data=True,
        smp=1,
        cache_index_pages=None,
    ):
        self.keyspace_name = keyspace_name
        self.table_name = table_name
        cluster = self.cluster
        if options_dict:
            cluster.set_configuration_options(values=options_dict)
        jvm_args = ["--smp", str(smp)]
        if cache_index_pages is not None:
            jvm_args += ["--cache-index-pages", "1" if cache_index_pages else "0"]
        cluster.populate(nodes).start(jvm_args=jvm_args)
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self.tablets = "tablets" in self.scylla_features
        create_ks(session=session, name=keyspace_name, rf=rf)

        if insert_data:
            create_c1c2_table(session)
            insert_c1c2(session, n=NUM_OF_QUERY_EXECUTIONS, ks=keyspace_name)

        return session

    def get_scylla_cache_reads_metrics(self, node, metrics):
        return get_node_metrics(get_ip_from_node(node), metrics=metrics)

    def run_query_and_check_metrics(self, node, session, query, metrics_validators: dict[str, str | Callable], num_runs: int = NUM_OF_QUERY_EXECUTIONS):
        metrics = [*metrics_validators.keys()]
        metric_data_before = self.get_scylla_cache_reads_metrics(node=node, metrics=metrics)
        logger.info("Metrics before query:\n%s", "\n".join(f"{metric}: {value}" for metric, value in metric_data_before.items()))
        logger.info('Running query "%s" %s times', query, num_runs)
        for _ in range(num_runs):
            session.execute(query)
        metric_data_after = self.get_scylla_cache_reads_metrics(node=node, metrics=metrics)
        logger.info("Metrics after query:\n%s", "\n".join(f"{metric}: {value}" for metric, value in metric_data_after.items()))
        metric_errors = []
        for metric, validator in metrics_validators.items():
            if isinstance(validator, str):
                validator_fn = self.METRIC_VALIDATORS.get(validator)
                if not validator_fn:
                    raise ValueError("Can't find validator with name %s", validator)
            elif callable(validator):
                validator_fn = validator
            else:
                raise ValueError("Only str or callable is acceptable")
            before = metric_data_before.get(metric, 0)
            after = metric_data_after.get(metric, 0)
            error = validator_fn(before, after)
            if error:
                metric_errors.append(f"Metric {metric} {error}, before:{before}, after:{after}")
        return metric_errors

    def cache_thresh(self):
        return 800 if not self.tablets else 150

    def verify_read_was_from_disk(self, node, query, session, index_cache_involved: bool = False):
        # TODO: After https://github.com/scylladb/scylla/issues/9968 remove index_cache_involved, assume that it is false
        errors = self.run_query_and_check_metrics(
            node,
            session,
            query,
            metrics_validators={
                "scylla_cache_reads": self.gen_less_than(self.cache_thresh()),
                # Internal reads can also use the sstable index page cache, so we
                # cannot make assumptions about the index metrics not changing
                "scylla_sstables_index_page_hits": self.gen_more_than(self.cache_thresh()) if index_cache_involved else "ignore",
                "scylla_sstables_index_page_cache_misses": "increased_by_at_least_1" if index_cache_involved else "ignore",
                "scylla_sstables_index_page_cache_populations": "increased_by_at_least_1" if index_cache_involved else "ignore",
            },
        )
        assert not errors, "Running query that is suppose to read from disk following errors found:\n" + "\n".join(errors)

    def verify_read_was_from_cache(self, node, query, session):
        errors = self.run_query_and_check_metrics(
            node,
            session,
            query,
            metrics_validators={
                "scylla_cache_reads": self.gen_more_than(self.cache_thresh()),
                "scylla_sstables_index_page_hits": "ignore",
                "scylla_sstables_index_page_cache_misses": "ignore",
                "scylla_sstables_index_page_cache_populations": "ignore",
            },
        )
        assert not errors, "Running query that is suppose to read from cache following errors found:\n" + "\n".join(errors)

    def test_simple_bypass_cache(self):
        session = self.prepare()
        node = self.cluster.nodelist()[0]
        query = "SELECT * FROM cf BYPASS CACHE"

        self.verify_read_was_from_disk(node=node, query=query, session=session)

    def test_multiple_bypass_cache(self):
        session = self.prepare()
        node = self.cluster.nodelist()[0]
        time.sleep(3)
        for _ in range(20):
            query = "SELECT * FROM cf BYPASS CACHE"
            self.verify_read_was_from_disk(node=node, query=query, session=session)

    def test_read_from_cache_and_then_bypass_cache(self):
        session = self.prepare()
        node = self.cluster.nodelist()[0]

        no_bypass_query = "SELECT * FROM cf"
        self.verify_read_was_from_cache(node=node, query=no_bypass_query, session=session)

        bypass_query = "SELECT * FROM cf BYPASS CACHE"
        self.verify_read_was_from_disk(node=node, query=bypass_query, session=session)

    def insert_data_for_scan_range(self):
        session = self.prepare(insert_data=False)
        # create a table
        create_cf(session=session, name="cf", key_type="int")
        # populate table with k (int) and values (anything)
        query = "INSERT INTO cf (key, c, v) VALUES ({}, '{}', '{}')"
        for idx in range(NUM_OF_QUERY_EXECUTIONS):
            varchar_c = "".join(random.choice(string.ascii_lowercase) for x in range(20))
            varchar_v = "".join(random.choice(string.ascii_lowercase) for x in range(20))
            session.execute(query.format(idx, varchar_c, varchar_v))
        return session

    @pytest.mark.skip(reason="https://github.com/scylladb/scylladb/issues/6045")
    def test_range_scan_bypass_cache(self):
        session = self.insert_data_for_scan_range()
        node = self.cluster.nodelist()[0]
        errors = self.run_query_and_check_metrics(
            node,
            session,
            "SELECT * FROM cf WHERE key > 10 and key < 20 ALLOW FILTERING",
            metrics_validators={
                "select_partition_range_scan": "increased_by_1",
                "select_partition_range_scan_no_bypass_cache": "not_changed",
            },
            num_runs=1,
        )
        assert not errors, "Running range query that is suppose to read from cache following errors found:\n" + "\n".join(errors)

        errors = self.run_query_and_check_metrics(
            node,
            session,
            "SELECT * FROM cf WHERE key > 10 and key < 20 ALLOW FILTERING BYPASS CACHE",
            metrics_validators={
                "select_partition_range_scan": "increased_by_1",
                "select_partition_range_scan_no_bypass_cache": "increased_by_1",
            },
            num_runs=1,
        )
        assert not errors, "Running range query that is suppose to read from disk following errors found:\n" + "\n".join(errors)

    def test_full_scan_bypass_cache(self):
        session = self.prepare()
        node = self.cluster.nodelist()[0]
        self.run_query_and_check_metrics(
            node,
            session,
            query="SELECT * FROM cf",
            num_runs=1,
            metrics_validators={
                "select_partition_range_scan": "increased_by_1",
                "select_partition_range_scan_no_bypass_cache": "increased_by_1",
            },
        )
        self.run_query_and_check_metrics(
            node,
            session,
            query="SELECT * FROM cf BYPASS CACHE",
            num_runs=1,
            metrics_validators={
                "select_partition_range_scan": "increased_by_1",
                "select_partition_range_scan_no_bypass_cache": "not_changed",
            },
        )

    @pytest.mark.parametrize("cache_index_pages", [True, False], ids=["cache_index_pages", "no_cache_index_pages"])
    def test_create_table_caching_disabled(self, cache_index_pages: bool):
        session = self.prepare(insert_data=False, cache_index_pages=cache_index_pages)
        node = self.cluster.nodelist()[0]
        create_c1c2_table(session, cf=self.table_name, caching=False)
        insert_c1c2(session, n=NUM_OF_QUERY_EXECUTIONS, cf=self.table_name, ks=self.keyspace_name)
        node.flush()
        query = f"select * from {self.table_name}"
        # TODO: After https://github.com/scylladb/scylla/issues/9968 is solved, remove index_cache_involved=True
        self.verify_read_was_from_disk(node=node, query=query, session=session, index_cache_involved=cache_index_pages)

    @pytest.mark.parametrize("cache_index_pages", [True, False], ids=["cache_index_pages", "no_cache_index_pages"])
    def test_alter_table_caching_disable(self, cache_index_pages: bool):
        session = self.prepare(insert_data=False, cache_index_pages=cache_index_pages)
        node = self.cluster.nodelist()[0]
        create_c1c2_table(session, cf=self.table_name)
        insert_c1c2(session, n=NUM_OF_QUERY_EXECUTIONS, cf=self.table_name, ks=self.keyspace_name)
        node.flush()
        query = f"select * from {self.table_name}"
        self.verify_read_was_from_cache(node=node, query=query, session=session)
        # disabling caching for table and checking read comes from disk
        session.execute(f"ALTER TABLE {self.table_name} WITH caching = {{'enabled':false}}")
        # TODO: After https://github.com/scylladb/scylla/issues/9968 is solved, remove index_cache_involved=True
        self.verify_read_was_from_disk(node=node, query=query, session=session, index_cache_involved=cache_index_pages)

    @pytest.mark.parametrize("cache_index_pages", [True, False], ids=["cache_index_pages", "no_cache_index_pages"])
    def test_alter_table_caching_enable(self, cache_index_pages: bool):
        session = self.prepare(insert_data=False, cache_index_pages=cache_index_pages)
        node = self.cluster.nodelist()[0]
        create_c1c2_table(session, cf=self.table_name, caching=False)
        insert_c1c2(session, n=NUM_OF_QUERY_EXECUTIONS, cf=self.table_name, ks=self.keyspace_name)
        node.flush()
        query = f"select * from {self.table_name}"
        # TODO: After https://github.com/scylladb/scylla/issues/9968 is solved, remove index_cache_involved=True
        self.verify_read_was_from_disk(node=node, query=query, session=session, index_cache_involved=cache_index_pages)
        # enabling caching for table and checking read comes from cache
        session.execute(f"ALTER TABLE {self.table_name} WITH caching = {{'enabled':true}}")
        self.verify_read_was_from_cache(node=node, query=query, session=session)

    def verify_used_memory_grow(self, node, session):
        grew = 0
        metric = ["scylla_cache_bytes_used"]
        for _ in range(NUM_OF_QUERY_EXECUTIONS):
            cache_bytes_used_before_write = self.get_scylla_cache_reads_metrics(node=node, metrics=metric)[metric[0]]
            insert_c1c2(session, keys=list(range(self.first_key + 10)), cf=self.table_name, ks=self.keyspace_name)
            self.cluster.nodetool(f"flush -- {self.keyspace_name} {self.table_name}")
            cache_bytes_used_after_write = self.get_scylla_cache_reads_metrics(node=node, metrics=metric)[metric[0]]
            if cache_bytes_used_before_write < cache_bytes_used_after_write:
                grew += 1
            self.first_key += 10
        return grew > NUM_OF_QUERY_EXECUTIONS / 2

    def test_writes_caching_disabled(self):
        session = self.prepare(insert_data=False)
        node = self.cluster.nodelist()[0]
        create_c1c2_table(session, cf=self.table_name, caching=False)
        insert_c1c2(session, n=NUM_OF_QUERY_EXECUTIONS, cf=self.table_name, ks=self.keyspace_name)
        self.first_key = 0
        assert not self.verify_used_memory_grow(node=node, session=session), "expected to have writes without cache"
        alter_cmd = f"ALTER TABLE {self.keyspace_name}.{self.table_name} WITH CACHING = {{'enabled': 'true'}}"
        session.execute(alter_cmd)
        assert self.verify_used_memory_grow(node=node, session=session), "expected to have writes through cache"

    def test_writes_caching_enabled(self):
        session = self.prepare(insert_data=False)
        node = self.cluster.nodelist()[0]
        create_c1c2_table(session, cf=self.table_name)
        insert_c1c2(session, n=NUM_OF_QUERY_EXECUTIONS, cf=self.table_name, ks=self.keyspace_name)
        self.first_key = 0
        assert self.verify_used_memory_grow(node=node, session=session), "expected to have writes through cache"
        alter_cmd = f"ALTER TABLE {self.keyspace_name}.{self.table_name} WITH CACHING = {{'enabled': 'false'}}"
        session.execute(alter_cmd)
        assert not self.verify_used_memory_grow(node=node, session=session), "expected to have writes without cache"
