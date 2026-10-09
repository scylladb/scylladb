#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Home for functionality that provides context managers, and anything related to
making those context managers function.
"""

import logging
from contextlib import contextmanager

import requests

from test.cluster.dtest.ccmlib.scylla_node import ScyllaNode
from test.cluster.dtest.tools.cluster import minimum_scylla_version
from test.cluster.dtest.tools.env import ALLOW_NOISY_LOGGING


@contextmanager
def log_filter(log_id, expected_strings=None):
    """
    Context manager which allows silencing logs until exit.
    Log records matching expected_strings will be filtered out of logging.
    If expected_strings is not provided, everything is filtered for that log.
    """
    logger = logging.getLogger(log_id)
    log_filter = _make_filter_class(expected_strings)
    logger.addFilter(log_filter)
    yield
    if log_filter.records_silenced > 0:
        print("Logs were filtered to remove messages deemed unimportant, total count: %d" % log_filter.records_silenced)
    logger.removeFilter(log_filter)


def _make_filter_class(expected_strings):
    """
    Builds an anon-ish filtering class and returns it.

    Returns a logfilter if filtering should take place, otherwise a nooplogfilter.

    We're just using a class here as a one-off object with a filter method, for
    use as a filter object on the desired log.
    """

    class NoopLogFilter:
        records_silenced = 0

        @classmethod
        def filter(cls, record):
            return True

    class LogFilter:
        records_silenced = 0

        @classmethod
        def filter(cls, record):
            if expected_strings is None:
                cls.records_silenced += 1
                return False

            for s in expected_strings:
                if s in record.msg or s in record.name:
                    cls.records_silenced += 1
                    return False

            return True

    if ALLOW_NOISY_LOGGING:
        return NoopLogFilter
    else:
        return LogFilter


# NOTE: the context managers below are restored verbatim (imports aside) from
# scylla-dtest's tools/context.py; they were trimmed when this module was
# first ported in-tree, but not-yet-adapted dtest/unported test modules still
# import them.


@contextmanager
def nodetool_context(node, start_command, end_command):
    """
    To be used when a nodetool command can affect the state of the node,
    like disablebinary/disablegossip, and it is needed to keep the node in said
    state temporarily.

    :param node: the db node where the nodetool command are to be executed on
    :param start_command: the command to execute before yielding the context
    :param end_command: the command to execute as the closing step
    :return:
    """
    try:
        result = node.nodetool(start_command)
        yield result
    finally:
        node.nodetool(end_command)


@contextmanager
def disable_autocompaction(node, keyspace_name=None, table_name=None):  # noqa: PLR0912, PLR0915
    """
    temporarily disable autocompaction for specific keyspace / table(s)
    would call the api only if node is up.
    :param node: the target db node
    :param keyspace_name: name of the keyspace, or a list of keyspaces, or None for all keyspaces
    :param table_name: name of the table, or a list of tables in a given keyspace, or None for all tables in the given keyspace(s))
    :return: None
    """

    if node.status != "UP":
        yield
        return

    def maybe_wait_for_compactions(node, ks=None, table=None):
        if not isinstance(node, ScyllaNode) and minimum_scylla_version(node.cluster.version(), "5.1-rc0", "2022.2-rc0"):
            node.wait_for_compactions(ks, table)

    api_base = f"http://{node.address()}:10000"
    enabled = True

    keyspaces = []
    if not keyspace_name:
        response = requests.get(f"{api_base}/storage_service/keyspaces")
        response.raise_for_status()
        keyspaces = [ks for ks in response.json()]
    if type(keyspace_name) is list:
        assert not table_name
        keyspaces = keyspace_name
    else:
        keyspaces = [keyspace_name]

    if table_name:
        assert len(keyspaces) == 1
        api_url = f"{api_base}/column_family/autocompaction/{keyspace_name}:{table_name}"
        response = requests.get(api_url)
        response.raise_for_status()
        enabled = response.json()
        if not enabled:
            maybe_wait_for_compactions(node)
            yield
            return
        keyspaces = [keyspace_name]

    api_pfx = f"{api_base}/storage_service/auto_compaction"
    api_opts = ""
    if type(table_name) is list:
        api_opts = f"?cf={','.join(table_name)}"
    elif table_name:
        api_opts = f"?cf={table_name}"

    disabled = []
    try:
        for ks in keyspaces:
            response = requests.delete(f"{api_pfx}/{ks}{api_opts}")
            response.raise_for_status()
            disabled.append(ks)
            maybe_wait_for_compactions(node, ks, table_name)
        yield
    finally:
        bad_response = None
        for ks in disabled:
            response = requests.post(f"{api_pfx}/{ks}{api_opts}")
            try:
                response.raise_for_status()
            except:
                bad_response = response
        if bad_response:
            bad_response.raise_for_status()


@contextmanager
def disable_load_balancing(node):
    """
    Disable load balancing on the node.
    :param node: the target db node
    :return: None
    """
    if node.status != "UP":
        yield
        return

    response = requests.post(f"http://{node.address()}:10000/storage_service/tablets/balancing?enabled=false")
    response.raise_for_status()
    yield
    response = requests.post(f"http://{node.address()}:10000/storage_service/tablets/balancing?enabled=true")
    response.raise_for_status()


@contextmanager
def disable_sstable_modifications(node, keyspace_name=None, table_name=None):
    """
    Disables all sstable modifications on the node. No automatic compactions, nor migrations triggered by load balancing.
    Autocompactions are disabled for specific keyspace / table(s), but load balancing is disabled for the whole node.
    """
    with disable_load_balancing(node), disable_autocompaction(node, keyspace_name, table_name):
        yield
