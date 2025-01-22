#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import logging
import re
import time

import cassandra
import pytest
import requests
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import ExecutionProfile
from cassandra.policies import RetryPolicy

from test.cluster.dtest.tools.misc import retry_till_success


logger = logging.getLogger(__name__)
logger.debug(f"Python driver version in use: {cassandra.__version__}")


class FlakyRetryPolicy(RetryPolicy):
    """
    A retry policy that retries 5 times
    """

    max_retries: int = 5

    def on_read_timeout(self, *args, **kwargs):
        if kwargs["retry_num"] < 5:
            logger.debug("Retrying read after timeout. Attempt #" + str(kwargs["retry_num"]))
            return (self.RETRY, None)
        else:
            return (self.RETHROW, None)

    def on_write_timeout(self, *args, **kwargs):
        if kwargs["retry_num"] < 5:
            logger.debug("Retrying write after timeout. Attempt #" + str(kwargs["retry_num"]))
            return (self.RETRY, None)
        else:
            return (self.RETHROW, None)

    def on_unavailable(self, *args, **kwargs):
        if kwargs["retry_num"] < 5:
            logger.debug("Retrying request after UE. Attempt #" + str(kwargs["retry_num"]))
            return (self.RETRY, None)
        else:
            return (self.RETHROW, None)


def make_execution_profile(retry_policy=FlakyRetryPolicy(), consistency_level=ConsistencyLevel.ONE, **kwargs):
    return ExecutionProfile(retry_policy=retry_policy, consistency_level=consistency_level, **kwargs)


class WaitTimeoutExpiredError(Exception):
    pass


def forever_wait_for(func, step=1, text=None, **kwargs):
    """
    Wait indefinitely until func evaluates to True.

    This is similar to avocado.utils.wait.wait(), but there's no
    timeout, we'll just keep waiting for it.

    :param func: Function to evaluate.
    :param step: Amount of time to sleep before another try.
    :param text: Text to log, for debugging purposes.
    :param kwargs: Keyword arguments to func
    :return: Return value of func.
    """
    ok = False
    start_time = time.time()
    while not ok:
        ok = func(**kwargs)
        time.sleep(step)
        time_elapsed = time.time() - start_time
        if text is not None:
            logger.debug(f"{text} ({time_elapsed} s)")
    return ok


def wait_for(func, step=1, text=None, timeout=None, throw_exc=True, **kwargs):
    """
    Wrapper function to wait with timeout option.
    If no timeout received, 'forever_wait_for' method will be used.
    Otherwise the below function will be called.

    :param func: Function to evaluate.
    :param step: Time to sleep between attempts in seconds
    :param text: Text to print while waiting, for debug purposes
    :param timeout: Timeout in seconds
    :param throw_exc: Raise exception if timeout expired, but func result is not evaluated to True
    :param kwargs: Keyword arguments to func
    :return: Return value of func.
    """
    if not timeout:
        return forever_wait_for(func, step, text, **kwargs)
    ok = False
    start_time = time.time()
    while not ok:
        time.sleep(step)
        ok = func(**kwargs)
        if ok:
            break
        time_elapsed = time.time() - start_time
        if text is not None:
            logger.debug(f"({text} ({time_elapsed} s)")
        if time_elapsed > timeout:
            err = f"Wait for: {text}: timeout - {timeout} seconds - expired"
            logger.debug(err)
            if throw_exc:
                raise WaitTimeoutExpiredError(err)
            else:
                break
    return ok


class DtestTimeoutError(Exception):
    pass


def create_ks(session, name: str, rf: int | dict[str, int], tablets: int | None = None, replication_class: str = "NetworkTopologyStrategy"):
    """
    Create a keyspace with the given name

    rf: determines the replication factor.
    - It can be given as an integer to set the `replication_factor` replication option
    - or as a dict[str, int], like {"dc1": 1, "dc2": 2}, to explicitly set the replication factor per datacenter

    tablets: is an optional parameter to control the initial number of tablets.
    - Tablets are enabled by default, when supported (and tablets is None).
    - Set tablets = 0 to disable tablets for the created keyspace
    - Set tablets != 0 to set the initial number of tablets.
    """
    query = "CREATE KEYSPACE %s WITH replication={%s}"
    if isinstance(rf, int):
        # we assume simpleStrategy without tablets
        query = query % (name, f"'class':'{replication_class}', 'replication_factor':{rf}")
    else:
        assert len(rf) >= 0, "At least one datacenter/rf pair is needed"
        # we assume networkTopologyStrategy
        options = ", ".join(["'%s':%d" % (dc_value, rf_value) for dc_value, rf_value in rf.items()])
        query = query % (name, "'class':'NetworkTopologyStrategy', %s" % options)
    if tablets is not None:
        if tablets:
            query += " and tablets={'initial':%d}" % tablets
        else:
            query += " and tablets={'enabled':false}"

    return create_ks_query(session=session, name=name, query=query)


def create_ks_query(session, name, query):
    logger.debug("%s" % query)
    try:
        retry_till_success(session.execute, query=query, timeout=120, bypassed_exception=cassandra.OperationTimedOut)
    except cassandra.AlreadyExists:
        logger.warning("AlreadyExists executing create ks query '%s'" % query)

    session.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    # Also validates it was indeed created even though we ignored OperationTimedOut
    # Might happen some of the time because CircleCI disk IO is unreliable and hangs randomly
    session.execute(f"USE {name}")


def read_barrier(session):
    """To issue a read barrier it is sufficient to attempt dropping a
    non-existing table. We need to use `if exists`, otherwise the statement
    would fail on prepare/validate step which happens before a read barrier is
    performed.
    """
    session.execute("drop table if exists nosuchkeyspace.nosuchtable")


def get_auth_provider(user, password):
    return PlainTextAuthProvider(username=user, password=password)


def make_auth(user, password):
    def private_auth(node_ip):
        return {"username": user, "password": password}

    return private_auth


def data_size(node, ks, cf):
    """
    Return the size in bytes for given table in a node.
    This gets the size from nodetool cfstats output.
    This might brake if the format of nodetool cfstats change
    as it is looking for specific text "Space used (total)" in output.
    @param node: Node in which table size to be checked for
    @param ks: Keyspace name for the table
    @param cf: table name
    @return: data size in bytes
    """
    cfstats = node.nodetool(f"cfstats {ks}.{cf}")[0]
    regex = re.compile(r"[\t]")
    stats_lines = [regex.sub("", s) for s in cfstats.split("\n") if regex.sub("", s).startswith("Space used (total)")]
    if not len(stats_lines) == 1:
        msg = ('Expected output from `nodetool cfstats` to contain exactly 1 line starting with "Space used (total)". Found:\n') + cfstats
        raise RuntimeError(msg)
    space_used_line = stats_lines[0].split()

    if len(space_used_line) == 4:
        return float(space_used_line[3])
    else:
        msg = ("Expected format for `Space used (total)` in nodetool cfstats is `Space used (total): <number>`.Found:\n") + stats_lines[0]
        raise RuntimeError(msg)


def get_port_from_node(node):
    """
    Return the port that this node is listening on.
    We only use this to connect the native driver,
    so we only care about the binary port.
    """
    try:
        return node.network_interfaces["binary"][1]
    except Exception:  # noqa: BLE001
        raise RuntimeError(f"No network interface defined on this node object. {node.network_interfaces}")


def get_ip_from_node(node):
    if node.network_interfaces["binary"]:
        node_ip = node.network_interfaces["binary"][0]
    else:
        node_ip = node.network_interfaces["thrift"][0]
    return node_ip


def is_autocompaction_enabled(node, ks_name, table_name):
    """
    Return if autocompaction is enabled or not
    :param node: node to execute the API request
    :param ks_name: Keyspace name to verify if autocompaction is enabled
    :param table_name: table name to verify if autocompaction is enabled
    :return: True|False
    """
    node_ip = get_ip_from_node(node=node)
    response = requests.get(f"http://{node_ip}:10000/column_family/autocompaction/{ks_name}:{table_name}")
    response.raise_for_status()
    return response.json()


class Tester:
    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            fixture_dtest_setup = object.__getattribute__(self, "fixture_dtest_setup")
            return object.__getattribute__(fixture_dtest_setup, name)

    @pytest.fixture(scope="function", autouse=True)
    def set_dtest_setup_on_function(self, fixture_dtest_setup):
        self.fixture_dtest_setup = fixture_dtest_setup
        self.dtest_config = fixture_dtest_setup.dtest_config


# We default to UTF8Type because it's simpler to use in tests
def create_cf(  # noqa: PLR0912, PLR0913
    session,
    name,
    key_type="varchar",
    speculative_retry=None,
    read_repair=None,
    compression=None,
    gc_grace=None,
    columns=None,
    validation="UTF8Type",
    compact_storage=False,
    compaction_strategy="SizeTieredCompactionStrategy",
    primary_key=None,
    clustering=None,
    default_ttl=None,
    compaction=None,
    debug_query=False,
    caching=True,
    paxos_grace_seconds=None,
    dclocal_read_repair_chance=None,
    scylla_encryption_options=None,
    key_name: str = "key",
):
    compaction_fragment = None
    if compaction_strategy:
        compaction_fragment = "compaction = {'class': '%s', 'enabled': 'true'}" % compaction_strategy

    additional_columns = ""
    if columns is not None:
        for k, v in list(columns.items()):
            additional_columns = f"{additional_columns}, {k} {v}"

    if additional_columns == "":
        query = f"CREATE COLUMNFAMILY {name} ({key_name} {key_type}, c varchar, v varchar, PRIMARY KEY(key, c)) WITH comment='test cf'"
    elif primary_key:
        query = f"CREATE COLUMNFAMILY {name} ({key_name} {key_type}{additional_columns}, PRIMARY KEY({primary_key})) WITH comment='test cf'"
    else:
        query = f"CREATE COLUMNFAMILY {name} ({key_name} {key_type} PRIMARY KEY{additional_columns}) WITH comment='test cf'"

    if compaction is not None:
        query = f"{query} AND compaction={compaction}"
    elif compaction_fragment is not None:
        query = f"{query} AND {compaction_fragment}"

    if clustering:
        query = f"{query} AND CLUSTERING ORDER BY ({clustering})"

    if compression is not None:
        query = f"{query} AND compression = {{ 'sstable_compression': '{compression}Compressor' }}"
    else:
        # if a compression option is omitted, C* will default to lz4 compression
        query += " AND compression = {}"

    if read_repair is not None:
        query = f"{query} AND read_repair_chance={read_repair:f}"
    if dclocal_read_repair_chance is not None:
        query = f"{query} AND dclocal_read_repair_chance='{dclocal_read_repair_chance}'"
    if gc_grace is not None:
        query = "%s AND gc_grace_seconds=%d" % (query, gc_grace)
    if default_ttl is not None:
        query = "%s AND default_time_to_live=%d" % (query, default_ttl)
    if speculative_retry is not None:
        query = f"{query} AND speculative_retry='{speculative_retry}'"
    if compact_storage:
        query += " AND COMPACT STORAGE"
    if scylla_encryption_options is not None:
        query = f"{query} AND scylla_encryption_options={scylla_encryption_options}"
    if not caching:
        query += " AND caching = {'enabled':false}"

    if debug_query:
        logger.debug(query)
    if paxos_grace_seconds is not None:
        query = "%s AND paxos_grace_seconds=%d" % (query, paxos_grace_seconds)

    try:
        retry_till_success(session.execute, query=query, timeout=120, bypassed_exception=cassandra.OperationTimedOut)
    except cassandra.AlreadyExists:
        logger.warning("AlreadyExists executing create cf query '%s'" % query)
    session.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    # Going to ignore OperationTimedOut from create CF, so need to validate it was indeed created
    session.execute("SELECT * FROM %s LIMIT 1" % name)
