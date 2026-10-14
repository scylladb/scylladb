#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import datetime
import logging
import os
import random
import subprocess
import time
from concurrent.futures.thread import ThreadPoolExecutor
from itertools import count, groupby

import tabulate
from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.query import SimpleStatement
from cassandra.concurrent import execute_concurrent, execute_concurrent_with_args

from test.cluster.dtest.ccmlib.scylla_node import ScyllaType
from test.cluster.dtest.dtest_class import create_cf
from test.cluster.dtest.tools import assertions
from test.cluster.dtest.tools.misc import seconds_to_micros


logger = logging.getLogger(__name__)


def create_c1c2_table(session, cf="cf", read_repair=None, debug_query=True, compaction=None, caching=True, speculative_retry=None):  # noqa: PLR0913
    create_cf(session, cf, columns={"c1": "text", "c2": "text"}, read_repair=read_repair, debug_query=debug_query, compaction=compaction, caching=caching, speculative_retry=speculative_retry)


def insert_c1c2(  # noqa: PLR0913
    session,
    keys=None,
    n=None,
    consistency=ConsistencyLevel.QUORUM,
    c1_values=None,
    c2_values=None,
    ks="ks",
    cf="cf",
    concurrency=20,
):
    if (keys is None and n is None) or (keys is not None and n is not None):
        raise ValueError(f"Expected exactly one of 'keys' or 'n' arguments to not be None; got keys={keys}, n={n}")
    if (not c1_values and c2_values) or (c1_values and not c2_values):
        raise ValueError('Expected the "c1_values" and "c2_values" variables be empty or contain list of string')
    if n:
        keys = list(range(n))
    if c1_values and c2_values:
        statement = session.prepare(f"INSERT INTO {ks}.{cf} (key, c1, c2) VALUES (?, ?, ?)")
        statement.consistency_level = consistency
        execute_concurrent_with_args(session, statement, map(lambda x, y, z: [f"k{x}", y, z], keys, c1_values, c2_values), concurrency=concurrency)
    else:
        statement = session.prepare(f"INSERT INTO {ks}.{cf} (key, c1, c2) VALUES (?, 'value1', 'value2')")
        statement.consistency_level = consistency

        execute_concurrent_with_args(session, statement, [[f"k{k}"] for k in keys], concurrency=concurrency)


def query_c1c2(  # noqa: PLR0913
    session,
    key,
    consistency=ConsistencyLevel.QUORUM,
    tolerate_missing=False,
    must_be_missing=False,
    c1_value="value1",
    c2_value="value2",
    ks="ks",
    cf="cf",
):
    query = SimpleStatement(f"SELECT c1, c2 FROM {ks}.{cf} WHERE key='k{key}'", consistency_level=consistency)
    rows = list(session.execute(query))
    if not tolerate_missing and not must_be_missing:
        assertions.assert_length_equal(rows, 1)
        res = rows[0]
        assert len(res) == 2 and res[0] == c1_value and res[1] == c2_value, res
    if must_be_missing:
        assertions.assert_length_equal(rows, 0)


def rows_to_list(rows):
    new_list = [list(row) for row in rows]
    return new_list


def get_list_res(session, query, cl, ignore_order=False, result_as_string=False, timeout=None):
    simple_query = SimpleStatement(query, consistency_level=cl)
    if timeout is not None:
        res = session.execute(simple_query, timeout=timeout)
    else:
        res = session.execute(simple_query)
    list_res = rows_to_list(res)
    if ignore_order:
        list_res = sorted(list_res)
    if result_as_string:
        list_res = str(list_res)
    return list_res


def run_in_parallel(functions_list):
    """
    Runs the functions that are passed in proc_functions in parallel using threads.
    :param functions_list: variable holds list of dictionaries with threads definitions. Expected structure:
                           [{'func': <function pointer - the function will be runs from the thread>,
                             'args': (arg1, arg2, arg3), - explicit function arguments by order in the function
                             'kwargs': {<arg name1>: value, <arg name2>: value} - function arguments by name
                            }, - first thread definition
                            {{'func': <function pointer, 'args': (), 'kwargs': {}} - second thread, no arguments
                           ]
    :param functions_list: list
    :return: list of functions' return values
    :rtype: list
    """
    logger.debug(f"Threads start at {datetime.datetime.now()}")
    pool = ThreadPoolExecutor(max_workers=len(functions_list))
    tasks = []
    for func in functions_list:
        args = func["args"] if "args" in func else []
        kwargs = func["kwargs"] if "kwargs" in func else {}
        tasks.append(pool.submit(func["func"], *args, **kwargs))
    results = [task.result() for task in tasks]
    logger.debug(f"'{len(results)}' threads finished at {datetime.datetime.now()}")
    return results


def run_query_with_data_processing(
    session,
    query,
    consistency_level=ConsistencyLevel.ONE,
    session_timeout=None,
    group=False,
    groupby_column=None,
    restrict_column=None,
    restrict_value=None,
):
    if not session_timeout:
        session_timeout = 120
    result = list(session.execute(SimpleStatement(query, consistency_level=consistency_level), timeout=session_timeout))
    if result:
        if restrict_column:
            restrict_column_index = next(i for i, clmn in enumerate(result[0]._fields) if clmn == restrict_column)
            restrict_value = [restrict_value] if not isinstance(restrict_value, list) else restrict_value

        if group:
            groupby_column_index = next(i for i, clmn in enumerate(result[0]._fields) if clmn == groupby_column)
            result = [item[groupby_column_index] for item in result if item[restrict_column_index] in restrict_value] if restrict_value and restrict_column else [item[groupby_column_index] for item in result]
            result = [[key, len(list(group))] for key, group in groupby(sorted(result))]
        elif restrict_value and restrict_column:
            result = [item for item in result if item[restrict_column_index] in restrict_value]
    return result


# NOTE: the functions below are restored verbatim (imports aside) from
# scylla-dtest's tools/data.py; they were trimmed when this module was first
# ported in-tree, but not-yet-adapted dtest/unported test modules still import
# them.


def insert_c1c2_with_clustering(  # noqa: PLR0913
    session,
    clustering_key_values=None,
    n=None,
    consistency=ConsistencyLevel.QUORUM,
    c1_values=None,
    c2_values=None,
    ks="ks",
    cf="cf",
    partition_key_set_value=1,
    output_20_lines=True,
    concurrency=20,
):
    if clustering_key_values is None:
        clustering_key_values = []

    if c1_values is None:
        c1_values = []

    if c2_values is None:
        c2_values = []

    build_insert_params(clustering_key_values, n, c1_values, c2_values)

    partition_key_values = [partition_key_set_value] * len(clustering_key_values)

    statement = session.prepare(f"INSERT INTO {ks}.{cf} (pkey, ckey, c1, c2) VALUES (?, ?, ?, ?)")
    statement.consistency_level = consistency

    execute_concurrent_with_args(session, statement, map(lambda w, x, y, z: [w, x, y, z], partition_key_values, clustering_key_values, c1_values, c2_values), concurrency=concurrency)

    if output_20_lines:
        logger.debug("output of 20 lines after insertion:")
        query = SimpleStatement(f"SELECT * FROM {ks}.{cf} limit 20", consistency_level=consistency)
        rows = list(session.execute(query))
        logger.debug("\n".join(str(row) for row in rows))


def delete_c1c2(session, keys=None, n=None, consistency=ConsistencyLevel.QUORUM, ks="ks", cf="cf", concurrency=20):  # noqa: PLR0913
    # temp to trigger CI run those test

    if (keys is None and n is None) or (keys is not None and n is not None):
        raise ValueError(f"Expected exactly one of 'keys' or 'n' arguments to not be None; got keys={keys}, n={n}")
    if n:
        keys = list(range(n))

    statement = session.prepare(f"DELETE FROM {ks}.{cf} WHERE key=?")
    statement.consistency_level = consistency

    execute_concurrent_with_args(session, statement, [[f"k{k}"] for k in keys], concurrency=concurrency)


def insert_columns(session, key, columns_count, consistency=ConsistencyLevel.QUORUM, offset=0):
    upds = ["UPDATE cf SET v='value%d' WHERE key='k%s' AND c='c%06d'" % (i, key, i) for i in range(offset * columns_count, columns_count * (offset + 1))]
    query = "BEGIN BATCH %s; APPLY BATCH" % "; ".join(upds)
    simple_query = SimpleStatement(query, consistency_level=consistency)
    session.execute(simple_query)


def query_columns(session, key, columns_count, consistency=ConsistencyLevel.QUORUM, offset=0):
    query = SimpleStatement("SELECT c, v FROM cf WHERE key='k%s' AND c >= 'c%06d' AND c <= 'c%06d'" % (key, offset, columns_count + offset - 1), consistency_level=consistency)
    res = list(session.execute(query))
    assertions.assert_length_equal(res, columns_count)
    for i in range(columns_count):
        assert res[i][1] == f"value{i + offset}"


def drop_table(session, table_name, if_exists=False):
    session.execute("DROP TABLE {} {}".format("IF EXISTS" if if_exists else "", table_name))


def putget(cluster, session, cl=ConsistencyLevel.QUORUM):
    _put_with_overwrite(cluster, session, 1, cl)

    # reads by name
    # We do not support proper IN queries yet
    # if cluster.version() >= "1.2":
    #    session.execute('SELECT * FROM cf USING CONSISTENCY %s WHERE key=\'k0\' AND c IN (%s)' % (cl, ','.join(ks)))
    # else:
    #    session.execute('SELECT %s FROM cf USING CONSISTENCY %s WHERE key=\'k0\'' % (','.join(ks), cl))
    # _validate_row(cluster, session)
    # slice reads
    query = SimpleStatement("SELECT * FROM cf WHERE key='k0'", consistency_level=cl)
    rows = list(session.execute(query))
    _validate_row(cluster, rows)


def _put_with_overwrite(cluster, session, nb_keys, cl=ConsistencyLevel.QUORUM):
    for k in range(nb_keys):
        kvs = ["UPDATE cf SET v='value%d' WHERE key='k%s' AND c='c%02d'" % (i, k, i) for i in range(100)]
        query = SimpleStatement("BEGIN BATCH %s APPLY BATCH" % "; ".join(kvs), consistency_level=cl)
        session.execute(query)
        time.sleep(0.01)
    cluster.flush()
    for k in range(nb_keys):
        kvs = ["UPDATE cf SET v='value%d' WHERE key='k%s' AND c='c%02d'" % (i * 4, k, i * 2) for i in range(50)]
        query = SimpleStatement("BEGIN BATCH %s APPLY BATCH" % "; ".join(kvs), consistency_level=cl)
        session.execute(query)
        time.sleep(0.01)
    cluster.flush()
    for k in range(nb_keys):
        kvs = ["UPDATE cf SET v='value%d' WHERE key='k%s' AND c='c%02d'" % (i * 20, k, i * 5) for i in range(20)]
        query = SimpleStatement("BEGIN BATCH %s APPLY BATCH" % "; ".join(kvs), consistency_level=cl)
        session.execute(query)
        time.sleep(0.01)
    cluster.flush()


def _validate_row(cluster, res):
    assertions.assert_length_equal(res, 100)
    for i in range(100):
        if i % 5 == 0:
            assert res[i][2] == f"value{i * 4}", f"for {i}, expecting value{i * 4}, got {res[i][2]}"
        elif i % 2 == 0:
            assert res[i][2] == f"value{i * 2}", f"for {i}, expecting value{i * 2}, got {res[i][2]}"
        else:
            assert res[i][2] == f"value{i}", f"for {i}, expecting value{i}, got {res[i][2]}"


def range_putget(cluster, session, cl=ConsistencyLevel.QUORUM):
    keys = 100

    _put_with_overwrite(cluster, session, keys, cl)

    paged_results = session.execute("SELECT * FROM cf LIMIT 10000000")
    rows = [result for result in paged_results]

    assertions.assert_length_equal(rows, keys * 100)
    for k in range(keys):
        res = rows[:100]
        del rows[:100]
        _validate_row(cluster, res)


def get_keyspace_metadata(session, keyspace_name):
    cluster = session.cluster
    cluster.refresh_keyspace_metadata(keyspace_name)
    return cluster.metadata.keyspaces[keyspace_name]


def get_schema_metadata(session):
    cluster = session.cluster
    cluster.refresh_schema_metadata()
    return cluster.metadata


def get_table_metadata(session, keyspace_name, table_name):
    cluster = session.cluster
    cluster.refresh_table_metadata(keyspace_name, table_name)
    return cluster.metadata.keyspaces[keyspace_name].tables[table_name]


def get_view_id(session, keyspace_name, view_name):
    res = session.execute(f"select id from system_schema.views where keyspace_name='{keyspace_name}' and view_name='{view_name}'")
    assert res, f"Secondary index view named {view_name} has not built"
    return rows_to_list(res)[0][0]


def wait_for_schema_agreement(session):
    rows = list(session.execute("SELECT schema_version FROM system.local WHERE key='local'"))
    local_version = rows[0]

    all_match = True
    rows = list(session.execute("SELECT schema_version FROM system.peers"))
    for peer_version in rows:
        if peer_version != local_version:
            all_match = False
            break

    if all_match:
        return
    else:
        time.sleep(1)
        wait_for_schema_agreement(session)


def get_entity_id(session, table_or_view, keyspace_name, entity_name):
    system_table = table_or_view + "s"
    query = f"SELECT id FROM system_schema.{system_table} WHERE keyspace_name='{keyspace_name}' and {table_or_view}_name='{entity_name}'"
    entity_id = rows_to_list(session.execute(query))
    return entity_id[0][0]


def get_truncated_time_from_system_local(session):
    query = "SELECT truncated_at FROM system.local"
    truncated_time = rows_to_list(session.execute(query))
    return truncated_time


def get_truncated_time_from_system_truncated(session, table_id):
    query = f"SELECT truncated_at FROM system.truncated WHERE table_uuid={table_id}"
    truncated_time = rows_to_list(session.execute(query))
    return truncated_time[0]


def _index_creation(session, query, table_name, index_column, index_name=None, compaction=None):  # noqa: PLR0913
    index_column = [index_column] if isinstance(index_column, str) else index_column
    index_column = ", ".join([i for i in index_column])
    query = query.format(index_name=index_name, table_name=table_name, index_column=index_column)
    logger.debug(f"Create index: {query}")
    session.execute(query)
    if compaction:
        # Update appropriate to index materialized view with compaction storage
        session.execute("ALTER MATERIALIZED VIEW {}_index WITH compaction={}".format(index_name, {"class": compaction}))
    logger.debug(f"Index {index_name} has been created")


def create_index(session, table_name, index_column, index_name=None, compaction=None):
    query = "CREATE INDEX {index_name} ON {table_name} ({index_column})"
    _index_creation(session=session, query=query, table_name=table_name, index_column=index_column, index_name=index_name, compaction=compaction)


def create_local_index(session, table_name, pk_name, index_column, index_name=None, compaction=None):  # noqa: PLR0913
    query = "CREATE INDEX {index_name} ON {table_name} ((%s), {index_column})" % pk_name
    _index_creation(session=session, query=query, table_name=table_name, index_column=index_column, index_name=index_name, compaction=compaction)


def insert_c1c2_no_prepared(  # noqa: PLR0913
    session,
    keys=None,
    n=None,
    consistency=ConsistencyLevel.QUORUM,
    c1_values=None,
    c2_values=None,
    ks="ks",
    cf="cf",
):
    if keys is None:
        keys = []

    if c1_values is None:
        c1_values = []

    if c2_values is None:
        c2_values = []

    build_insert_params(keys, n, c1_values, c2_values)

    execute_concurrent(session, map(lambda x, y, z: (SimpleStatement(f"INSERT INTO {ks}.{cf} (key, c1, c2) VALUES ('k{x}', '{y}', '{z}')", consistency_level=consistency), None), keys, c1_values, c2_values))


def query_c1c2_concurrent(session, keys, consistency=ConsistencyLevel.QUORUM, tolerate_missing=False, must_be_missing=False, c1_values=None, c2_values=None, ks=None, cf=None, concurrency=20):  # noqa: PLR0913
    if c1_values is None:
        c1_values = ["value1"] * len(keys)

    if c2_values is None:
        c2_values = ["value2"] * len(keys)

    if len(c1_values) != len(c2_values) or len(c1_values) != len(keys):
        raise ValueError("Inconsistent 'c1/c2_values' contents. 'c1/c2_values' should be either a 'None' value or a list of the same length as a requested number of keys.")

    ks_cf = ""
    if ks:
        ks_cf += f"{ks}."
    if cf:
        ks_cf += f"{cf}"
    else:
        ks_cf += "cf"

    # prepare a query statement
    query = f"SELECT c1, c2 FROM {ks_cf} WHERE key=?"
    logger.debug("Select query: %s", query)
    pquery = session.prepare(query)
    pquery.consistency_level = consistency

    results = execute_concurrent_with_args(session, pquery, map(lambda x: [f"k{x}"], keys), concurrency=concurrency)
    for result, c1, c2 in zip(results, c1_values, c2_values):
        check_c1c2_result_one(result[0], list(result[1]), tolerate_missing, must_be_missing, c1, c2)


def build_insert_params(keys, n, c1_values, c2_values):
    if (len(keys) == 0 and n is None) or (len(keys) != 0 and n is not None):
        raise ValueError(f"Expected exactly one of 'keys' or 'n' arguments to not be None; got keys={keys}, n={n}")

    if n:
        keys.extend(list(range(n)))

    if len(c1_values) == 0:
        c1_values.extend(["value1"] * len(keys))

    if len(c2_values) == 0:
        c2_values.extend(["value2"] * len(keys))

    if len(c1_values) != len(c2_values) or len(c1_values) != len(keys):
        raise ValueError("Inconsistent 'c1/c2_values' contents. 'c1/c2_values' should be either a '[]' value or a list of the same length as a requested number of keys.")


def check_c1c2_result_one(success, rows, tolerate_missing, must_be_missing, c1_value, c2_value):  # noqa: PLR0913
    rows = list(rows)
    if not success:
        assert False, f"Query failed {rows}"

    if not tolerate_missing:
        assert len(rows) == 1, f"Wrong length, {len(rows)}"
        res = rows[0]
        assert len(res) == 2, f"Expected 2 columns in result, but got: {res}"
        assert res[0] == c1_value and res[1] == c2_value, f"Expected Row(c1='{c1_value}', c2='{c2_value}'), but got: {res}"

    if must_be_missing:
        assert len(rows) == 0, f"Number of rows {len(rows)}"


def print_table(table):
    logger.debug(tabulate.tabulate(tabular_data=[[str(getattr(row, column_name)) for column_name in table.column_names] for row in table.current_rows], headers=table.column_names))


def chunks_list(lst, num_chunks):
    for i in range(0, len(lst), num_chunks):
        yield lst[i : i + num_chunks]


def insert_c1cn(session, keys=None, consistency=ConsistencyLevel.QUORUM, nr_columns=5, column_size=None, ks="ks", cf="cf", concurrency=20):  # noqa: PLR0913
    if keys is None:
        keys = []

    cql_str = f"INSERT INTO {ks}.{cf} (key, "
    for nr in range(1, nr_columns + 1):
        if nr != nr_columns:
            cql_str += f"c{nr}, "
        else:
            cql_str += f"c{nr}) VALUES (?, "
    for nr in range(1, nr_columns + 1):
        if nr != nr_columns:
            cql_str += "?, "
        else:
            cql_str += "?) "

    statement = session.prepare(cql_str)
    statement.consistency_level = consistency

    # build column values for c1 to cn
    col_data = []
    for nr in range(1, nr_columns + 1):
        "x" * column_size
        if column_size:
            col_data.append("x" * column_size)
        else:
            col_data.append(f"column_data_{nr}")

    # build data for each row, including key and columns
    kv = []
    for key in keys:
        data = [f"k{key}"]
        data.extend(col_data)
        kv.append(data)

    execute_concurrent_with_args(session, statement, kv, concurrency=concurrency)


def prepare_statement(session, query, cl=ConsistencyLevel.ONE):
    """
    Prepare CQL query into statement and assign given consistency level to it.
    """
    logger.debug("Preparing statement: %s", query)
    res = session.prepare(query)
    res.consistency_level = cl
    return res


def get_rows_set_from_res(res):
    return set([tuple(res_list) for res_list in rows_to_list(res)])


def get_node_sstables_compression(node, keyspace: str = "keyspace1") -> list[str]:
    node_keyspace_folder = os.path.join(node.get_path(), "data", keyspace)
    stdout = subprocess.Popen([f"find {node_keyspace_folder} -type f -name *CompressionInfo* "], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, encoding="UTF-8").communicate()[0]
    lines = stdout.splitlines()
    compressions = []
    for compression_file_path in lines:
        stdout = subprocess.Popen([f" strings {compression_file_path} | grep Compressor"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, encoding="UTF-8").communicate()[0]
        if stdout:
            compressions.append(stdout.strip())
    logger.info("%s %s got compressions of: %s", node.name, keyspace, compressions)
    return compressions


def keyspace_has_tablets(session, keyspace) -> bool:
    """Return true if the keyspace was created with tablets.

    We support running cql-pytest against an older version of scylla, so we do
    the detection in a way that accounts for scylla possibly not even knowing
    what tablets is.

    If the keyspace was created with tablets, it will have an entry in
    `system_schema.scylla_keyspaces`, with `initial_tablets` set.
    So here, we simply query this table, looking for a partition for the
    appropriate keyspace. If the result has the `initial_tablets` column and it
    is set, the keyspace has tablets.
    """

    try:
        res = list(session.execute(f"SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name='{keyspace}'"))
    except InvalidRequest as exc:
        # Antique versions of Scylla and Cassandra do not have
        # the scylla_keyspaces table. They don't have tablets either, so
        # we should just return False.
        assert "unconfigured table scylla_keyspaces" in str(exc)
        return False

    # The row might exist due to storage related options, but the tablets
    # related fields are null.
    # So we check that:
    # * the row exists
    # * `initial_tablets` has a value
    if not res:
        return False
    return getattr(res[0], "initial_tablets", None) is not None


def generate_int32_pks_for_vnodes(node, num_pks: int) -> list[int]:
    """Generates int32 partition keys for a vnode-based table, ensuring that at least one partition key is assigned to each shard of the given node"""
    rand_pks = set()

    # Generate atleast one PK for each shard
    pk_type = ScyllaType.make_partition_key("Int32Type")
    for shard_id in range(node._smp):
        for pk_candidate in count(0):
            # find shard of the key
            serialized_value = node.run_scylla_types("serialize", pk_type, pk_candidate)
            shardof_output = node.run_scylla_types("shardof", pk_type, serialized_value, extra_args=["--shards", node._smp])
            # shardof output looks like this:
            # (1): token: -4069959284402364209, shard: 0
            pk_shard_id = int(shardof_output.rsplit(":", 1)[-1])
            if pk_shard_id == shard_id:
                rand_pks.add(pk_candidate)
                break
    assert len(rand_pks) == node._smp

    # Generate rest of the PKs randomly
    while len(rand_pks) < num_pks:
        rand_pks.add(random.randint(-2147483647, 2147483647))

    return list(rand_pks)


def simulate_write_process_in_minutes(  # noqa: PLR0913
    cluster,
    session,
    keyspace,
    table_name,
    duration_minutes=20,
    start_from_minute=0,
    flush_period_seconds=30,
    flushing_exclude_nodes=None,
    num_pks=10,
    size=1,
    concurrency=20,
) -> tuple[list[int], int]:
    """Simulate a write process across duration minutes.

    We use `USING TIMESTAMP` to distribute the writes evenly
    across the entire range, simulating a write every second (to
    several partitions).
    flush_period_seconds allow to control how many time windows could be
    in sstable
    used schema:
     pk PRIMARY KEY int
     ck clustering key int
     v  int

    Arguments:
        session {Session} -- opened session to node
        keyspace {str} -- keyspace name
        table_name {str} -- table name

    Keyword Arguments:
        duration_minutes {number} -- how many minutes to simulate (default: {20})
        flush_period_seconds {number} -- in how many seconds flush memtable (default: {30})
        start_from_minute {number} -- start minute to write data
        flushing_nodes {list} -- list of nodes, which should be flushed.
        flushing_exclude_nodes {list} -- list of nodes where should not be flushed
        num_ps {Iterable| int} -- if Iterable, then a sequence of primary keys
                                  if int, number of random generated primary keys
        size {int} -- size in value in bytes

    """
    exclude_nodes = flushing_exclude_nodes if flushing_exclude_nodes else []
    insert_statement = session.prepare(f"INSERT INTO {keyspace}.{table_name} (pk, ck, v) VALUES (?, ?, ?)USING TIMESTAMP ?")
    v = b"a" * size
    pk_list = []

    if isinstance(num_pks, list):
        pk_list = num_pks
    elif keyspace_has_tablets(session, keyspace):
        # there is no reliable way to deduce which shard a key belongs
        # to when the table uses tablets; so generate a random sample.
        pk_list = random.sample(range(-2147483647, 2147483647), num_pks)
    else:
        pk_list = generate_int32_pks_for_vnodes(cluster.nodelist()[0], num_pks)
    # log the generated pk list; it will be useful for debugging when a test fails.
    logger.warning(f"generated pks : {','.join(str(pk) for pk in pk_list)}")

    flushing_nodes = [node for node in cluster.nodelist() if node not in exclude_nodes]
    for t in range(start_from_minute * 60, duration_minutes * 60):
        execute_concurrent_with_args(session, insert_statement, [(pk, t, v, seconds_to_micros(t)) for pk in pk_list], concurrency=concurrency)

        # Flush every flush period in seconds on each node
        if t % flush_period_seconds == 0:
            for node in flushing_nodes:
                node.flush(verbose=False)
    total_rows = (duration_minutes - start_from_minute) * 60 * len(pk_list)
    return pk_list, total_rows
