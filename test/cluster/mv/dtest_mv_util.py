#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Helpers for the materialized view tests ported from scylla-dtest
materialized_views_test.py: the dtests' cluster layout and keyspace, their
table and view generator (TableManager), view build waits, row count and
query comparisons, and nodetool commands run against a node."""

import asyncio
import glob
import os
import logging
import random
import re
import string
import subprocess
import time
from itertools import groupby

from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement

from test.cluster.util import strip_asan_warnings
from test.cqlpy.util import run_cqlsh_at
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# Like dtest nodes, keyspaces use vnodes.
VNODES = {"tablets_mode_for_new_keyspaces": "disabled"}


def rack_layout(nodes, rf) -> list[dict]:
    """The dtests' generate_cluster_topology_based_rf(): RF racks per DC,
    named RAC<n>, with the nodes spread over them. nodes is a count (one DC,
    dc1) or a list of counts per DC (dc1, dc2...); rf an int, or a dict of
    RF per DC name. A dict of {dc: {rack: count}} is the topology itself."""
    if isinstance(nodes, dict):
        return [{"dc": dc, "rack": rack} for dc, racks in nodes.items() for rack, n in racks.items() for _ in range(n)]
    counts = nodes if isinstance(nodes, list) else [nodes]
    layout = []
    for j, dc_nodes in enumerate(counts):
        dc = f"dc{j + 1}"
        dc_rf = rf.get(dc) if isinstance(rf, dict) else rf
        racks = dc_rf if dc_rf > 0 else dc_nodes
        per_rack, extra = divmod(dc_nodes, racks)
        for i in range(racks):
            layout += [{"dc": dc, "rack": f"RAC{i + 1}"}] * (per_rack + (1 if i < extra else 0))
    return layout


async def prepare(manager: ScyllaClusterManager, nodes=3, rf=1, config=None, consistency_level=None, user_table=False, exclusive=False, auth=None,
                  request_timeout=None, fetch_size=None):
    """The dtests' prepare(): a cluster, a session to the first node, and an
    RF keyspace "ks" (rf an int, or a dict of RF per DC) used by the session,
    optionally with the users table and its view. auth is a (user, password)
    pair for clusters with authentication. nodes is as in rack_layout()."""
    auth_kw = {"auth_provider": PlainTextAuthProvider(username=auth[0], password=auth[1])} if auth else {}
    layout = rack_layout(nodes, rf)
    servers = await manager.servers_add(len(layout), config={**VNODES, **(config or {})}, property_file=layout,
                                        driver_connect_opts=auth_kw or None)
    cql = session_for(manager, servers[0], exclusive=exclusive, **auth_kw)
    if consistency_level is not None:
        cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = consistency_level
    if request_timeout is not None:
        cql.get_execution_profile(EXEC_PROFILE_DEFAULT).request_timeout = request_timeout
    if fetch_size is not None:
        cql.default_fetch_size = fetch_size
    if isinstance(rf, dict):
        replication = ", ".join(f"'{dc}':{n}" for dc, n in rf.items())
        cql.execute(f"CREATE KEYSPACE ks WITH replication={{'class':'NetworkTopologyStrategy', {replication}}}")
    else:
        cql.execute(f"CREATE KEYSPACE ks WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':{rf}}}")
    cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    cql.execute("USE ks")
    if user_table:
        cql.execute("CREATE TABLE ks.users (username varchar, password varchar, gender varchar, session_token varchar, state varchar, birth_year bigint, PRIMARY KEY (username));")
        cql.execute("CREATE MATERIALIZED VIEW ks.users_by_state AS SELECT * FROM ks.users WHERE STATE IS NOT NULL AND username IS NOT NULL PRIMARY KEY (state, username)")
    return servers, cql


def session_for(manager: ScyllaClusterManager, server, exclusive=False, **kwargs):
    """A session to the server, whose statements default to CL=ONE like
    dtest sessions (test.py sessions default to LOCAL_QUORUM)."""
    if exclusive:
        cql = manager.con_gen([server.ip_addr], load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr]), **kwargs).connect()
    else:
        cql = manager.con_gen([server.ip_addr], **kwargs).connect()
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
    return cql


async def add_node(manager: ScyllaClusterManager, dc="dc1", rack="RAC1", config=None):
    """The dtests' _add_new_node(): bootstrap a node and return an exclusive
    session to it."""
    server = await manager.server_add(config={**VNODES, **(config or {})}, property_file={"dc": dc, "rack": rack})
    return server, session_for(manager, server, exclusive=True)


async def nodetool(manager: ScyllaClusterManager, server, *args, check=True, timeout=None):
    """Runs "scylla nodetool <args>" against the server, like ccm's
    node.nodetool(). Returns the CompletedProcess."""
    exe = await manager.server_get_exe(server.server_id)
    cmd = [exe, "nodetool", "-h", server.ip_addr, *args]
    proc = await asyncio.to_thread(subprocess.run, cmd, capture_output=True, text=True, timeout=timeout)
    proc.stderr = strip_asan_warnings(proc.stderr)
    if check and proc.returncode:
        raise RuntimeError(f"{' '.join(cmd)} failed: {proc.stdout} {proc.stderr}")
    return proc


async def flush_all_nodes(manager: ScyllaClusterManager, servers):
    """The dtests' cluster.flush(): flush every running node."""
    running = {s.server_id for s in await manager.running_servers()}
    for s in servers:
        if s.server_id in running:
            await manager.api.flush_all_keyspaces(s.ip_addr)


def wait_for_view(cql, servers_alive: int, ks, view, timeout=600, cl=None):
    """The dtests' wait_for_view(): wait until view_build_status reports the
    view built on every live node."""
    if cl is None:
        cl = ConsistencyLevel.ONE if servers_alive == 1 else ConsistencyLevel.QUORUM
    query = SimpleStatement(f"SELECT host_id, status FROM system_distributed.view_build_status "
                            f"WHERE keyspace_name = '{ks}' AND view_name = '{view}'", consistency_level=cl)
    deadline = time.time() + timeout
    while True:
        try:
            done = {r.host_id for r in cql.execute(query) if r.status == "SUCCESS"}
            if len(done) >= servers_alive:
                return
        except Exception as e:  # noqa: BLE001
            logger.debug(f"wait_for_view {ks}.{view}: {e}")
        assert time.time() < deadline, f"view {ks}.{view} is not built"
        time.sleep(0.5)


def eventually(fun, trials=64):
    """Runs fun until it succeeds, up to trials times, a second apart."""
    for i in range(trials - 1):
        try:
            return fun()
        except Exception as e:  # noqa: BLE001
            logger.debug(f"[{i + 1}/{trials}]: {e}: will retry in 1 second")
            time.sleep(1)
    return fun()


def query(cql, statement, cl=ConsistencyLevel.ONE, timeout=120):
    return list(cql.execute(SimpleStatement(statement, consistency_level=cl), timeout=timeout))


def row_count(cql, table, cl=ConsistencyLevel.ONE, timeout=120):
    return query(cql, f"SELECT count(*) FROM {table}", cl, timeout)[0][0]


def assert_row_count(cql, table, expected, cl=ConsistencyLevel.ONE, num_attempts=1):
    for attempt in range(num_attempts):
        count = row_count(cql, table, cl)
        if count == expected or attempt == num_attempts - 1:
            assert count == expected, f"Expected a row count of {expected} in table '{table}', but got {count}"
            return
        time.sleep(10)


def grouped(cql, statement, cl, column=None, restrict_column=None, restrict_values=None):
    """The dtests' run_query_with_data_processing(): the rows, or with
    column, the sorted [value, count] pairs of that column."""
    res = query(cql, statement, cl)
    if not res:
        return res
    if restrict_column:
        res = [r for r in res if getattr(r, restrict_column) in restrict_values]
    if column:
        values = sorted(getattr(r, column) for r in res)
        return [[k, len(list(g))] for k, g in groupby(values)]
    return [list(r) for r in res]


def assert_two_queries_equal(cql1, query1, cql2, query2, cl=ConsistencyLevel.ONE, column1=None, column2=None, **kw):
    exp = grouped(cql1, query1, cl, column1, kw.get("restrict_column1"), kw.get("restrict_values1"))
    act = grouped(cql2, query2, cl, column2, kw.get("restrict_column2"), kw.get("restrict_values2"))
    assert exp == act, f"Expected {exp}, but got {act}. Query1: {query1}; Query2: {query2}"


def create_cf(cql, table, columns=None, key_type="varchar"):
    """The dtests' create_cf()."""
    cols = "".join(f", {k} {v}" for k, v in (columns or {}).items())
    if cols:
        q = f"CREATE COLUMNFAMILY {table} (key {key_type} PRIMARY KEY{cols}) WITH comment='test cf'"
    else:
        q = f"CREATE COLUMNFAMILY {table} (key {key_type}, c varchar, v varchar, PRIMARY KEY(key, c)) WITH comment='test cf'"
    cql.execute(f"{q} AND compaction = {{'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'}} AND compression = {{}}")
    cql.execute(f"SELECT * FROM {table} LIMIT 1")


def insert_c1c2(cql, n, ks="ks", cf="cf", c1_values=None, c2_values=None, consistency=ConsistencyLevel.QUORUM):
    stmt = cql.prepare(f"INSERT INTO {ks}.{cf} (key, c1, c2) VALUES (?, ?, ?)")
    stmt.consistency_level = consistency
    execute_concurrent_with_args(cql, stmt, [[f"k{k}", c1, c2] for k, c1, c2 in zip(range(n), c1_values, c2_values)], concurrency=20)


class TableManager:
    """The dtests' TableManager, for its uses in the ported tests: a table
    of an "id varint" partition key and generated int and text columns
    named clmn_<type><i>, prefilled with random values (ten distinct values
    repeated), and views keyed by the id and one column."""

    def __init__(self, cql, servers_alive, columns, keyspace="ks", table_name="tm_table", int_pk_columns=0):
        """columns is {type: (amount, min, max)}. int_pk_columns is the
        number of int columns added to the partition key after id: the
        dtests' TableManager default pk_columns={"int": {"by count": 1}} is
        int_pk_columns=1, their pk_columns={} is 0."""
        self.cql = cql
        self.servers_alive = servers_alive
        self.keyspace = keyspace
        self.table_name = table_name
        self.columns = columns  # {type: (amount, min, max)}
        self.columns_list = ["id varint"]
        for ctype, (amount, _, _) in columns.items():
            self.columns_list += [f"clmn_{ctype}{i} {ctype}" for i in range(amount)]
        self.column_names_list = [c.split(" ")[0] for c in self.columns_list]
        self.pk_list = ["id"] + [c for c in self.column_names_list[1:] if f"{c} int" in self.columns_list][:int_pk_columns]
        self.materialized_views = {}

    @property
    def name(self):
        return f"{self.keyspace}.{self.table_name}"

    def create_table(self):
        self.cql.execute(f"CREATE TABLE IF NOT EXISTS {self.name} ({', '.join(self.columns_list)}, PRIMARY KEY(({', '.join(self.pk_list)})))")

    def _values(self, rows, ready=None):
        data = {}
        for ctype, (_, lo, hi) in self.columns.items():
            def rand(n):
                if ready and ctype in ready and n:
                    return list(ready[ctype])
                if ctype == "int":
                    return [random.randint(lo, hi) for _ in range(n)]
                return ["".join(random.choice(string.ascii_lowercase) for _ in range(lo, hi)) for _ in range(n)]
            dupl = len(ready[ctype]) if ready and ctype in ready else 10
            data[ctype] = rand(dupl) * (rows // dupl) + rand(rows % dupl)
        return [data[c.split(" ")[1]] for c in self.columns_list[1:]]

    def prefill_table(self, rows, data=None, start_id_from=0, consistency=ConsistencyLevel.QUORUM, delay=0):
        time.sleep(delay)
        values = self._values(rows, data)
        stmt = self.cql.prepare(f"INSERT INTO {self.name} ({', '.join(self.column_names_list)}) VALUES ({', '.join('?' * len(self.columns_list))})")
        stmt.consistency_level = consistency
        execute_concurrent_with_args(self.cql, stmt, ([k + start_id_from] + [v[k] for v in values] for k in range(rows)))

    def int_columns(self):
        """The int columns that are not in the primary key."""
        return [c.split(" ")[0] for c in self.columns_list[1:] if c.endswith(" int") and c.split(" ")[0] not in self.pk_list]

    def multiple_deletes(self, ids, delay=0):
        """The dtests' multiple_deletes({"id": ids}): delete the rows one by one."""
        time.sleep(delay)
        for _id in ids:
            self.cql.execute(f"delete from {self.name} where id={_id}")

    def multiple_int_updates_by_id(self, update_to, filter_values=None, updates=100, same_id=True, delay=0, ids=None):
        """Updates a random int column of rows picked at random (among those
        whose value is in filter_values, or among ids) to random values in
        update_to."""
        time.sleep(delay)
        column = random.choice(self.int_columns())
        rows = list(self.cql.execute(f"select * from {self.name}"))
        _id = None
        for _ in range(updates):
            if ids:
                _id = random.choice(ids)
            elif not same_id or _id is None:
                if not same_id:
                    rows = list(self.cql.execute(f"select * from {self.name}"))
                for _ in range(len(rows) + 1):
                    r = random.choice(rows)
                    if not filter_values or getattr(r, column) in filter_values:
                        _id = r.id
                        break
            if _id is not None:
                self.cql.execute(f"UPDATE {self.name} SET {column} = {random.randint(*update_to)} WHERE id = {_id}")

    def select_all_mvs(self, reads=100, by_id=False):
        max_id = self.get_max_id() if by_id else 0
        for _ in range(reads):
            mv = random.choice(list(self.materialized_views))
            if by_id:
                self.cql.execute(f"select * from {self.keyspace}.{mv} where id={random.randint(0, max_id)}")
            else:
                self.cql.execute(f"select * from {self.keyspace}.{mv} LIMIT 10")

    def get_max_id(self):
        _id = list(self.cql.execute(f"select max(id) as id from {self.name}"))[0].id
        return _id or 0

    def create_mv(self, column, wait=True, select=None):
        """A view keyed by (id, column) selecting the column (or the given
        select list), named <table>_mv_<n>."""
        mv_name = f"{self.table_name}_mv_{len(self.materialized_views)}"
        self.cql.execute(f"CREATE MATERIALIZED VIEW {self.keyspace}.{mv_name} AS SELECT {select or column} FROM {self.name} "
                         f"WHERE id IS NOT NULL and {column} IS NOT NULL PRIMARY KEY (id, {column})")
        if wait:
            wait_for_view(self.cql, self.servers_alive, self.keyspace, mv_name)
        self.materialized_views[mv_name] = column
        return mv_name

    def create_mvs_by_one_column(self, amount, wait=False):
        """The dtests' _create_mvs_by_one_column(): one view per column."""
        for i in range(1, amount + 1):
            self.create_mv(self.column_names_list[i], wait=wait)


def validate_data_in_mvs(cql, tm, table_rows, mv_rows, cl):
    """The dtests' _validate_data_in_mvs(): row counts of the table and of
    each view, and the per-value counts of each view's column."""
    for mv, column in tm.materialized_views.items():
        assert_row_count(cql, tm.name, table_rows, cl)
        assert_row_count(cql, f"{tm.keyspace}.{mv}", mv_rows, cl)
        eventually(lambda: assert_two_queries_equal(cql, f"select * from {tm.name}", cql, f"select * from {tm.keyspace}.{mv}", cl, column, column))


async def run_in_parallel(*funcs):
    """The dtests' run_in_parallel(): run blocking functions and coroutines
    together; the first error is raised."""
    tasks = [f if asyncio.iscoroutine(f) else asyncio.to_thread(f) for f in funcs]
    return await asyncio.gather(*tasks)



async def stop_nodes(manager: ScyllaClusterManager, servers):
    """The dtests' cluster.stop_nodes(..., wait_other_notice=True)."""
    await asyncio.gather(*(manager.server_stop_gracefully(s.server_id) for s in servers))
    for s in servers:
        await manager.others_not_see_server(s.ip_addr)


async def start_nodes(manager: ScyllaClusterManager, servers):
    """The dtests' cluster.start_nodes(..., wait_other_notice=True)."""
    for s in servers:
        await manager.server_start(s.server_id)
    await manager.servers_see_each_other(await manager.running_servers())


async def delete_sstables(manager: ScyllaClusterManager, server, ks, tables):
    """The dtests' remove_files_in_folder(get_node_cf_dir(...)): remove the
    files in the table directories of a stopped node."""
    workdir = await manager.server_get_workdir(server.server_id)
    for t in tables:
        for table_dir in glob.glob(os.path.join(workdir, "data", ks, f"{t}-*")):
            for f in os.listdir(table_dir):
                path = os.path.join(table_dir, f)
                if os.path.isfile(path):
                    os.remove(path)


async def wait_for_view_update_generation(manager: ScyllaClusterManager, server, timeout=60):
    """The dtests' wait_for_view_update_generation(): wait until the node's
    view update generator has no pending work."""
    names = ["scylla_view_update_generator_pending_registrations", "scylla_view_update_generator_queued_batches_count",
             "scylla_view_update_generator_sstables_to_move_count", "scylla_view_update_generator_sstables_pending_work"]
    deadline = time.time() + timeout
    while True:
        metrics = await manager.metrics.query(server.ip_addr)
        if not any(metrics.get(n) for n in names):
            return
        assert time.time() < deadline, "view update generation did not finish"
        await asyncio.sleep(1)


async def cqlsh_rows(server, query):
    """The rows cqlsh prints for the query, as lists of strings, like the
    dtests' node.run_cqlsh() parsing."""
    res = await asyncio.to_thread(run_cqlsh_at, server.ip_addr, 9042, query)
    # cqlsh may print warnings, like a missing pytz module, on stderr.
    assert res.returncode == 0, res
    lines = []
    for line in res.stdout.strip().split("\n"):
        if not line.strip():
            break
        lines.append(line.strip())
    return [[c.strip() for c in line.split("|")] for line in lines[2:]]


async def wait_for_token_ring_and_group0_consistency(manager: ScyllaClusterManager, server, expected_members, timeout=120):
    """The dtests' wait_for_token_ring_and_group0_consistency(node, n): the
    group 0 members, read with cqlsh, and the token ring members, from the
    REST API, are the same n nodes, with at least one voter."""
    deadline = time.time() + timeout
    while True:
        try:
            group0_id = (await cqlsh_rows(server, "select value from system.scylla_local where key = 'raft_group0_id'"))[0][0]
            members = await cqlsh_rows(server, f"select server_id, can_vote from system.raft_state where group_id = {group0_id} and disposition = 'CURRENT'")
            ring = await manager.api.client.get_json("/storage_service/host_id", server.ip_addr)
            ring_ids = {m["value"] for m in ring}
            assert len(members) == expected_members, f"group 0 members: {members}"
            assert len(ring_ids) == expected_members, f"token ring members: {ring_ids}"
            assert any(can_vote == "True" for _, can_vote in members), "No voters in group0"
            assert not {server_id for server_id, _ in members} - ring_ids
            return
        except (AssertionError, IndexError) as e:
            if time.time() > deadline:
                raise
            logger.debug(f"token ring and group 0 not consistent yet: {e}")
            await asyncio.sleep(1)


async def repair(manager: ScyllaClusterManager, server, keyspace=None, tables=(), partitioner_range=False, timeout=None):
    """ccm's node.repair(): "nodetool repair" of a vnodes keyspace, with its
    tables and --partitioner-range; without a keyspace, "nodetool repair"
    and then "nodetool cluster repair" (of the tablets keyspaces)."""
    options = [keyspace, *tables] if keyspace else []
    if partitioner_range:
        options.append("--partitioner-range")
    await nodetool(manager, server, "repair", *options, timeout=timeout)
    if not keyspace:
        await nodetool(manager, server, "cluster", "repair", timeout=timeout)


# The patterns the dtests' check_errors() always filters out (dtest_setup's
# __filter_errors()): benign errors of nodes being stopped and started.
DTEST_IGNORED_ERRORS = [
    r"Compaction for .* deliberately stopped",
    r"update compaction history failed:.*ignored",
    r"Failed to complete splitting of table .* due to seastar::rpc::remote_verb_error",
    r"Failed to complete splitting of table .* due to seastar::named_gate_closed_exception",
    r"(Startup|start) failed:.*(seastar::sleep_aborted|raft::request_aborted)",
    r"Timer callback failed: seastar::gate_closed_exception",
    r"rpc - client .*(connection dropped|fail to connect)",
    r"rpc::unknown_verb_error",
    r"raft_rpc - Failed to send",
    r"raft_topology.*(seastar::broken_promise|rpc::closed_error)",
    r"Failed to handle STREAM_MUTATION_FRAGMENTS.*rpc::stream_closed",
]


async def check_errors(manager: ScyllaClusterManager, server, exclude_errors=(), from_mark=None):
    """The dtests' check_errors(node, exclude_errors): no ERROR lines in the
    node's log (since from_mark), except those containing an excluded text
    and those the dtests always ignore."""
    log = await manager.server_open_log(server.server_id)
    errors = [line for line in await log.grep_for_errors(distinct_errors=True, from_mark=from_mark)
              if not any(re.search(e, line) for e in [*exclude_errors, *DTEST_IGNORED_ERRORS])]
    assert not errors, "\n".join(errors)


async def compact_all_nodes(manager: ScyllaClusterManager):
    """ccm's cluster.compact(): "nodetool compact" on every running node."""
    for s in await manager.running_servers():
        await nodetool(manager, s, "compact")


async def sync_hinted_handoff(manager: ScyllaClusterManager, server, timeout=60):
    """The dtests' sync_hinted_handoff(node): create a hints sync point on
    the node for all nodes, and wait up to timeout seconds for it."""
    sync_point = await manager.api.client.post_json("/hinted_handoff/sync_point", host=server.ip_addr)
    result = await manager.api.client.get_json("/hinted_handoff/sync_point", host=server.ip_addr,
                                               params={"id": str(sync_point), "timeout": str(timeout)})
    assert result == "DONE", f"hinted_handoff_sync_point: id={sync_point} result={result} timeout={timeout}"
