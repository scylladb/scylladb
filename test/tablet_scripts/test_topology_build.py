#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import csv
import ipaddress
from types import SimpleNamespace
from uuid import UUID

import pytest

from tablets.topology import CSV_DELIMITER
from tablets.topology import LOAD_PER_NODE_COLUMNS
from tablets.topology import LOAD_PER_NODE_TABLE
from tablets.topology import TABLETS_COLUMNS
from tablets.topology import TABLETS_TABLE
from tablets.topology import TABLET_SIZES_COLUMNS
from tablets.topology import TABLET_SIZES_TABLE
from tablets.topology import TOPOLOGY_COLUMNS
from tablets.topology import TOPOLOGY_TABLE
from tablets.topology import Topology
from tablets.topology import TopologyFromSnapshot
from tablets.topology import parse_replica_list
from tablets.topology import parse_replica_size_map
from tablets.topology import rows_from_cql
from tablets.topology import rows_from_csv


TABLE = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
VIEW = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
HOST1 = UUID("11111111-1111-1111-1111-111111111111")
HOST2 = UUID("22222222-2222-2222-2222-222222222222")

# Dumps of the system tables a snapshot is made of. Columns are in an arbitrary order and
# include ones the topology does not care about, as real dumps do.
TABLETS_CSV = f"""\
table_id;last_token;base_table;keyspace_name;table_name;replicas;new_replicas;stage;resize_seq_number
{TABLE};0;;ks;tbl;{{({HOST1}, 0), ({HOST2}, 1)}};;none;0
{TABLE};100;;ks;tbl;{{({HOST1}, 3)}};{{({HOST2}, 0)}};streaming;0
{VIEW};0;{TABLE};ks;tbl_view;;;none;0
{VIEW};100;{TABLE};ks;tbl_view;;;none;0
"""

TOPOLOGY_CSV = f"""\
key;host_id;shard_count;node_state;num_tokens;version;datacenter
local;{HOST1};4;normal;256;7;dc1
local;{HOST2};8;normal;256;7;dc1
"""

LOAD_PER_NODE_CSV = f"""\
node;dc;rack;ip;storage_capacity;effective_capacity
{HOST1};dc1;rack1;10.0.0.1;1000;800
{HOST2};dc1;rack2;10.0.0.2;2000;1600
"""

TABLET_SIZES_CSV = f"""\
table_id;last_token;replicas
{TABLE};0;{{{HOST1}: 10, {HOST2}: 20}}
{TABLE};100;{{{HOST1}: 30}}
"""


def build_from_snapshot() -> Topology:
    """
    Builds the topology the way TopologyFromSnapshot does, from dumps of the system tables.
    """
    topo = Topology()
    topo._build({
        TABLETS_TABLE.name: rows_from_csv(TABLETS_CSV.splitlines(), TABLETS_COLUMNS),
        LOAD_PER_NODE_TABLE.name: rows_from_csv(LOAD_PER_NODE_CSV.splitlines(), LOAD_PER_NODE_COLUMNS),
        TOPOLOGY_TABLE.name: rows_from_csv(TOPOLOGY_CSV.splitlines(), TOPOLOGY_COLUMNS),
        TABLET_SIZES_TABLE.name: rows_from_csv(TABLET_SIZES_CSV.splitlines(), TABLET_SIZES_COLUMNS),
    })
    return topo


def build_from_live_cluster() -> Topology:
    """
    Builds the topology the way LiveClusterTopologySource does, from the same data as
    build_from_snapshot(), but shaped as the CQL driver returns it.
    """
    tablets = [
        SimpleNamespace(table_id=TABLE, last_token=0, base_table=None, keyspace_name="ks",
                        table_name="tbl", replicas=[(HOST1, 0), (HOST2, 1)], new_replicas=None,
                        stage="none", resize_seq_number=0),
        SimpleNamespace(table_id=TABLE, last_token=100, base_table=None, keyspace_name="ks",
                        table_name="tbl", replicas=[(HOST1, 3)], new_replicas=[(HOST2, 0)],
                        stage="streaming", resize_seq_number=0),
        SimpleNamespace(table_id=VIEW, last_token=0, base_table=TABLE, keyspace_name="ks",
                        table_name="tbl_view", replicas=None, new_replicas=None, stage="none",
                        resize_seq_number=0),
        SimpleNamespace(table_id=VIEW, last_token=100, base_table=TABLE, keyspace_name="ks",
                        table_name="tbl_view", replicas=None, new_replicas=None, stage="none",
                        resize_seq_number=0),
    ]
    topology = [
        SimpleNamespace(key="local", host_id=HOST1, shard_count=4, node_state="normal",
                        num_tokens=256, version=7, datacenter="dc1"),
        SimpleNamespace(key="local", host_id=HOST2, shard_count=8, node_state="normal",
                        num_tokens=256, version=7, datacenter="dc1"),
    ]
    load_per_node = [
        SimpleNamespace(node=HOST1, dc="dc1", rack="rack1", ip=ipaddress.ip_address("10.0.0.1"),
                        storage_capacity=1000, effective_capacity=800),
        SimpleNamespace(node=HOST2, dc="dc1", rack="rack2", ip=ipaddress.ip_address("10.0.0.2"),
                        storage_capacity=2000, effective_capacity=1600),
    ]
    tablet_sizes = [
        SimpleNamespace(table_id=TABLE, last_token=0, replicas={HOST1: 10, HOST2: 20}),
        SimpleNamespace(table_id=TABLE, last_token=100, replicas={HOST1: 30}),
    ]

    topo = Topology()
    topo._build({
        TABLETS_TABLE.name: rows_from_cql(tablets, TABLETS_COLUMNS),
        LOAD_PER_NODE_TABLE.name: rows_from_cql(load_per_node, LOAD_PER_NODE_COLUMNS),
        TOPOLOGY_TABLE.name: rows_from_cql(topology, TOPOLOGY_COLUMNS),
        TABLET_SIZES_TABLE.name: rows_from_cql(tablet_sizes, TABLET_SIZES_COLUMNS),
    })
    return topo


def test_a_snapshot_and_a_live_cluster_build_the_same_topology() -> None:
    """
    Both sources feed Topology._build() the same rows, differing only in how they got them,
    so the same cluster state has to come out identical either way.
    """
    assert build_from_snapshot() == build_from_live_cluster()


@pytest.mark.parametrize("build", [build_from_snapshot, build_from_live_cluster])
def test_build_populates_tablets_hosts_and_sizes(build) -> None:
    topo = build()

    assert topo.host_count() == 2
    host1 = topo.require_host(HOST1)
    assert (host1.dc, host1.rack, host1.ip) == ("dc1", "rack1", "10.0.0.1")
    assert (host1.storage_capacity, host1.effective_capacity) == (1000, 800)
    assert (host1.node_state, host1.num_tokens) == ("normal", 256)
    assert host1.is_normal_token_owner()
    # system.topology is authoritative about shard counts, overriding what tablet replicas
    # suggested, which is only a lower bound.
    assert (host1.shard_count, topo.require_host(HOST2).shard_count) == (4, 8)
    assert topo._version == 7

    tablets = topo.get_tablet_map(TABLE).tablets
    assert [tablet.last_token for tablet in tablets] == [0, 100]
    assert tablets[0].replicas == [(HOST1, 0), (HOST2, 1)]
    assert tablets[0].new_replicas is None
    assert tablets[0].stage == "none"
    # A tablet being migrated has its new replicas and the stage it reached.
    assert tablets[1].replicas == [(HOST1, 3)]
    assert tablets[1].new_replicas == [(HOST2, 0)]
    assert tablets[1].stage == "streaming"

    assert topo.get_tablet_size(TABLE, tablets[0], (HOST1, 0)) == 10
    assert topo.get_tablet_size(TABLE, tablets[0], (HOST2, 1)) == 20
    assert topo.get_tablet_size(TABLE, tablets[1], (HOST1, 3)) == 30


@pytest.mark.parametrize("build", [build_from_snapshot, build_from_live_cluster])
def test_build_makes_a_colocated_table_share_the_base_tables_tablets(build) -> None:
    topo = build()

    assert topo.get_table_name(VIEW) == "ks.tbl_view"
    assert topo.get_base_table_id(VIEW) == TABLE
    # A colocated table is registered once, however many rows system.tablets has for it,
    # and has no tablets of its own.
    assert topo.get_tablet_map(VIEW).tablets is topo.get_tablet_map(TABLE).tablets
    assert set(topo.iter_table_ids()) == {TABLE, VIEW}
    assert set(topo.iter_table_ids(include_colocated=False)) == {TABLE}
    # Physical tablets are counted once, under the base table.
    assert [table_id for table_id, _ in topo.all_tablets()] == [TABLE, TABLE]


def test_a_replica_the_server_has_no_size_for_counts_as_nothing() -> None:
    """
    A replica which has just been migrated to has no size yet, and system.tablet_sizes says
    so in missing_replicas. That is a snapshot of a cluster mid-migration rather than a
    broken one, so it counts for nothing instead of stopping the whole report.
    """
    sizes_dump = (f"table_id;last_token;replicas;missing_replicas\n"
                  f"{TABLE};0;{{{HOST1}: 10}};{{{HOST2}}}\n"
                  f"{TABLE};100;{{{HOST1}: 30}};{{}}\n")
    topo = Topology()
    topo._build({
        TABLETS_TABLE.name: rows_from_csv(TABLETS_CSV.splitlines(), TABLETS_COLUMNS),
        TABLET_SIZES_TABLE.name: rows_from_csv(sizes_dump.splitlines(), TABLET_SIZES_COLUMNS),
    })
    tablets = topo.get_tablet_map(TABLE).tablets

    assert topo.get_tablet_size(TABLE, tablets[0], (HOST1, 0)) == 10
    assert topo.get_tablet_size(TABLE, tablets[0], (HOST2, 1)) == 0

    # A replica with neither a size nor a word about it is a snapshot that does not add up.
    with pytest.raises(Exception, match="Tablet size not available"):
        topo.get_tablet_size(TABLE, tablets[1], (HOST2, 0))


def test_a_node_the_cluster_left_out_is_not_reported_for_holding_no_sizes(capsys) -> None:
    """
    It reports no size for anything it holds, which is what being left out looks like. Saying
    so would bury the replicas which have no size for a reason worth looking into.
    """
    load_dump = (f"node;dc;rack;storage_capacity;up;excluded\n"
                 f"{HOST1};dc1;rack1;1000;True;False\n"
                 f"{HOST2};dc1;rack2;2000;False;True\n")
    sizes_dump = (f"table_id;last_token;replicas;missing_replicas\n"
                  f"{TABLE};0;{{{HOST1}: 10}};{{{HOST2}}}\n")

    topo = Topology()
    topo._build({
        LOAD_PER_NODE_TABLE.name: rows_from_csv(load_dump.splitlines(), LOAD_PER_NODE_COLUMNS),
        TABLET_SIZES_TABLE.name: rows_from_csv(sizes_dump.splitlines(), TABLET_SIZES_COLUMNS),
    })

    topo.report_missing_tablet_sizes()

    assert capsys.readouterr().err == ""
    # It is still recorded, so a replica on it counts for nothing rather than stopping a report.
    assert (TABLE, 0, HOST2) in topo._missing_tablet_sizes


def test_a_replica_of_a_working_node_with_no_size_is_still_reported(capsys) -> None:
    load_dump = (f"node;dc;rack;storage_capacity;up;excluded\n"
                 f"{HOST1};dc1;rack1;1000;True;False\n"
                 f"{HOST2};dc1;rack2;2000;True;False\n")
    sizes_dump = (f"table_id;last_token;replicas;missing_replicas\n"
                  f"{TABLE};0;{{{HOST1}: 10}};{{{HOST2}}}\n")

    topo = Topology()
    topo._build({
        LOAD_PER_NODE_TABLE.name: rows_from_csv(load_dump.splitlines(), LOAD_PER_NODE_COLUMNS),
        TABLET_SIZES_TABLE.name: rows_from_csv(sizes_dump.splitlines(), TABLET_SIZES_COLUMNS),
    })

    topo.report_missing_tablet_sizes()

    assert "1 replicas have no size" in capsys.readouterr().err


def test_the_missing_sizes_are_grouped_by_node_and_by_table(capsys) -> None:
    """
    A handful of replicas on one node reads differently from a table missing everywhere.
    """
    load_dump = f"node;dc;rack;ip;storage_capacity\n{HOST1};dc1;rack1;10.0.0.1;1000\n"
    sizes_dump = (f"table_id;last_token;replicas;missing_replicas\n"
                  f"{TABLE};0;{{}};{{{HOST1}, {HOST2}}}\n"
                  f"{TABLE};100;{{}};{{{HOST1}}}\n")

    topo = Topology()
    topo._build({
        TABLETS_TABLE.name: rows_from_csv(TABLETS_CSV.splitlines(), TABLETS_COLUMNS),
        LOAD_PER_NODE_TABLE.name: rows_from_csv(load_dump.splitlines(), LOAD_PER_NODE_COLUMNS),
        TABLET_SIZES_TABLE.name: rows_from_csv(sizes_dump.splitlines(), TABLET_SIZES_COLUMNS),
    })
    topo.report_missing_tablet_sizes()

    err = capsys.readouterr().err.splitlines()
    assert err[0].startswith("Note: 3 replicas have no size")
    # The heaviest first, and a node with no ip is named by its id.
    assert err[1] == f"  hosts: 10.0.0.1 (2), {HOST2} (1)"
    assert err[2] == "  tables: ks.tbl (3)"


def test_the_missing_sizes_name_only_the_heaviest_few_and_say_so(capsys) -> None:
    """
    Enough to say where the weight is without turning a note into a report. What is left out
    is said, rather than inferred from a total which does not add up.
    """
    hosts = [UUID(f"{digit}" * 8 + "-1111-1111-1111-111111111111") for digit in range(1, 6)]
    sizes_dump = "table_id;last_token;replicas;missing_replicas\n" + "".join(
        f"{TABLE};{token};{{}};{{{host}}}\n" for token, host in enumerate(hosts))

    topo = Topology()
    topo._build({TABLET_SIZES_TABLE.name: rows_from_csv(sizes_dump.splitlines(), TABLET_SIZES_COLUMNS)})
    topo.report_missing_tablet_sizes()

    by_host = capsys.readouterr().err.splitlines()[1]
    assert by_host.count("(1)") == 3 and by_host.endswith("+2")


def test_a_node_which_no_longer_owns_tokens_is_not_reported_for_holding_no_sizes(capsys) -> None:
    """
    A node which has left reports no size for what it still holds, same as an excluded one.
    """
    topology_dump = (f"version;host_id;shard_count;node_state;num_tokens\n"
                     f"6538;{HOST1};4;left;256\n")
    sizes_dump = (f"table_id;last_token;replicas;missing_replicas\n"
                  f"{TABLE};0;{{}};{{{HOST1}}}\n")

    topo = Topology()
    topo._build({
        TOPOLOGY_TABLE.name: rows_from_csv(topology_dump.splitlines(), TOPOLOGY_COLUMNS),
        TABLET_SIZES_TABLE.name: rows_from_csv(sizes_dump.splitlines(), TABLET_SIZES_COLUMNS),
    })
    topo.report_missing_tablet_sizes()

    assert capsys.readouterr().err == ""


def test_the_report_of_missing_sizes_says_it_of_what_the_report_shows(capsys) -> None:
    """
    A replica of a table nobody asked about is not a hole in the numbers on screen, so the
    report passes the filters it is drawn under.
    """
    other_table = UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
    load_dump = (f"node;dc;rack;ip;storage_capacity\n"
                 f"{HOST1};dc1;rack1;10.0.0.1;1000\n"
                 f"{HOST2};dc1;rack2;10.0.0.2;2000\n")
    sizes_dump = (f"table_id;last_token;replicas;missing_replicas\n"
                  f"{TABLE};0;{{}};{{{HOST1}}}\n"
                  f"{other_table};0;{{}};{{{HOST2}}}\n")

    topo = Topology()
    topo._build({
        TABLETS_TABLE.name: rows_from_csv(TABLETS_CSV.splitlines(), TABLETS_COLUMNS),
        LOAD_PER_NODE_TABLE.name: rows_from_csv(load_dump.splitlines(), LOAD_PER_NODE_COLUMNS),
        TABLET_SIZES_TABLE.name: rows_from_csv(sizes_dump.splitlines(), TABLET_SIZES_COLUMNS),
    })

    topo.report_missing_tablet_sizes(accepts_table=lambda table: table == TABLE)
    err = capsys.readouterr().err
    assert "Note: 1 replicas" in err and "10.0.0.1 (1)" in err

    topo.report_missing_tablet_sizes(accepts_host=lambda host: host.ip == "10.0.0.2")
    err = capsys.readouterr().err
    assert "Note: 1 replicas" in err and "10.0.0.2 (1)" in err

    # Nothing the report shows is missing a size, so it has nothing to say.
    topo.report_missing_tablet_sizes(accepts_table=lambda table: False)
    assert capsys.readouterr().err == ""


def test_build_infers_shard_counts_from_replicas_when_system_topology_is_absent() -> None:
    topo = Topology()
    topo._build({TABLETS_TABLE.name: rows_from_csv(TABLETS_CSV.splitlines(), TABLETS_COLUMNS)})

    # The highest shard a replica landed on is a lower bound on the host's shard count.
    # HOST2 only reaches shard 1 through the tablet migrating onto it.
    assert topo.require_host(HOST1).shard_count == 4
    assert topo.require_host(HOST2).shard_count == 2
    # Hosts are known from tablet replicas alone, but nothing about their capacity is.
    assert topo.require_host(HOST1).storage_capacity is None
    # Without system.topology membership is unknown, and a host is then assumed to be a
    # member, so that a partial snapshot still reports load.
    assert topo.require_host(HOST1).is_normal_token_owner()


def test_build_still_infers_shard_counts_for_a_host_system_topology_missed() -> None:
    """
    Watching replicas to infer shard counts is skipped when system.topology accounted for
    every host, so a host it did not account for has to keep it on.
    """
    topology_dump = f"key;host_id;shard_count;node_state;num_tokens;version\nlocal;{HOST1};4;normal;256;7\n"
    load_dump = f"node;dc;rack;ip;storage_capacity\n{HOST2};dc1;rack2;10.0.0.2;2000\n"

    topo = Topology()
    topo._build({
        TOPOLOGY_TABLE.name: rows_from_csv(topology_dump.splitlines(), TOPOLOGY_COLUMNS),
        LOAD_PER_NODE_TABLE.name: rows_from_csv(load_dump.splitlines(), LOAD_PER_NODE_COLUMNS),
        TABLETS_TABLE.name: rows_from_csv(TABLETS_CSV.splitlines(), TABLETS_COLUMNS),
    })

    # HOST1 keeps what system.topology said, HOST2 gets the lower bound its replicas imply.
    assert topo.require_host(HOST1).shard_count == 4
    assert topo.require_host(HOST2).shard_count == 2


def test_build_keeps_host_location_when_its_capacity_is_unknown() -> None:
    """
    A node which has not reported a capacity yet is still placed in its dc and rack, so it
    is not silently left out of rack level reporting.
    """
    dump = f"node;dc;rack;ip;storage_capacity;effective_capacity\n{HOST1};dc1;rack1;10.0.0.1;;\n"

    topo = Topology()
    topo._build({LOAD_PER_NODE_TABLE.name: rows_from_csv(dump.splitlines(), LOAD_PER_NODE_COLUMNS)})

    host = topo.require_host(HOST1)
    assert (host.dc, host.rack, host.ip) == ("dc1", "rack1", "10.0.0.1")
    assert (host.storage_capacity, host.effective_capacity) == (None, None)


def test_build_places_a_node_which_system_load_per_node_does_not_know() -> None:
    """
    A node which has left the cluster keeps its location in system.topology while dropping
    out of system.load_per_node, so a report can still say where it was.
    """
    topology_dump = (f"version;host_id;shard_count;node_state;num_tokens;datacenter;rack\n"
                     f"6538;{HOST1};4;left;256;dc1;rack1\n")
    load_dump = f"node;dc;rack;ip;storage_capacity\n{HOST2};dc1;rack2;10.0.0.2;2000\n"

    topo = Topology()
    topo._build({TOPOLOGY_TABLE.name: rows_from_csv(topology_dump.splitlines(), TOPOLOGY_COLUMNS),
                 LOAD_PER_NODE_TABLE.name: rows_from_csv(load_dump.splitlines(), LOAD_PER_NODE_COLUMNS)})

    host = topo.require_host(HOST1)
    assert (host.dc, host.rack, host.node_state) == ("dc1", "rack1", "left")
    # It has no address anywhere: system.topology carries no ip column.
    assert host.ip is None


def test_system_load_per_node_has_the_last_word_on_where_a_node_is() -> None:
    """
    Both tables carry the location; the one which only knows current members is the more
    current of the two, and is read second.
    """
    topology_dump = (f"version;host_id;shard_count;node_state;num_tokens;datacenter;rack\n"
                     f"6538;{HOST1};4;normal;256;dc1;old_rack\n")
    load_dump = f"node;dc;rack;ip;storage_capacity\n{HOST1};dc1;rack1;10.0.0.1;1000\n"

    topo = Topology()
    topo._build({TOPOLOGY_TABLE.name: rows_from_csv(topology_dump.splitlines(), TOPOLOGY_COLUMNS),
                 LOAD_PER_NODE_TABLE.name: rows_from_csv(load_dump.splitlines(), LOAD_PER_NODE_COLUMNS)})

    assert topo.require_host(HOST1).rack == "rack1"


def test_build_reads_whether_a_node_is_up_and_whether_it_is_excluded() -> None:
    """
    A node the cluster cannot reach reports no sizes and no capacity, which on its own reads
    as a node holding nothing. These say which it is.
    """
    dump = (f"node;dc;rack;ip;storage_capacity;up;excluded\n"
            f"{HOST1};dc1;rack1;10.0.0.1;1000;False;True\n"
            f"{HOST2};dc1;rack2;10.0.0.2;2000;True;False\n")

    topo = Topology()
    topo._build({LOAD_PER_NODE_TABLE.name: rows_from_csv(dump.splitlines(), LOAD_PER_NODE_COLUMNS)})

    assert (topo.require_host(HOST1).up, topo.require_host(HOST1).excluded) == (False, True)
    assert (topo.require_host(HOST2).up, topo.require_host(HOST2).excluded) == (True, False)


def test_a_snapshot_taken_before_the_status_columns_says_nothing_either_way() -> None:
    """
    Neither up nor down, so a report can tell "not reported" from "down".
    """
    topo = Topology()
    topo._build({LOAD_PER_NODE_TABLE.name: rows_from_csv(LOAD_PER_NODE_CSV.splitlines(),
                                                         LOAD_PER_NODE_COLUMNS)})

    assert (topo.require_host(HOST1).up, topo.require_host(HOST1).excluded) == (None, None)


def test_optional_columns_read_as_none_whichever_source_lacks_them() -> None:
    """
    A snapshot or a cluster which predates an optional column is read without it, rather
    than failing, and both end up with the same row.
    """
    dump = f"node;storage_capacity\n{HOST1};1000\n"

    csv_row, = rows_from_csv(dump.splitlines(), LOAD_PER_NODE_COLUMNS)
    cql_row, = rows_from_cql([SimpleNamespace(node=HOST1, storage_capacity=1000)], LOAD_PER_NODE_COLUMNS)

    assert csv_row == cql_row
    assert (csv_row.node, csv_row.storage_capacity) == (HOST1, 1000)
    assert (csv_row.effective_capacity, csv_row.dc, csv_row.rack, csv_row.ip) == (None, None, None, None)


@pytest.mark.parametrize("parse, text, expected", [
    pytest.param(parse_replica_list, f"[({HOST1}, 0), ({HOST2}, 13)]",
                 [(HOST1, 0), (HOST2, 13)], id="replicas-in-brackets"),
    pytest.param(parse_replica_list, f"{{({HOST1}, 0), ({HOST2}, 13)}}",
                 [(HOST1, 0), (HOST2, 13)], id="replicas-in-braces"),
    pytest.param(parse_replica_size_map, f"{{{HOST1}: 18006543, {HOST2}: 90}}",
                 {HOST1: 18006543, HOST2: 90}, id="sizes-by-host"),
    pytest.param(parse_replica_list, "[]", [], id="no-replicas"),
    pytest.param(parse_replica_size_map, "{}", {}, id="no-sizes"),
])
def test_replicas_are_parsed_from_either_spelling(parse, text: str, expected) -> None:
    """
    One regex reads every collection these columns come in: a list of pairs, "[(host, shard)]",
    a set of them, "{(host, shard)}", and a map of host to size, "{host: size}". It matches a
    pair at a time and never the brackets, so the collection kind cannot matter.

    An empty collection is a tablet whose sizes are not known.
    """
    assert parse(text) == expected


def test_a_malformed_host_id_is_rejected() -> None:
    """
    A host id of the right width but not a valid one is rejected rather than taken on trust.
    """
    with pytest.raises(ValueError, match="badly formed hexadecimal UUID string"):
        parse_replica_list("(-1111111-1111-1111-1111-111111111111, 0)")


def test_rows_from_csv_reads_empty_and_null_fields_as_none() -> None:
    dump = f"node;dc;rack;ip;storage_capacity;effective_capacity\n{HOST1};dc1;null;;1000;null\n"

    row, = rows_from_csv(dump.splitlines(), LOAD_PER_NODE_COLUMNS)

    # A dump spells a null either as an empty field or as "null", depending on what wrote it.
    assert (row.rack, row.ip, row.effective_capacity) == (None, None, None)
    assert (row.dc, row.storage_capacity) == ("dc1", 1000)


def test_a_snapshot_keeps_values_which_need_quoting(tmp_path) -> None:
    """
    A quoted CQL identifier can hold the delimiter, or a newline, and has to survive the dump.
    """
    snapshot_dir = tmp_path / "snap"
    snapshot_dir.mkdir()
    with open(snapshot_dir / "system_tablets.csv", "w", newline="") as dump:
        writer = csv.writer(dump, delimiter=CSV_DELIMITER)
        writer.writerow(["table_id", "last_token", "keyspace_name", "table_name",
                         "replicas", "new_replicas", "stage"])
        writer.writerow([TABLE, 0, "odd;name", "wrapped\nname", f"{{({HOST1}, 0)}}", "", "none"])

    topo = TopologyFromSnapshot(str(snapshot_dir)).get_topology()

    assert topo.get_table_name(TABLE) == "odd;name.wrapped\nname"
    assert topo.get_tablet_map(TABLE).tablets[0].replicas == [(HOST1, 0)]


def test_rows_from_csv_rejects_a_dump_missing_a_required_column() -> None:
    dump = f"table_id;last_token;keyspace_name;replicas;new_replicas;stage\n{TABLE};0;ks;{{}};;none\n"

    with pytest.raises(Exception, match="Required column 'table_name' is missing"):
        list(rows_from_csv(dump.splitlines(), TABLETS_COLUMNS))


def test_rows_from_csv_rejects_a_dump_which_is_not_in_the_expected_format() -> None:
    """
    Dumps have to be CSV, so cqlsh's default output, which separates columns with '|' and pads
    them, is reported as the wrong format rather than as a pile of missing columns.
    """
    dump = (" table_id | last_token | keyspace_name | table_name\n"
            "----------+------------+---------------+-----------\n"
            f" {TABLE} |          0 |            ks |       tbl\n")

    with pytest.raises(Exception, match="not CSV with a ';' delimiter"):
        list(rows_from_csv(dump.splitlines(), TABLETS_COLUMNS))


def test_build_of_nothing_yields_an_empty_topology() -> None:
    topo = Topology()
    topo._build({})

    assert topo.host_count() == 0
    assert list(topo.iter_table_ids()) == []
