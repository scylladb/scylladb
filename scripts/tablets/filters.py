#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Shared filtering helpers for tablet-analysis scripts.

Usage:

- Pair `add_cluster_filter_options()` with `get_tablet_filter()` or `get_host_filter()`
  when a script iterates or aggregates tablet replicas.
- Pair `add_table_filter_options()` with `get_table_filter()` when a script iterates or
  aggregates tables.

A builder resolves what the arguments name against the topology once and returns a
predicate. Scripts pass the predicate down instead of the arguments, so nothing below
the entry point has to know which flags exist.

Every builder assumes its options were added to the parser, so the attributes are there;
an option left out on the command line is None and selects everything.
"""

from __future__ import annotations

import argparse
from typing import Callable

from tablets.topology import Host
from tablets.topology import parse_uuid
from tablets.topology import RackId
from tablets.topology import resolve_table_id
from tablets.topology import TabletReplica
from tablets.topology import TableId
from tablets.topology import Topology


HostFilter = Callable[[Host], bool]
TabletFilter = Callable[[TabletReplica], bool]
TableFilter = Callable[[TableId], bool]


def add_cluster_filter_options(parser: argparse.ArgumentParser) -> None:
    """
    Adds replica-level cluster filter options.

    Pair this with `get_tablet_filter()` or `get_host_filter()`.
    """
    parser.add_argument("--host", help="Restrict to replicas on the given host IP or host UUID")
    parser.add_argument("--shard", type=parse_shard_location_arg,
                        help="Restrict to replicas on the given shard, either as <host>:<shard> or as a bare shard number on any host")
    parser.add_argument("--rack", type=parse_rack_arg, help="Restrict to replicas in the given rack, in the form <DC>/<rack>")
    parser.add_argument("--dc", help="Restrict to replicas in the given datacenter")


def add_table_filter_options(parser: argparse.ArgumentParser) -> None:
    """
    Adds table-level filter options.

    Pair this with `get_table_filter()`.
    """
    parser.add_argument("--table", help="Table selector: exact ks.table, unique bare table name, or table UUID")
    parser.add_argument("--keyspace", help="Restrict to tables in the given keyspace")


def filter_args(**overrides) -> argparse.Namespace:
    """
    The arguments the filter options define, selecting everything except what the caller
    narrows.

    The names are exactly what add_cluster_filter_options() and add_table_filter_options()
    add, and what every builder below assumes is present.
    """
    return argparse.Namespace(**{"host": None, "shard": None, "rack": None, "dc": None,
                                 "table": None, "keyspace": None, **overrides})


def parse_shard_location_arg(value: str) -> int | tuple[str, int]:
    if ":" not in value:
        try:
            return int(value)
        except ValueError as exc:
            raise argparse.ArgumentTypeError("Shard must be an integer or have the form <host>:<shard>") from exc

    host_arg, sep, shard_arg = value.rpartition(":")
    if not sep or not host_arg or not shard_arg:
        raise argparse.ArgumentTypeError("Shard location must have the form <host>:<shard>")
    try:
        shard_id = int(shard_arg)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Shard number must be an integer") from exc
    return host_arg, shard_id


def parse_rack_arg(value: str) -> RackId:
    dc, sep, rack = value.partition("/")
    if not sep or not dc or not rack:
        raise argparse.ArgumentTypeError("Rack must have the form <DC>/<rack>")
    return dc, rack


def resolve_host_id(topo: Topology, host_arg: str):
    matching_hosts = [host.id for host in topo.all_hosts() if host.ip == host_arg]
    if len(matching_hosts) > 1:
        raise Exception(f"Ambiguous host ip: {host_arg}")
    if matching_hosts:
        return matching_hosts[0]

    try:
        host_id = parse_uuid(host_arg)
    except ValueError as exc:
        raise Exception(f"Unknown host: {host_arg}") from exc
    if topo.get_host(host_id) is None:
        raise Exception(f"Unknown host: {host_arg}")
    return host_id


def _resolve_shard(args, topo: Topology) -> int | TabletReplica | None:
    """
    Resolves --shard to a bare shard number, or to the replica it names.
    """
    if args.shard is None:
        return None
    if isinstance(args.shard, int):
        return args.shard
    host_arg, shard_id = args.shard
    return resolve_host_id(topo, host_arg), shard_id


def select_all(_) -> bool:
    """
    A filter selecting everything, for a caller with nothing to narrow by.
    """
    return True


def get_host_filter(args, topo: Topology) -> HostFilter:
    """
    Builds the predicate for the location filters which select a whole host.

    Only --host, --shard, --rack and --dc can reject a host; --table and --keyspace select
    tables, so they never do. Lets a caller keep a host holding no matching replica, which
    the tablet filter alone cannot decide.
    """
    host_id = resolve_host_id(topo, args.host) if args.host is not None else None
    shard = _resolve_shard(args, topo)
    # A bare --shard number selects that shard on every host, so it excludes no host.
    shard_host_id = shard[0] if isinstance(shard, tuple) else None
    rack, dc = args.rack, args.dc

    def accepts(host: Host) -> bool:
        if host_id is not None and host.id != host_id:
            return False
        if shard_host_id is not None and host.id != shard_host_id:
            return False
        if rack is not None and (host.dc, host.rack) != rack:
            return False
        if dc is not None and host.dc != dc:
            return False
        return True

    return accepts


def get_tablet_filter(args, topo: Topology) -> TabletFilter:
    """
    Builds the predicate selecting tablet replicas.

    A replica is selected when --shard admits it and its host passes the host filter.
    """
    accepts_host = get_host_filter(args, topo)
    shard = _resolve_shard(args, topo)
    host_id = resolve_host_id(topo, args.host) if args.host is not None else None
    has_location_filter = args.rack is not None or args.dc is not None

    def accepts(replica: TabletReplica) -> bool:
        host_id_of, shard_id = replica

        if shard is not None:
            if isinstance(shard, int):
                if shard_id != shard:
                    return False
            elif replica != shard:
                return False

        host = topo.get_host(host_id_of)
        if host is not None:
            return accepts_host(host)

        # An unknown host has no location, so only the filters not needing one can judge it.
        if has_location_filter:
            return False
        return host_id is None or host_id_of == host_id

    return accepts


def resolve_table_filter_id(args, topo: Topology) -> TableId | None:
    """
    Resolves --table to the one table it names, or None when it was not given.
    """
    if args.table is None:
        return None

    if "." not in args.table and args.keyspace is not None:
        return resolve_table_id(topo, f"{args.keyspace}.{args.table}")

    table_id = resolve_table_id(topo, args.table)
    if args.keyspace is not None and topo.get_keyspace_name(table_id) != args.keyspace:
        raise Exception(f"Table {args.table} is not in keyspace {args.keyspace}")
    return table_id


def get_table_filter(args, topo: Topology) -> TableFilter:
    """
    Builds the predicate selecting tables by --table and --keyspace.
    """
    selected_table_id = resolve_table_filter_id(args, topo)
    keyspace = args.keyspace

    def accepts(table_id: TableId) -> bool:
        if selected_table_id is not None and table_id != selected_table_id:
            return False
        if keyspace is not None and topo.get_keyspace_name(table_id) != keyspace:
            return False
        return True

    return accepts
