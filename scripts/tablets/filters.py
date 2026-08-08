#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Shared filtering helpers for tablet-analysis scripts.

Usage:

- Pair `add_cluster_filter_options()` with `filter_tablet()` when a script
  iterates or aggregates tablet replicas.
- Pair `add_table_filter_options()` with `filter_table_id()` when a script
  iterates or aggregates tables.

The helpers are intended to be used together: the option helpers define the
CLI surface, while the filter helpers apply the corresponding selection logic.
"""

from __future__ import annotations

import argparse

from tablets.topology import parse_uuid
from tablets.topology import RackId
from tablets.topology import resolve_table_id
from tablets.topology import TabletReplica
from tablets.topology import TableId
from tablets.topology import Topology


def add_cluster_filter_options(parser: argparse.ArgumentParser) -> None:
    """
    Adds replica-level cluster filter options.

    Pair this with `filter_tablet()`.
    """
    parser.add_argument("--host", help="Restrict to replicas on the given host IP or host UUID")
    parser.add_argument("--shard", type=parse_shard_location_arg,
                        help="Restrict to replicas on the given shard, either as <host>:<shard> or as a bare shard number on any host")
    parser.add_argument("--rack", type=parse_rack_arg, help="Restrict to replicas in the given rack, in the form <DC>/<rack>")
    parser.add_argument("--dc", help="Restrict to replicas in the given datacenter")


def add_table_filter_options(parser: argparse.ArgumentParser) -> None:
    """
    Adds table-level filter options.

    Pair this with `filter_table_id()`.
    """
    parser.add_argument("--table", help="Table selector: exact ks.table, unique bare table name, or table UUID")
    parser.add_argument("--keyspace", help="Restrict to tables in the given keyspace")


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

    host_id = parse_uuid(host_arg)
    if topo.get_host(host_id) is None:
        raise Exception(f"Unknown host: {host_arg}")
    return host_id


def _get_resolved_host_filter(args, topo: Topology):
    if getattr(args, "host", None) is None:
        return None
    if not hasattr(args, "_resolved_host_filter"):
        args._resolved_host_filter = resolve_host_id(topo, args.host)
    return args._resolved_host_filter


def _get_resolved_shard_filter(args, topo: Topology):
    if getattr(args, "shard", None) is None:
        return None
    if not hasattr(args, "_resolved_shard_filter"):
        if isinstance(args.shard, int):
            args._resolved_shard_filter = args.shard
        else:
            host_arg, shard_id = args.shard
            args._resolved_shard_filter = (resolve_host_id(topo, host_arg), shard_id)
    return args._resolved_shard_filter


def resolve_table_filter_id(args, topo: Topology) -> TableId | None:
    table_arg = getattr(args, "table", None)
    if table_arg is None:
        return None
    if hasattr(args, "_resolved_table_filter"):
        return args._resolved_table_filter

    keyspace_arg = getattr(args, "keyspace", None)
    if "." not in table_arg and keyspace_arg is not None:
        table_id = resolve_table_id(topo, f"{keyspace_arg}.{table_arg}")
        args._resolved_table_filter = table_id
        return table_id

    table_id = resolve_table_id(topo, table_arg)
    if keyspace_arg is not None and topo.get_keyspace_name(table_id) != keyspace_arg:
        raise Exception(f"Table {table_arg} is not in keyspace {keyspace_arg}")
    args._resolved_table_filter = table_id
    return table_id


def filter_table_id(args, topo: Topology, table_id: TableId) -> bool:
    selected_table_id = resolve_table_filter_id(args, topo)
    if selected_table_id is not None and table_id != selected_table_id:
        return False

    keyspace_arg = getattr(args, "keyspace", None)
    if keyspace_arg is not None and topo.get_keyspace_name(table_id) != keyspace_arg:
        return False

    return True


def filter_tablet(args, topo: Topology, replica: TabletReplica) -> bool:
    host_id, shard_id = replica

    host_filter = _get_resolved_host_filter(args, topo)
    if host_filter is not None and host_id != host_filter:
        return False

    shard_filter = _get_resolved_shard_filter(args, topo)
    if shard_filter is not None:
        if isinstance(shard_filter, int):
            if shard_id != shard_filter:
                return False
        elif replica != shard_filter:
            return False

    rack_filter = getattr(args, "rack", None)
    if rack_filter is not None:
        host = topo.get_host(host_id)
        if host is None or (host.dc, host.rack) != rack_filter:
            return False

    dc_filter = getattr(args, "dc", None)
    if dc_filter is not None:
        host = topo.get_host(host_id)
        if host is None or host.dc != dc_filter:
            return False

    return True
