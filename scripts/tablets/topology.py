#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Provides a model for tablet metadata, topology and related statistics.
Primarily intended to be used for analyzing and visualizing information relevant for load balancing.

Provides a framework for obtaining topology information from either a live cluster or a snapshot.

To obtain a snapshot from a live cluster, use snapshot.py:

  $ ./snapshot.py --cluster <host>
  ./tablet_snap_260714_123456/

All scripts can then use the snapshot directory as a topology source, e.g.:

  $ ./scylla-tablets.py cluster --snapshot ./tablet_snap_260714_123456/

Scripts integrate with topology source abstraction by:

 1) Adding topology source arguments to the script's argument parser:

    add_topology_source_args(parser)

 2) Creating a topology source from the parsed arguments:

    topo_src = get_topology_source_from_args(args)

 3) Using the topology source to obtain a snapshot of topology:

    topo = topo_src.get_topology()

"""

from __future__ import annotations

import argparse
import csv
import io
import os
import re
import tarfile
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import AbstractContextManager, ExitStack, nullcontext
from dataclasses import dataclass, field
from functools import partial
from types import SimpleNamespace
from typing import Callable, Dict, Iterable, Iterator, List, NewType, Optional, Sequence, Tuple

UUID = uuid.UUID
TableId = NewType("TableId", UUID)
TableName = NewType("TableName", str)
KeyspaceName = NewType("KeyspaceName", str)
DatacenterName = NewType("DatacenterName", str)
RackName = NewType("RackName", str)
RackId = Tuple[DatacenterName, RackName] # Unique within cluster
Token = NewType("Token", int)
HostId = NewType("HostId", UUID)
ShardNumber = NewType("ShardNumber", int)
ShardId = Tuple[HostId, ShardNumber] # Unique within cluster
TabletReplica = ShardId
ReplicaList = List[TabletReplica]


@dataclass
class Tablet:
    last_token : Token
    replicas : ReplicaList
    new_replicas : Optional[ReplicaList] = None
    stage : Optional[str] = None


@dataclass
class TabletMap:
    table : TableId
    base_table : Optional[TableId] = None
    # Ordered by last_token. Colocated tables share the base table's list.
    tablets : list[Tablet] = field(default_factory=list)


@dataclass
class Host:
    id : HostId
    shard_count : int
    storage_capacity: Optional[int] = None
    effective_capacity: Optional[int] = None
    dc: Optional[DatacenterName] = None
    rack: Optional[RackName] = None
    ip: Optional[str] = None
    node_state: Optional[str] = None # system.topology#node_state
    num_tokens: Optional[int] = None # system.topology#num_tokens

    def is_normal_token_owner(self) -> bool:
        if self.node_state is None or self.num_tokens is None:
            # Snapshot is missing system.topology, assume a member
            return True
        if self.node_state != "normal" or self.num_tokens == 0:
            return False
        return True


@dataclass
class Anonymizer:
    keyspaces: Dict[KeyspaceName, KeyspaceName] = field(default_factory=dict)
    tables: Dict[TableId, Tuple[KeyspaceName, TableName]] = field(default_factory=dict)
    next_keyspace_idx: int = 1
    next_table_idx_by_keyspace: Dict[KeyspaceName, int] = field(default_factory=lambda: defaultdict(lambda: 1))

    def get_table_name(self, table_id: TableId, keyspace_name: KeyspaceName) -> str:
        """
        Returns a deterministic anonymized name for a table.

        New tables extend the mapping while existing aliases remain stable.
        """
        if table_id not in self.tables:
            if keyspace_name not in self.keyspaces:
                self.keyspaces[keyspace_name] = f"ks{self.next_keyspace_idx}"
                self.next_keyspace_idx += 1
            anonymized_keyspace = self.keyspaces[keyspace_name]
            table_idx = self.next_table_idx_by_keyspace[anonymized_keyspace]
            self.tables[table_id] = (anonymized_keyspace, f"table{table_idx}")
            self.next_table_idx_by_keyspace[anonymized_keyspace] += 1
        anonymized_keyspace, anonymized_table = self.tables[table_id]
        return f"{anonymized_keyspace}.{anonymized_table}"

    def get_keyspace_name(self, table_id: TableId, keyspace_name: KeyspaceName) -> KeyspaceName:
        """
        Returns the anonymized keyspace name for a table, registering it if needed.
        """
        self.get_table_name(table_id, keyspace_name)
        return self.tables[table_id][0]


# A dump names the same few ids once per replica, and uuid.UUID() is the priciest part of
# loading. Bounded by the cluster's hosts and tables.
_PARSED_UUIDS: Dict[str, UUID] = {}


def _parse_id(text: str) -> UUID:
    """
    Parses an id known to be text, caching it.
    """
    parsed = _PARSED_UUIDS.get(text)
    if parsed is None:
        parsed = _PARSED_UUIDS[text] = uuid.UUID(text)
    return parsed


TOKEN_RING_SIZE = 1 << 64


def iter_token_fractions(tablets: Sequence[Tablet]) -> Iterator[Tuple[Tablet, float]]:
    """
    Pairs each tablet with the fraction of the token ring it owns.

    A tablet spans from the previous tablet's last token to its own, the first wrapping
    around from the last. A lone tablet owns the whole ring: it wraps to itself, which
    the span arithmetic would otherwise read as owning nothing.
    """
    if not tablets:
        return
    if len(tablets) == 1:
        yield tablets[0], 1.0
        return

    prev_last_token = tablets[-1].last_token
    for tablet in tablets:
        yield tablet, ((tablet.last_token - prev_last_token) % TOKEN_RING_SIZE) / TOKEN_RING_SIZE
        prev_last_token = tablet.last_token


def parse_uuid(value: str | uuid.UUID | None) -> UUID | None:
    if value is None or isinstance(value, uuid.UUID):
        return value
    return _parse_id(value)


# Both replica columns are (host id, number) pairs, differing only in how a pair is spelled:
#
#   system.tablets#replicas, list<frozen<tuple<uuid, int>>>, number is a shard:
#     [(35d5e1a4-a3e8-4d33-84c9-2b6f1a0c9e77, 2), (7c4b8f10-1d94-4a52-9d0e-3f8a5c2b7e61, 0)]
#   system.tablet_sizes#replicas, map<uuid, bigint>, number is a size in bytes:
#     {35d5e1a4-a3e8-4d33-84c9-2b6f1a0c9e77: 18006543, 7c4b8f10-1d94-4a52-9d0e-3f8a5c2b7e61: 90}
#
# One pattern for both. Brackets are not matched, so cqlsh COPY's [] and snapshot.py's {} both
# read. Matching the id by width beat every other form tried by ~25%.
_REPLICA_PAIR_RE = re.compile(r'([0-9a-fA-F-]{36})[,:]\s*(\d+)')


def parse_replica_list(value: str) -> ReplicaList:
    """
    Parses tablet replicas, e.g.
    "[(35d5e1a4-a3e8-4d33-84c9-2b6f1a0c9e77, 2)]" to [(UUID('35d5e1a4-...'), 2)].
    """
    return [(_parse_id(host), int(number)) for host, number in _REPLICA_PAIR_RE.findall(value)]


def parse_replica_size_map(value: str) -> Dict[HostId, int]:
    """
    Parses per-replica tablet sizes in bytes, e.g.
    "{35d5e1a4-a3e8-4d33-84c9-2b6f1a0c9e77: 18006543}" to {UUID('35d5e1a4-...'): 18006543}.
    """
    return {_parse_id(host): int(number) for host, number in _REPLICA_PAIR_RE.findall(value)}


def format_replica_list(replicas: ReplicaList) -> str:
    """
    Renders tablet replicas back to the form parse_replica_list() reads, e.g.
    [(UUID('35d5e1a4-...'), 2)] to "{(35d5e1a4-a3e8-4d33-84c9-2b6f1a0c9e77, 2)}".
    """
    return "{" + ", ".join(f"({host}, {shard})" for host, shard in replicas) + "}"


def format_replica_size_map(sizes: Dict[HostId, int]) -> str:
    """
    Renders per-replica tablet sizes back to the form parse_replica_size_map() reads, e.g.
    {UUID('35d5e1a4-...'): 18006543} to "{35d5e1a4-a3e8-4d33-84c9-2b6f1a0c9e77: 18006543}".
    """
    return "{" + ", ".join(f"{host}: {size}" for host, size in sizes.items()) + "}"


@dataclass(frozen=True)
class Column:
    """
    A column of a system table which Topology is built from.
    """

    name: str
    # Text form to the value type the CQL driver yields. Never called with a null value.
    parse: Callable[[str], object]
    # Inverse of parse(), for snapshot.py to dump. Never called with a null value.
    format: Callable[[object], str] = str
    # A missing required column fails the build; a missing optional one reads as None.
    required: bool = True


TABLETS_COLUMNS = (
    Column("table_id", parse_uuid),
    Column("keyspace_name", str),
    Column("table_name", str),
    Column("last_token", int),
    Column("replicas", parse_replica_list, format_replica_list),
    Column("new_replicas", parse_replica_list, format_replica_list),
    Column("stage", str),
    Column("base_table", parse_uuid, required=False),
)

TOPOLOGY_COLUMNS = (
    Column("version", int),
    Column("host_id", parse_uuid),
    Column("shard_count", int),
    Column("node_state", str, required=False),
    Column("num_tokens", int, required=False),
    # Where the node sits. system.load_per_node says the same of the nodes it lists, and
    # drops a node which has left, so this is what still places one.
    Column("datacenter", str, required=False),
    Column("rack", str, required=False),
)

LOAD_PER_NODE_COLUMNS = (
    Column("node", parse_uuid),
    Column("storage_capacity", int),
    Column("effective_capacity", int, required=False),
    Column("dc", str, required=False),
    Column("rack", str, required=False),
    Column("ip", str, required=False),
)

TABLET_SIZES_COLUMNS = (
    Column("table_id", parse_uuid),
    Column("last_token", int),
    Column("replicas", parse_replica_size_map, format_replica_size_map),
)


@dataclass(frozen=True)
class SnapshotTable:
    """
    A system table which Topology is derived from, and which a snapshot therefore holds.
    """

    # The CQL table to read, e.g. "system.tablets".
    name: str
    # The file holding its dump inside a snapshot, e.g. "system_tablets.csv".
    file: str
    # Columns read from it. A dump holds all of them, being a "SELECT *".
    columns: Sequence[Column]


TOPOLOGY_TABLE = SnapshotTable("system.topology", "system_topology.csv", TOPOLOGY_COLUMNS)
TABLETS_TABLE = SnapshotTable("system.tablets", "system_tablets.csv", TABLETS_COLUMNS)
LOAD_PER_NODE_TABLE = SnapshotTable("system.load_per_node", "system_load_per_node.csv", LOAD_PER_NODE_COLUMNS)
TABLET_SIZES_TABLE = SnapshotTable("system.tablet_sizes", "system_tablet_sizes.csv", TABLET_SIZES_COLUMNS)

# What snapshot.py captures and a snapshot is read for. Adding one here covers both ends; only
# Topology._build() needs teaching what its rows mean. Order is the dump order.
SNAPSHOT_TABLES = (TOPOLOGY_TABLE, TABLETS_TABLE, LOAD_PER_NODE_TABLE, TABLET_SIZES_TABLE)


# A row with column values as attributes, as the CQL driver returns them. Columns the source
# does not have read as None.
Row = SimpleNamespace

# Dumps are CSV with a header, as snapshot.py and "cqlsh COPY ... WITH DELIMITER=';' AND
# HEADER=TRUE" write them. Not a comma: collection values are full of those.
CSV_DELIMITER = ";"

# How a dump may spell a null.
CSV_NULLS = ("", "null")


def _csv_column_indexes(header: Sequence[str], columns: Sequence[Column]) -> List[Tuple[Optional[int], Column]]:
    """
    Locates the columns in a dump's header by name, None for the ones it does not have.

    Raises:
        Exception: a required column is missing, or the header names none of them.
    """
    index_by_name = {name: idx for idx, name in enumerate(header)}
    if not any(column.name in index_by_name for column in columns):
        raise Exception(f"Dump is not CSV with a '{CSV_DELIMITER}' delimiter and a header row,"
                        f" its first line names none of the expected columns: {header[0][:60]!r}")

    indexes = []
    for column in columns:
        idx = index_by_name.get(column.name)
        if idx is None and column.required:
            raise Exception(f"Required column '{column.name}' is missing")
        indexes.append((idx, column))
    return indexes


def rows_from_csv(lines: Iterable[str], columns: Sequence[Column]) -> Iterator[Row]:
    """
    Converts a dump of a system table into rows in CQL shape, lazily, a row at a time.

    Values are parsed into the types the driver would have yielded, columns located by name,
    quoting the csv module's, matching snapshot.py's writer.

    Raises:
        Exception: see _csv_column_indexes().
    """
    reader = csv.reader(lines, delimiter=CSV_DELIMITER)
    # A blank line, e.g. a trailing one, holds no fields at all and is not a row.
    header = next((row for row in reader if row), None)
    if header is None:
        return
    fields = _csv_column_indexes(header, columns)

    for row in reader:
        if not row:
            continue
        values = {}
        for idx, column in fields:
            if idx is not None and idx >= len(row):
                raise Exception(
                    f"Truncated row in dump: column '{column.name}' is at index {idx} "
                    f"but the row has {len(row)} fields: {row}")
            # A column the dump lacks reads as null; a row too short is corruption.
            value = row[idx] if idx is not None else ""
            values[column.name] = None if value in CSV_NULLS else column.parse(value)
        yield Row(**values)


def rows_from_cql(rows: Iterable, columns: Sequence[Column]) -> Iterator[Row]:
    """
    Adapts driver rows to the row shape Topology is built from, filling in columns the cluster
    does not have, as a snapshot's missing ones are filled in.
    """
    for row in rows:
        yield Row(**{column.name: getattr(row, column.name, None) for column in columns})


@dataclass
class Topology:
    """
    Snapshot of tablet topology and related metadata.
    """

    # topology version obtained from system.topology#version.
    # Note: size stats may change without "version" changing.
    _version: int | None = None
    _hosts: Dict[HostId, Host] = field(default_factory=dict)
    _tablet_maps: Dict[TableId, TabletMap] = field(default_factory=dict)
    _tables: Dict[TableId, Tuple[KeyspaceName, TableName]] = field(default_factory=dict)
    # Per-replica tablet sizes in bytes, keyed by (table_id, last_token) -> {host_id: size}.
    _tablet_sizes: Dict[Tuple[TableId, Token], Dict[HostId, int]] = field(default_factory=dict)
    # Lower bound applied to every reported tablet size, in bytes.
    _min_tablet_size: int = 0
    _anonymizer: Optional[Anonymizer] = None

    def has_table(self, table: TableId) -> bool:
        """
        Returns whether a table id is present in the topology.
        """
        return table in self._tables

    def host_count(self) -> int:
        """
        Returns the number of known hosts.
        """
        return len(self._hosts)

    def all_host_ids(self):
        """
        Returns an iterable over HostIds of all known hosts.
        """
        return self._hosts.keys()

    def all_hosts(self):
        """
        Returns an iterable over Host objects representing all known hosts.
        """
        return self._hosts.values()

    def get_host(self, host_id: HostId) -> Host | None:
        """
        Returns a host by id, or None when it is not known.
        """
        return self._hosts.get(host_id)

    def require_host(self, host_id: HostId) -> Host:
        """
        Returns a host by id, raising if it is not known.
        """
        host = self.get_host(host_id)
        if host is None:
            raise Exception(f"Unknown host: {host_id}")
        return host

    def all_normal_token_owner_hosts(self):
        """
        Returns normal topology members that own tokens.
        This is the set of nodes which is supposed to be balanced.
        Other nodes should be ignored when calculating imbalance.
        """
        return (host for host in self._hosts.values() if host.is_normal_token_owner())

    def iter_table_ids(self, include_colocated: bool = True) -> Iterator[TableId]:
        """
        Returns known table ids.

        When include_colocated is false, only base tables are returned.
        """
        for table_id, tablet_map in self._tablet_maps.items():
            if include_colocated or not tablet_map.base_table:
                yield table_id

    def iter_tables(self, include_colocated: bool = True) -> Iterator[tuple[TableId, str]]:
        """
        Returns known tables as ``(table_id, "keyspace.table")`` pairs.

        When include_colocated is false, only base tables are returned.
        """
        for table_id in self.iter_table_ids(include_colocated=include_colocated):
            yield table_id, self.get_table_name(table_id)

    def all_tablets(self) -> Iterator[tuple[TableId, Tablet]]:
        """
        Returns all physical tablets as (base_table_id, tablet) pairs.
        """
        for table_id in self.iter_table_ids(include_colocated=False):
            tablet_map = self._tablet_maps[table_id]
            for tablet in tablet_map.tablets:
                yield table_id, tablet

    def get_table_name(self, table: TableId) -> str:
        ks_name, table_name = self._tables[table]
        if self._anonymizer is not None:
            return self._anonymizer.get_table_name(table, ks_name)
        return f"{ks_name}.{table_name}"

    def get_keyspace_name(self, table: TableId) -> str:
        """
        Returns the keyspace name for a table id.
        """
        ks_name, _ = self._tables[table]
        if self._anonymizer is not None:
            return self._anonymizer.get_keyspace_name(table, ks_name)
        return ks_name

    def set_anonymizer(self, anonymizer: Optional[Anonymizer]) -> None:
        """
        Configures name anonymization for this topology snapshot.
        """
        self._anonymizer = anonymizer

    def get_base_table_id(self, table: TableId) -> TableId:
        """
        Returns the base table id for a table, or the table itself if it is not colocated.
        """
        tablet_map = self._tablet_maps[table]
        return tablet_map.base_table or table

    def get_tablet_map(self, table: TableId) -> TabletMap:
        """
        Returns the tablet map for a table.

        For colocated tables, the returned map is the table's own TabletMap,
        but its tablets list is shared with the base table.
        """
        return self._tablet_maps[table]

    def _link_colocated_tablets(self) -> None:
        """
        Makes colocated table maps share the base table's tablet list.
        """
        for table_id, tablet_map in self._tablet_maps.items():
            if not tablet_map.base_table:
                continue
            if tablet_map.base_table not in self._tablet_maps:
                raise Exception(f"Base table {tablet_map.base_table} for colocated table {table_id} is missing")
            tablet_map.tablets = self._tablet_maps[tablet_map.base_table].tablets

    def _build(self, rows_by_table: Dict[str, Iterable[Row]]) -> None:
        """
        Builds this topology from rows of the system tables, keyed by table name, as
        rows_from_cql() and rows_from_csv() yield them. A table left out contributes nothing.
        Each is consumed in a single forward pass, so a source may stream it.
        """
        # Nodes first, so the tablets pass has nothing left to work out about them.
        self._build_topology(rows_by_table.get(TOPOLOGY_TABLE.name, ()))
        self._build_load_per_node(rows_by_table.get(LOAD_PER_NODE_TABLE.name, ()))
        self._build_tablets(rows_by_table.get(TABLETS_TABLE.name, ()))
        self._build_tablet_sizes(rows_by_table.get(TABLET_SIZES_TABLE.name, ()))
        self._link_colocated_tablets()

    def _get_or_add_host(self, host_id: HostId, shard_count: Optional[int] = None) -> Host:
        """
        Returns the host with the given id, registering it if it is not known yet.
        """
        host = self._hosts.get(host_id)
        if host is None:
            host = Host(host_id, shard_count)
            self._hosts[host_id] = host
        return host

    def _build_tablets(self, rows: Iterable[Row]) -> None:
        """
        Populates tables and their tablet maps from system.tablets.

        A host system.topology missed is registered from its replicas, shard count inferred as
        the highest shard seen plus one. That costs a lookup per replica, so it is skipped when
        every known host already has one.
        """
        infer_hosts_from_replicas = not self._hosts or any(host.shard_count is None
                                                           for host in self._hosts.values())
        max_shard_per_host: Dict[HostId, int] = {}

        for row in rows:
            table_id = row.table_id
            if table_id not in self._tablet_maps:
                self._tables[table_id] = (row.keyspace_name, row.table_name)
                self._tablet_maps[table_id] = TabletMap(table_id, base_table=row.base_table)

            if row.base_table:
                # It shares its base table's tablets. See _link_colocated_tablets().
                continue

            replicas = row.replicas or []
            # An empty new_replicas means no migration is in progress, same as a missing one.
            new_replicas = row.new_replicas or None
            if infer_hosts_from_replicas:
                for replica_list in (replicas, new_replicas or ()):
                    for host_id, shard in replica_list:
                        # Writes are a handful per cluster, lookups are per replica.
                        if shard > max_shard_per_host.get(host_id, -1):
                            max_shard_per_host[host_id] = shard

            self._tablet_maps[table_id].tablets.append(
                Tablet(last_token=row.last_token,
                       replicas=replicas,
                       new_replicas=new_replicas,
                       stage=row.stage))

        for host_id, max_shard in max_shard_per_host.items():
            host = self._get_or_add_host(host_id)
            if host.shard_count is None:
                host.shard_count = max_shard + 1

    def _build_load_per_node(self, rows: Iterable[Row]) -> None:
        """
        Augments hosts with storage capacity and location from system.load_per_node.
        """
        for row in rows:
            host = self._get_or_add_host(row.node)
            if row.dc is not None:
                host.dc = row.dc
            if row.rack is not None:
                host.rack = row.rack
            if row.ip is not None:
                host.ip = str(row.ip)  # an inet is an ipaddress object over CQL
            if row.storage_capacity is not None:
                host.storage_capacity = row.storage_capacity
            if row.effective_capacity is not None:
                host.effective_capacity = row.effective_capacity

    def _build_topology(self, rows: Iterable[Row]) -> None:
        """
        Augments hosts with authoritative shard counts, membership state and location from
        system.topology, and records the topology version.

        Read before system.load_per_node, which repeats the location of every node it lists.
        The two agree; this one also covers the nodes that table has dropped.
        """
        for row in rows:
            if row.version is not None:
                self._version = row.version
            host = self._get_or_add_host(row.host_id)
            if row.shard_count is not None:
                host.shard_count = row.shard_count
            host.node_state = row.node_state
            host.num_tokens = row.num_tokens
            if row.datacenter is not None:
                host.dc = row.datacenter
            if row.rack is not None:
                host.rack = row.rack

    def _build_tablet_sizes(self, rows: Iterable[Row]) -> None:
        """
        Populates per-replica tablet sizes from system.tablet_sizes.
        """
        for row in rows:
            # dict() because the driver's own map type would not compare equal to a dump's.
            self._tablet_sizes[(row.table_id, row.last_token)] = dict(row.replicas or {})

    def get_tablet_size(self, table: TableId, tablet: Tablet, replica: TabletReplica) -> int:
        """
        Returns the size in bytes of a given tablet replica.
        Raises an exception if the size is not available (e.g. when tablet sizes were not loaded).
        """
        tablet_id = (table, tablet.last_token)
        sizes = self._tablet_sizes.get(tablet_id)
        if sizes is None:
            raise Exception(f"Tablet size not available for tablet {tablet_id}. ")
        host_id = replica[0]
        if host_id not in sizes:
            raise Exception(f"Tablet size not available for tablet {tablet_id} on host {host_id}")
        return max(sizes[host_id], self._min_tablet_size)

    def get_host_effective_capacity(self, host: Host) -> int:
        """
        Returns the effective storage capacity in bytes of a given host.

        Does not fall back to storage_capacity, which would present absolute
        capacity as if it were effective.
        """
        if host.effective_capacity is None:
            raise Exception(f"Effective capacity of host {host.id} is missing.")
        return host.effective_capacity


class TopologySource(ABC):
    # Setting this will affect reported size stats for tablets, making this a lower-bound on reported size.
    # Load balancer treats tablets smaller than 50 MiB as having 50 MiB size for balancing purposes,
    # so when displaying balance reporting, it may make sense to treat the size the same way as load balancer
    # does.
    min_tablet_size: Optional[int] = None
    anonymizer: Optional[Anonymizer] = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @abstractmethod
    def is_live(self) -> bool:
        """
        is_live() == true indicates that topology in this source can change in real time and subsequent calls
        to get_topology() may return different results.
        The object returned by get_topology() is not modified by this source, and consumers should not modify it either.
        It may be reused by the source for subsequent calls to get_topology().

        is_live() == false indicates that topology is static and subsequent calls to get_topology()
        will return the same result.
        """
        pass

    @abstractmethod
    def get_topology(self) -> Topology:
        """
        Returns a snapshot of Topology.
        The returned Topology object is not updated by the source, even if the topology source is live.
        """
        pass

    def close(self) -> None:
        """
        Releases any underlying resources owned by this topology source.
        """
        pass

    def _with_shared_options(self, topo: Topology) -> Topology:
        """
        Applies the options which do not depend on where the topology came from.
        """
        topo._min_tablet_size = self.min_tablet_size or 0
        topo.set_anonymizer(self.anonymizer)
        return topo


def resolve_table_id(topo: Topology, table_arg: str) -> TableId:
    """
    Resolves a user-provided table selector to a table id.

    The selector may be either:
    - an exact table id string, in which case UUID parsing errors propagate, or
    - an exact ``keyspace.table`` name.
    - a bare table name, when it is unique within the cluster.

    Raises:
        Exception: if the table does not exist or the name is ambiguous.
    """
    if "." not in table_arg:
        matches = [table_id for table_id, name in topo.iter_tables() if name.split(".", 1)[1] == table_arg]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise Exception(f"Ambiguous table name: {table_arg}")

        try:
            table_id = parse_uuid(table_arg)
        except ValueError as exc:
            raise Exception(f"Unknown table or table id: {table_arg}") from exc
        if not topo.has_table(table_id):
            raise Exception(f"Unknown table or table id: {table_arg}")
        return table_id

    matches = [table_id for table_id, name in topo.iter_tables() if name == table_arg]
    if not matches:
        raise Exception(f"Unknown table: {table_arg}")
    if len(matches) > 1:
        raise Exception(f"Ambiguous table name: {table_arg}")
    return matches[0]


# A snapshot file, opened by calling it, yielding a context manager over its lines.
LineSource = Callable[[], AbstractContextManager[Iterable[str]]]


def open_snapshot_files(snapshot_path: str) -> Dict[SnapshotTable, LineSource]:
    """
    Returns ``{table: line source}`` for the tables a snapshot holds, given its directory or a
    tar archive of one. Files are matched by base name, so the archive layout does not matter.

    A directory's files are read on the fly. Archive members are read up front: gzip has no
    member index, so getmembers() already decompressed everything.
    """
    sources: Dict[SnapshotTable, LineSource] = {}

    if os.path.isdir(snapshot_path):
        for table in SNAPSHOT_TABLES:
            path = os.path.join(snapshot_path, table.file)
            if os.path.exists(path):
                # newline="" leaves line endings to the csv module, so a newline inside a
                # quoted value does not end its row.
                sources[table] = partial(open, path, newline="")
        return sources

    if not os.path.exists(snapshot_path):
        raise Exception(f"Snapshot does not exist: {snapshot_path}")
    if not tarfile.is_tarfile(snapshot_path):
        raise Exception(f"Snapshot is neither a directory nor a tar archive: {snapshot_path}")

    table_by_file = {table.file: table for table in SNAPSHOT_TABLES}
    with tarfile.open(snapshot_path, "r:*") as tar:
        for member in tar.getmembers():
            table = table_by_file.get(os.path.basename(member.name))
            if table is None or not member.isfile():
                continue
            extracted = tar.extractfile(member)
            if extracted is None:
                continue
            with io.TextIOWrapper(extracted, encoding="utf-8", newline="") as text_lines:
                sources[table] = partial(nullcontext, list(text_lines))
    return sources


class TopologyFromSnapshot(TopologySource):
    """
    Topology is constructed from a snapshot directory or from a tar archive holding one.

    system_tablets.csv is required. Other files are optional and augment the base
    topology when present.
    """

    def __init__(self, snapshot_dir: str):
        self.snapshot_dir = snapshot_dir

        files = open_snapshot_files(snapshot_dir)
        if TABLETS_TABLE not in files:
            raise Exception(f"Snapshot is missing required file: {TABLETS_TABLE.file} in {snapshot_dir}")

        topo = Topology()
        # Files stay open for the whole build, so rows are read as _build() walks them.
        with ExitStack() as open_files:
            def rows(table: SnapshotTable) -> Iterator[Row]:
                source = files.get(table)
                if source is None:
                    return iter(())
                return rows_from_csv(open_files.enter_context(source()), table.columns)

            topo._build({table.name: rows(table) for table in SNAPSHOT_TABLES})

        self.topo = topo

    def get_topology(self) -> Topology:
        return self._with_shared_options(self.topo)

    def is_live(self):
        return False


class LiveClusterTopologySource(TopologySource):
    """
    Topology is constructed by querying system tables from a live cluster.
    """

    def __init__(self, cluster, session):
        self.cluster = cluster
        self.session = session
        self.queries = {table: session.prepare(f"SELECT * FROM {table.name}")
                        for table in SNAPSHOT_TABLES}

    def get_topology(self) -> Topology:
        pending = {table: self.session.execute_async(query) for table, query in self.queries.items()}

        topo = Topology()
        topo._build({table.name: rows_from_cql(result.result(), table.columns)
                     for table, result in pending.items()})

        return self._with_shared_options(topo)

    def is_live(self):
        return True

    def close(self) -> None:
        self.cluster.shutdown()


def parse_size(s: str) -> int:
    """
    Parses a size in bytes, optionally with a binary unit suffix (e.g. "1GiB", "512MiB", "1024").
    """
    m = re.fullmatch(r'\s*(\d+(?:\.\d+)?)\s*([KMGTP]i?B?)?\s*', s, re.IGNORECASE)
    if not m:
        raise argparse.ArgumentTypeError(f"Invalid size: '{s}'")
    value = float(m.group(1))
    unit = (m.group(2) or "").rstrip("bB").rstrip("iI").upper()
    factor = {"": 1, "K": 1 << 10, "M": 1 << 20, "G": 1 << 30, "T": 1 << 40, "P": 1 << 50}[unit]
    return int(value * factor)


def add_topology_source_args(parser: argparse.ArgumentParser):
    # Options for selecting topology source
    source_group = parser.add_mutually_exclusive_group(required=False)
    source_group.add_argument(
        "--snapshot",
        metavar="PATH",
        help="Read topology from a snapshot directory, or from a tar archive holding one (as created by snapshot.py --gz)."
    )
    source_group.add_argument(
        "--cluster",
        metavar="URL",
        help="Read data from a live cluster, connecting to a given host"
    )

    # Live topology source options
    parser.add_argument(
        "--port",
        metavar="port",
        help="Port number to use when connecting to a cluster"
    )
    # -u and -p spelled as cqlsh spells them.
    parser.add_argument(
        "-u", "--user",
        metavar="user",
        help="Username to use when connecting to a cluster"
    )
    parser.add_argument(
        "-p", "--password",
        metavar="password",
        help="Password to use when connecting to a cluster"
    )

    # Options independent of the topology source
    parser.add_argument(
        "--min-tablet-size",
        type=parse_size,
        help="Lower bound applied to every tablet size, e.g. '1GiB' or in bytes"
    )
    parser.add_argument(
        "--anonymize",
        action="store_true",
        help="Replace keyspace and table names with deterministic ksN.tableM aliases"
    )


def get_topology_source_from_args(args: argparse.Namespace) -> TopologySource:
    if args.snapshot:
        src = TopologyFromSnapshot(args.snapshot)
    else:
        src = get_live_topology_source_from_args(args)

    src.min_tablet_size = args.min_tablet_size
    src.anonymizer = Anonymizer() if args.anonymize else None

    return src


def get_live_topology_source_from_args(args: argparse.Namespace) -> TopologySource:
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster

    auth = None
    if args.user is not None or args.password is not None:
        auth = PlainTextAuthProvider(username=args.user, password=args.password)

    port = int(args.port) if args.port is not None else 9042
    cluster = Cluster([args.cluster or "127.0.0.1"], port=port, auth_provider=auth)
    session = cluster.connect()
    return LiveClusterTopologySource(cluster, session)
