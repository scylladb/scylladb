#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import logging
import os
import re
import signal
import subprocess
import tempfile
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import aiohttp
import pytest

from test.pylib.manager_client import ManagerClient
from test.pylib.minio_server import MinioServer

logger = logging.getLogger(__name__)


# ── Helpers ─────────────────────────────────────────────────────────────────

def format_bytes(b: float) -> str:
    """Format a byte count into a human-readable string."""
    if b >= 1_073_741_824:
        return f'{b / 1_073_741_824:.2f} GB'
    if b >= 1_048_576:
        return f'{b / 1_048_576:.2f} MB'
    if b >= 1024:
        return f'{b / 1024:.2f} KB'
    return f'{b:.0f} B'


def format_rate(bps: float) -> str:
    """Format a byte-rate into a human-readable string with /s suffix."""
    return f'{format_bytes(bps)}/s'


# ── Configuration ───────────────────────────────────────────────────────────
# All tunables are loaded from environment variables with sensible defaults.
#
# To use AWS S3 instead of local MinIO:
#   export S3_BACKEND=aws
#   export S3_ADDRESS=https://s3.us-east-2.amazonaws.com
#   export S3_BUCKET=your-bucket-name
#   export S3_REGION=us-east-2
#   export AWS_ACCESS_KEY_ID=...
#   export AWS_SECRET_ACCESS_KEY=...
#   export AWS_SESSION_TOKEN=...          (optional, for temporary credentials)

@dataclass(frozen=True)
class PerfTestConfig:
    """Central configuration for the perf test, loaded from environment variables."""

    # Storage backend: 'minio' or 'aws'
    s3_backend: str = os.environ.get('S3_BACKEND', 'minio')
    s3_address: str = os.environ.get('S3_ADDRESS', 'http://127.0.0.1:9000')
    s3_bucket: str = os.environ.get('S3_BUCKET', 'testbucket')
    s3_region: str = os.environ.get('S3_REGION', 'local')

    # MinIO-specific
    minio_host: str = os.environ.get('MINIO_HOST', '127.0.0.1')
    minio_data_dir: str = os.environ.get('MINIO_DATA_DIR', tempfile.gettempdir())

    # CQL proxy
    cql_alias: str = os.environ.get('CQL_ALIAS', '127.1.15.5')
    cql_port: int = int(os.environ.get('CQL_PORT', '9042'))

    # Scylla node settings
    nr_nodes: int = int(os.environ.get('SCYLLA_NR_NODES', '1'))
    smp: str = os.environ.get('SCYLLA_SMP', '2')
    memory: str = os.environ.get('SCYLLA_MEMORY', '1G')
    num_tokens: int = int(os.environ.get('SCYLLA_NUM_TOKENS', '256'))
    enable_cache: str = os.environ.get('SCYLLA_ENABLE_CACHE', '0')
    enable_repair_based_node_ops: bool = os.environ.get('SCYLLA_RBNO', '0') == '1'
    log_level: str = os.environ.get('SCYLLA_LOG_LEVEL', 's3=trace:http=debug')
    prometheus_base_ip: str = os.environ.get('SCYLLA_PROMETHEUS_BASE_IP', '127.45.0')
    prometheus_port: int = int(os.environ.get('SCYLLA_PROMETHEUS_PORT', '9180'))

    # Schema
    keyspace_name: str = os.environ.get('SCYLLA_KEYSPACE', 'keyspace1')
    replication_factor: int = int(os.environ.get('SCYLLA_RF', '1'))
    initial_tablets: int = int(os.environ.get('SCYLLA_INITIAL_TABLETS', '4'))
    nr_columns: int = int(os.environ.get('SCYLLA_NR_COLUMNS', '10'))

    # Compaction
    disable_autocompaction: bool = os.environ.get('SCYLLA_DISABLE_AUTOCOMPACTION', '1') == '1'

    # Tracing
    trace_probability: float = float(os.environ.get('SCYLLA_TRACE_PROBABILITY', '0.01'))
    trace_window_seconds: int = int(os.environ.get('SCYLLA_TRACE_WINDOW_SECS', '60'))

    # Collection intervals (seconds)
    metrics_interval: int = int(os.environ.get('METRICS_INTERVAL_SECS', '10'))
    log_analysis_interval: int = int(os.environ.get('LOG_ANALYSIS_INTERVAL_SECS', '10'))
    trace_dump_interval: int = int(os.environ.get('TRACE_DUMP_INTERVAL_SECS', '30'))
    heartbeat_interval: int = int(os.environ.get('HEARTBEAT_INTERVAL_SECS', '120'))

    # Network monitoring interface
    net_iface: str = os.environ.get('NET_IFACE', 'lo')

    @property
    def object_storage_conf(self) -> list:
        return [{
            'name': self.s3_address,
            'aws_region': self.s3_region,
            'iam_role_arn': '',
            'type': 's3',
        }]


# ── Storage backend abstraction ─────────────────────────────────────────────

class StorageBackend(ABC):
    """Abstract S3-compatible storage backend.

    Hides whether we're using a local MinIO instance or real AWS S3.
    """

    def __init__(self, config: PerfTestConfig):
        self._config = config

    @property
    def address(self) -> str:
        return self._config.s3_address

    @property
    def bucket(self) -> str:
        return self._config.s3_bucket

    @property
    def region(self) -> str:
        return self._config.s3_region

    @property
    def object_storage_conf(self) -> list:
        return self._config.object_storage_conf

    @abstractmethod
    async def setup(self) -> None:
        """Prepare the storage backend (start server, create bucket, etc.)."""

    @abstractmethod
    async def teardown(self) -> None:
        """Tear down the storage backend and clean up resources."""


class AWSS3Backend(StorageBackend):
    """Uses real AWS S3. Validates credentials are present."""

    async def setup(self) -> None:
        required_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
        missing = [v for v in required_vars if not os.environ.get(v)]
        if missing:
            raise RuntimeError(f"Missing required environment variables for AWS S3: {', '.join(missing)}")
        logger.info("Using AWS S3: %s bucket=%s region=%s", self.address, self.bucket, self.region)

    async def teardown(self) -> None:
        pass


class MinIOBackend(StorageBackend):
    """Manages a local MinIO instance using the shared MinioServer implementation."""

    def __init__(self, config: PerfTestConfig):
        super().__init__(config)
        self._server: Optional[MinioServer] = None

    async def setup(self) -> None:
        self._server = MinioServer(
            tempdir_base=self._config.minio_data_dir,
            address=self._config.minio_host,
            logger=logger,
        )
        await self._server.start()
        logger.info("MinIO started at %s:%d, bucket=%s, mc alias=local",
                    self._server.address, self._server.port, self._server.bucket_name)
        subprocess.run(
            ['mc', 'alias', 'set', 'local',
             f'http://{self._server.address}:{self._server.port}',
             self._server.access_key, self._server.secret_key],
            check=True, capture_output=True)
        logger.info("  mc alias 'local' configured, try: mc du local/%s", self._server.bucket_name)

    async def teardown(self) -> None:
        if self._server:
            await self._server.stop()
            self._server = None
        logger.info("MinIO stopped and data cleaned up")

    @property
    def address(self) -> str:
        if self._server and self._server.port:
            return f'http://{self._server.address}:{self._server.port}'
        return self._config.s3_address

    @property
    def bucket(self) -> str:
        if self._server:
            return self._server.bucket_name
        return self._config.s3_bucket

    @property
    def object_storage_conf(self) -> list:
        return MinioServer.create_conf(self.address, self.region)


def create_storage_backend(config: PerfTestConfig) -> StorageBackend:
    """Factory: create the right backend from configuration."""
    if config.s3_backend == 'aws':
        return AWSS3Backend(config)
    return MinIOBackend(config)


# ── S3 log analyzer ────────────────────────────────────────────────────────

class S3LogAnalyzer:
    """Parses Scylla S3 trace logs and produces per-file-type request stats."""

    FILE_TYPES = ['Data.db', 'Partitions.db', 'Rows.db', 'Statistics.db',
                  'TOC.txt', 'Digest.crc32', 'CRC.db', 'Scylla',
                  'TemporaryHashes.db.tmp']

    _RE_S3_OP = re.compile(
        r's3 - (HEAD|GET|PUT|DELETE|POST)\s+(?:uploads\s+|complete\s+)?/\S+?/([^/\s]+?)(?:\s|$)')

    def __init__(self, log_file_path: str, interval: int = 10):
        self.log_file_path = log_file_path
        self.output_file = os.path.join(tempfile.gettempdir(), 'scylla_s3_log_stats.txt')
        self._interval = interval
        self._last_pos = 0
        self._task: Optional[asyncio.Task] = None
        self._ops: dict = defaultdict(lambda: defaultdict(int))

    def _classify_file(self, filename: str) -> str:
        for ft in self.FILE_TYPES:
            if filename == ft or filename.startswith(ft + '_') or filename.startswith(ft + '.'):
                return ft
        return 'other'

    def _parse_new_lines(self) -> None:
        try:
            with open(self.log_file_path, 'r', errors='replace') as f:
                f.seek(self._last_pos)
                for line in f:
                    if 's3 -' not in line:
                        continue
                    m = self._RE_S3_OP.search(line)
                    if m:
                        method, filename = m.group(1), m.group(2)
                        self._ops[method][self._classify_file(filename)] += 1
                self._last_pos = f.tell()
        except OSError as e:
            logger.warning("Failed to read log file: %s", e)

    def _format_table(self) -> str:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        all_file_types = set()
        all_methods = set()
        for method, ft_dict in self._ops.items():
            all_methods.add(method)
            all_file_types.update(ft_dict.keys())
        all_file_types = sorted(all_file_types)
        methods = sorted(all_methods)

        header_str = f'{"File Type":<20s}' + ''.join(f'{m:>10s}' for m in methods) + f'{"Total":>10s}'
        table_width = len(header_str) + 6
        sep = '─' * table_width

        lines = [
            f'┌{sep}┐',
            f'│ S3 Log Analysis  {now}{" " * (table_width - 39)}│',
            f'├{sep}┤',
            f'│   {header_str}   │',
            f'│   {"─" * (table_width - 6)}   │',
        ]

        for ft in all_file_types:
            row = f'{ft:<20s}'
            total = 0
            for m in methods:
                count = self._ops[m].get(ft, 0)
                total += count
                row += f'{count:>10d}'
            row += f'{total:>10d}'
            lines.append(f'│   {row}   │')

        lines.append(f'│   {"─" * (table_width - 6)}   │')
        row = f'{"TOTAL":<20s}'
        grand_total = 0
        for m in methods:
            col_total = sum(self._ops[m].values())
            grand_total += col_total
            row += f'{col_total:>10d}'
        row += f'{grand_total:>10d}'
        lines.append(f'│   {row}   │')
        lines.append(f'└{sep}┘')
        return '\n'.join(lines)

    async def _collect_loop(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            self._parse_new_lines()
            table = self._format_table()
            with open(self.output_file, 'w') as f:
                f.write(table + '\n')
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._interval)
            except asyncio.TimeoutError:
                pass

    def start(self, stop_event: asyncio.Event) -> asyncio.Task:
        logger.info("S3 log stats file: %s", self.output_file)
        logger.info("  Watch with: watch -n1 cat %s", self.output_file)
        self._task = asyncio.ensure_future(self._collect_loop(stop_event))
        return self._task

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass


# ── Prometheus metrics collector ────────────────────────────────────────────

class MetricsCollector:
    """Periodically scrapes Scylla prometheus metrics and writes a formatted table to a file."""

    S3_METRICS = [
        'scylla_s3_total_read_requests',
        'scylla_s3_total_write_requests',
        'scylla_s3_total_read_bytes',
        'scylla_s3_total_write_bytes',
        'scylla_s3_total_read_latency_sec',
        'scylla_s3_total_write_latency_sec',
        'scylla_s3_total_read_prefetch_bytes',
        'scylla_s3_nr_connections',
        'scylla_s3_nr_active_connections',
        'scylla_s3_downloads_blocked_on_memory',
    ]
    CACHE_METRICS = [
        'scylla_sstables_index_page_cache_hits',
        'scylla_sstables_index_page_cache_misses',
        'scylla_sstables_index_page_cache_evictions',
        'scylla_sstables_index_page_cache_populations',
        'scylla_sstables_index_page_cache_bytes',
    ]
    ROW_CACHE_METRICS = [
        'scylla_column_family_cache_hit_rate',
        'scylla_column_family_memtable_partition_hits',
        'scylla_column_family_memtable_partition_writes',
        'scylla_column_family_memtable_row_hits',
    ]

    def __init__(self, prometheus_address: str, port: int = 9180,
                 net_iface: str = 'lo', interval: int = 10):
        self._prom_url = f'http://{prometheus_address}:{port}/metrics'
        self._net_iface = net_iface
        self._interval = interval
        self.output_file = os.path.join(tempfile.gettempdir(), 'scylla_s3_metrics.txt')
        self._prev_metrics: dict = {}
        self._prev_time: Optional[float] = None
        self._prev_net: Optional[tuple] = None
        self._task: Optional[asyncio.Task] = None

    async def _fetch_metrics(self) -> dict:
        all_names = set(self.S3_METRICS + self.CACHE_METRICS + self.ROW_CACHE_METRICS)
        result = {name: 0.0 for name in all_names}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self._prom_url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    text = await resp.text()
            for line in text.split('\n'):
                if line.startswith('#') or not line.strip():
                    continue
                parts = line.split()
                if len(parts) < 2:
                    continue
                name = parts[0].split('{')[0]
                if name in all_names:
                    try:
                        result[name] += float(parts[-1])
                    except ValueError:
                        pass
        except Exception as e:
            logger.warning("Failed to fetch metrics from %s: %s", self._prom_url, e)
        return result

    def _read_net_stats(self) -> tuple:
        try:
            with open('/proc/net/dev', 'r') as f:
                for line in f:
                    if self._net_iface in line:
                        parts = line.split()
                        return int(parts[1]), int(parts[9])
        except (OSError, IndexError, ValueError):
            pass
        return 0, 0

    def _format_table(self, metrics: dict, dt: float) -> str:
        prev = self._prev_metrics
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        def rate(key):
            if key not in prev:
                return 0.0
            return (metrics[key] - prev[key]) / dt if dt > 0 else 0.0

        def delta(key):
            if key not in prev:
                return metrics[key]
            return metrics[key] - prev[key]

        read_ops_rate = rate('scylla_s3_total_read_requests')
        write_ops_rate = rate('scylla_s3_total_write_requests')
        read_bytes_rate = rate('scylla_s3_total_read_bytes')
        write_bytes_rate = rate('scylla_s3_total_write_bytes')

        read_ops_delta = delta('scylla_s3_total_read_requests')
        write_ops_delta = delta('scylla_s3_total_write_requests')
        read_lat_delta = delta('scylla_s3_total_read_latency_sec')
        write_lat_delta = delta('scylla_s3_total_write_latency_sec')

        s3_avg_read_lat_ms = (read_lat_delta / read_ops_delta * 1000) if read_ops_delta > 0 else 0
        s3_avg_write_lat_ms = (write_lat_delta / write_ops_delta * 1000) if write_ops_delta > 0 else 0

        idx_hits = metrics['scylla_sstables_index_page_cache_hits']
        idx_misses = metrics['scylla_sstables_index_page_cache_misses']
        idx_total = idx_hits + idx_misses
        idx_hit_ratio = (idx_hits / idx_total * 100) if idx_total > 0 else 0

        idx_hits_delta = delta('scylla_sstables_index_page_cache_hits')
        idx_misses_delta = delta('scylla_sstables_index_page_cache_misses')
        idx_delta_total = idx_hits_delta + idx_misses_delta
        idx_hit_ratio_interval = (idx_hits_delta / idx_delta_total * 100) if idx_delta_total > 0 else 0

        row_cache_hit_rate = metrics.get('scylla_column_family_cache_hit_rate', 0.0)

        mem_part_writes = metrics.get('scylla_column_family_memtable_partition_writes', 0)
        mem_part_hits = metrics.get('scylla_column_family_memtable_partition_hits', 0)
        mem_part_ratio = (mem_part_hits / mem_part_writes * 100) if mem_part_writes > 0 else 0
        mem_row_hits = metrics.get('scylla_column_family_memtable_row_hits', 0)

        rx_bytes, tx_bytes = self._read_net_stats()
        if self._prev_net and dt > 0:
            rx_rate = (rx_bytes - self._prev_net[0]) / dt
            tx_rate = (tx_bytes - self._prev_net[1]) / dt
        else:
            rx_rate = 0
            tx_rate = 0
        self._prev_net = (rx_bytes, tx_bytes)

        sep = '─' * 62
        lines = [
            f'┌{sep}┐',
            f'│ Scylla S3 Performance Metrics       {now}  │',
            f'├{sep}┤',
            f'│ S3 Operations (rate/s)                                       │',
            f'│   Read ops:  {read_ops_rate:>12.1f}/s    Write ops: {write_ops_rate:>12.1f}/s   │',
            f'│   Read BW:   {format_rate(read_bytes_rate):>12s}    Write BW:  {format_rate(write_bytes_rate):>12s}   │',
            f'├{sep}┤',
            f'│ S3 Latency (avg per op, this interval)                       │',
            f'│   Read:      {s3_avg_read_lat_ms:>10.2f} ms    Write:     {s3_avg_write_lat_ms:>10.2f} ms   │',
            f'├{sep}┤',
            f'│ S3 Cumulative                                                │',
            f'│   Total reads:    {metrics["scylla_s3_total_read_requests"]:>14.0f}    Total writes: {metrics["scylla_s3_total_write_requests"]:>10.0f}   │',
            f'│   Total read:  {format_bytes(metrics["scylla_s3_total_read_bytes"]):>16s}    Total written:{format_bytes(metrics["scylla_s3_total_write_bytes"]):>10s}   │',
            f'│   Prefetch:   {format_bytes(metrics["scylla_s3_total_read_prefetch_bytes"]):>16s}                              │',
            f'│   Connections: {metrics["scylla_s3_nr_connections"]:>5.0f}  Active: {metrics["scylla_s3_nr_active_connections"]:>5.0f}  Blocked: {metrics["scylla_s3_downloads_blocked_on_memory"]:>5.0f}   │',
            f'├{sep}┤',
            f'│ Index Page Cache                                             │',
            f'│   Hit ratio (total):    {idx_hit_ratio:>8.2f}%                            │',
            f'│   Hit ratio (interval): {idx_hit_ratio_interval:>8.2f}%                            │',
            f'│   Hits: {idx_hits:>12.0f}  Misses: {idx_misses:>12.0f}                  │',
            f'│   Evictions: {metrics["scylla_sstables_index_page_cache_evictions"]:>10.0f}  Cached: {format_bytes(metrics["scylla_sstables_index_page_cache_bytes"]):>12s}          │',
            f'├{sep}┤',
            f'│ Row Cache                                                    │',
            f'│   Cache hit rate:       {row_cache_hit_rate:>8.4f}                            │',
            f'│   Memtable partition hit ratio: {mem_part_ratio:>8.2f}%                   │',
            f'│   Memtable partition hits/writes: {mem_part_hits:>8.0f}/{mem_part_writes:>8.0f}          │',
            f'│   Memtable row hits (overwrites): {mem_row_hits:>10.0f}                  │',
            f'├{sep}┤',
            f'│ Network ({self._net_iface})                                             │',
            f'│   RX: {format_rate(rx_rate):>14s}    TX: {format_rate(tx_rate):>14s}               │',
            f'│   RX total: {format_bytes(rx_bytes):>12s}    TX total: {format_bytes(tx_bytes):>12s}           │',
            f'└{sep}┘',
        ]
        return '\n'.join(lines)

    async def _collect_loop(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            now = time.monotonic()
            dt = (now - self._prev_time) if self._prev_time else 0
            metrics = await self._fetch_metrics()

            table = self._format_table(metrics, dt)
            with open(self.output_file, 'w') as f:
                f.write(table + '\n')

            self._prev_metrics = metrics
            self._prev_time = now

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._interval)
            except asyncio.TimeoutError:
                pass

    def start(self, stop_event: asyncio.Event) -> asyncio.Task:
        logger.info("Metrics file: %s", self.output_file)
        logger.info("  Watch with: watch -n1 cat %s", self.output_file)
        self._task = asyncio.ensure_future(self._collect_loop(stop_event))
        return self._task

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass


# ── CQL TCP proxy ───────────────────────────────────────────────────────────

class CQLProxy:
    """Transparent TCP proxy so a stable CQL alias always reaches a Scylla node."""

    def __init__(self, listen_host: str, listen_port: int,
                 dest_host: str, dest_port: int):
        self._listen_host = listen_host
        self._listen_port = listen_port
        self._dest_host = dest_host
        self._dest_port = dest_port
        self._server: Optional[asyncio.AbstractServer] = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_connection, self._listen_host, self._listen_port)
        logger.info("CQL proxy: %s:%d -> %s:%d",
                     self._listen_host, self._listen_port,
                     self._dest_host, self._dest_port)

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        logger.info("Waiting for CQL proxy to stop")
        await self._server.wait_closed()

    async def _handle_connection(self, reader: asyncio.StreamReader,
                                 writer: asyncio.StreamWriter) -> None:
        try:
            dr, dw = await asyncio.open_connection(self._dest_host, self._dest_port)

            async def _forward(src: asyncio.StreamReader,
                               dst: asyncio.StreamWriter) -> None:
                try:
                    while True:
                        data = await src.read(65536)
                        if not data:
                            break
                        dst.write(data)
                        await dst.drain()
                except Exception:
                    pass
                finally:
                    dst.close()

            await asyncio.gather(_forward(reader, dw), _forward(dr, writer))
        except Exception:
            writer.close()


# ── Trace dumper ────────────────────────────────────────────────────────────

class TraceDumper:
    """Periodically dumps system_traces.sessions and events to a file."""

    def __init__(self, cql, window_seconds: int = 60, interval: int = 30):
        self._cql = cql
        self._window_seconds = window_seconds
        self._interval = interval
        self.output_file = os.path.join(tempfile.gettempdir(), 'scylla_traces.txt')
        self._task: Optional[asyncio.Task] = None

    def start(self, stop_event: asyncio.Event) -> asyncio.Task:
        logger.info("Traces file: %s", self.output_file)
        logger.info("  Watch with: watch -n5 cat %s", self.output_file)
        self._task = asyncio.ensure_future(self._dump_loop(stop_event))
        return self._task

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _dump_loop(self, stop_event: asyncio.Event) -> None:
        from cassandra import ConsistencyLevel
        from cassandra.query import SimpleStatement

        while not stop_event.is_set():
            try:
                await self._dump_once(SimpleStatement, ConsistencyLevel)
            except Exception as e:
                logger.error("Failed to dump traces: %s: %s", type(e).__name__, e, exc_info=True)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._interval)
            except asyncio.TimeoutError:
                pass

    async def _dump_once(self, SimpleStatement, ConsistencyLevel) -> None:
        sessions = await self._cql.run_async(SimpleStatement(
            "SELECT session_id, command, started_at, duration, parameters FROM system_traces.sessions",
            consistency_level=ConsistencyLevel.ONE))

        logger.debug("Traces: %d total sessions", len(sessions))
        if sessions:
            for s in sessions[:3]:
                logger.debug("  session: started_at=%s cmd=%s duration=%s", s.started_at, s.command, s.duration)
            max_ts = max(s.started_at for s in sessions if s.started_at)
            logger.debug("  max_ts=%s", max_ts)

        if sessions:
            max_ts = max(s.started_at for s in sessions if s.started_at)
            recent_sessions = [s for s in sessions
                               if s.started_at and (max_ts - s.started_at).total_seconds() <= self._window_seconds]
        else:
            recent_sessions = []
        recent_sessions.sort(key=lambda s: s.started_at, reverse=True)

        recent_sids = {s.session_id for s in recent_sessions}

        events_by_session: dict = defaultdict(list)
        if recent_sids:
            # Batch IN queries to stay under the default max-partition-key-restrictions-per-query (100)
            sid_list = list(recent_sids)
            batch_size = 100
            for i in range(0, len(sid_list), batch_size):
                batch = sid_list[i:i + batch_size]
                in_clause = ", ".join(str(sid) for sid in batch)
                events = await self._cql.run_async(SimpleStatement(
                    f"SELECT session_id, event_id, activity, source, source_elapsed "
                    f"FROM system_traces.events WHERE session_id IN ({in_clause})",
                    consistency_level=ConsistencyLevel.ONE))
                for row in events:
                    events_by_session[row.session_id].append(row)
        for sid in events_by_session:
            events_by_session[sid].sort(key=lambda r: r.source_elapsed or 0)

        with open(self.output_file, 'w') as tf:
            now = datetime.now()
            tf.write(f"=== Traces dump at {now.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
            tf.write(f"=== {len(recent_sessions)} sessions (last {self._window_seconds}s), "
                     f"{sum(len(v) for v in events_by_session.values())} events ===\n\n")

            for sess in recent_sessions:
                sid = sess.session_id
                duration_ms = (sess.duration or 0) / 1000.0
                tf.write(f"{'─' * 100}\n")
                tf.write(f"Session {sid}  |  {sess.command}  |  {sess.started_at}  |  duration={duration_ms:.1f}ms\n")
                if sess.parameters:
                    params = dict(sess.parameters)
                    query = params.get('query', '')
                    if query:
                        tf.write(f"  query: {query[:200]}\n")
                tf.write(f"  {'elapsed_us':>10s}  {'delta_us':>10s}  activity\n")
                tf.write(f"  {'─' * 10}  {'─' * 10}  {'─' * 60}\n")
                prev_elapsed = 0
                for ev in events_by_session.get(sid, []):
                    elapsed = ev.source_elapsed or 0
                    delta = elapsed - prev_elapsed
                    tf.write(f"  {elapsed:>10d}  {delta:>+10d}  {ev.activity}\n")
                    prev_elapsed = elapsed
                tf.write('\n')

        logger.info("Traces dumped: %d sessions (last %ds)", len(recent_sessions), self._window_seconds)


# ── Schema setup ────────────────────────────────────────────────────────────

async def create_schema(cql, config: PerfTestConfig, backend: StorageBackend) -> None:
    """Create the S3-backed keyspace and tables."""
    ks = config.keyspace_name
    await cql.run_async(
        f"CREATE KEYSPACE {ks}"
        f" WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {config.replication_factor}}}"
        f" AND TABLETS = {{'initial': {config.initial_tablets}}}"
        f" AND STORAGE = {{'type': 'S3', 'endpoint': '{backend.address}', 'bucket': '{backend.bucket}'}}"
    )
    logger.info("Created S3-backed keyspace '%s' (storage at %s)", ks, backend.address)

    compaction_clause = (
        "  AND compaction = {'class': 'NullCompactionStrategy'}" if config.disable_autocompaction else ""
    )

    columns = ", ".join([f'"C{i}" blob' for i in range(config.nr_columns)])
    await cql.run_async(
        f'CREATE TABLE {ks}.standard1 ('
        f'  key blob PRIMARY KEY, {columns}'
        f") WITH compression = {{}}"
        f"{compaction_clause}"
        f"  AND comment = 'Created for S3 keyspace longevity test'"
    )
    logger.info("Created table %s.standard1", ks)

    wide_columns = ", ".join([f'"C{i}" blob' for i in range(config.nr_columns)])
    await cql.run_async(
        f'CREATE TABLE {ks}.wide_table ('
        f'  pk blob, ck int, {wide_columns},'
        f'  PRIMARY KEY (pk, ck)'
        f") WITH compression = {{}}"
        f"{compaction_clause}"
        f"  AND comment = 'Wide partition table for range query test'"
    )
    logger.info("Created table %s.wide_table", ks)


# ── Main test ───────────────────────────────────────────────────────────────

@pytest.mark.skip_slow(reason="manual-only perf test, not intended for CI")
@pytest.mark.asyncio
async def test_boot(manager: ManagerClient):
    config = PerfTestConfig()

    # Set up storage backend (MinIO or AWS S3)
    backend = create_storage_backend(config)
    await backend.setup()

    scylla_cfg = {
        'enable_repair_based_node_ops': config.enable_repair_based_node_ops,
        'num_tokens': config.num_tokens,
        'object_storage_endpoints': backend.object_storage_conf,
        'experimental_features': ['keyspace-storage-options'],
    }
    cmdline = [
        '--smp', config.smp,
        '-m', config.memory,
        '--enable-cache', config.enable_cache,
        '--logger-log-level', config.log_level,
    ]

    servers = []
    for i in range(config.nr_nodes):
        logger.info("Booting node %d/%d", i + 1, config.nr_nodes)
        start = time.time()
        node_cfg = scylla_cfg.copy()
        node_cfg['prometheus_address'] = f'{config.prometheus_base_ip}.{i + 1}'
        srv = await manager.server_add(config=node_cfg, cmdline=cmdline)
        servers.append(srv)
        logger.info("Node %d booted in %.2fs, CQL address: %s:%d",
                     i + 1, time.time() - start, srv.ip_addr, config.cql_port)

    # CQL proxy: stable alias -> first node
    proxy = CQLProxy(config.cql_alias, config.cql_port,
                     str(servers[0].ip_addr), config.cql_port)
    await proxy.start()

    # Schema
    cql = manager.get_cql()
    await create_schema(cql, config, backend)

    # Signal handling
    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, stop_event.set)
    loop.add_signal_handler(signal.SIGTERM, stop_event.set)

    # Start metrics collector
    prom_addr = f'{config.prometheus_base_ip}.1'
    metrics_collector = MetricsCollector(
        prom_addr, port=config.prometheus_port,
        net_iface=config.net_iface, interval=config.metrics_interval)
    metrics_collector.start(stop_event)

    # Start S3 log analyzer
    log_file = await manager.server_open_log(server_id=servers[0].server_id)
    s3_log_analyzer = S3LogAnalyzer(str(log_file.file), interval=config.log_analysis_interval)
    s3_log_analyzer.start(stop_event)

    # Enable tracing
    node_ip = servers[0].ip_addr
    try:
        await manager.api.set_trace_probability(node_ip=str(node_ip), probability=config.trace_probability)
        logger.info("Probabilistic tracing enabled at %.2f%% on %s",
                     config.trace_probability * 100, node_ip)
    except Exception as e:
        logger.warning("Failed to set trace probability: %s", e)

    # Start trace dumper
    trace_dumper = TraceDumper(cql, window_seconds=config.trace_window_seconds,
                              interval=config.trace_dump_interval)
    trace_dumper.start(stop_event)

    start_wait = time.time()
    try:
        while not stop_event.is_set():
            elapsed = time.time() - start_wait
            logger.info("Time elapsed: %.2fs", elapsed)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=config.heartbeat_interval)
            except asyncio.TimeoutError:
                pass
        logger.info("Caught interrupt, shutting down Scylla nodes...")
    finally:
        for srv in servers:
            try:
                logger.info("Stopping server %s", srv.server_id)
                await manager.server_stop_gracefully(srv.server_id)
                logger.info("Server %s stopped", srv.server_id)
            except Exception as e:
                logger.warning("Failed to stop server %s gracefully, forcing: %s", srv.server_id, e)
                try:
                    await manager.server_stop(srv.server_id)
                except Exception as e2:
                    logger.error("Failed to force stop server %s: %s", srv.server_id, e2)
        await metrics_collector.stop()
        await s3_log_analyzer.stop()
        await trace_dumper.stop()
        await proxy.stop()
        await backend.teardown()
        logger.info("All Scylla nodes stopped.")
