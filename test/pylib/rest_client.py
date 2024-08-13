#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""Asynchronous helper for Scylla REST API operations.
"""
from __future__ import annotations                           # Type hints as strings
from abc import ABCMeta
from collections.abc import Mapping
import logging
import os.path
from typing import Any, Optional, AsyncIterator
from contextlib import asynccontextmanager
from aiohttp import request, BaseConnector, UnixConnector, ClientTimeout
import pytest
from test.pylib.internal_types import IPAddress, HostID
from cassandra.pool import Host                          # type: ignore # pylint: disable=no-name-in-module


logger = logging.getLogger(__name__)


class HTTPError(Exception):
    def __init__(self, uri, code, params, json, message):
        super().__init__(message)
        self.uri = uri
        self.code = code
        self.params = params
        self.json = json
        self.message = message

    def __str__(self):
        return f"HTTP error {self.code}, uri: {self.uri}, " \
               f"params: {self.params}, json: {self.json}, body:\n{self.message}"


# TODO: support ssl and verify_ssl
class RESTClient(metaclass=ABCMeta):
    """Base class for sesion-free REST client"""
    connector: Optional[BaseConnector]
    uri_scheme: str   # e.g. http, http+unix
    default_host: str
    default_port: Optional[int]
    # pylint: disable=too-many-arguments

    async def _fetch(self, method: str, resource: str, response_type: Optional[str] = None,
                     host: Optional[str] = None, port: Optional[int] = None,
                     params: Optional[Mapping[str, str]] = None,
                     json: Optional[Mapping] = None, timeout: Optional[float] = None, allow_failed: bool = False) -> Any:
        # Can raise exception. See https://docs.aiohttp.org/en/latest/web_exceptions.html
        assert method in ["GET", "POST", "PUT", "DELETE"], f"Invalid HTTP request method {method}"
        assert response_type is None or response_type in ["text", "json"], \
                f"Invalid response type requested {response_type} (expected 'text' or 'json')"
        # Build the URI
        port = port if port else self.default_port if hasattr(self, "default_port") else None
        port_str = f":{port}" if port else ""
        assert host is not None or hasattr(self, "default_host"), "_fetch: missing host for " \
                "{method} {resource}"
        host_str = host if host is not None else self.default_host
        uri = self.uri_scheme + "://" + host_str + port_str + resource
        logging.debug(f"RESTClient fetching {method} {uri}")

        client_timeout = ClientTimeout(total = timeout if timeout is not None else 300)
        async with request(method, uri,
                           connector = self.connector if hasattr(self, "connector") else None,
                           params = params, json = json, timeout = client_timeout) as resp:
            if allow_failed:
                return await resp.json()
            if resp.status != 200:
                text = await resp.text()
                raise HTTPError(uri, resp.status, params, json, text)
            if response_type is not None:
                # Return response.text() or response.json()
                return await getattr(resp, response_type)()
        return None

    async def get(self, resource_uri: str, host: Optional[str] = None, port: Optional[int] = None,
                  params: Optional[Mapping[str, str]] = None, allow_failed: bool = False) -> Any:
        return await self._fetch("GET", resource_uri, host = host, port = port, params = params, allow_failed=allow_failed)

    async def get_text(self, resource_uri: str, host: Optional[str] = None,
                       port: Optional[int] = None, params: Optional[Mapping[str, str]] = None,
                       timeout: Optional[float] = None) -> str:
        ret = await self._fetch("GET", resource_uri, response_type = "text", host = host,
                                port = port, params = params, timeout = timeout)
        assert isinstance(ret, str), f"get_text: expected str but got {type(ret)} {ret}"
        return ret

    async def get_json(self, resource_uri: str, host: Optional[str] = None,
                       port: Optional[int] = None, params: Optional[Mapping[str, str]] = None,
                       allow_failed: bool = False) -> Any:
        """Fetch URL and get JSON. Caller must check JSON content types."""
        ret = await self._fetch("GET", resource_uri, response_type = "json", host = host,
                                port = port, params = params, allow_failed = allow_failed)
        return ret

    async def post(self, resource_uri: str, host: Optional[str] = None,
                   port: Optional[int] = None, params: Optional[Mapping[str, str]] = None,
                   json: Optional[Mapping] = None, timeout: Optional[float] = None) -> None:
        await self._fetch("POST", resource_uri, host = host, port = port, params = params,
                          json = json, timeout = timeout)

    async def post_json(self, resource_uri: str, host: Optional[str] = None,
                   port: Optional[int] = None, params: Optional[Mapping[str, str]] = None,
                   json: Optional[Mapping] = None, timeout: Optional[float] = None) -> None:
        ret = await self._fetch("POST", resource_uri, response_type = "json", host = host, port = port, params = params,
                          json = json, timeout = timeout)
        return ret

    async def put_json(self, resource_uri: str, data: Optional[Mapping] = None, host: Optional[str] = None,
                       port: Optional[int] = None, params: Optional[dict[str, str]] = None,
                       response_type: Optional[str] = None, timeout: Optional[float] = None) -> Any:
        ret = await self._fetch("PUT", resource_uri, response_type = response_type, host = host,
                                port = port, params = params, json = data, timeout = timeout)
        return ret

    async def delete(self, resource_uri: str, host: Optional[str] = None,
                     port: Optional[int] = None, params: Optional[dict[str, str]] = None,
                     json: Optional[Mapping] = None) -> None:
        await self._fetch("DELETE", resource_uri, host = host, port = port, params = params,
                          json = json)


class UnixRESTClient(RESTClient):
    """An async helper for REST API operations using AF_UNIX socket"""

    def __init__(self, sock_path: str):
        # NOTE: using Python requests style URI for Unix domain sockets to avoid using "localhost"
        #       host parameter is ignored but set to socket name as convention
        self.uri_scheme: str = "http+unix"
        self.default_host: str = f"{os.path.basename(sock_path)}"
        self.connector = UnixConnector(path=sock_path)

    async def shutdown(self):
        await self.connector.close()


class TCPRESTClient(RESTClient):
    """An async helper for REST API operations"""

    def __init__(self, port: int):
        self.uri_scheme = "http"
        self.connector = None
        self.default_port: int = port


class ScyllaRESTAPIClient():
    """Async Scylla REST API client"""

    def __init__(self, port: int = 10000):
        self.client = TCPRESTClient(port)

    async def get_host_id(self, server_ip: IPAddress) -> HostID:
        """Get server id (UUID)"""
        host_uuid = await self.client.get_text("/storage_service/hostid/local", host=server_ip)
        assert isinstance(host_uuid, str) and len(host_uuid) > 10, \
                f"get_host_id: invalid {host_uuid}"
        host_uuid = host_uuid.lstrip('"').rstrip('"')
        return HostID(host_uuid)

    async def get_host_id_map(self, dst_server_ip: IPAddress) -> list[HostID]:
        """Retrieve the mapping of endpoint to host ID"""
        data = await self.client.get_json("/storage_service/host_id/", dst_server_ip)
        assert(type(data) == list)
        return data

    async def get_ownership(self, dst_server_ip: IPAddress, keyspace: str = None, table: str = None) -> list:
        """Retrieve the ownership"""
        if keyspace is None and table is None:
            api_path = f"/storage_service/ownership/"
        elif table is None:
            api_path = f"/storage_service/ownership/{keyspace}"
        else:
            api_path = f"/storage_service/ownership/{keyspace}?cf={table}"
        data = await self.client.get_json(api_path, dst_server_ip)
        return data

    async def get_down_endpoints(self, node_ip: IPAddress) -> list[IPAddress]:
        """Retrieve down endpoints from gossiper's point of view """
        data = await self.client.get_json("/gossiper/endpoint/down/", node_ip)
        assert(type(data) == list)
        return data

    async def remove_node(self, initiator_ip: IPAddress, host_id: HostID,
                          ignore_dead: list[IPAddress], timeout: float) -> None:
        """Initiate remove node of host_id in initiator initiator_ip"""
        logger.info("remove_node for %s on %s", host_id, initiator_ip)
        await self.client.post("/storage_service/remove_node",
                               params = {"host_id": host_id,
                                         "ignore_nodes": ",".join(ignore_dead)},
                               host = initiator_ip, timeout = timeout)
        logger.debug("remove_node for %s finished", host_id)

    async def decommission_node(self, host_ip: str, timeout: float) -> None:
        """Initiate decommission node of host_ip"""
        logger.debug("decommission_node %s", host_ip)
        await self.client.post("/storage_service/decommission", host = host_ip,
                               timeout = timeout)
        logger.debug("decommission_node %s finished", host_ip)

    async def rebuild_node(self, host_ip: str, timeout: float) -> None:
        """Initiate rebuild of a node with host_ip"""
        logger.debug("rebuild_node %s", host_ip)
        await self.client.post("/storage_service/rebuild", host = host_ip,
                               timeout = timeout)
        logger.debug("rebuild_node %s finished", host_ip)

    async def get_gossip_generation_number(self, node_ip: str, target_ip: str) -> int:
        """Get the current generation number of `target_ip` observed by `node_ip`."""
        data = await self.client.get_json(f"/gossiper/generation_number/{target_ip}",
                                          host = node_ip)
        assert(type(data) == int)
        return data

    async def get_joining_nodes(self, node_ip: str) -> list:
        """Get the list of joining nodes according to `node_ip`."""
        data = await self.client.get_json(f"/storage_service/nodes/joining", host=node_ip)
        assert(type(data) == list)
        return data

    async def get_alive_endpoints(self, node_ip: str) -> list:
        """Get the list of alive nodes according to `node_ip`."""
        data = await self.client.get_json(f"/gossiper/endpoint/live", host=node_ip)
        assert(type(data) == list)
        return data

    async def enable_injection(self, node_ip: str, injection: str, one_shot: bool, parameters: dict[str, Any] = {}) -> None:
        """Enable error injection named `injection` on `node_ip`. Depending on `one_shot`,
           the injection will be executed only once or every time the process passes the injection point.
           Note: this only has an effect in specific build modes: debug,dev,sanitize.
        """
        await self.client.post(f"/v2/error_injection/injection/{injection}",
                               host=node_ip, params={"one_shot": str(one_shot)}, json={ key: str(value) for key, value in parameters.items() })

    async def get_injection(self, node_ip: str, injection: str) -> list[dict[str, Any]]:
        """Read the state of the error injection named `injection` on `node_ip`.
           The returned information includes whether the error injections is
           active, as well as any parameters it might have.
           Note: this only has an effect in specific build modes: debug,dev,sanitize.
        """
        return await self.client.get_json(f"/v2/error_injection/injection/{injection}", host=node_ip)

    async def move_tablet(self, node_ip: str, ks: str, table: str, src_host: HostID, src_shard: int, dst_host: HostID, dst_shard: int, token: int) -> None:
        await self.client.post(f"/storage_service/tablets/move", host=node_ip, params={
            "ks": ks,
            "table": table,
            "src_host": str(src_host),
            "src_shard": str(src_shard),
            "dst_host": str(dst_host),
            "dst_shard": str(dst_shard),
            "token": str(token)
        })

    async def quiesce_topology(self, node_ip: str) -> None:
        await self.client.post(f"/storage_service/quiesce_topology", host=node_ip)

    async def add_tablet_replica(self, node_ip: str, ks: str, table: str, dst_host: HostID, dst_shard: int, token: int) -> None:
        await self.client.post(f"/storage_service/tablets/add_replica", host=node_ip, params={
            "ks": ks,
            "table": table,
            "dst_host": str(dst_host),
            "dst_shard": str(dst_shard),
            "token": str(token)
        })

    async def del_tablet_replica(self, node_ip: str, ks: str, table: str, host: HostID, shard: int, token: int) -> None:
        await self.client.post(f"/storage_service/tablets/del_replica", host=node_ip, params={
            "ks": ks,
            "table": table,
            "host": str(host),
            "shard": str(shard),
            "token": str(token)
        })

    async def enable_tablet_balancing(self, node_ip: str) -> None:
        await self.client.post(f"/storage_service/tablets/balancing", host=node_ip, params={"enabled": "true"})

    async def disable_tablet_balancing(self, node_ip: str) -> None:
        await self.client.post(f"/storage_service/tablets/balancing", host=node_ip, params={"enabled": "false"})

    async def disable_injection(self, node_ip: str, injection: str) -> None:
        await self.client.delete(f"/v2/error_injection/injection/{injection}", host=node_ip)

    async def get_enabled_injections(self, node_ip: str) -> list[str]:
        data = await self.client.get_json("/v2/error_injection/injection", host=node_ip)
        assert(type(data) == list)
        assert(type(e) == str for e in data)
        return data

    async def message_injection(self, node_ip: str, injection: str) -> None:
        await self.client.post(f"/v2/error_injection/injection/{injection}/message", host=node_ip)

    async def inject_disconnect(self, node_ip: str, ip_to_disconnect_from: str) -> None:
        await self.client.post(f"/v2/error_injection/disconnect/{ip_to_disconnect_from}", host=node_ip)

    async def get_logger_level(self, node_ip: str, logger: str) -> str:
        """Get logger level"""
        return await self.client.get_text(f"/system/logger/{logger}", host=node_ip)

    async def set_logger_level(self, node_ip: str, logger: str, level: str) -> None:
        """Set logger level"""
        assert level in ["debug", "info", "warning", "trace"]
        await self.client.post(f"/system/logger/{logger}?level={level}", host=node_ip)

    async def flush_keyspace(self, node_ip: str, ks: str) -> None:
        """Flush keyspace"""
        await self.client.post(f"/storage_service/keyspace_flush/{ks}", host=node_ip)

    async def backup(self, node_ip: str, ks: str, tag: str, dest: str, bucket: str) -> str:
        """Backup keyspace's snapshot"""
        params = {"keyspace": ks,
                  "endpoint": dest,
                  "bucket": bucket,
                  "snapshot": tag}
        return await self.client.post_json(f"/storage_service/backup", host=node_ip, params=params)

    async def restore(self, node_ip: str, ks: str, cf: str, tag: str, dest: str, bucket: str) -> str:
        """Restore keyspace:table from backup"""
        params = {"keyspace": ks,
                  "table": cf,
                  "endpoint": dest,
                  "bucket": bucket,
                  "snapshot": tag}
        return await self.client.post_json(f"/storage_service/restore", host=node_ip, params=params)

    async def take_snapshot(self, node_ip: str, ks: str, tag: str) -> None:
        """Take keyspace snapshot"""
        params = { 'kn': ks, 'tag': tag }
        await self.client.post(f"/storage_service/snapshots", host=node_ip, params=params)

    async def cleanup_keyspace(self, node_ip: str, ks: str) -> None:
        """Cleanup keyspace"""
        await self.client.post(f"/storage_service/keyspace_cleanup/{ks}", host=node_ip)

    async def load_new_sstables(self, node_ip: str, keyspace: str, table: str, primary_replica : bool = False) -> None:
        """Load sstables from upload directory"""
        primary_replica_value = 'true' if primary_replica else 'false'
        await self.client.post(f"/storage_service/sstables/{keyspace}?cf={table}&primary_replica_only={primary_replica_value}", host=node_ip)

    async def drop_sstable_caches(self, node_ip: str) -> None:
        """Drop sstable caches"""
        await self.client.post(f"/system/drop_sstable_caches", host=node_ip)

    async def keyspace_flush(self, node_ip: str, keyspace: str, table: Optional[str] = None) -> None:
        """Flush the specified or all tables in the keyspace"""
        url = f"/storage_service/keyspace_flush/{keyspace}"
        if table is not None:
            url += f"?cf={table}"
        await self.client.post(url, host=node_ip)

    async def keyspace_compaction(self, node_ip: str, keyspace: str, table: Optional[str] = None) -> None:
        """Compact the specified or all tables in the keyspace"""
        url = f"/storage_service/keyspace_compaction/{keyspace}"
        params = {}
        if table is not None:
            params["cf"] = table
        await self.client.post(url, host=node_ip, params=params)

    async def dump_llvm_profile(self, node_ip : str):
        """Dump llvm profile to disk that can later be used for PGO or coverage reporting.
           no-op if the scylla binary is not instrumented."""
        url = "/system/dump_llvm_profile"
        await self.client.post(url, host=node_ip)

    async def upgrade_to_raft_topology(self, node_ip: str) -> None:
        """Start the upgrade to raft topology"""
        await self.client.post("/storage_service/raft_topology/upgrade", host=node_ip)

    async def raft_topology_upgrade_status(self, node_ip: str) -> str:
        """Returns the current state of upgrade to raft topology"""
        data = await self.client.get_json("/storage_service/raft_topology/upgrade", host=node_ip)
        assert(type(data) == str)
        return data

    async def get_raft_leader(self, node_ip: str, group_id: Optional[str] = None) -> HostID:
        """Returns host ID of the current leader of the given raft group as seen by the registry on the contact node.
           When group_id is not specified, group0 is used."""
        params = {}
        if group_id:
            params["group_id"] = group_id
        data = await self.client.get_json("/raft/leader_host", host=node_ip, params=params)
        return HostID(data)

    async def repair(self, node_ip: str, keyspace: str, table: str, ranges: str = '') -> None:
        """Repair the given table and wait for it to complete"""
        if ranges:
            params = {"columnFamilies": table, "ranges": ranges}
        else:
            params = {"columnFamilies": table}
        sequence_number = await self.client.post_json(f"/storage_service/repair_async/{keyspace}", host=node_ip, params=params)
        status = await self.client.get_json(f"/storage_service/repair_status", host=node_ip, params={"id": str(sequence_number)})
        if status != 'SUCCESSFUL':
            raise Exception(f"Repair id {sequence_number} on node {node_ip} for table {keyspace}.{table} failed: status={status}")

    def __get_autocompaction_url(self, keyspace: str, table: Optional[str] = None) -> str:
        """Return autocompaction url for the given keyspace/table"""
        return f"/storage_service/auto_compaction/{keyspace}" if not table else \
            f"/column_family/autocompaction/{keyspace}:{table}"

    async def enable_autocompaction(self, node_ip: str, keyspace: str, table: Optional[str] = None) -> None:
        """Enable autocompaction for the given keyspace/table"""
        await self.client.post(self.__get_autocompaction_url(keyspace, table), host=node_ip)

    async def disable_autocompaction(self, node_ip: str, keyspace: str, table: Optional[str] = None) -> None:
        """Disable autocompaction for the given keyspace/table"""
        await self.client.delete(self.__get_autocompaction_url(keyspace, table), host=node_ip)

    async def get_sstable_info(self, node_ip: str, keyspace: Optional[str] = None, table: Optional[str] = None):
        url = "/storage_service/sstable_info"
        params = []
        if keyspace:
            params.append(f"keyspace={keyspace}")
        if table:
            params.append(f"cf={table}")
        if params:
            url += f"?{'&'.join(params)}"

        data = await self.client.get_json(url, host=node_ip)
        assert(type(data) == list)
        return data

    async def get_task_status(self, node_ip: str, task_id: str):
        return await self.client.get_json(f'/task_manager/task_status/{task_id}', host=node_ip)

    async def wait_task(self, node_ip: str, task_id: str):
        return await self.client.get_json(f'/task_manager/wait_task/{task_id}', host=node_ip)

    async def abort_task(self, node_ip: str, task_id: str):
        await self.client.post(f'/task_manager/abort_task/{task_id}', host=node_ip)

class ScyllaMetrics:
    def __init__(self, lines: list[str]):
        self.lines: list[str] = lines

    def lines_by_prefix(self, prefix: str):
        """Returns all metrics whose name starts with a prefix, e.g.
           metrics.lines_by_prefix('scylla_hints_manager_')
        """
        return [l for l in self.lines if l.startswith(prefix)]

    def get(self, name: str, labels = None, shard: str ='total'):
        """Get the metric value by name. Allows to specify additional labels filter, e.g.
           metrics.get('scylla_transport_cql_errors_total', {'type': 'protocol_error'}).
           If shard is not set, returns the sum of metric values across all shards,
           otherwise returns the metric value from the specified shard.
        """
        result = None
        for l in self.lines:
            if not l.startswith(name):
                continue
            labels_start = l.find('{')
            labels_finish = l.find('}')
            if labels_start == -1 or labels_finish == -1:
                raise ValueError(f'invalid metric format [{l}]')
            def match_kv(kv):
                key, val = kv.split('=')
                val = val.strip('"')
                return shard == 'total' or val == shard if key == 'shard' \
                    else labels is None or labels.get(key, None) == val
            match = all(match_kv(kv) for kv in l[labels_start + 1:labels_finish].split(','))
            if match:
                value = float(l[labels_finish + 2:])
                if result is None:
                    result = value
                else:
                    result += value
                if shard != 'total':
                    break
        return result


class ScyllaMetricsClient:
    """Async Scylla Metrics API client"""

    def __init__(self, port: int = 9180):
        self.client = TCPRESTClient(port)

    async def query(self, server_ip: IPAddress) -> ScyllaMetrics:
        data = await self.client.get_text('/metrics', host=server_ip)
        return ScyllaMetrics(data.split('\n'))


class InjectionHandler():
    """An async client for communicating with injected code by REST API"""
    def __init__(self, api: ScyllaRESTAPIClient, injection: str, node_ip: str):
        self.api = api
        self.injection = injection
        self.node_ip = node_ip

    async def message(self) -> None:
        await self.api.message_injection(self.node_ip, self.injection)

@asynccontextmanager
async def inject_error(api: ScyllaRESTAPIClient, node_ip: IPAddress, injection: str,
                       parameters: dict[str, Any] = {}) -> AsyncIterator[InjectionHandler]:
    """Attempts to inject an error. Works only in specific build modes: debug,dev,sanitize.
       It will trigger a test to be skipped if attempting to enable an injection has no effect.
       This is a context manager for enabling and disabling when done, therefore it can't be
       used for one shot.
    """
    await api.enable_injection(node_ip, injection, False, parameters)
    enabled = await api.get_enabled_injections(node_ip)
    logging.info(f"Error injections enabled on {node_ip}: {enabled}")
    if not enabled:
        pytest.skip("Error injection not enabled in Scylla - try compiling in dev/debug/sanitize mode")
    try:
        yield InjectionHandler(api, injection, node_ip)
    finally:
        logger.info(f"Disabling error injection {injection}")
        await api.disable_injection(node_ip, injection)


async def inject_error_one_shot(api: ScyllaRESTAPIClient, node_ip: IPAddress, injection: str, parameters: dict[str, Any] = {}) -> InjectionHandler:
    """Attempts to inject an error. Works only in specific build modes: debug,dev,sanitize.
       It will trigger a test to be skipped if attempting to enable an injection has no effect.
       This is a one-shot injection enable.
    """
    await api.enable_injection(node_ip, injection, True, parameters)
    enabled = await api.get_enabled_injections(node_ip)
    logging.info(f"Error injections enabled on {node_ip}: {enabled}")
    if not enabled:
        pytest.skip("Error injection not enabled in Scylla - try compiling in dev/debug/sanitize mode")
    return InjectionHandler(api, injection, node_ip)


async def read_barrier(api: ScyllaRESTAPIClient, node_ip: IPAddress, group_id: Optional[str] = None) -> None:
    """ Issue a read barrier on the specific host for the group_id.

        :param api: the REST API client
        :param node_ip: the node IP address for which the read barrier will be posted
        :param group_id: the optional group id (default=group0)
    """
    params = {}
    if group_id:
        params["group_id"] = group_id

    await api.client.post("/raft/read_barrier", host=node_ip, params=params)


def get_host_api_address(host: Host) -> IPAddress:
    """ Returns the API address of the host.

        The API address can be different than the RPC (node) address under certain circumstances.
        In particular, in case the RPC address has been modified.
    """
    return host.listen_address if host.listen_address else host.address
