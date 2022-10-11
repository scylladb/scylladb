#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""Asynchronous helper for Scylla REST API operations.
"""
import logging
import pytest
import os.path
import aiohttp
from typing import Optional
from contextlib import asynccontextmanager


logger = logging.getLogger(__name__)


class RESTSession:
    def __init__(self, connector: aiohttp.BaseConnector = None):
        self.session: aiohttp.ClientSession = aiohttp.ClientSession(connector = connector)

    async def close(self) -> None:
        """End session"""
        await self.session.close()

    async def get(self, resource_uri: str) -> aiohttp.ClientResponse:
        """Fetch remote resource or raise"""
        # Can raise exception. See https://docs.aiohttp.org/en/latest/web_exceptions.html
        resp = await self.session.get(resource_uri)
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"status code: {resp.status}, body text: {text}")
        return resp

    async def get_text(self, resource_uri: str) -> str:
        """Fetch remote resource text response or raise"""
        resp = await self.get(resource_uri)
        return await resp.text()

    async def post(self, resource_uri: str, params: Optional[dict[str, str]]) \
            -> aiohttp.ClientResponse:
        """Post to remote resource or raise"""
        resp = await self.session.post(resource_uri, params=params)
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"status code: {resp.status}, body text: {text}, "
                               f"resource {resource_uri} params {params}")
        return resp

    async def put_json(self, resource_uri: str, json: dict) \
            -> aiohttp.ClientResponse:
        """Put JSON"""
        resp = await self.session.request(method="PUT", url=resource_uri, json=json)
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"status code: {resp.status}, body text: {text}, "
                               f"resource {resource_uri} json {json}")
        return resp

    async def delete(self, resource_uri: str) -> aiohttp.ClientResponse:
        resp = await self.session.delete(url=resource_uri)
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"status code: {resp.status}, body text: {text}")
        return resp


class UnixRESTClient:
    """An async helper for REST API operations using AF_UNIX socket"""

    def __init__(self, sock_path: str):
        self.sock_name: str = os.path.basename(sock_path)
        self.session = RESTSession(aiohttp.UnixConnector(path=sock_path))

    async def close(self) -> None:
        """End session"""
        await self.session.close()

    async def get(self, resource: str) -> aiohttp.ClientResponse:
        return await self.session.get(self._resource_uri(resource))

    async def put_json(self, resource: str, json: dict) -> aiohttp.ClientResponse:
        """Put JSON"""
        return await self.session.put_json(self._resource_uri(resource), json=json)

    async def get_text(self, resource: str) -> str:
        """Fetch remote resource text response or raise"""
        return await self.session.get_text(self._resource_uri(resource))

    async def post(self, resource: str, params: Optional[dict[str, str]] = None) \
            -> aiohttp.ClientResponse:
        """Post to remote resource or raise"""
        return await self.session.post(self._resource_uri(resource), params)

    def _resource_uri(self, resource: str) -> str:
        # NOTE: using Python requests style URI for Unix domain sockets to avoid using "localhost"
        #       host parameter is ignored
        return f"http+unix://{self.sock_name}{resource}"


class TCPRESTClient:
    """An async helper for REST API operations"""

    def __init__(self, port: int):
        self.port: int = port
        self.session = RESTSession()

    async def close(self) -> None:
        """End session"""
        await self.session.close()

    async def get(self, resource: str, host: str) -> aiohttp.ClientResponse:
        return await self.session.get(self._resource_uri(resource, host))

    async def put_json(self, resource: str, host: str, json: dict) -> aiohttp.ClientResponse:
        """Put JSON"""
        return await self.session.put_json(self._resource_uri(resource, host), json=json)

    async def get_text(self, resource: str, host: str) -> str:
        """Fetch remote resource text response or raise"""
        return await self.session.get_text(self._resource_uri(resource, host))

    async def post(self, resource: str, host: str, params: Optional[dict[str, str]] = None) \
            -> aiohttp.ClientResponse:
        """Post to remote resource or raise"""
        return await self.session.post(self._resource_uri(resource, host), params)

    async def delete(self, resource: str, host: str) -> aiohttp.ClientResponse:
        return await self.session.delete(self._resource_uri(resource, host))

    def _resource_uri(self, resource: str, host: str) -> str:
        return f"http://{host}:{self.port}{resource}"


class ScyllaRESTAPIClient():
    """Async Scylla REST API client"""

    def __init__(self, port: int = 10000):
        self.client = TCPRESTClient(port)

    async def close(self):
        """Close session"""
        await self.client.close()

    async def get_host_id(self, server_id: str) -> str:
        """Get server id (UUID)"""
        host_uuid = await self.client.get_text("/storage_service/hostid/local", host=server_id)
        host_uuid = host_uuid.lstrip('"').rstrip('"')
        return host_uuid

    async def remove_node(self, initiator_ip: str, server_uuid: str, ignore_dead: list[str]) -> None:
        """Initiate remove node of server_uuid in initiator initiator_ip"""
        resp = await self.client.post("/storage_service/remove_node",
                               params={"host_id": server_uuid, "ignore_nodes": ",".join(ignore_dead)},
                               host=initiator_ip)
        logger.info("remove_node status %s for %s", resp.status, server_uuid)

    async def decommission_node(self, node_ip: str) -> None:
        """Initiate remove node of server_uuid in initiator initiator_ip"""
        resp = await self.client.post("/storage_service/decommission", host=node_ip)
        logger.debug("decommission_node status %s for %s", resp.status, node_ip)

    async def get_gossip_generation_number(self, node_ip: str, target_ip: str) -> int:
        """Get the current generation number of `target_ip` observed by `node_ip`."""
        resp = await self.client.get(f"/gossiper/generation_number/{target_ip}", host=node_ip)
        data = await resp.json()
        assert(type(data) == int)
        return data

    async def enable_injection(self, node_ip: str, injection: str, one_shot: bool) -> None:
        """Enable error injection named `injection` on `node_ip`. Depending on `one_shot`,
           the injection will be executed only once or every time the process passes the injection point.
           Note: this only has an effect in specific build modes: debug,dev,sanitize.
        """
        await self.client.post(f"/v2/error_injection/injection/{injection}",
                               host=node_ip, params={"one_shot": str(one_shot)})

    async def disable_injection(self, node_ip: str, injection: str) -> None:
        await self.client.delete(f"/v2/error_injection/injection/{injection}", host=node_ip)

    async def get_enabled_injections(self, node_ip: str) -> list[str]:
        resp = await self.client.get("/v2/error_injection/injection", host=node_ip)
        data = await resp.json()
        assert(type(data) == list)
        assert(type(e) == str for e in data)
        return data


@asynccontextmanager
async def inject_error(api: ScyllaRESTAPIClient, node_ip: str, injection: str, one_shot: bool):
    """Attempts to inject an error. Works only in specific build modes: debug,dev,sanitize.
       It will trigger a test to be skipped if attempting to enable an injection has no effect.
    """
    await api.enable_injection(node_ip, injection, one_shot)
    enabled = await api.get_enabled_injections(node_ip)
    logging.info(f"Error injections enabled on {node_ip}: {enabled}")
    if not enabled:
        pytest.skip("Error injection not enabled in Scylla - try compiling in dev/debug/sanitize mode")
    try:
        yield
    finally:
        logger.info(f"Disabling error injection {injection}")
        await api.disable_injection(node_ip, injection)
