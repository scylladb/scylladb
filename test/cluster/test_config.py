# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import aiohttp
import logging
import pytest
import requests
import sys
import threading
import time

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import read_barrier
from test.pylib.util import wait_for

async def wait_for_config(manager, server, config_name, value):
    async def config_value_equal():
        await read_barrier(manager.api, server.ip_addr)
        resp = await manager.api.get_config(server.ip_addr, config_name)
        logging.info(f"Obtained config via REST api - config_name={config_name} value={value}")
        if resp == value:
            return True
        return None
    await wait_for(config_value_equal, deadline=time.time() + 60)

@pytest.mark.asyncio
async def test_non_liveupdatable_config(manager):

    server = await manager.server_add()
    not_liveupdatable_param = "auto_bootstrap"
    liveupdatable_param = "compaction_enforce_min_threshold"

    logging.info("Verify initial (default) config values")
    await wait_for_config(manager, server, not_liveupdatable_param, True)
    await wait_for_config(manager, server, liveupdatable_param, False)

    logging.info("Change config values - verify only liveupdatable config is changed")
    await manager.server_update_config(server.server_id, not_liveupdatable_param, False)
    await manager.server_update_config(server.server_id, liveupdatable_param, True)
    await wait_for_config(manager, server, liveupdatable_param, True)
    await wait_for_config(manager, server, not_liveupdatable_param, True)


# Default Prometheus metrics port
PROMETHEUS_PORT = 9180

# Accept header for requesting Prometheus protobuf format with native histograms
PROMETHEUS_PROTOBUF_ACCEPT_HEADER = 'application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited'

@pytest.mark.asyncio
async def test_prometheus_allow_protobuf_default(manager):
    """
    Test that prometheus_allow_protobuf is enabled by default,
    while ensuring the configuration can be changed if needed.
    """
    logging.info("Starting server with default configuration")
    server = await manager.server_add()

    logging.info("Verify prometheus_allow_protobuf defaults to true")
    await wait_for_config(manager, server, "prometheus_allow_protobuf", True)

    logging.info("Test that the configuration can be explicitly disabled")
    server2 = await manager.server_add(config={'prometheus_allow_protobuf': False})
    await wait_for_config(manager, server2, "prometheus_allow_protobuf", False)

    logging.info("Test that the configuration can be explicitly enabled")
    server3 = await manager.server_add(config={'prometheus_allow_protobuf': True})
    await wait_for_config(manager, server3, "prometheus_allow_protobuf", True)

@pytest.mark.asyncio
async def test_prometheus_protobuf_native_histogram(manager):
    """
    Test that when prometheus_allow_protobuf is enabled, the server actually
    returns metrics in protobuf format with native histogram support when requested.
    """
    logging.info("Starting server with prometheus_allow_protobuf enabled")
    server = await manager.server_add(config={'prometheus_allow_protobuf': True})

    metrics_url = f"http://{server.ip_addr}:{PROMETHEUS_PORT}/metrics"

    logging.info(f"Requesting metrics in protobuf format from {metrics_url}")

    # Request metrics with Accept header for protobuf format
    headers = {
        'Accept': PROMETHEUS_PROTOBUF_ACCEPT_HEADER
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(metrics_url, headers=headers) as resp:
            assert resp.status == 200, f"Expected status 200, got {resp.status}"

            # Check that we got protobuf content type in response
            content_type = resp.headers.get('Content-Type', '')
            logging.info(f"Response Content-Type: {content_type}")

            # When protobuf is supported and requested, we should get protobuf back
            assert 'application/vnd.google.protobuf' in content_type, \
                f"Expected protobuf content type, got: {content_type}"

            # Read the response body
            body = await resp.read()

            # Verify we got non-empty protobuf data
            assert len(body) > 0, "Expected non-empty protobuf response"

            logging.info(f"Successfully received protobuf response with {len(body)} bytes")

    logging.info("Test that disabling prometheus_allow_protobuf prevents protobuf responses")
    server2 = await manager.server_add(config={'prometheus_allow_protobuf': False})
    metrics_url2 = f"http://{server2.ip_addr}:{PROMETHEUS_PORT}/metrics"

    async with aiohttp.ClientSession() as session:
        async with session.get(metrics_url2, headers=headers) as resp:
            assert resp.status == 200, "Fail reading metrics from {metrics_url2}"

            content_type = resp.headers.get('Content-Type', '')
            logging.info(f"Response Content-Type (protobuf disabled): {content_type}")

            # When protobuf is disabled, we should get text format even if requested
            # The server should return text/plain or not include protobuf in content-type
            assert 'application/vnd.google.protobuf' not in content_type, \
                f"Expected text format when protobuf disabled, got: {content_type}"

            logging.info("Confirmed that protobuf is not returned when disabled")
