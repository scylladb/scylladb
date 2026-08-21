#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import aiohttp
import logging
import pytest

from test.cluster.test_config import wait_for_config

# Default Prometheus metrics port
PROMETHEUS_PORT = 9180

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

# Accept header for requesting Prometheus protobuf format with native histograms
PROMETHEUS_PROTOBUF_ACCEPT_HEADER = 'application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited'

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

    headers = {
        'Accept': PROMETHEUS_PROTOBUF_ACCEPT_HEADER
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(metrics_url, headers=headers) as resp:
            assert resp.status == 200

            # When protobuf is supported and requested, we should get protobuf back
            content_type = resp.headers.get('Content-Type', '')
            logging.info(f"Response Content-Type: {content_type}")
            assert 'application/vnd.google.protobuf' in content_type

            body = await resp.read()
            assert len(body) > 0

            logging.info(f"Successfully received protobuf response with {len(body)} bytes")

    logging.info("Test that disabling prometheus_allow_protobuf prevents protobuf responses")
    server2 = await manager.server_add(config={'prometheus_allow_protobuf': False})
    metrics_url2 = f"http://{server2.ip_addr}:{PROMETHEUS_PORT}/metrics"

    async with aiohttp.ClientSession() as session:
        async with session.get(metrics_url2, headers=headers) as resp:
            assert resp.status == 200

            # When protobuf is disabled, we should get text format even if requested
            content_type = resp.headers.get('Content-Type', '')
            logging.info(f"Response Content-Type (protobuf disabled): {content_type}")
            assert 'application/vnd.google.protobuf' not in content_type

            logging.info("Confirmed that protobuf is not returned when disabled")
