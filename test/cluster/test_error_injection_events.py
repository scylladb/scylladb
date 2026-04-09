#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
"""
Test for error injection event stream functionality.

This test demonstrates the new SSE-based error injection event system
that eliminates the need for log parsing in tests.
"""
import asyncio
import logging
import pytest

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import injection_event_stream

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.skip_mode('release', 'error injections are not supported in release mode')
async def test_injection_event_stream_basic(manager: ManagerClient):
    """
    Test basic error injection event stream functionality.
    
    This test verifies that:
    1. We can connect to the SSE event stream
    2. Events are received when injections are triggered
    3. We can wait for specific injections without log parsing
    """
    servers = await manager.servers_add(1)
    server_ip = servers[0].ip_addr
    
    # Connect to the injection event stream
    async with injection_event_stream(server_ip) as event_stream:
        logger.info("Connected to injection event stream")
        
        # Enable a simple injection
        test_injection_name = "test_injection_event_basic"
        await manager.api.enable_injection(server_ip, test_injection_name, one_shot=True)
        
        # Trigger the injection by calling message_injection
        # In real tests, the injection would be triggered by actual code execution
        await manager.api.message_injection(server_ip, test_injection_name)
        
        # Wait for the injection event (no log parsing needed!)
        try:
            event = await event_stream.wait_for_injection(test_injection_name, timeout=10.0)
            logger.info(f"Received injection event: {event}")
            
            # Verify event structure
            assert event['injection'] == test_injection_name
            assert 'type' in event
            assert 'shard' in event
            logger.info(f"✓ Injection triggered on shard {event['shard']} with type {event['type']}")
        except asyncio.TimeoutError:
            pytest.fail(f"Injection event for '{test_injection_name}' not received within timeout")


@pytest.mark.asyncio
@pytest.mark.skip_mode('release', 'error injections are not supported in release mode')
async def test_injection_event_stream_multiple_injections(manager: ManagerClient):
    """
    Test that we can track multiple injections via the event stream.
    """
    servers = await manager.servers_add(1)
    server_ip = servers[0].ip_addr
    
    async with injection_event_stream(server_ip) as event_stream:
        logger.info("Connected to injection event stream")
        
        # Enable multiple injections
        injection_names = [
            "test_injection_1",
            "test_injection_2",
            "test_injection_3",
        ]
        
        for name in injection_names:
            await manager.api.enable_injection(server_ip, name, one_shot=False)
        
        # Trigger injections in sequence
        for name in injection_names:
            await manager.api.message_injection(server_ip, name)
            
            # Wait for each injection event
            event = await event_stream.wait_for_injection(name, timeout=10.0)
            logger.info(f"✓ Received event for {name}: type={event['type']}, shard={event['shard']}")
        
        # Cleanup
        for name in injection_names:
            await manager.api.disable_injection(server_ip, name)
        
        logger.info("✓ All injection events received successfully")


@pytest.mark.asyncio
@pytest.mark.skip_mode('release', 'error injections are not supported in release mode')
async def test_injection_event_vs_log_parsing_comparison(manager: ManagerClient):
    """
    Demonstration test comparing the old log parsing approach vs new event stream approach.
    
    This shows how the new SSE event stream eliminates the need for log parsing,
    making tests faster and more reliable.
    """
    servers = await manager.servers_add(1)
    server = servers[0]
    
    injection_name = "test_comparison_injection"
    
    # OLD APPROACH: Log parsing (commented to show the pattern)
    # -----------------------------------------------------
    # log = await manager.server_open_log(server.server_id)
    # mark = await log.mark()
    # await manager.api.enable_injection(server.ip_addr, injection_name, one_shot=True)
    # # ... trigger some operation that hits the injection ...
    # mark, _ = await log.wait_for(f'{injection_name}: waiting', from_mark=mark)
    # # Now we know the injection was hit by parsing logs
    # -----------------------------------------------------
    
    # NEW APPROACH: Event stream (no log parsing!)
    # -----------------------------------------------------
    async with injection_event_stream(server.ip_addr) as event_stream:
        logger.info("✓ Connected to injection event stream (no log parsing needed)")
        
        # Enable and trigger injection
        await manager.api.enable_injection(server.ip_addr, injection_name, one_shot=True)
        await manager.api.message_injection(server.ip_addr, injection_name)
        
        # Wait for injection event - fast and reliable!
        event = await event_stream.wait_for_injection(injection_name, timeout=10.0)
        logger.info(f"✓ Injection detected via event stream: {event}")
        
        # No log parsing, no regex matching, no waiting for log flushes
        # Just direct event notification from the injection point
    # -----------------------------------------------------
    
    logger.info("✓ New event stream approach is faster and more reliable than log parsing!")
