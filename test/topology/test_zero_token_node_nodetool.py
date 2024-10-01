import subprocess
import asyncio
from collections.abc import Coroutine
from test.pylib.manager_client import ManagerClient
import pytest


@pytest.mark.asyncio
async def validate_status_operation(result, live_eps, down_eps, leaving):
    """Validates the nodetool status output"""

    lines = result.split('\n')
    i = 0
    dc_line = lines[i]
    assert dc_line.startswith("Datacenter:")
    result_dc = dc_line.split()[1]
    assert result_dc == "datacenter1"
    dc_line_len = len(dc_line)

    i += 1
    assert lines[i] == "=" * dc_line_len

    i += 1
    assert lines[i] == "Status=Up/Down"

    i += 1
    assert lines[i] == "|/ State=Normal/Leaving/Joining/Moving"

    i += 1
    assert lines[i].split() == ["--", "Address", "Load", "Tokens", "Owns", "Host", "ID", "Rack"]

    for j in range(len(live_eps + down_eps)):
        i += 1
        assert lines[i] != ""
        status_state, ep = tuple(lines[i].split())[:2]

        if ep in live_eps:
            assert status_state[0] == 'U'
        else:
            assert status_state[0] == 'D'

        if ep in leaving:
            assert status_state[1] == 'L'
        else:
            assert status_state[1] == 'N'


async def wait_for_first_completed(coros: list[Coroutine]):
    done, pending = await asyncio.wait([asyncio.create_task(c) for c in coros], return_when=asyncio.FIRST_COMPLETED)
    for t in pending:
        t.cancel()
    for t in done:
        await t


@pytest.mark.asyncio
async def test_zero_token_node_normal(manager: ManagerClient):
    await manager.servers_add(3)
    servers = await manager.running_servers()

    await manager.servers_add(servers_num=2, config={'join_ring': False})

    exe_path = await manager.server_get_exe(servers[0].server_id)
    cmd = [exe_path, "nodetool", "status"] + ["--logger-log-level",
                                              "scylla-nodetool=trace",
                                              "-h", servers[0].ip_addr]

    result = subprocess.run(cmd, capture_output=True, text=True)

    live_eps = await manager.api.client.get_json("/gossiper/endpoint/live", host=servers[0].ip_addr)
    down_eps = await manager.api.client.get_json("/gossiper/endpoint/down", host=servers[0].ip_addr)
    leaving = await manager.api.client.get_json("/storage_service/nodes/leaving", host=servers[0].ip_addr)

    await validate_status_operation(result.stdout, live_eps, down_eps, leaving)


@pytest.mark.asyncio
async def test_zero_token_node_down_leaving(manager: ManagerClient):
    await manager.servers_add(3)
    servers = await manager.running_servers()
    [await manager.api.enable_injection(s.ip_addr, 'delay_node_removal', one_shot=True) for s in servers]

    zero_token_node = await manager.server_add(config={'join_ring': False})
    await manager.server_stop_gracefully(zero_token_node.server_id)
    task = asyncio.create_task(manager.remove_node(servers[0].server_id, zero_token_node.server_id))

    exe_path = await manager.server_get_exe(servers[0].server_id)
    cmd = [exe_path, "nodetool", "status"] + ["--logger-log-level",
                                              "scylla-nodetool=trace",
                                              "-h", servers[0].ip_addr]

    logs = [await manager.server_open_log(srv.server_id) for srv in servers]

    await wait_for_first_completed([log.wait_for("Waiting for node removal") for log in logs])

    result = subprocess.run(cmd, capture_output=True, text=True)

    post_wait_servers = await manager.running_servers()
    live_eps = await manager.api.client.get_json("/gossiper/endpoint/live", host=servers[0].ip_addr)
    down_eps = await manager.api.client.get_json("/gossiper/endpoint/down", host=servers[0].ip_addr)
    leaving = await manager.api.client.get_json("/storage_service/nodes/leaving", host=servers[0].ip_addr)

    await validate_status_operation(result.stdout, live_eps, down_eps, leaving)
    [await manager.api.message_injection(s.ip_addr, 'delay_node_removal') for s in post_wait_servers]

    await task


@pytest.mark.asyncio
async def test_zero_token_node_down_normal(manager: ManagerClient):
    await manager.servers_add(3)
    servers = await manager.running_servers()

    zero_token_node = await manager.server_add(config={'join_ring': False})
    await manager.server_stop_gracefully(zero_token_node.server_id)

    exe_path = await manager.server_get_exe(servers[0].server_id)
    cmd = [exe_path, "nodetool", "status"] + ["--logger-log-level",
                                              "scylla-nodetool=trace",
                                              "-h", servers[0].ip_addr]

    result = subprocess.run(cmd, capture_output=True, text=True)

    live_eps = await manager.api.client.get_json("/gossiper/endpoint/live", host=servers[0].ip_addr)
    down_eps = await manager.api.client.get_json("/gossiper/endpoint/down", host=servers[0].ip_addr)
    leaving = await manager.api.client.get_json("/storage_service/nodes/leaving", host=servers[0].ip_addr)

    await validate_status_operation(result.stdout, live_eps, down_eps, leaving)
