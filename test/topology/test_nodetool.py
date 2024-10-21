import pytest
import asyncio
import subprocess
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_first_completed
from test.topology.conftest import skip_mode


async def validate_status_operation(result, live_eps, down_eps, leaving, zero_token_eps):
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

    visited_eps = set()
    for j in range(len(live_eps + down_eps)):
        i += 1
        assert lines[i] != ""
        pieces = tuple(lines[i].split())
        if len(pieces) == 8:
            status_state, ep, load, load_unit, tokens, owns, host_id, rack = pieces
        else:
            status_state, ep, load, tokens, owns, host_id, rack = pieces

        assert ep not in visited_eps
        visited_eps.add(ep)

        assert ep in (live_eps + down_eps)

        assert status_state[0] == ('U' if ep in live_eps else 'D')
        assert status_state[1] == ('L' if ep in leaving else 'N')

        if ep in zero_token_eps:
            assert int(tokens) == 0
        else:
            assert int(tokens) != 0

        assert host_id != ""
        assert rack == "rack1"

    i += 1
    assert lines[i] == ""


@pytest.mark.asyncio
async def test_zero_token_node_normal(manager: ManagerClient):
    zero_token_nodes = await manager.servers_add(servers_num=2, config={'join_ring': False})

    servers = await manager.running_servers()

    exe_path = await manager.server_get_exe(servers[0].server_id)
    cmd = [exe_path, "nodetool", "status"] + ["--logger-log-level",
                                              "scylla-nodetool=trace",
                                              "-h", servers[0].ip_addr]

    result = subprocess.run(cmd, capture_output=True, text=True)

    live_eps = await manager.api.client.get_json("/gossiper/endpoint/live", host=servers[0].ip_addr)
    down_eps = await manager.api.client.get_json("/gossiper/endpoint/down", host=servers[0].ip_addr)
    leaving = await manager.api.client.get_json("/storage_service/nodes/leaving", host=servers[0].ip_addr)

    server_eps = [s.ip_addr for s in servers]

    assert len(server_eps) == len(live_eps)
    for ep in live_eps:
        assert ep in server_eps
    assert down_eps == []
    assert leaving == []

    zero_token_eps = [z.ip_addr for z in zero_token_nodes]
    validate_status_operation(result.stdout, live_eps, down_eps, leaving, zero_token_eps)

    zero_token_cmd = [exe_path, "nodetool", "status"] + ["--logger-log-level",
                                                         "scylla-nodetool=trace",
                                                         "-h", zero_token_eps[0]]

    zero_token_result = subprocess.run(zero_token_cmd, capture_output=True, text=True)
    validate_status_operation(zero_token_result.stdout, live_eps, down_eps, leaving, zero_token_eps)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_zero_token_node_down_leaving(manager: ManagerClient):
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

    await wait_for_first_completed([log.wait_for("delay_node_removal: waiting for message") for log in logs])

    result = subprocess.run(cmd, capture_output=True, text=True)

    live_eps = await manager.api.client.get_json("/gossiper/endpoint/live", host=servers[0].ip_addr)
    down_eps = await manager.api.client.get_json("/gossiper/endpoint/down", host=servers[0].ip_addr)
    leaving = await manager.api.client.get_json("/storage_service/nodes/leaving", host=servers[0].ip_addr)

    server_eps = [s.ip_addr for s in servers]

    assert len(server_eps) == len(live_eps)
    for ep in live_eps:
        assert ep in server_eps
    assert down_eps == [zero_token_node.ip_addr]
    assert leaving == [zero_token_node.ip_addr]

    validate_status_operation(result.stdout, live_eps, down_eps, leaving, [zero_token_node.ip_addr])
    [await manager.api.message_injection(s.ip_addr, 'delay_node_removal') for s in servers]

    await task


@pytest.mark.asyncio
async def test_zero_token_node_down_normal(manager: ManagerClient):
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

    server_eps = [s.ip_addr for s in servers]

    assert len(server_eps) == len(live_eps)
    for ep in live_eps:
        assert ep in server_eps
    assert down_eps == [zero_token_node.ip_addr]
    assert leaving == []

    validate_status_operation(result.stdout, live_eps, down_eps, leaving, [zero_token_node.ip_addr])


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_regular_node_joining(manager: ManagerClient):
    servers = await manager.running_servers()
    [await manager.api.enable_injection(s.ip_addr, 'delay_node_bootstrap', one_shot=True) for s in servers]

    task = asyncio.create_task(manager.server_add())

    exe_path = await manager.server_get_exe(servers[0].server_id)
    cmd = [exe_path, "nodetool", "status"] + ["--logger-log-level",
                                              "scylla-nodetool=trace",
                                              "-h", servers[0].ip_addr]

    logs = [await manager.server_open_log(srv.server_id) for srv in servers]

    await wait_for_first_completed([log.wait_for("delay_node_bootstrap: waiting for message") for log in logs])
    result = subprocess.run(cmd, capture_output=True, text=True)

    live_eps = await manager.api.client.get_json("/gossiper/endpoint/live", host=servers[0].ip_addr)
    down_eps = await manager.api.client.get_json("/gossiper/endpoint/down", host=servers[0].ip_addr)
    leaving = await manager.api.client.get_json("/storage_service/nodes/leaving", host=servers[0].ip_addr)

    assert len(live_eps) == 4
    assert down_eps == []
    assert leaving == []

    validate_status_operation(result.stdout, live_eps, down_eps, leaving, [])
    [await manager.api.message_injection(s.ip_addr, 'delay_node_bootstrap') for s in servers]

    await task
