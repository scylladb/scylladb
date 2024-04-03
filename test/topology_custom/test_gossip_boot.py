import asyncio
import pytest

from test.pylib.manager_client import ManagerClient
from test.topology.conftest import skip_mode

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_gossip_boot(manager: ManagerClient):
    """
    Regression test for scylladb/scylladb#17493.
    """

    cfg = {'error_injections_at_startup': ['gossiper_replicate_sleep'],
           'experimental_features': list[str](),
           'enable_user_defined_functions': False}

    servers = [await manager.server_add(config=cfg) for _ in range(3)]
    logs = [await manager.server_open_log(s.server_id) for s in servers]

    for log in logs:
        for s in servers:
            await log.wait_for(f'handle_state_normal for {s.ip_addr}.*finished', timeout=60)
