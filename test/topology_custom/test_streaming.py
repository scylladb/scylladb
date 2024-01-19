from test.pylib.manager_client import ManagerClient
import pytest

@pytest.mark.asyncio
async def test_drop_table_during_streaming_receiver_side(manager: ManagerClient):
    servers = [await manager.server_add(config={
        'error_injections_at_startup': ['stream_mutation_fragments_table_dropped'],
        'enable_repair_based_node_ops': False,
        'enable_user_defined_functions': False,
        'experimental_features': []
    }) for _ in range(2)]

@pytest.mark.asyncio
async def test_drop_table_during_streaming_sender_side(manager: ManagerClient):
    servers = [await manager.server_add(config={
        'error_injections_at_startup': ['stream_transfer_task_execute_table_dropped'],
        'enable_repair_based_node_ops': False,
        'enable_user_defined_functions': False,
        'experimental_features': []
    }) for _ in range(2)]
