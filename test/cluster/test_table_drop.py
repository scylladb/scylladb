import asyncio
import logging
import os
import shutil
from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient
from test.pylib.util import unique_name
import pytest

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_drop_table_during_streaming_receiver_side(manager: ManagerClient):
    servers = [await manager.server_add(config={
        'error_injections_at_startup': ['stream_mutation_fragments_table_dropped'],
        'enable_repair_based_node_ops': False,
        'enable_user_defined_functions': False,
        'tablets_mode_for_new_keyspaces': 'disabled'
    }) for _ in range(2)]

@pytest.mark.asyncio
async def test_drop_table_during_flush(manager: ManagerClient):
    servers = [await manager.server_add() for _ in range(2)]

    await manager.api.enable_injection(servers[0].ip_addr, "flush_tables_on_all_shards_table_drop", True)

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k%3});") for k in range(64)])
        await manager.api.keyspace_flush(servers[0].ip_addr, ks, "test")


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_drop_table_during_load_and_stream(manager: ManagerClient):
    """Verify that dropping a table while load_and_stream is in progress
    does not crash.  The stream_in_progress() phaser guard acquired in
    sstables_loader::load_and_stream keeps the table object alive until
    streaming completes, so table::stop() blocks until the guard is
    released — preventing a use-after-free on the replica::table&
    reference held by the streamer.

    Uses the 'load_and_stream_before_streaming_batch' error injection
    to pause load_and_stream inside the streaming loop (after the
    streamer is created and holds a replica::table& reference), then
    issues DROP TABLE concurrently and verifies both operations complete
    gracefully.

    A single node is sufficient: load_and_stream streams SSTables to
    the natural replicas (the local node in this case) via RPC.
    """
    server = await manager.server_add()

    cql = manager.get_cql()

    ks = unique_name("ks_")
    cf = "test"

    await cql.run_async(
        f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}")
    try:
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk int PRIMARY KEY, c int)")

        # Insert data and flush to create SSTables on disk
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.{cf} (pk, c) VALUES ({k}, {k})") for k in range(64)])
        await manager.api.flush_keyspace(server.ip_addr, ks)

        # Take snapshot so we have SSTables to copy into upload dir
        snap_name = unique_name("snap_")
        await manager.api.take_snapshot(server.ip_addr, ks, snap_name)

        # Copy snapshot SSTables into the upload directory.
        # load_and_stream will read these and stream to replicas.
        workdir = await manager.server_get_workdir(server.server_id)
        cf_dir = os.listdir(f"{workdir}/data/{ks}")[0]
        cf_path = os.path.join(f"{workdir}/data/{ks}", cf_dir)
        upload_dir = os.path.join(cf_path, "upload")
        os.makedirs(upload_dir, exist_ok=True)

        snapshots_dir = os.path.join(cf_path, "snapshots", snap_name)
        exclude_list = ["manifest.json", "schema.cql"]
        for item in os.listdir(snapshots_dir):
            if item not in exclude_list:
                shutil.copy2(os.path.join(snapshots_dir, item), os.path.join(upload_dir, item))

        # Enable injection that pauses load_and_stream inside the streaming
        # loop, after the streamer is created with a replica::table& reference.
        # one_shot=True: the test only needs one pause to demonstrate the race;
        # after firing once per shard the injection disables itself, avoiding
        # blocking on subsequent batches.
        await manager.api.enable_injection(server.ip_addr, "load_and_stream_before_streaming_batch", one_shot=True)
        server_log = await manager.server_open_log(server.server_id)
        log_mark = await server_log.mark()

        # Start load_and_stream in the background — it will pause at the injection.
        refresh_task = asyncio.create_task(
            manager.api.load_new_sstables(server.ip_addr, ks, cf, load_and_stream=True))

        # Wait until at least one shard hits the injection point
        await server_log.wait_for("load_and_stream_before_streaming_batch: waiting for message", from_mark=log_mark)
        logger.info("load_and_stream paused at injection point")

        # Drop the table while streaming is paused.  With the stream_in_progress
        # guard the DROP will block until the guard is released.
        drop_task = asyncio.ensure_future(cql.run_async(f"DROP TABLE {ks}.{cf}"))

        # Give the DROP a moment to be submitted and reach the server.
        # A log-based wait would be more robust but there is no dedicated log
        # message for "DROP blocked on phaser"; the sleep is acceptable here.
        await asyncio.sleep(1)

        # Release the injection — streaming resumes.
        # With the fix, the phaser guard keeps the table object alive and
        # streaming completes (or fails gracefully).  Without the fix,
        # the table is already destroyed and the node crashes
        # (use-after-free).
        await manager.api.message_injection(server.ip_addr, "load_and_stream_before_streaming_batch")
        logger.info("Released injection, waiting for load_and_stream to complete")

        # Wait for both operations with a timeout — if the node crashed the
        # REST call / CQL query will never return.
        refresh_error = None
        try:
            await asyncio.wait_for(refresh_task, timeout=30)
            logger.info("load_and_stream completed")
        except asyncio.TimeoutError:
            refresh_error = "load_and_stream timed out — node likely crashed"
            logger.info(refresh_error)
        except Exception as e:
            refresh_error = str(e)
            logger.info(f"load_and_stream finished with error: {e}")

        drop_error = None
        try:
            await asyncio.wait_for(drop_task, timeout=30)
            logger.info("DROP TABLE completed")
        except asyncio.TimeoutError:
            drop_error = "DROP TABLE timed out"
            logger.info(drop_error)
        except Exception as e:
            drop_error = str(e)
            logger.info(f"DROP TABLE finished with error: {e}")

        # The critical assertion: the node must still be alive.
        # Without the stream_in_progress() guard, the table is destroyed
        # while streaming holds a dangling reference, causing a crash
        # (SEGV or ASAN heap-use-after-free).
        crash_matches = await server_log.grep(
            r"Segmentation fault|AddressSanitizer|heap-use-after-free|ABORTING",
            from_mark=log_mark)
        assert not crash_matches, \
            "Node crashed during load_and_stream — " \
            "stream_in_progress() guard is needed to keep the table alive"

        # DROP TABLE must complete.
        assert not drop_error, f"DROP TABLE failed unexpectedly: {drop_error}"

        # load_and_stream may fail with a "column family not found" error:
        # database::drop_table() removes the table from metadata (so
        # find_column_family() fails) before cleanup_drop_table_on_all_shards()
        # awaits the phaser.  When streaming resumes and opens an RPC channel,
        # the receiver-side handler calls find_column_family() which throws.
        # This is the expected graceful failure — the important thing is
        # no crash (checked above).
        if refresh_error:
            assert "Can't find a column family" in refresh_error, \
                f"load_and_stream failed with unexpected error: {refresh_error}"
    finally:
        # Clean up keyspace if it still exists
        try:
            await cql.run_async(f"DROP KEYSPACE IF EXISTS {ks}")
        except Exception:
            pass
