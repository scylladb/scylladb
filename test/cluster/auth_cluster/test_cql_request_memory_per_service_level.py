#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
End-to-end tests for the per-service-level split of the CQL coordinator's
request admission memory budget (service/memory_limiter.{hh,cc},
service/request_memory_limiter.{hh,cc}).

The budget - ``cql_request_memory_fraction`` of the shard's memory - is divided
between tenants, one per scheduling group and therefore one per service level:

  - a dedicated share per service level, split in proportion to its ``SHARES``,
    which no other service level can take;
  - a shared pool, ``cql_request_memory_shared_pool_fraction`` of the budget,
    that any service level may borrow from once its own share runs out.

The tests drive that from the outside: real roles on real service levels, real
CQL traffic, and the ``scylla_transport_cql_requests_*`` metrics as the oracle.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager

import pytest
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import WhiteListRoundRobinPolicy

from test.cluster.auth_cluster import extra_scylla_config_options as auth_config
from test.cluster.util import new_test_keyspace, new_test_table
from test.pylib.driver_utils import safe_driver_shutdown
from test.pylib.rest_client import ScyllaMetrics, ScyllaMetricsLine, get_host_api_address, read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import unique_name, wait_for

logger = logging.getLogger(__name__)


MEMORY_FRACTION = "cql_request_memory_fraction"
SHARED_POOL_FRACTION = "cql_request_memory_shared_pool_fraction"

# service::memory_limiter::budget() clamps the fraction to [0.01, 1.0], so 0.01
# is the smallest budget a node can be started with. A small budget is what makes
# it possible to saturate a service level with a handful of requests.
SMALLEST_MEMORY_FRACTION = 0.01

# Per-service-level gauges/counters, labelled with scheduling_group_name.
TOTAL = "scylla_transport_cql_requests_memory_total"
AVAILABLE = "scylla_transport_cql_requests_memory_available"
BORROWED = "scylla_transport_cql_requests_memory_borrowed_from_shared_pool"
BLOCKED_CURRENT = "scylla_transport_cql_requests_blocked_memory_current"
BLOCKED_TOTAL = "scylla_transport_cql_requests_blocked_memory"
# Shared pool, not labelled with a scheduling group.
POOL_TOTAL = "scylla_transport_cql_requests_shared_pool_total_memory"
POOL_AVAILABLE = "scylla_transport_cql_requests_shared_pool_available_memory"
POOL_WAITING = "scylla_transport_cql_requests_shared_pool_waiting_service_levels"

SHARD_MEMORY = "scylla_memory_total_memory"

# main.cc: default_service_level_configuration.shares
DEFAULT_SL_SHARES = 1000

# transport/server.cc: mem_estimate = 2 * frame_length + 8000
REQUEST_MEMORY_OVERHEAD = 8000

# Every wait gets a deadline so that a regression fails instead of hanging.
TIMEOUT = 120

# How many /metrics samples the flood has to stay blocked for, so that "the other
# service level was never blocked" is backed by more than a single reading.
SUSTAINED_SAMPLES = 50


#
# Metric plumbing
#

def _metric(metrics: ScyllaMetrics, name: str, labels: dict | None = None) -> float | None:
    """Sum the series named exactly `name`, or None when there is no such series.

    ScyllaMetrics.get() matches the name by prefix, which would conflate
    ..._blocked_memory (a counter) with ..._blocked_memory_current (a gauge).
    """
    labels = labels or {}
    total = None
    for line in metrics.lines_by_prefix(name):
        parsed = ScyllaMetricsLine.from_string(line)
        if parsed is None or parsed.name != name:
            continue
        if not all(parsed.labels.get(k) == str(v) for k, v in labels.items()):
            continue
        total = parsed.value if total is None else total + parsed.value
    return total


def _sl_metric(metrics: ScyllaMetrics, name: str, sl: str) -> float:
    """Value of `name` for service level `sl`, 0 when the series is not there."""
    return _metric(metrics, name, {"scheduling_group_name": f"sl:{sl}"}) or 0.0


def _per_sl(metrics: ScyllaMetrics, name: str) -> dict[str, float]:
    """{scheduling_group_name: value} for every series of `name`."""
    out: dict[str, float] = {}
    for line in metrics.lines_by_prefix(name):
        parsed = ScyllaMetricsLine.from_string(line)
        if parsed is None or parsed.name != name:
            continue
        sg = parsed.labels.get("scheduling_group_name")
        if sg is None:
            continue
        out[sg] = out.get(sg, 0.0) + parsed.value
    return out


async def _metrics(manager: ScyllaClusterManager, ip: str) -> ScyllaMetrics:
    return await manager.metrics.query(ip)


def _budget(metrics: ScyllaMetrics) -> float:
    """The whole request memory budget, as the node itself accounts for it.

    memory_limiter::adjust() hands the dedicated portion to the tenants and the
    rest to the shared pool, so the two always add up to the budget.
    """
    return sum(_per_sl(metrics, TOTAL).values()) + (_metric(metrics, POOL_TOTAL) or 0.0)


#
# Cluster / service level scaffolding
#

async def _start_node(manager: ScyllaClusterManager, *, shared_pool_fraction: float,
                      memory_fraction: float = SMALLEST_MEMORY_FRACTION):
    """A single-shard node whose CQL request memory budget is small enough to saturate."""
    config = {
        **auth_config,
        # MustRestart, so it has to go in through scylla.yaml at startup.
        MEMORY_FRACTION: memory_fraction,
        SHARED_POOL_FRACTION: shared_pool_fraction,
    }
    server = await manager.server_add(config=config, cmdline=["--smp", "1"])
    cql, hosts = await manager.get_ready_cql([server])
    return server, cql, hosts[0]


async def _create_service_level(cql, host, name: str, shares: int) -> None:
    await cql.run_async(f"CREATE SERVICE LEVEL {name} WITH SHARES = {shares}", host=host)


async def _create_role_on_service_level(manager: ScyllaClusterManager, cql, host,
                                        role: str, sl: str) -> None:
    # Superuser so that the role can read the test table without the permissions
    # cache getting in the way; this test is about memory, not about auth.
    await cql.run_async(
        f"CREATE ROLE {role} WITH PASSWORD = '{role}' AND LOGIN = true AND SUPERUSER = true",
        host=host)
    await cql.run_async(f"ATTACH SERVICE LEVEL {sl} TO {role}", host=host)
    await read_barrier(manager.api, get_host_api_address(host))


async def _open_role_session(manager: ScyllaClusterManager, server, role: str):
    """A session authenticated as `role`, so its connections land on the role's
    service level's scheduling group and charge that service level's tenant."""
    # A load balancing policy of its own: con_gen()'s default is one shared
    # RoundRobinPolicy instance, and these tests open a good few clusters.
    cluster = manager.con_gen([server.ip_addr], manager.port, manager.use_ssl,
                              PlainTextAuthProvider(username=role, password=role),
                              WhiteListRoundRobinPolicy([server.ip_addr]))
    # The memory estimate is computed from the frame length *on the wire*
    # (transport/server.cc reads f.length before decompressing), so compression
    # would shrink the flood's frames and with them the memory they are charged.
    cluster.compression = False
    cluster.schema_metadata_enabled = False
    cluster.token_metadata_enabled = False
    session = await asyncio.to_thread(cluster.connect)
    return cluster, session


async def _wait_for_service_level_tenant(manager: ScyllaClusterManager, ip: str, sl: str) -> float:
    """Wait until the service level has a tenant with a dedicated share, and return it."""
    async def has_share():
        metrics = await _metrics(manager, ip)
        total = _metric(metrics, TOTAL, {"scheduling_group_name": f"sl:{sl}"})
        return total if total else None
    return await wait_for(has_share, time.time() + TIMEOUT, label=f"tenant for sl:{sl}")


#
# Load generators
#

async def _sample_metrics(manager: ScyllaClusterManager, ip: str, stop: asyncio.Event,
                          on_sample, period: float = 0.05) -> None:
    """Poll /metrics while load is running.

    The interesting gauges (blocked_memory_current, borrowed_from_shared_pool)
    drop back to 0 as soon as the memory changes hands, so they have to be
    sampled in flight - reading them once the load is over proves nothing.
    """
    while not stop.is_set():
        try:
            metrics = await _metrics(manager, ip)
        except Exception:
            logger.warning("metrics query failed", exc_info=True)
            await asyncio.sleep(0.1)
            continue
        on_sample(metrics)
        if period:
            await asyncio.sleep(period)


async def _run_queries(session, statement, params, stop: asyncio.Event,
                       state: dict, name: str) -> None:
    """Run `statement` back to back until `stop`, counting successes and failures."""
    while not stop.is_set():
        try:
            await session.run_async(statement, params)
            state[f"{name}_done"] += 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            state[f"{name}_failed"] += 1
            state[f"{name}_error"] = exc
            logger.info("%s query failed: %s", name, exc)
            # Don't spin if the failure is immediate and permanent.
            await asyncio.sleep(0.05)


def _flood_blob_size(memory_target: float) -> int:
    """Payload size for a request that should take `memory_target` bytes of budget.

    transport/server.cc charges 2 * frame_length + 8000 per request.
    """
    return max(4096, min(4 * 1024 * 1024, int((memory_target - REQUEST_MEMORY_OVERHEAD) / 2)))


@asynccontextmanager
async def _flood(manager: ScyllaClusterManager, server, table: str, role: str,
                 memory_target: float, *, connections: int = 8, per_connection: int = 12):
    """Flood `role`'s service level with large concurrent requests.

    Admission is serialised per connection - the CQL server reads a frame, waits
    for memory, then reads the body - so the flood needs several connections to
    keep several requests sitting in the admission queue at once. Each request is
    sized to take `memory_target` bytes of the budget.

    The requests are filtering scans of an empty table: the frames are large, so
    they cost real admission memory, but the queries themselves neither write
    data nor read any, which keeps the node from being swamped by anything other
    than the memory limit under test.
    """
    blob_size = _flood_blob_size(memory_target)
    payload = os.urandom(blob_size)
    estimate = 2 * blob_size + REQUEST_MEMORY_OVERHEAD
    logger.info("flooding sl of role %s: %d connections x %d requests of ~%d bytes "
                "(~%d bytes of budget each)",
                role, connections, per_connection, blob_size, estimate)

    clusters = []
    sessions = []
    state = {"flood_done": 0, "flood_failed": 0, "flood_error": None}
    stop = asyncio.Event()
    tasks: list[asyncio.Task] = []
    try:
        for _ in range(connections):
            cluster, session = await _open_role_session(manager, server, role)
            clusters.append(cluster)
            sessions.append(session)
        query = f"SELECT pk FROM {table} WHERE v = ? ALLOW FILTERING"
        # Prepared per session, because a PreparedStatement belongs to the
        # Cluster that produced it - and prepared everywhere before the flood
        # starts, so that a PREPARE does not have to queue behind it.
        statements = [await asyncio.to_thread(session.prepare, query) for session in sessions]
        for session, select in zip(sessions, statements):
            for _ in range(per_connection):
                tasks.append(asyncio.create_task(
                    _run_queries(session, select, [payload], stop, state, "flood")))
        yield state, stop, estimate
    finally:
        stop.set()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for cluster in clusters:
            safe_driver_shutdown(cluster)
        logger.info("flood finished: %d requests done, %d failed",
                    state["flood_done"], state["flood_failed"])


@asynccontextmanager
async def _light_load(session, query: str, state: dict, name: str, concurrency: int = 2):
    """A trickle of small queries, to keep requests in flight while the
    configuration or the set of service levels changes underneath them."""
    state.setdefault(f"{name}_done", 0)
    state.setdefault(f"{name}_failed", 0)
    state.setdefault(f"{name}_error", None)
    # Prepared, so that the request's memory estimate is just the frame plus the
    # fixed overhead: an unprepared QUERY also gets charged the query parser's
    # cost estimate, which would make the estimate depend on unrelated traffic.
    statement = await asyncio.to_thread(session.prepare, query)
    stop = asyncio.Event()
    tasks = [asyncio.create_task(_run_queries(session, statement, [], stop, state, name))
             for _ in range(concurrency)]
    try:
        yield stop
    finally:
        stop.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _advanced_by(state: dict, name: str, baseline: int, count: int):
    done = state[f"{name}_done"] - baseline
    return done if done >= count else None


#
# 1. Isolation: a flood in one service level must not touch another's share.
#

async def test_flood_in_one_service_level_does_not_block_another(manager: ScyllaClusterManager):
    """The headline property: with a strictly private budget per service level
    (shared pool fraction 0), flooding one service level until its requests
    queue for memory must leave another service level completely unthrottled.

    Sampled while the flood runs, because blocked_memory_current is a gauge that
    falls back to 0 the moment the pressure goes away.
    """
    server, cql, host = await _start_node(manager, shared_pool_fraction=0.0)
    ip = server.ip_addr

    flood_sl, quiet_sl = f"sl_flood_{unique_name()}", f"sl_quiet_{unique_name()}"
    flood_role, quiet_role = f"r_flood_{unique_name()}", f"r_quiet_{unique_name()}"
    # Equal shares: the two service levels are entitled to exactly as much
    # memory as each other, and small shares keep those shares easy to saturate.
    shares = 10
    await _create_service_level(cql, host, flood_sl, shares)
    await _create_service_level(cql, host, quiet_sl, shares)
    await _create_role_on_service_level(manager, cql, host, flood_role, flood_sl)
    await _create_role_on_service_level(manager, cql, host, quiet_role, quiet_sl)

    flood_share = await _wait_for_service_level_tenant(manager, ip, flood_sl)
    quiet_share = await _wait_for_service_level_tenant(manager, ip, quiet_sl)
    metrics = await _metrics(manager, ip)
    budget = _budget(metrics)
    assert (_metric(metrics, POOL_TOTAL) or 0) == 0, \
        "cql_request_memory_shared_pool_fraction=0 must leave the shared pool empty"
    assert flood_share == pytest.approx(quiet_share, rel=0.01, abs=8), \
        f"equal shares must get equal budgets, got {flood_share} and {quiet_share}"
    logger.info("budget=%d, private budget per service level: %d bytes each",
                budget, flood_share)

    observed = {
        "flood_blocked_peak": 0.0,
        "flood_available_min": flood_share,
        "quiet_available_min": quiet_share,
        "quiet_blocked_peak": 0.0,
        "quiet_blocked_while_flood_blocked": 0.0,
        "flood_borrowed_peak": 0.0,
        "samples": 0,
        "samples_with_flood_blocked": 0,
        "quiet_done_when_flood_blocked": None,
    }
    quiet_state = {"quiet_done": 0, "quiet_failed": 0, "quiet_error": None}
    stop_sampler = asyncio.Event()
    sampler = None

    def on_sample(metrics: ScyllaMetrics) -> None:
        flood_blocked = _sl_metric(metrics, BLOCKED_CURRENT, flood_sl)
        quiet_blocked = _sl_metric(metrics, BLOCKED_CURRENT, quiet_sl)
        observed["samples"] += 1
        observed["flood_blocked_peak"] = max(observed["flood_blocked_peak"], flood_blocked)
        observed["quiet_blocked_peak"] = max(observed["quiet_blocked_peak"], quiet_blocked)
        observed["flood_borrowed_peak"] = max(
            observed["flood_borrowed_peak"], _sl_metric(metrics, BORROWED, flood_sl))
        observed["flood_available_min"] = min(
            observed["flood_available_min"], _sl_metric(metrics, AVAILABLE, flood_sl))
        if flood_blocked > 0:
            observed["samples_with_flood_blocked"] += 1
            observed["quiet_blocked_while_flood_blocked"] = max(
                observed["quiet_blocked_while_flood_blocked"], quiet_blocked)
            # The other service level's own share must still be there for it,
            # not handed over to the flood.
            observed["quiet_available_min"] = min(
                observed["quiet_available_min"], _sl_metric(metrics, AVAILABLE, quiet_sl))
            if observed["quiet_done_when_flood_blocked"] is None:
                observed["quiet_done_when_flood_blocked"] = quiet_state["quiet_done"]

    async with new_test_keyspace(manager, "WITH REPLICATION = {'replication_factor': 1}", host) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, v blob", host=host) as tbl:
            quiet_cluster, quiet_session = await _open_role_session(manager, server, quiet_role)
            try:
                async with _light_load(quiet_session, "SELECT key FROM system.local",
                                       quiet_state, "quiet", concurrency=1):
                    # Each flood request is sized to take most of the flooded
                    # service level's own share, so that a second one does not
                    # fit and the remaining connections queue for memory.
                    async with _flood(manager, server, tbl, flood_role, 0.7 * flood_share) \
                            as (flood_state, _flood_stop, estimate):
                        assert estimate < budget, \
                            f"a request of {estimate} bytes would be rejected outright: " \
                            f"{MEMORY_FRACTION} also caps a single request, at {budget} bytes"
                        if estimate >= flood_share:
                            logger.info("a single flood request (%d bytes) outgrows the whole "
                                        "share (%d bytes), so it is admitted only when the "
                                        "tenant is idle", estimate, flood_share)
                        sampler = asyncio.create_task(
                            _sample_metrics(manager, ip, stop_sampler, on_sample))

                        # The flooded service level must end up queueing requests
                        # for memory.
                        async def flood_is_blocked():
                            return observed["flood_blocked_peak"] or None
                        await wait_for(flood_is_blocked, time.time() + TIMEOUT,
                                       label=f"sl:{flood_sl} blocked on memory")
                        logger.info("sl:%s peak requests blocked on memory: %d",
                                    flood_sl, observed["flood_blocked_peak"])

                        # Keep the flood queued for a while, so that "the other
                        # service level was never blocked" rests on a few
                        # hundred milliseconds' worth of samples rather than one.
                        async def flood_sustained():
                            blocked = observed["samples_with_flood_blocked"]
                            return blocked if blocked >= SUSTAINED_SAMPLES else None
                        await wait_for(flood_sustained, time.time() + TIMEOUT,
                                       label=f"sl:{flood_sl} stays blocked on memory")

                        # ... and while it is, the other service level must keep
                        # getting served.
                        baseline = observed["quiet_done_when_flood_blocked"] or 0
                        await wait_for(
                            lambda: _advanced_by(quiet_state, "quiet", baseline, 20),
                            time.time() + TIMEOUT,
                            label=f"sl:{quiet_sl} keeps completing queries under the flood")

                        # Throttled, but not stuck: the flooded service level has
                        # to keep making progress on its own share too.
                        assert flood_state["flood_done"] > 0, \
                            f"sl:{flood_sl} completed no request at all, last error: " \
                            f"{flood_state['flood_error']}"
                        stop_sampler.set()
            finally:
                stop_sampler.set()
                if sampler is not None:
                    sampler.cancel()
                    await asyncio.gather(sampler, return_exceptions=True)
                safe_driver_shutdown(quiet_cluster)

    logger.info("samples=%d (with the flood blocked: %d), quiet queries done=%d, "
                "flood available floor=%d, quiet available floor=%d",
                observed["samples"], observed["samples_with_flood_blocked"],
                quiet_state["quiet_done"], observed["flood_available_min"],
                observed["quiet_available_min"])

    assert quiet_state["quiet_failed"] == 0, \
        f"the unflooded service level must not fail queries, last error: " \
        f"{quiet_state['quiet_error']}"
    assert observed["quiet_blocked_while_flood_blocked"] == 0, \
        f"sl:{quiet_sl} had requests blocked on memory while sl:{flood_sl} was flooding; " \
        f"the budgets are not isolated"
    assert observed["quiet_blocked_peak"] == 0, \
        f"sl:{quiet_sl} never had enough traffic of its own to block on memory, so any " \
        f"blocking means it was starved by sl:{flood_sl}"
    assert observed["flood_borrowed_peak"] == 0, \
        "with an empty shared pool there is nothing to borrow"
    # The flood really did eat into its own share...
    assert observed["flood_available_min"] < flood_share, \
        "the flood never consumed any of its service level's memory"
    # ... and left the other service level's share alone: the quiet queries only
    # ever hold a small part of it.
    assert observed["quiet_available_min"] > 0, \
        f"sl:{quiet_sl} ran out of its own memory while sl:{flood_sl} was flooding"

    metrics = await _metrics(manager, ip)
    assert _sl_metric(metrics, BLOCKED_TOTAL, flood_sl) > 0, \
        f"sl:{flood_sl} should have counted requests that waited for memory"
    assert _sl_metric(metrics, BLOCKED_TOTAL, quiet_sl) == 0, \
        f"no request of sl:{quiet_sl} should ever have waited for memory"


#
# 2. The budget is split in proportion to SHARES.
#

async def test_budget_is_split_in_proportion_to_shares(manager: ScyllaClusterManager):
    """Each service level's own share of the budget is proportional to its SHARES,
    and the shares plus the shared pool add up to the whole budget."""
    pool_fraction = 0.5
    server, cql, host = await _start_node(manager, shared_pool_fraction=pool_fraction)
    ip = server.ip_addr

    # Deliberately unequal, and a 1:2:4 ratio that rounding cannot blur.
    sls = {f"sl_a_{unique_name()}": 100, f"sl_b_{unique_name()}": 200, f"sl_c_{unique_name()}": 400}
    for name, shares in sls.items():
        await _create_service_level(cql, host, name, shares)
    for name in sls:
        await _wait_for_service_level_tenant(manager, ip, name)

    metrics = await _metrics(manager, ip)
    totals = _per_sl(metrics, TOTAL)
    pool_total = _metric(metrics, POOL_TOTAL) or 0.0
    shard_memory = _metric(metrics, SHARD_MEMORY, {"shard": 0})
    assert shard_memory, "could not read the shard's memory size"
    logger.info("shard memory=%d, per-service-level totals=%s, shared pool=%d",
                shard_memory, totals, pool_total)

    # The whole budget is cql_request_memory_fraction of the shard's memory, and
    # nothing of it is unaccounted for.
    budget = sum(totals.values()) + pool_total
    expected_budget = int(shard_memory * SMALLEST_MEMORY_FRACTION)
    assert budget == pytest.approx(expected_budget, abs=16), \
        f"the dedicated shares plus the shared pool ({budget}) should add up to " \
        f"{MEMORY_FRACTION} of the shard's memory ({expected_budget})"
    assert pool_total == pytest.approx(budget * pool_fraction, abs=16), \
        f"the shared pool should hold {pool_fraction} of the budget"

    # Proportionality between the service levels this test created, which needs
    # no assumption about the service levels the node makes for itself.
    names = list(sls)
    base_name, base_shares = names[0], sls[names[0]]
    base_total = totals[f"sl:{base_name}"]
    assert base_total > 0
    for name in names[1:]:
        expected = base_total * sls[name] / base_shares
        assert totals[f"sl:{name}"] == pytest.approx(expected, rel=0.02, abs=16), \
            f"sl:{name} has {sls[name]} shares against sl:{base_name}'s {base_shares}, " \
            f"so it should get {expected} bytes, not {totals[f'sl:{name}']}"

    # The default service level is created with 1000 shares (main.cc), which ties
    # the split to a share count this test did not choose itself.
    assert "sl:default" in totals, \
        f"the default service level should have a tenant of its own, got {list(totals)}"
    expected_default = base_total * DEFAULT_SL_SHARES / base_shares
    assert totals["sl:default"] == pytest.approx(expected_default, rel=0.02, abs=16), \
        f"sl:default has {DEFAULT_SL_SHARES} shares, so it should get {expected_default} bytes"


#
# 3. The shared pool can be resized live.
#

async def test_shared_pool_fraction_live_update(manager: ScyllaClusterManager):
    """Walk cql_request_memory_shared_pool_fraction through 0.2 -> 0 -> 0.5 while
    requests are in flight: the pool and the per-service-level shares must track
    it, the total must be conserved, and no query may hang or fail."""
    server, cql, host = await _start_node(manager, shared_pool_fraction=0.2)
    ip = server.ip_addr

    sls = {f"sl_a_{unique_name()}": 100, f"sl_b_{unique_name()}": 300}
    role = f"r_a_{unique_name()}"
    for name, shares in sls.items():
        await _create_service_level(cql, host, name, shares)
    for name in sls:
        await _wait_for_service_level_tenant(manager, ip, name)
    await _create_role_on_service_level(manager, cql, host, role, list(sls)[0])

    cluster, session = await _open_role_session(manager, server, role)
    state = {}
    try:
        # Load on the service level whose share is about to be resized, so the
        # resize happens while its tenant is admitting requests.
        async with _light_load(session, "SELECT key FROM system.local", state, "load",
                               concurrency=4) as _stop:
            await wait_for(lambda: _advanced_by(state, "load", 0, 10),
                           time.time() + TIMEOUT, label="load starts")

            metrics = await _metrics(manager, ip)
            budget = _budget(metrics)
            assert budget > 0
            logger.info("budget=%d", budget)

            previous_totals = None
            previous_fraction = None
            for fraction in (0.2, 0.0, 0.5):
                if fraction != 0.2:
                    await manager.server_update_config(server.server_id,
                                                       SHARED_POOL_FRACTION, fraction)

                async def pool_matches(fraction=fraction):
                    metrics = await _metrics(manager, ip)
                    pool = _metric(metrics, POOL_TOTAL)
                    if pool is None:
                        return None
                    return metrics if pool == pytest.approx(budget * fraction, abs=16) else None

                metrics = await wait_for(pool_matches, time.time() + TIMEOUT,
                                         label=f"shared pool follows fraction {fraction}")
                totals = _per_sl(metrics, TOTAL)
                pool = _metric(metrics, POOL_TOTAL) or 0.0
                logger.info("fraction=%s: pool=%d, totals=%s", fraction, pool, totals)

                # Nothing of the budget goes missing while it is re-split.
                assert sum(totals.values()) + pool == pytest.approx(budget, abs=16), \
                    f"the budget is not conserved at fraction {fraction}"
                # The dedicated portion is still split by shares.
                names = list(sls)
                ratio = totals[f"sl:{names[1]}"] / totals[f"sl:{names[0]}"]
                assert ratio == pytest.approx(sls[names[1]] / sls[names[0]], rel=0.02), \
                    f"the shares split broke at fraction {fraction}: ratio {ratio}"
                # Each service level's own share scales with the dedicated portion.
                if previous_totals is not None:
                    scale = (1.0 - fraction) / (1.0 - previous_fraction)
                    for name in sls:
                        expected = previous_totals[f"sl:{name}"] * scale
                        assert totals[f"sl:{name}"] == pytest.approx(expected, rel=0.02, abs=16), \
                            f"sl:{name} should have gone from {previous_totals[f'sl:{name}']} " \
                            f"to {expected} bytes when the pool went to {fraction}"
                previous_totals, previous_fraction = totals, fraction

                # Queries must keep flowing across every resize.
                baseline = state["load_done"]
                await wait_for(lambda: _advanced_by(state, "load", baseline, 10),
                               time.time() + TIMEOUT,
                               label=f"queries progress at fraction {fraction}")
                assert state["load_failed"] == 0, \
                    f"a query failed at fraction {fraction}: {state['load_error']}"
    finally:
        safe_driver_shutdown(cluster)

    assert state["load_failed"] == 0, f"a query failed: {state['load_error']}"
    logger.info("queries completed across the resizes: %d", state["load_done"])


#
# 4. Service levels come and go while requests are running.
#

async def test_service_level_lifecycle_resplits_the_budget(manager: ScyllaClusterManager):
    """CREATE / ALTER ... WITH SHARES / DROP a service level under load. Every
    change must re-split the dedicated portion, conserve the total, and leave
    the running queries and the node's log clean."""
    pool_fraction = 0.5
    server, cql, host = await _start_node(manager, shared_pool_fraction=pool_fraction)
    ip = server.ip_addr
    log = await manager.server_open_log(server.server_id)

    sl_a, sl_b = f"sl_a_{unique_name()}", f"sl_b_{unique_name()}"
    sl_new = f"sl_new_{unique_name()}"
    role = f"r_a_{unique_name()}"
    await _create_service_level(cql, host, sl_a, 100)
    await _create_service_level(cql, host, sl_b, 200)
    await _wait_for_service_level_tenant(manager, ip, sl_a)
    await _wait_for_service_level_tenant(manager, ip, sl_b)
    await _create_role_on_service_level(manager, cql, host, role, sl_a)

    cluster, session = await _open_role_session(manager, server, role)
    state = {}
    mark = await log.mark()
    try:
        async with _light_load(session, "SELECT key FROM system.local", state, "load",
                               concurrency=4) as _stop:
            await wait_for(lambda: _advanced_by(state, "load", 0, 10),
                           time.time() + TIMEOUT, label="load starts")

            metrics = await _metrics(manager, ip)
            budget = _budget(metrics)
            before = _per_sl(metrics, TOTAL)
            logger.info("before the lifecycle: budget=%d, totals=%s", budget, before)

            async def totals_when(pred, label):
                async def check():
                    metrics = await _metrics(manager, ip)
                    totals = _per_sl(metrics, TOTAL)
                    return (metrics, totals) if pred(totals) else None
                return await wait_for(check, time.time() + TIMEOUT, label=label)

            def conserved(metrics, totals, label):
                pool = _metric(metrics, POOL_TOTAL) or 0.0
                assert sum(totals.values()) + pool == pytest.approx(budget, abs=16), \
                    f"the budget is not conserved after {label}: totals={totals}, pool={pool}"
                assert pool == pytest.approx(budget * pool_fraction, abs=16), \
                    f"the shared pool changed size after {label}"

            # CREATE: the newcomer gets a share of its own and the others shrink.
            await _create_service_level(cql, host, sl_new, 400)
            metrics, after_create = await totals_when(
                lambda t: t.get(f"sl:{sl_new}", 0) > 0, f"sl:{sl_new} gets a share")
            logger.info("after CREATE %s WITH SHARES = 400: %s", sl_new, after_create)
            conserved(metrics, after_create, "CREATE")
            assert after_create[f"sl:{sl_new}"] == pytest.approx(
                after_create[f"sl:{sl_a}"] * 4, rel=0.02, abs=16), \
                "the new service level's share should be 4x the 100-share one's"
            assert after_create[f"sl:{sl_a}"] < before[f"sl:{sl_a}"], \
                "an extra service level must dilute the existing shares"

            # ALTER: the share follows the new SHARES value.
            await cql.run_async(f"ALTER SERVICE LEVEL {sl_new} WITH SHARES = 800", host=host)
            metrics, after_alter = await totals_when(
                lambda t: t.get(f"sl:{sl_new}", 0) > after_create[f"sl:{sl_new}"] * 1.2,
                f"sl:{sl_new} grows after ALTER")
            logger.info("after ALTER %s WITH SHARES = 800: %s", sl_new, after_alter)
            conserved(metrics, after_alter, "ALTER")
            assert after_alter[f"sl:{sl_new}"] == pytest.approx(
                after_alter[f"sl:{sl_a}"] * 8, rel=0.02, abs=16), \
                "after ALTER the new service level's share should be 8x the 100-share one's"

            # DROP: the share goes back to the others. The dropped service level's
            # tenant keeps its series until the connections that used it are gone,
            # but with no dedicated memory left.
            await cql.run_async(f"DROP SERVICE LEVEL {sl_new}", host=host)
            metrics, after_drop = await totals_when(
                lambda t: t.get(f"sl:{sl_new}", 0) == 0, f"sl:{sl_new} gives up its share")
            logger.info("after DROP %s: %s", sl_new, after_drop)
            conserved(metrics, after_drop, "DROP")
            for name in (sl_a, sl_b):
                assert after_drop[f"sl:{name}"] == pytest.approx(
                    before[f"sl:{name}"], rel=0.02, abs=16), \
                    f"sl:{name} should be back to {before[f'sl:{name}']} bytes after the DROP"

            baseline = state["load_done"]
            await wait_for(lambda: _advanced_by(state, "load", baseline, 10),
                           time.time() + TIMEOUT, label="queries progress after the lifecycle")
    finally:
        safe_driver_shutdown(cluster)

    assert state["load_failed"] == 0, \
        f"a query failed while service levels were changing: {state['load_error']}"
    logger.info("queries completed across the lifecycle: %d", state["load_done"])

    # The limiter must not have complained about its own accounting. Both
    # service/memory_limiter.cc and service/request_memory_limiter.cc log under
    # the "request_memory_limiter" name, and the pool's over-borrow /
    # over-repay complaints come out at WARN rather than ERROR.
    limiter_complaints = await log.grep(r"\b(ERROR|WARN)\b.*\brequest_memory_limiter\b",
                                        from_mark=mark)
    assert not limiter_complaints, \
        f"the request memory limiter complained: {limiter_complaints}"
    internal_errors = await log.grep(r"\bon_internal_error\b|Assertion .* failed",
                                     from_mark=mark)
    assert not internal_errors, f"internal errors in the log: {internal_errors}"


#
# 5. Borrowing from the shared pool.
#

async def test_service_level_borrows_from_the_shared_pool(manager: ScyllaClusterManager):
    """With almost the whole budget in the shared pool, a service level's own
    share is tiny, so its requests have to borrow - and the borrowing has to show
    up in scylla_transport_cql_requests_memory_borrowed_from_shared_pool."""
    pool_fraction = 0.9
    server, cql, host = await _start_node(manager, shared_pool_fraction=pool_fraction)
    ip = server.ip_addr

    sl = f"sl_borrow_{unique_name()}"
    role = f"r_borrow_{unique_name()}"
    await _create_service_level(cql, host, sl, 10)
    own_share = await _wait_for_service_level_tenant(manager, ip, sl)
    await _create_role_on_service_level(manager, cql, host, role, sl)

    metrics = await _metrics(manager, ip)
    pool_total = _metric(metrics, POOL_TOTAL) or 0.0
    budget = _budget(metrics)
    assert pool_total == pytest.approx(budget * pool_fraction, abs=16)
    logger.info("own share=%d, shared pool=%d", own_share, pool_total)
    assert own_share < pool_total, \
        "the point of this test is a dedicated share much smaller than the pool"

    # One request should take far more than the service level's own share, but a
    # small enough slice of the pool that it is admitted rather than queued.
    memory_target = max(4 * own_share, 0.02 * pool_total)
    observed = {"borrowed_peak": 0.0, "pool_available_min": pool_total,
                "pool_waiting_peak": 0.0, "samples": 0}
    stop_sampler = asyncio.Event()
    sampler = None

    def on_sample(metrics: ScyllaMetrics) -> None:
        observed["samples"] += 1
        observed["borrowed_peak"] = max(observed["borrowed_peak"],
                                        _sl_metric(metrics, BORROWED, sl))
        available = _metric(metrics, POOL_AVAILABLE)
        if available is not None:
            observed["pool_available_min"] = min(observed["pool_available_min"], available)
        observed["pool_waiting_peak"] = max(observed["pool_waiting_peak"],
                                            _metric(metrics, POOL_WAITING) or 0.0)

    async with new_test_keyspace(manager, "WITH REPLICATION = {'replication_factor': 1}", host) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, v blob", host=host) as tbl:
            try:
                async with _flood(manager, server, tbl, role, memory_target) \
                        as (flood_state, _flood_stop, estimate):
                    assert estimate > own_share, \
                        "a request has to outgrow the dedicated share for borrowing to happen"
                    assert estimate < budget, \
                        f"a request of {estimate} bytes would be rejected outright: " \
                        f"{MEMORY_FRACTION} also caps a single request, at {budget} bytes"
                    sampler = asyncio.create_task(
                        _sample_metrics(manager, ip, stop_sampler, on_sample))

                    async def borrowed_seen():
                        return observed["borrowed_peak"] or None
                    await wait_for(borrowed_seen, time.time() + TIMEOUT,
                                   label=f"sl:{sl} borrows from the shared pool")
                    logger.info("sl:%s peak borrowed from the shared pool: %d (of %d), "
                                "peak service levels waiting for the pool: %d",
                                sl, observed["borrowed_peak"], pool_total,
                                observed["pool_waiting_peak"])
                    assert flood_state["flood_done"] > 0, \
                        "the borrowed memory should have carried requests through"
            finally:
                stop_sampler.set()
                if sampler is not None:
                    sampler.cancel()
                    await asyncio.gather(sampler, return_exceptions=True)

    assert observed["borrowed_peak"] > own_share, \
        f"sl:{sl} borrowed only {observed['borrowed_peak']} bytes, which its own " \
        f"{own_share} bytes could have covered"
    assert observed["pool_available_min"] < pool_total, \
        "borrowing must show up as less memory available in the shared pool"

    # Once the flood is over the pool gets everything back.
    async def pool_repaid():
        metrics = await _metrics(manager, ip)
        available = _metric(metrics, POOL_AVAILABLE)
        borrowed = _sl_metric(metrics, BORROWED, sl)
        if borrowed == 0 and available == pytest.approx(pool_total, abs=16):
            return True
        return None
    await wait_for(pool_repaid, time.time() + TIMEOUT, label="the shared pool is repaid")


#
# 6. The tenants are back after a restart.
#

async def test_service_levels_get_their_tenants_after_a_restart(manager: ScyllaClusterManager):
    """A service level that already existed when the node started still has to get
    its dedicated share.

    memory_limiter::start() only creates the default service level's tenant; the
    rest are created from the service level subscription, which at boot is driven
    by the controller seeding its cache from the system tables. That happens much
    later in the boot sequence than the limiter is started, so this is worth an
    end-to-end check of its own.
    """
    pool_fraction = 0.5
    server, cql, host = await _start_node(manager, shared_pool_fraction=pool_fraction)
    ip = server.ip_addr

    sls = {f"sl_a_{unique_name()}": 100, f"sl_b_{unique_name()}": 400}
    for name, shares in sls.items():
        await _create_service_level(cql, host, name, shares)
    before = {name: await _wait_for_service_level_tenant(manager, ip, name) for name in sls}

    await manager.server_restart(server.server_id)
    cql, hosts = await manager.get_ready_cql([server])
    host = hosts[0]

    # Waited for rather than read straight away: the node answers CQL before the
    # service level cache has necessarily been seeded.
    after = {name: await _wait_for_service_level_tenant(manager, ip, name) for name in sls}
    logger.info("dedicated shares before the restart=%s, after=%s", before, after)
    for name in sls:
        assert after[name] == pytest.approx(before[name], rel=0.02, abs=16), \
            f"sl:{name} had {before[name]} bytes of its own before the restart and " \
            f"{after[name]} after"

    metrics = await _metrics(manager, ip)
    totals = _per_sl(metrics, TOTAL)
    pool_total = _metric(metrics, POOL_TOTAL) or 0.0
    budget = sum(totals.values()) + pool_total
    logger.info("per-service-level totals after the restart=%s, shared pool=%d",
                totals, pool_total)

    base_name = next(iter(sls))
    base_total = after[base_name]
    assert base_total > 0
    for name, shares in sls.items():
        expected = base_total * shares / sls[base_name]
        assert totals[f"sl:{name}"] == pytest.approx(expected, rel=0.02, abs=16), \
            f"sl:{name} has {shares} shares against sl:{base_name}'s {sls[base_name]}, " \
            f"so it should get {expected} bytes, not {totals[f'sl:{name}']}"
    assert pool_total == pytest.approx(budget * pool_fraction, abs=16), \
        f"the shared pool should still hold {pool_fraction} of the budget"

    # The default service level's tenant is the one the limiter creates itself,
    # keyed by the default scheduling group. A service level whose own record the
    # limiter failed to find must not have had its shares charged to it.
    expected_default = base_total * DEFAULT_SL_SHARES / sls[base_name]
    assert totals["sl:default"] == pytest.approx(expected_default, rel=0.02, abs=16), \
        f"sl:default has {DEFAULT_SL_SHARES} shares, so it should get " \
        f"{expected_default} bytes, not {totals['sl:default']}"
