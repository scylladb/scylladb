---
name: unflake-test
description: Diagnose and fix flaky ScyllaDB tests — root-cause taxonomy, fix strategies, verification
license: LicenseRef-ScyllaDB-Source-Available-1.1
metadata:
  audience: developers
  workflow: test-stability
---

<!--
Copyright (C) 2026-present ScyllaDB

SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
-->

Diagnose a flaky test → classify the symptom → apply the corresponding fix pattern → verify with repeated runs.

## Phase 1 — Classify the symptom

| Symptom | Likely root cause |
|---|---|
| Times out waiting for injection to fire | REST poll races slow injection; injection consumed by competing operation before test polls it |
| Fails only in debug mode | Timing slack too tight; real clock used; async cleanup too slow in debug |
| Schema/keyspace query returns stale data | Query hit node that hasn't applied latest mutation; missing read barrier or host pin |
| Fails only under xdist parallelism | Shared mutable state between workers (S3 prefix, cluster pool, metrics carryover) |
| Fails on some machines but not others | Infrastructure: kernel bug, slow disk, CCM file race |
| Passes some runs, fails others non-deterministically | Relies on real clock, sleep, or non-deterministic trigger (row deletion, commitlog corruption) |
| Intermittently missing rows in distributed read | Eventual consistency violation; different nodes at different states |
| Assert fails on async cleanup | Async destruction (`foreign_ptr`, raft apply) not waited for |
| Error log in server output causes failure | Harmless race caught by log grepping; use framework error markers instead |
| Fails after node restart | Driver connection closed by Python driver bug; or gossip not propagated |
| Fails on timeout in non-timeout test | Real `timeout_clock` used where `db::no_timeout` should be |
| Schema change race | Schema read timed out because group0 raft op timeout too short under load |

## Phase 2 — Apply the fix

*These are starting points, not recipes. Read the cited commit and the failing test before applying one — several of these patterns trade a flake for a hang if applied blindly.*

### 1. Increase the injection-enter wait deadline

`wait_for_injection_enter` defaults to a 60s deadline — under load (dev/debug mode, concurrent operations) the injection may not be reached in time even though nothing is actually stuck.

**Fix:** pass an explicit, longer `deadline`. If the same call path also goes through a REST client with its own timeout, raise that too so it doesn't fire first.

```python
# Before:
await manager.api.wait_for_injection_enter(servers[0].ip_addr, "my_injection")

# After:
await manager.api.wait_for_injection_enter(servers[0].ip_addr, "my_injection",
                                           deadline=time.time() + 600.0)
```

Examples:
- `45d920b132` — raised the injection-enter deadline from 60s to 600s for `test_repair_failure_on_split_rejection`
- `bfaf8f1cc2` — same fix for `test_sstables_incrementally_released_during_streaming` and `test_reject_split_compaction`
- `5c2d8d7240` — raised the REST client timeout on the same call path so it stays above the 600s deadline

### 2. Replace non-deterministic triggers with error injections

Deleting rows to trigger a split revoke, or relying on commitlog corruption, is inherently non-deterministic.

**Fix:** use explicit error injection APIs (`enable_injection`, `message_injection`).

```python
# Before (non-deterministic):
await cql.run_async(f"DELETE FROM {table} WHERE pk=...")

# After (deterministic):
await manager.api.enable_injection(servers[0].ip_addr, "injection_name", one_shot=True)
await manager.api.message_injection(servers[0].ip_addr, "injection_name")
```

Examples:
- `1f4edd8683b2` — replaced row-deletion-based split revoke with error injection
- `6389099dfba7` — replaced `node restart + trace-level logging` with injection point + tracing

### 3. Replace real-time clock with `manual_clock`

`lowres_clock` (real time) is inherently timing-dependent and flaky in debug mode.

**Fix:** make the clock a template parameter so tests can use `manual_clock`.

```cpp
// Before:
class my_cache {
    clock_type::time_point _last_refresh;
    ...
};

// After:
template<typename Clock = lowres_clock>
class my_cache {
    using clock = Clock;
    typename clock::time_point _last_refresh;
    ...
};

// The test must also drive the clock — manual_clock never advances by itself.
// 32b7cab917da adds a manual_clock_sleep_fn helper used in place of every sleep:
manual_clock_sleep_fn(150ms).get();
REQUIRE_EVENTUALLY_EQUAL<size_t>([&] { return cache.size(); }, 2, manual_clock_sleep_fn);
```

Example: `32b7cab917da` — `loading_cache_test` switched from `lowres_clock` to `manual_clock`, and replaced every sleep with `manual_clock_sleep_fn`.

### 4. Replace `sleep()` with `seastar::condition_variable`

Fixed-duration sleeps are unreliable when the machine is under load. Never reach for `std::condition_variable` here — it blocks the OS thread and stalls every other continuation on the shard.

**Fix:** signal completion through the future-based `seastar::condition_variable`.

```cpp
// Before (unreliable):
co_await sleep(100ms);

// After (event-driven):
seastar::condition_variable wait_for_abort;
// ... in the waiting fiber:
co_await wait_for_abort.when();
// ... in the completing fiber:
wait_for_abort.signal();
```

Example: `10208c83ca68` — replaced sleep with `seastar::condition_variable` in the DNS abort test.

### 5. Use read barrier + host pin for schema queries

Querying `system_schema.keyspaces` from an arbitrary node may hit a node that hasn't applied the latest schema change.

**Fix:** pin queries to a specific node and issue a `read_barrier()` before querying.

```python
async def get_replication_options(ks, host, ip_addr):
    await read_barrier(manager.api, ip_addr)
    res = await cql.run_async(
        f"SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '{ks}'",
        host=host
    )
    return parse_replication_options(res[0].replication_v2 or res[0].replication)

# `host` is a driver Host object, not an IP string — obtain it at the call site:
host = (await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 30))[0]
opts = await get_replication_options(ks, host, servers[0].ip_addr)
```

Examples:
- `78d3d5b5651f` — fixed rack list conversion tests with read barrier + host pin
- `63f50bae19ff` — same pattern for `test_enforce_rack_list_option`
- `111cccf8bad7` — wait for raft state to be applied before checking permissions

### 6. Use eventual-consistency retry loops for distributed state

Distributed state may not be immediately visible on all nodes. A single assert can fail if the query hits a lagging node.

**Fix:** retry until the condition is met (with a generous deadline).

```python
deadline = time.time() + 60
while time.time() < deadline:
    rows = await cql.run_async("...", host=host)
    if len(rows) == expected_count:
        break
    await asyncio.sleep(0.5)
else:
    assert False, f"expected {expected_count} rows, got {len(rows)}"
```

Examples:
- `c098e9a327e0` — changed `view_build_status` asserts to retry loops
- `cace55aaaf7f` — retry async dictionary cleanup metric check

### 7. Fix consistency level / replication factor

Using `CL=LOCAL_ONE` means only one node acknowledges the write — subsequent checks may find the write hasn't propagated.

**Fix:** use `CL=QUORUM` when the test checks that a write reached multiple nodes. Use `RF=3` when each node needs to host a specific tablet replica.

```python
# Before:
await cql.run_async("INSERT INTO ...")

# After (CL goes through an execution profile, not a kwarg):
quorum = cql.execution_profile_clone_update(EXEC_PROFILE_DEFAULT,
                                            consistency_level=ConsistencyLevel.QUORUM)
await cql.run_async("INSERT INTO ...", execution_profile=quorum)
```

Examples:
- `29de9478512e` — `LOCAL_ONE` → `QUORUM` for write rejection test
- `62e27e0f770b` — `RF=1` → `RF=3` for tablet split test

### 8. Reorder operations to avoid races

When two concurrent Scylla operations (split, migration, decommission, bootstrap) interfere, reorder test steps so they execute sequentially.

**Fix:** sequence operations so one completes (or is held by injection) before the next begins.

```python
# Before (race: split + migration happen concurrently):
servers.append(await manager.server_add(...))
await manager.enable_tablet_balancing()
await manager.api.wait_for_injection_enter(servers[0].ip_addr, "split_sstable_rewrite")

# After (split triggered first, migration added later):
await manager.enable_tablet_balancing()
await manager.api.wait_for_injection_enter(servers[0].ip_addr, "split_sstable_rewrite")
servers.append(await manager.server_add(...))
```

Example: `8f6033f00d11` — reordered split/migration test to trigger split with 1 server.

### 9. Wait for prerequisite conditions

Don't proceed until a necessary state is established (gossip UP, view active, raft applied, server ready).

```python
# Wait for mutual gossip visibility across every peer before decommission
await manager.servers_see_each_other(await manager.running_servers())

# Wait for a GSI to become ACTIVE (synchronous helper, takes a boto3 Table + index name)
wait_for_gsi(table, 'my_index')

# Wait for a server to be fully serving — pass the expected up-state to the API
# that starts it, rather than polling afterwards. server_start()/server_add() take
# it (default SERVING); server_restart() does not.
await manager.server_start(server_id, expected_server_up_state=ServerUpState.SERVING)
```

Examples:
- `fc5aba1cc4ab` — wait for mutual gossip visibility with `servers_see_each_other()`
- `67d2ea4c4b05` — added `wait_for_gsi()` before IndexStatus check

### 10. Use framework markers instead of manual log parsing

A hand-rolled `grep` on server output misses error filtering that the test framework already does.

**Fix:** remove the grep; use `@pytest.mark.check_nodes_for_errors` to let the framework handle it.

```python
# Before:
log = await manager.server_open_log(server_id)
assert not await log.grep_for_errors()

# After:
@pytest.mark.check_nodes_for_errors
async def test_my_test(...):
    ...
```

Example: `65638ad6c5` — replaced manual grep with `@check_nodes_for_errors` marker.

### 11. Make timeouts configurable, not hardcoded

Hardcoded timeouts force tradeoffs between CI reliability and fast production failure detection.

**Fix:** make the timeout a configuration option with a tight default, and let the test framework override it.

```python
# Before:
timeout = 60  # hardcoded
```

```cpp
// db/config.hh — declare the knob
named_value<uint32_t> group0_raft_op_timeout_in_ms;

// db/config.cc — tight production default, live-updatable
, group0_raft_op_timeout_in_ms(this, "group0_raft_op_timeout_in_ms", liveness::LiveUpdate,
        value_status::Used, 60000,
        "The time in milliseconds that group0 allows a Raft operation to complete.")
```

The test framework then overrides it:

```python
# test/pylib/scylla_cluster.py — CI-wide override
'group0_raft_op_timeout_in_ms': 300000,

# or per-test, through the config passed to server_add()
await manager.server_add(config={'group0_raft_op_timeout_in_ms': 600000})
```

Example: `fbcf77d134c2` — made `group0_raft_op_timeout_in_ms` configurable.

### 12. Fix the production code bug

Sometimes the test is exposing a real bug. The test is correct; the code is wrong.

```
view_building_worker: don't hold staging tasks mutex across group0 add_entry
create_staging_sstable_tasks() acquired _started_staging_tasks_mutex to
read _started_staging_tasks while building the command, but kept holding
it across the group0 add_entry() call that commits the command.

This can deadlock group0 apply. Fix by releasing the mutex right after
the loop, before add_entry().
```

Example: `b43cb873f696` — fixed mutex held across `group0 add_entry()`.

### 13. Isolate test from environment

Shared infrastructure, hardware quirks, and interfering features can cause flakiness.

```python
# Unique prefix per test run (avoids shared-minio conflicts):
bucket = f"backup-{uuid.uuid4()}"

# Force the AIO reactor backend (avoids an io-uring kernel bug):
cmdline = ['--reactor-backend=linux-aio']

# Disable interfering features during sensitive operations:
await manager.api.disable_tablet_balancing(servers[0].ip_addr)
```

Examples:
- `8c4cbb9efb2f` — unique S3 prefix per test run
- `5f697d373f46` — force the AIO reactor backend
- `4c9efc08d8c4` — disable tablet load balancing before dropping keyspace

### 14. Rewrite in C++ when Python can't check internal state

Python tests can't accurately check cache contents because they can pick up system table activity.

**Fix:** rewrite as a C++ test using the C++ API for targeted state inspection.

Example: `dd63b76eab96` — rewrote `test_writes_caching_{disabled,enabled}` from Python to C++.

### 15. Reconnect the driver after node restarts

Python driver bug #295 can close connections after a rolling restart, causing subsequent requests to fail.

**Fix:** reconnect the driver after any restart.

```python
await manager.driver_connect()
```

Example: `a56115f77b19` — deflake driver reconnections in recovery procedure tests.

### 16. Reset state between test scenarios

Metrics, variables, and state that carry over between scenarios can cause cross-scenario contamination.

**Fix:** initialize all state explicitly at the start of each scenario.

Example: `1175e1ed4928` — fixed per-scenario metric initialization that carried over from previous scenarios.

### 17. Avoid real timeouts in non-timeout tests

Using `db::timeout_clock::now()` in tests that don't test timeout behavior makes them timing-dependent.

**Fix:** use `db::no_timeout` when the test doesn't need timeout behavior.

```cpp
// Before:
semaphore.obtain_permit(schema, get_name(), 1024, db::timeout_clock::now(), {});

// After:
semaphore.obtain_permit(schema, get_name(), 1024, db::no_timeout, {});
```

Example: `4d8eb02b8d95` — moved reader_concurrency_semaphore_test away from `timeout_clock::now()`.

### 18. Force coordinator + replica on same shard

For tracing-dependent tests, the Python driver only waits for coordinator traces, not replica traces.

**Fix:** use `--smp=1` to force coordinator and replica on the same shard.

```
cmdline = ['--smp=1']
```

Example: `c35b82b8605e` — avoided CQL tracing race with `--smp=1`.

### 19. Lower log levels for harmless races

When a harmless race produces error logs that the test framework detects, lower the log level rather than fixing the race (which could cause regressions).

Only when the race is genuinely harmless and the log line is specific to it. Never lower the level of a logger that also carries real errors — narrow the message or the logger first, otherwise a future real failure goes unnoticed.

Example: `9ebd6df43ad1` — reduced snitch log level from error to warning because CCM file creation race was harmless.

### 20. Skip or remove the test (last resort)

If the behavior is inherently non-deterministic or the environment can't support it:
1. Skip with a `skip` marker and file a tracking issue
2. Remove the test entirely if a better test already covers the behavior

```python
@pytest.mark.skip(reason="flaky — see SCYLLADB-XXXX")
async def test_my_test(...):
    ...
```

Examples:
- `82e1678fbeb8` — skip `test_mv_tablets_empty_ip` in debug mode
- `20ba8d4e8cc7` — skip flaky `test_one_big_mutation_corrupted_on_startup`
- `ee5883770a19` — remove non-deterministic check from `test_schema_is_recovered_after_dying`

## Phase 3 — Verify

1. **Repeat in the mode where it failed** — run the test many times in exactly the mode (debug/dev/release) that exposed the flakiness:
   ```
   # If the test runs in under 10s, repeat 1000 times:
   ./test.py --mode=<mode> <test_path> --repeat 1000 --max-failures 1

   # If the test runs 10s or longer, repeat 100 times:
   ./test.py --mode=<mode> <test_path> --repeat 100 --max-failures 1
   ```

2. If the fix is a production code change: verify the test fails before the fix and passes after (to confirm it's actually testing the right thing).

3. **Document the root cause** in the commit message. Include a timeline from failure logs when possible:
   ```
   Root cause: With RF=1, 1 tablet, and 2 servers, enabling tablet balancing
   triggers BOTH a split AND a migration. Migration cleanup stops all
   compactions for the tablet, forcibly releasing the one_shot injection.

   Timeline from the failure:
     23:15:11.389 - split_sstable_rewrite injection enters (compaction held)
     23:15:11.396 - topology coordinator starts tablet migration (402->403)
     23:15:13.839 - migration cleanup stops split compaction on server 402
     23:15:13.844 - one_shot injection consumed (enter_count lost)
     ~23:15:14+   - test calls wait_for_injection_enter -> always sees 0
     23:16:14     - timeout after 60s
   ```

4. Reference the tracking issue:
   ```
   Fixes: https://github.com/scylladb/scylladb/issues/XXXXX
   ```
