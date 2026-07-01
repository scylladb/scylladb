---
name: unflake-test
description: Diagnose and fix flaky ScyllaDB tests — root-cause taxonomy, fix strategies, verification
license: MIT
compatibility: opencode
metadata:
  audience: developers
  workflow: test-stability
---

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

### 1. Replace polling with event-driven wait

`wait_for_injection_enter` uses REST polling with a 60s default — under load the injection may not be reached in time.

**Fix:** use `log.wait_for` instead, which waits for a specific log line and is event-driven.

```python
# Before:
await manager.api.wait_for_injection_enter(servers[0].ip_addr, "maybe_split_new_sstable_wait")

# After:
mark, _ = await log.wait_for("maybe_split_new_sstable_wait: waiting for message", from_mark=mark)
```

Example: `2eb20e0f7e55` — replaced REST-based `wait_for_injection_enter` with `log.wait_for` for repair failure test.

### 2. Replace non-deterministic triggers with error injections

Deleting rows to trigger a split revoke, or relying on commitlog corruption, is inherently non-deterministic.

**Fix:** use explicit error injection APIs (`enable_injection`, `message_injection`).

```python
# Before (non-deterministic):
cql.execute(f"DELETE FROM {table} WHERE pk=...")

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
```

Example: `32b7cab917da` — `loading_cache_test` switched from `lowres_clock` to `manual_clock`.

### 4. Replace `sleep()` with `condition_variable`

Fixed-duration sleeps are unreliable when the machine is under load.

**Fix:** use a `condition_variable` to signal completion.

```cpp
// Before (unreliable):
std::this_thread::sleep_for(5s);

// After (event-driven):
std::condition_variable cv;
// ... signal cv in the completion callback ...
cv.wait_for(lock, timeout);
```

Example: `10208c83ca68` — replaced sleep with condition_variable in DNS abort test.

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
    rows = list(cql.execute(f"..."))
    if len(rows) == expected_count:
        break
    await asyncio.sleep(0.5)
else:
    assert False, f"expected {expected_count} rows, got {len(rows)}"
```

Examples:
- `c098e9a327e0` — changed `view_build_status` asserts to retry loops
- `cace55aaaf7f` — retry async dictionary cleanup metric check
- `2eb20e0f7e55` — use `log.wait_for` until log line appears

### 7. Fix consistency level / replication factor

Using `CL=LOCAL_ONE` means only one node acknowledges the write — subsequent checks may find the write hasn't propagated.

**Fix:** use `CL=QUORUM` when the test checks that a write reached multiple nodes. Use `RF=3` when each node needs to host a specific tablet replica.

```python
# Before:
cql.execute("INSERT INTO ...")

# After:
cql.execute("INSERT INTO ...", consistency_level=ConsistencyLevel.QUORUM)
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
# Wait for restarted node to be UP on all peers before decommission
await log.wait_for(f"{node_ip} is now UP", from_mark=mark)

# Wait for GSI to be ACTIVE before checking IndexStatus
await wait_for_gsi(cql, table_arn, "ACTIVE")

# Wait for server to be ready before requesting repair
await server.ready()
```

Examples:
- `fc5aba1cc4ab` — wait for node UP in gossip before decommission
- `67d2ea4c4b05` — added `wait_for_gsi()` before IndexStatus check
- `111cccf8bad7` — wait for raft state before permission check

### 10. Use framework markers instead of manual log parsing

A hand-rolled `grep` on server output misses error filtering that the test framework already does.

**Fix:** remove the grep; use `@pytest.mark.check_nodes_for_errors` to let the framework handle it.

```python
# Before:
output = await manager.server_get_log(server_id)
assert "ERROR" not in output

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

# After:
# raft: make group0 Raft operation timeout configurable
# group0_raft_op_timeout_in_ms defaults to 60000 (60s).
# Test framework overrides to 300000 (300s) for CI.
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

# Force specific IO backend (avoids io-uring kernel bug):
cmdline = ['--io-properties-file', '...']

# Disable interfering features during sensitive operations:
await manager.api.enable_injection(servers[0].ip_addr, "tablet_load_balancing_disabled")
```

Examples:
- `8c4cbb9efb2f` — unique S3 prefix per test run
- `5f697d373f46` — force AIO backend
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
semaphore.make_reader_v2(db::timeout_clock::now(), ...);

// After:
semaphore.make_reader_v2(db::no_timeout, ...);
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
