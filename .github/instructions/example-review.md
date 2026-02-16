# ScyllaDB Reviewer Skill - Example Review

This document demonstrates how the reviewer skill would analyze and provide feedback on a sample code change.

## Sample Code Change

### Before (Original Code)
```cpp
// In file: service/storage_service.cc

seastar::future<> remove_node(host_id node) {
    auto endpoint = gossiper.get_endpoint(node);
    endpoint->shutdown();
    
    // Wait for shutdown
    seastar::sleep(std::chrono::seconds(5)).get();
    
    std::vector<node_info> nodes;
    for (auto& n : topology.get_all_nodes()) {
        nodes.push_back(n);
    }
    
    return seastar::make_ready_future<>();
}
```

### Test Code
```python
# test/cluster/test_node_removal.py

async def test_remove_node():
    # Create cluster
    manager = await start_cluster(3)
    
    # Remove a node
    await manager.remove_node(2)
    
    # Wait a bit
    await asyncio.sleep(2)
    
    # Check remaining nodes
    assert len(manager.nodes) == 2
```

---

## Reviewer Skill Analysis

### Phase 1: Critical Issues (P0)

#### 🚨 P0: Blocking Async Operation
**Location:** Line 5-6
```cpp
seastar::sleep(std::chrono::seconds(5)).get();
```

**Issue:** This code blocks the reactor thread by calling `.get()` on a future. This will freeze the entire shard for 5 seconds, preventing all other operations from proceeding.

**Impact:** Production outage - all operations on this shard will hang

**Fix:**
```cpp
co_await seastar::sleep(std::chrono::seconds(5));
```

Or better yet, use proper shutdown synchronization:
```cpp
co_await endpoint->wait_for_shutdown();
```

**Reference:** See `.github/instructions/reviewer.instructions.md` Section 1 (Async/Seastar Violations)

---

#### 🚨 P0: Memory Management Issue
**Location:** Line 8-11
```cpp
std::vector<node_info> nodes;
for (auto& n : topology.get_all_nodes()) {
    nodes.push_back(n);
}
```

**Issue:** Copying potentially large `node_info` objects without pre-allocation. This will cause multiple reallocations as the vector grows.

**Impact:** Performance degradation in hot path, unnecessary allocations

**Fix:**
```cpp
auto all_nodes = topology.get_all_nodes();
std::vector<node_info> nodes;
nodes.reserve(all_nodes.size());  // Pre-allocate
for (const auto& n : all_nodes) {  // const& to avoid copies
    nodes.push_back(n);
}
```

Or better, use references:
```cpp
const auto& nodes = topology.get_all_nodes();
// Work with references directly
```

**Reference:** See `.github/instructions/reviewer.instructions.md` Section 3 (Memory Management)

---

#### 🚨 P0: Test Flakiness
**Location:** Test line 8-9
```python
await asyncio.sleep(2)
```

**Issue:** Hardcoded sleep creates a race condition. If the node removal takes longer than 2 seconds (e.g., under load), the test will check before removal completes.

**Impact:** Flaky test that fails intermittently in CI

**Fix:**
```python
# Wait for node removal to complete properly
await manager.wait_for_node_removal(node_id=2, timeout=30)

# Or use proper synchronization
await manager.wait_for_topology_stable()
```

**Reference:** See `.github/instructions/reviewer.instructions.md` Section 4 (Test Quality)

---

#### 🚨 P0: Missing Error Handling
**Location:** Line 3
```cpp
auto endpoint = gossiper.get_endpoint(node);
```

**Issue:** `get_endpoint()` likely throws if the node doesn't exist. During concurrent operations or restarts, this could crash the process.

**Impact:** Process crash in production

**Fix:**
```cpp
auto endpoint = gossiper.find_endpoint(node);
if (!endpoint) {
    co_return coroutine::make_exception(
        std::runtime_error(format("Node {} not found in gossiper", node))
    );
}
```

**Reference:** See `.github/instructions/reviewer.instructions.md` Section 5 (Error Handling)

---

### Phase 2: Design Issues (P1)

#### ⚠️ P1: Missing Consistency Level in Test
**Location:** Test setup (not shown)

**Issue:** The test doesn't specify consistency levels for data operations. Default CL may allow stale reads.

**Fix:**
```python
# When setting up test data
await cql.execute(
    "INSERT INTO test_table ...",
    consistency_level=ConsistencyLevel.ALL
)
```

**Reference:** Common pattern from PR #27894, #27397

---

### Phase 3: Polish (P2)

#### 📝 P2: Missing Subsystem Prefix
**Commit message:** "Fix node removal"

**Issue:** No subsystem prefix makes it harder to browse changelog and assess relevance.

**Fix:**
```
service: Fix node removal to avoid blocking reactor

- Replace .get() with co_await for async operations
- Add proper error handling for missing nodes
- Use shutdown synchronization instead of sleep
```

**Reference:** See `.github/instructions/reviewer.instructions.md` Section 11 (Code Organization)

---

#### 📝 P2: Missing Test Documentation
**Test function:** No docstring

**Fix:**
```python
async def test_remove_node():
    """Test that removing a node updates cluster topology correctly.
    
    Verifies that:
    1. Node is properly removed from gossiper
    2. Remaining nodes reflect updated topology
    3. Operations continue on remaining nodes
    
    Reproduces: Issue #XXXXX (if applicable)
    """
```

---

## Summary

### Issues Found
- **P0 (Critical):** 4 issues - Must fix before merge
  - Blocking async operation (reactor hang)
  - Memory management (performance)
  - Test flakiness (CI reliability)
  - Missing error handling (crash risk)
  
- **P1 (High):** 1 issue - Should fix
  - Missing consistency level in test
  
- **P2 (Medium):** 2 issues - Nice to fix
  - Missing commit prefix
  - Missing test documentation

### Overall Assessment
**BLOCKING:** This PR cannot be merged in its current state due to P0 issues.

The blocking async operation on line 5-6 is particularly critical as it can cause production outages. Please address all P0 issues before re-review.

---

## Corrected Version

### Fixed Code
```cpp
seastar::future<> remove_node(host_id node) {
    // Find endpoint (may not exist during concurrent operations)
    auto endpoint = gossiper.find_endpoint(node);
    if (!endpoint) {
        co_return coroutine::make_exception(
            std::runtime_error(format("Node {} not found in gossiper", node))
        );
    }
    
    // Async shutdown
    co_await endpoint->shutdown();
    
    // Wait for completion with synchronization
    co_await endpoint->wait_for_shutdown();
    
    // Use reference to existing collection
    const auto& nodes = topology.get_all_nodes();
    
    logger.info("Successfully removed node {} (remaining: {})", node, nodes.size());
    co_return;
}
```

### Fixed Test
```python
async def test_remove_node():
    """Test that removing a node updates cluster topology correctly.
    
    Verifies that node removal properly updates cluster state and allows
    continued operations on remaining nodes.
    """
    # Create cluster
    manager = await start_cluster(3)
    
    # Insert test data with strong consistency
    await cql.execute(
        "INSERT INTO test_table (key, value) VALUES (?, ?)",
        [1, "test"],
        consistency_level=ConsistencyLevel.ALL
    )
    
    # Remove a node
    await manager.remove_node(2)
    
    # Wait for topology to stabilize (no race condition)
    await manager.wait_for_topology_stable()
    
    # Verify state
    assert len(manager.nodes) == 2
    
    # Verify operations still work
    result = await cql.execute(
        "SELECT value FROM test_table WHERE key = ?",
        [1]
    )
    assert result[0].value == "test"
```

---

## Key Takeaways

This example demonstrates how the reviewer skill:
1. **Prioritizes** critical issues that can cause outages
2. **Provides context** explaining why each issue matters
3. **Offers specific fixes** with code examples
4. **References guidelines** for learning
5. **Follows review workflow** (Phase 1 → 2 → 3)
6. **Blocks merge** when P0 issues exist
7. **Educates** contributors about ScyllaDB patterns

---

**Note:** This is a synthetic example created to demonstrate the reviewer skill. Real reviews would be based on actual code changes in pull requests.
