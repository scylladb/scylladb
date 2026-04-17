# ScyllaDB Code Review Skill

**Purpose:** Perform in-depth code reviews similar to ScyllaDB maintainers  
**Based on:** Analysis of **1,009 PRs** (2022-2025) and **~12,222 review comments**  
**Last Updated:** February 2026

---

## Overview

This document captures common review patterns, feedback themes, and critical checks from ScyllaDB maintainers. Based on comprehensive analysis of over 1,000 pull requests spanning 4 years, it provides structured guidance for high-quality code reviews that maintain ScyllaDB's standards for correctness, performance, and readability.

**Analysis Scope:**
- 1,009 merged PRs analyzed (2022-2025)
- ~12,222 review comments examined
- 169 high-activity PRs (30+ comments) analyzed in depth
- 25+ distinct review patterns identified

## External C++ Resources

This review skill is supplemented by standard C++ best practices:

- **[ISO C++ FAQ](https://isocpp.org/faq)** - Comprehensive C++ guidance on language features, best practices, and common pitfalls
- **[C++ Core Guidelines](https://isocpp.github.io/CppCoreGuidelines/CppCoreGuidelines)** - Modern C++ best practices covering resource management, interfaces, functions, classes, and more

When reviewing C++ code, consider these resources alongside ScyllaDB-specific patterns documented here. The Core Guidelines are particularly relevant for:
- Resource management (RAII, smart pointers, lifetimes)
- Function design (parameter passing, error handling)
- Class design (constructors, operators, interfaces)
- Concurrency (though ScyllaDB uses Seastar patterns instead of std::thread)

**Note:** ScyllaDB has specific conventions (e.g., Seastar async patterns, `seastar::lw_shared_ptr`) that may differ from general C++ guidelines. ScyllaDB-specific patterns take precedence.

## Review Priority Levels

- **P0 (CRITICAL):** Must be fixed - can cause crashes, data loss, or severe performance issues
- **P1 (HIGH):** Should be fixed - impacts correctness, maintainability, or moderate performance
- **P2 (MEDIUM):** Nice to fix - style, documentation, minor optimizations

---

## P0: Critical Issues (Must Fix Before Merge)

### 1. Async/Seastar Violations 

**Why Critical:** Can block the reactor thread, causing the entire system to hang

**Check for:**
- `.get()` calls on futures (converts async to blocking)
- Missing `co_await` keywords in coroutines
- Synchronous I/O or blocking operations in async contexts
- Using `std::mutex` instead of `seastar::semaphore`

**Example Issues:**
```cpp
// ❌ WRONG: Blocks the reactor
auto result = some_future().get();

// ✅ CORRECT: Async all the way
auto result = co_await some_future();

// ❌ WRONG: Blocking I/O
std::ifstream file("data.txt");

// ✅ CORRECT: Seastar async I/O
co_await file_io.read(...);
```

**Feedback Template:**
```
This function performs blocking operations using `.get()` calls which can block 
the entire reactor thread. Consider making this function async and using `co_await`.

Example:
```cpp
seastar::future<T> func() {
    auto result = co_await async_operation();
    co_return result;
}
```
```

### 2. Exception Handling in Data Path

**Why Critical:** Exceptions in hot paths cause performance degradation and can be incorrectly propagated

**Check for:**
- Throwing exceptions in loops or per-row operations
- Using exceptions for control flow
- Missing `noexcept` when functions truly don't throw
- Incorrect `noexcept` on functions that can throw (including transitive calls)
- Unhandled exceptions in coroutines
- Functions with `noexcept` that call throwing functions

**Common noexcept Issues (from PR #27476 analysis):**
- Container operations beyond inline capacity throw (e.g., `small_vector` when exceeding N)
- Functions marked `noexcept` but calling other functions that can throw
- Need to check entire call chain, not just direct function body
- Coroutines can keep `noexcept` - exceptions convert to exceptional futures

**Example Issues:**
```cpp
// ❌ WRONG: Exception for control flow
try {
    if (!condition) throw control_exception();
} catch (control_exception&) {
    // handle
}

// ✅ CORRECT: Use return values or std::expected
if (!condition) {
    return std::unexpected(error_code);
}

// ❌ WRONG: Can throw but marked noexcept
void process() noexcept {
    vector.push_back(item); // can throw std::bad_alloc
}

// ✅ CORRECT: Remove noexcept
void process() {
    vector.push_back(item);
}

// ❌ WRONG: small_vector exceeds capacity
small_vector<T, 3> get_items() noexcept {
    small_vector<T, 3> result;
    for (int i = 0; i < 10; i++) {  // Will exceed capacity=3!
        result.push_back(compute(i));  // Throws std::bad_alloc
    }
    return result;
}

// ✅ CORRECT: Either remove noexcept or ensure size <= capacity
small_vector<T, 10> get_items() noexcept {  // Increased capacity
    small_vector<T, 10> result;
    for (int i = 0; i < 10; i++) {
        result.push_back(compute(i));  // Won't exceed inline capacity
    }
    return result;
}

// ✅ CORRECT: Coroutines can keep noexcept (exceptions → futures)
seastar::future<void> process() noexcept {
    try {
        co_await work_that_might_throw();
    } catch (...) {
        // Exception converted to exceptional future automatically
    }
}
```

**Feedback Template:**
```
In the data path, avoid exceptions for performance. Consider:
1. Using `std::expected<T, E>` or `boost::outcome` for results
2. Returning error codes or status enums
3. If exceptions must be used, catch and convert to exceptional_future

Also, this function is marked `noexcept` but can throw `std::bad_alloc` from 
vector allocation. Either remove `noexcept` or ensure the operation truly cannot throw.

Note: Check the entire call chain - if this function calls other functions that
can throw, this function cannot be noexcept (unless it's a coroutine where exceptions
automatically convert to exceptional futures).

See PR #27476 for detailed discussion on noexcept specifications.
```

### 3. Memory Management Issues

**Why Critical:** Can cause memory leaks, crashes, or severe performance degradation

**Check for:**
- Raw `new`/`delete` usage (should use smart pointers)
- Missing RAII patterns
- Unnecessary copies of large objects (pass by `const&` or `&&`)
- Allocations in hot paths without pre-allocation
- Wrong choice of smart pointer type

**Example Issues:**
```cpp
// ❌ WRONG: Raw pointer, manual management
auto* obj = new MyObject();
// ... what if exception is thrown?
delete obj;

// ✅ CORRECT: RAII with unique_ptr
auto obj = std::make_unique<MyObject>();

// ❌ WRONG: Expensive copy in hot path
void process(large_object obj) { }

// ✅ CORRECT: Pass by const reference
void process(const large_object& obj) { }

// ❌ WRONG: Repeated allocations
for (int i = 0; i < n; i++) {
    vec.push_back(compute(i));
}

// ✅ CORRECT: Pre-allocate
vec.reserve(n);
for (int i = 0; i < n; i++) {
    vec.push_back(compute(i));
}
```

**Feedback Template:**
```
This code uses raw `new`/`delete` which can leak if an exception is thrown. 
Use RAII with `std::unique_ptr` or `seastar::lw_shared_ptr`.

Also, consider pre-allocating the vector since the size is known:
```cpp
vec.reserve(known_size);
```
This will avoid multiple reallocations in the hot path.
```

### 4. Test Quality Issues

**Why Critical:** Flaky tests waste CI resources and hide real bugs

**Check for:**
- Hardcoded `sleep()` calls (use proper synchronization)
- Missing consistency levels in distributed tests
- Tests that don't validate the fix
- Tests without proper mode guards (debug/release)
- High tolerance for failures (e.g., "passes if only fails 10% of time")

**Example Issues:**
```python
# ❌ WRONG: Race condition with sleep
await insert_data(key, value)
await asyncio.sleep(1)  # Hope it replicates?
assert await read_data(key) == value

# ✅ CORRECT: Use consistency level
await insert_data(key, value, consistency_level=Consistency.ALL)
assert await read_data(key) == value

# ❌ WRONG: Test doesn't validate the fix
def test_fix():
    # Does something
    assert True  # Always passes, even without the fix

# ✅ CORRECT: Ensure test fails without fix
def test_fix():
    # First verify test would fail without fix
    # Then apply fix and verify it passes
    result = the_fixed_function()
    assert result == expected_value  # Fails without fix
```

**Feedback Template:**
```
This test has potential reliability issues:

1. **Timing**: Instead of `sleep(1)`, use `consistency_level=Consistency.ALL` 
   when inserting test data to ensure replicas are synchronized.

2. **Validation**: Did you verify this test fails without your fix? Please run 
   the test with your fix temporarily disabled to confirm it catches the bug.

3. **Stability**: Consider running with `--repeat 100` to check for flakiness.
```

### 5. Tablets Compatibility Issues

**Why Critical:** Code using vnodes assumptions breaks with tablets feature (modern ScyllaDB)

**Check for:**
- Using `calculate_natural_endpoints()` (vnodes only)
- Accessing `token_metadata` directly instead of through ERM
- Assumptions about token ownership without tablets support
- Maintenance/recovery operations incompatible with tablets

**Example Issues:**
```cpp
// ❌ WRONG: Doesn't work with tablets (vnodes only)
auto& strat = table.get_replication_strategy();
auto endpoints = strat.calculate_natural_endpoints(token, tm);

// ✅ CORRECT: Works with both vnodes and tablets
auto erm = table.get_effective_replication_map();
auto endpoints = erm->get_natural_endpoints(token);

// ❌ WRONG: Direct token_metadata access
auto& tm = get_token_metadata();
auto endpoints = tm.get_topology().get_endpoints(token);

// ✅ CORRECT: Use ERM abstraction
auto endpoints = erm->get_natural_endpoints(token);
```

**Feedback Template:**
```
This code uses `calculate_natural_endpoints()` which only works with vnodes. 
With tablets enabled, this will not work correctly.

Use the effective replication map (ERM) instead:
```cpp
auto erm = table.get_effective_replication_map();
auto endpoints = erm->get_natural_endpoints(token);
```

This works for both vnodes and tablets configurations.

See PR #15974, #21207 for examples.
```

---

## P1: High Priority Issues (Should Fix)

### 5. Poor Naming and API Design

**Check for:**
- Overly generic function names without context
- Inconsistent verb usage in related functions
- Functions in wrong namespace
- Unclear API boundaries

**Example Issues:**
```cpp
// ❌ WRONG: Too generic
namespace cql3::restrictions {
    json to_json(const data& d);  // to_json of what?
}

// ✅ CORRECT: Specific and clear
namespace vector_search {
    json to_json(const restrictions& r);  // Clear context
}
// or
json to_vector_search_json(const restrictions& r);

// ❌ WRONG: Inconsistent verbs
void parse_data();
void extract_info();
void process_items();

// ✅ CORRECT: Consistent naming
void parse_data();
void parse_info();
void parse_items();
```

**Feedback Template:**
```
The function name 'to_json' is too general. Since this is converting restrictions 
to JSON specifically for vector-search consumption, consider:

1. `restrictions_to_vector_search_json()` (descriptive)
2. Moving to `vector_search::` namespace with name `to_json()`
3. Adding a comment explaining this is for the vector-store protocol only

Also, you used 'parse', 'extract', and 'process' in this patch. Pick one verb 
for consistency since they all do similar things.
```

### 6. Missing Error Handling

**Check for:**
- Unchecked function calls that can fail
- Missing null pointer checks
- Silently ignored errors
- Poor error messages without context

**Example Issues:**
```cpp
// ❌ WRONG: Unchecked access
auto node = topology.get_node(host_id);  // Throws if not found
node->process();

// ✅ CORRECT: Check first
auto node = topology.find_node(host_id);
if (!node) {
    return make_exception_future<>(std::runtime_error(
        format("Node {} not found in topology", host_id)));
}
node->process();

// ❌ WRONG: Silent error
if (auto err = do_something()) {
    // Ignore error
}

// ✅ CORRECT: Handle or log
if (auto err = do_something()) {
    logger.warn("Failed to do_something: {}", err);
    return std::unexpected(err);
}
```

**Feedback Template:**
```
This code calls `get_node(host_id)` which throws if the node doesn't exist.
During restart scenarios, the node may not be in topology yet.

Consider using `find_node(host_id)` and checking for nullptr:
```cpp
auto node = topology.find_node(host_id);
if (!node) {
    on_internal_error(logger, format("Expected node {} in topology", host_id));
}
```

If the node must exist at this point, add an assertion with context for debugging.
```

### 7. Resource Management Problems

**Check for:**
- Manual resource management instead of RAII
- Incorrect smart pointer choices
- Leaked resources in error paths
- Missing cleanup on exceptions

**Example Issues:**
```cpp
// ❌ WRONG: Manual management
semaphore sem;
sem.acquire().get();
try {
    do_work();
} catch (...) {
    sem.signal();
    throw;
}
sem.signal();

// ✅ CORRECT: RAII
auto units = get_units(sem);
do_work();

// ❌ WRONG: std::shared_ptr for single-shard ownership
auto obj = std::make_shared<MyObject>();

// ✅ CORRECT: seastar::lw_shared_ptr for same-shard
auto obj = make_lw_shared<MyObject>();
```

**Feedback Template:**
```
This code manually manages semaphore units, which can leak if an exception is thrown.
Use RAII with `get_units()`:

```cpp
auto units = co_await get_units(sem, 1);
co_await do_work();
// units automatically released
```

Also, for same-shard sharing, prefer `seastar::lw_shared_ptr` over `std::shared_ptr` 
for better performance.
```

### 8. Missing Test Coverage

**Check for:**
- Bug fixes without tests
- New features without tests
- Missing negative test cases
- No edge case testing

**Feedback Template:**
```
This PR fixes a bug but doesn't include a test. Please add a test that:
1. Reproduces the original bug (fails without your fix)
2. Passes with your fix applied
3. Documents which issue it tests (add comment with issue number)

Example:
```python
async def test_issue_12345_node_restart():
    """Test that node restart doesn't cause data loss.
    
    Reproduces: https://github.com/scylladb/scylladb/issues/12345
    """
    # Test implementation
```
```

---

## P2: Medium Priority Issues (Nice to Fix)

### 9. Code Style and Formatting

**Check for:**
- Missing spaces after commas
- Wrong indentation
- Old-style streams instead of fmt
- PEP 8 violations in Python

**Example Issues:**
```cpp
// ❌ Style issues
function(a,b,c);  // Missing spaces
std::cout << value << std::endl;  // Use fmt

// ✅ Correct style
function(a, b, c);
fmt::print("{}\n", value);
```

**Feedback Template:**
```
nit: Missing space after comma
nit: Consider using `fmt::print()` instead of streams for consistency
```

### 10. Documentation Issues

**Check for:**
- Comments that explain "what" instead of "why"
- Obvious comments that restate code
- Missing precondition documentation
- Undocumented public APIs

**Example Issues:**
```cpp
// ❌ WRONG: States the obvious
// Increment counter
counter++;

// ✅ CORRECT: Explains why
// Track retries for backoff calculation
counter++;

// ❌ WRONG: Missing preconditions
void process(node* n) {
    n->update();  // What if n is null?
}

// ✅ CORRECT: Document assumptions
/// Process node. 
/// Precondition: n must be non-null and in the topology
void process(node* n) {
    assert(n != nullptr);
    n->update();
}
```

**Feedback Template:**
```
nit: Eliminate this comment - the function name is self-explanatory.

For public APIs, please add documentation explaining:
- What the function does
- Preconditions and assumptions
- Return value meaning
- Any side effects
```

### 11. Code Organization

**Check for:**
- Missing subsystem prefixes in commit messages
- Unrelated changes mixed together
- Copy-pasted code that should be extracted
- Changes that should be squashed

**Feedback Template:**
```
Please add a subsystem prefix to the commit message (e.g., 'raft:', 'cql:', 'test:').
This helps with:
- Browsing changelog
- Quick relevance assessment
- Bisecting issues
- Generating release notes

Example: "raft: Fix node restart issue" instead of "Fix node restart issue"
```

### 12. Minor Optimizations

**Check for:**
- Redundant operations
- Inefficient data structures for use case
- Unnecessary intermediate structures

**Feedback Template:**
```
Could you convert `_restrictions` directly to JSON without the intermediate structure?
This would avoid an extra allocation and copy.
```

---

## Code Review Workflow

### Phase 1: Critical Issues (5 minutes)
1. Scan for async violations (`.get()`, blocking calls)
2. Check exception handling in data paths
3. Identify memory management issues
4. Flag test reliability problems

**If P0 issues found:** Stop here and request fixes before deeper review

### Phase 2: Design Review (10 minutes)
1. Evaluate API design and naming
2. Check error handling completeness
3. Review resource management patterns
4. Assess test coverage

### Phase 3: Polish (5 minutes)
1. Style and formatting
2. Documentation quality
3. Code simplification opportunities
4. Commit message quality

---

## Review Output Format

**IMPORTANT: Keep reviews concise and actionable**

### Summary (Required)
Provide a **single sentence** summarizing the most critical issue(s), **only if confident**:
- ✅ "P0: This PR blocks the reactor with `.get()` calls in 3 locations - must use `co_await` instead"
- ✅ "P1: Missing error handling for null pointer in `process_node()` - add `find_node()` check"
- ✅ "No critical issues found. Minor: Consider pre-allocating vector in hot path (line 42)"

**If uncertain or no major issues:** Skip the summary, go directly to specific comments.

### Detailed Comments (As Needed)
- Focus on P0/P1 issues
- Be specific with file/line references
- Provide concrete fix suggestions
- Avoid long explanations of "why" unless critical for correctness

### What to Avoid
❌ **Long introductions** explaining what you're about to review  
❌ **Restating the obvious** ("This PR adds feature X...")  
❌ **Walls of text** - people will skip them  
❌ **Explaining your methodology** - just provide findings  
❌ **Academic-style reviews** - this is engineering, not a thesis defense

### Example Good Review
```
P0: Blocks reactor at service.cc:123 - replace `future.get()` with `co_await future`

P1: Missing null check at topology.cc:45 - use `find_node()` instead of `get_node()`

nit: Line 67 - missing space after comma
```

### Example Bad Review (Too Verbose)
```
## Comprehensive Analysis

I have performed a thorough examination of this pull request, analyzing 
the changes across multiple dimensions including correctness, performance, 
and maintainability. Let me walk you through my findings...

### Background
This PR introduces functionality that...

[3 more paragraphs of context that nobody will read]
```

---

## Reviewer Tone Guidelines

### DO: Be Specific and Educational
✅ "This can block the reactor thread because `.get()` waits synchronously. Use `co_await` instead."  
✅ "For consistency with the rest of the codebase, prefer X over Y"  
✅ "This works, but for better performance consider..."  
✅ "See similar pattern in `service/storage_service.cc:4351`"

### DON'T: Be Vague or Harsh
❌ "This is wrong" (no context)  
❌ "You should know better" (condescending)  
❌ "Maybe fix this?" (too vague)  
❌ Long tangents on unrelated topics

### Use "nit:" for Minor Issues
```
nit: missing space after comma
nit: extra blank line
nit: can be simplified to X
```

### Offer Alternatives
```
Consider one of these approaches:
1. Option A - simpler but less flexible
2. Option B - more complex but handles edge cases
3. Option C - matches pattern used in module X

I'd lean toward B because...
```

---

## Key Reviewer Mantras

Based on frequency and emphasis in ScyllaDB reviews:

1. **"Make it obvious"** - Self-documenting code > comments
2. **"Don't block the reactor"** - Always async/await, never `.get()`
3. **"Keep commits bisectable"** - Each commit builds and passes tests
4. **"Test what you fix"** - Bug fixes need tests
5. **"Subsystem prefixes matter"** - For changelog and bisecting
6. **"Don't allocate in hot paths"** - Performance matters
7. **"RAII everything"** - No manual resource management
8. **"Fail fast with context"** - Check assumptions, log useful info
9. **"One fiber per connection"** - Realistic concurrency patterns
10. **"Results over exceptions"** - In data path, avoid exception overhead

---

## Common Anti-Patterns to Catch

### 1. Blocking Async Operations
```cpp
❌ auto result = async_func().get();
✅ auto result = co_await async_func();
```

### 2. Exceptions for Control Flow
```cpp
❌ try { if (!ok) throw control_exception(); } catch { }
✅ if (!ok) { return handle_error(); }
```

### 3. Manual Resource Management
```cpp
❌ sem.acquire().get(); try { work(); } finally { sem.signal(); }
✅ auto units = co_await get_units(sem); co_await work();
```

### 4. Generic Naming
```cpp
❌ void process(data d);  // Process how?
✅ void parse_vector_search_query(data d);
```

### 5. Flaky Test Timing
```python
❌ await insert(x); await sleep(1); assert read(x)
✅ await insert(x, cl=ALL); assert read(x)
```

### 6. Missing Null Checks
```cpp
❌ auto node = get_node(id); node->update();
✅ auto node = find_node(id); if (node) node->update();
```

### 7. Poor Error Messages
```cpp
❌ throw std::runtime_error("error");
✅ throw std::runtime_error(format("Node {} not found in topology", host_id));
```

### 8. Copy-Paste Without Refactoring
```cpp
❌ Same 10 lines in 3 places
✅ Extract to function, call from 3 places
```

### 9. Overly Coupled Code
```cpp
❌ One function doing 5 different things
✅ Break into focused, single-purpose functions
```

### 10. Missing Test Consistency Levels
```python
❌ cql.execute("INSERT ...") # Default CL
✅ cql.execute("INSERT ...", consistency_level=ConsistencyLevel.ALL)
```

---

## Integration with Existing Guidelines

When reviewing, reference these existing instruction files:

1. **`.github/instructions/cpp.instructions.md`** - C++ style, Seastar patterns, memory management
2. **`.github/instructions/python.instructions.md`** - Python style, testing patterns
3. **`.github/copilot-instructions.md`** - Build system, test philosophy, code philosophy

Example reference:
```
Per the C++ guidelines (cpp.instructions.md), prefer `seastar::lw_shared_ptr` 
for same-shard sharing rather than `std::shared_ptr`.
```

---

## Distinguishing Automated vs Human Review

### Good for Automated Review (This Skill)
- ✅ Style violations (spacing, formatting)
- ✅ Common async anti-patterns (`.get()` calls)
- ✅ Missing `noexcept` specifications
- ✅ Generic naming issues
- ✅ Test synchronization patterns
- ✅ Obvious comments
- ✅ Missing error checks

### Better Left to Human Reviewers
- 🧑 Architecture and design decisions
- 🧑 Complex performance trade-off analysis
- 🧑 API design philosophy
- 🧑 Test coverage sufficiency
- 🧑 Security implications
- 🧑 Business logic correctness
- 🧑 Cross-module impact assessment

**Guideline:** When in doubt, flag for human review with context:
```
This might need a closer look from a maintainer: [explanation of concern]
@avikivity - this touches load shedding logic you authored
```

---

## Example High-Value Review Comments

### Performance Issue
```
⚠️ P0: Performance issue in hot path

This code runs for every row processed. Consider:
1. Pre-allocating the vector since size is known: `vec.reserve(row_count)`
2. Using `small_vector<T, 16>` for common case (avoids heap allocation)
3. Passing by `string_view` instead of `string` to avoid allocation

The current version allocates on every call, which will show up in profiles.
```

### Correctness Issue
```
⚠️ P0: Potential crash

This function calls `get_node(id)` which throws if the node doesn't exist.
During certain restart scenarios, the node may not be in topology yet.

Suggested fix:
```cpp
auto node = topology.find_node(id);
if (!node) {
    on_internal_error(logger, format("Node {} expected in topology", id));
    co_return make_exception_future<>(std::runtime_error(...));
}
```

If the node must exist here, the `on_internal_error` will help debug why it doesn't.
```

### Test Quality Issue
```
⚠️ P1: Test doesn't validate the fix

This test has a subtle issue: it doesn't verify that the fix actually works.

Please:
1. Temporarily disable your fix
2. Run the test and confirm it fails
3. Re-enable the fix
4. Run the test and confirm it passes

This ensures the test is actually validating the behavior and will catch regressions.

Also consider running `./test.py --mode=dev --repeat 100 test/...` to check for flakiness.
```

### API Design Issue
```
⚠️ P1: API naming

The function name `process()` is too generic. Since this converts internal 
restrictions to JSON for vector-search consumption, consider:

**Option 1:** More descriptive name
```cpp
json restrictions_to_vector_search_json(const restrictions& r)
```

**Option 2:** Move to appropriate namespace
```cpp
namespace vector_search {
    json to_json(const restrictions& r)
}
```

**Option 3:** Add clarifying comment
```cpp
/// Converts CQL restrictions to JSON format for vector-store protocol
json to_json(const restrictions& r)
```

I'd suggest Option 2 as it provides context through namespace and keeps the name concise.
```

---

## Notable ScyllaDB Reviewers and Focus Areas

When flagging for human review, consider reviewer expertise:

- **avikivity**: Performance, async patterns, test quality, architecture
- **denesb**: Reader concurrency, memory management, correctness
- **bhalevy**: Style, noexcept specifications, resource management
- **tgrabiec**: Architecture, load balancing, design patterns
- **nyh**: Naming, API clarity, code organization
- **patjed41**: Testing, Python style, edge cases
- **gleb-cloudius**: State management, topology coordination

Example: `@avikivity - this changes load shedding logic, please review performance implications`

---

## Conclusion

This reviewer skill is designed to:
1. **Prevent critical bugs** from merging (P0 issues)
2. **Help contributors learn** ScyllaDB patterns through educational feedback
3. **Reduce burden** on human reviewers for repetitive issues
4. **Maintain consistent** code quality standards

**Remember:** The goal is not to catch every issue, but to catch the most important ones and provide actionable, educational feedback that helps contributors improve.

When in doubt:
- **Be specific** with examples
- **Explain why** something matters
- **Offer alternatives** when suggesting changes
- **Reference** existing code/guidelines
- **Flag for human review** if uncertain

---

**Version:** 1.0  
**Last Updated:** February 2026  
**Based on:** Analysis of 200+ PRs, 700+ review comments
