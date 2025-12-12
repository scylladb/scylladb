# SCYLLA_ASSERT to scylla_assert() Conversion Summary

## Objective

Replace crash-inducing `SCYLLA_ASSERT` with exception-throwing `scylla_assert()` to prevent cluster-wide crashes and maintain availability.

## What Was Done

### 1. Infrastructure Implementation ✓

Created new `scylla_assert()` macro in `utils/assert.hh`:
- Based on `on_internal_error()` for exception-based error handling
- Supports optional custom error messages via variadic arguments
- Uses `seastar::format()` for string formatting
- Compatible with C++23 standard (uses `__VA_OPT__`)

**Key difference from SCYLLA_ASSERT:**
```cpp
// Old: Crashes the process immediately
SCYLLA_ASSERT(condition);

// New: Throws exception (or aborts based on config)
scylla_assert(condition);
scylla_assert(condition, "custom error message: {}", value);
```

### 2. Comprehensive Analysis ✓

Analyzed entire codebase to identify safe vs unsafe conversion locations:

**Statistics:**
- Total SCYLLA_ASSERT usages: ~1307 (including tests)
- Non-test usages: ~886
- **Unsafe to convert**: 223 usages (25%)
  - In noexcept functions: 187 usages across 50 files
  - In destructors: 36 usages across 25 files
- **Safe to convert**: ~668 usages (75%)
- **Converted in this PR**: 112 usages (16.8% of safe conversions)

### 3. Documentation ✓

Created comprehensive documentation:

1. **Conversion Guide** (`docs/dev/scylla_assert_conversion.md`)
   - Explains safe vs unsafe contexts
   - Provides conversion strategy
   - Lists all completed conversions
   - Includes testing guidance

2. **Unsafe Locations Report** (`docs/dev/unsafe_scylla_assert_locations.md`)
   - Detailed listing of 223 unsafe locations
   - Organized by file with line numbers
   - Separated into noexcept and destructor categories

### 4. Sample Conversions ✓

Converted 112 safe SCYLLA_ASSERT usages across 32 files as demonstration:

| File | Conversions | Context |
|------|------------|---------|
| db/large_data_handler.{cc,hh} | 5 | Future-returning functions |
| db/schema_applier.cc | 1 | Coroutine function |
| db/system_distributed_keyspace.cc | 1 | Regular function |
| db/commitlog/commitlog_replayer.cc | 1 | Coroutine function |
| db/view/row_locking.cc | 2 | Regular function |
| db/size_estimates_virtual_reader.cc | 1 | Lambda in coroutine |
| db/corrupt_data_handler.cc | 2 | Lambdas in future-returning function |
| raft/tracker.cc | 2 | Unreachable code (switch defaults) |
| service/topology_coordinator.cc | 11 | Coroutine functions (topology operations) |
| service/storage_service.cc | 28 | Critical node lifecycle operations |
| sstables/* (22 files) | 58 | SSTable operations (read/write/compress/index) |

All conversions were in **safe contexts** (non-noexcept, non-destructor functions). 3 assertions in storage_service.cc remain as SCYLLA_ASSERT (in noexcept functions).

## Why These Cannot Be Converted

### Unsafe Context #1: noexcept Functions (187 usages)

**Problem**: Throwing from noexcept causes `std::terminate()`, same as crash.

**Example** (from `locator/production_snitch_base.hh`):
```cpp
virtual bool prefer_local() const noexcept override {
    SCYLLA_ASSERT(_backreference != nullptr);  // Cannot convert!
    return _backreference->prefer_local();
}
```

**Solution for these**: Keep as SCYLLA_ASSERT or use `on_fatal_internal_error()`.

### Unsafe Context #2: Destructors (36 usages)

**Problem**: Destructors are implicitly noexcept, throwing causes `std::terminate()`.

**Example** (from `utils/file_lock.cc`):
```cpp
~file_lock() noexcept {
    if (_fd.get() != -1) {
        SCYLLA_ASSERT(_fd.get() != -1);  // Cannot convert!
        auto r = ::flock(_fd.get(), LOCK_UN);
        SCYLLA_ASSERT(r == 0);  // Cannot convert!
    }
}
```

**Solution for these**: Keep as SCYLLA_ASSERT.

## Benefits of scylla_assert()

1. **Prevents Cluster-Wide Crashes**
   - Exception can be caught and handled gracefully
   - Failed node doesn't bring down entire cluster

2. **Maintains Availability**
   - Service can continue with degraded functionality
   - Better than complete crash

3. **Better Error Reporting**
   - Includes backtrace via `on_internal_error()`
   - Supports custom error messages
   - Configurable abort-on-error for testing

4. **Backward Compatible**
   - SCYLLA_ASSERT still exists for unsafe contexts
   - Can be gradually adopted

## Testing

- Created manual test in `test/manual/test_scylla_assert.cc`
- Verifies passing and failing assertions
- Tests custom error messages
- Code review passed with improvements made

## Next Steps (Future Work)

1. **Gradual Conversion**
   - Convert remaining ~653 safe SCYLLA_ASSERT usages incrementally
   - Prioritize high-impact code paths first

2. **Review noexcept Functions**
   - Evaluate if some can be made non-noexcept
   - Consider using `on_fatal_internal_error()` where appropriate

3. **Integration Testing**
   - Run full test suite with conversions
   - Monitor for any unexpected behavior
   - Validate exception propagation

4. **Automated Analysis Tool**
   - Create tool to identify safe conversion candidates
   - Generate conversion patches automatically
   - Track conversion progress

## Files Modified in This PR

### Core Implementation
- `utils/assert.hh` - Added scylla_assert() macro

### Conversions
- `db/large_data_handler.cc`
- `db/large_data_handler.hh`
- `db/schema_applier.cc`
- `db/system_distributed_keyspace.cc`
- `db/commitlog/commitlog_replayer.cc`
- `db/view/row_locking.cc`
- `db/size_estimates_virtual_reader.cc`
- `db/corrupt_data_handler.cc`
- `raft/tracker.cc`
- `service/topology_coordinator.cc`
- `service/storage_service.cc`
- `sstables/` (22 files across trie/, mx/, and core sstables)

### Documentation
- `docs/dev/scylla_assert_conversion.md`
- `docs/dev/unsafe_scylla_assert_locations.md`
- `test/manual/test_scylla_assert.cc`

## Conclusion

This PR establishes the infrastructure and methodology for replacing SCYLLA_ASSERT with scylla_assert() to improve cluster availability. The sample conversions demonstrate the approach, while comprehensive documentation enables future work.

**Key Achievement**: Provided a safe path forward for converting 75% (~668) of SCYLLA_ASSERT usages to exception-based assertions, while clearly documenting the 25% (~223) that must remain as crash-inducing assertions due to language constraints. Converted 112 usages as demonstration (16.8% of safe conversions), prioritizing critical files like storage_service.cc (node lifecycle) and all sstables files (data persistence), with ~556 remaining.
