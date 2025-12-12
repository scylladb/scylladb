# SCYLLA_ASSERT to scylla_assert() Conversion Guide

## Overview

This document tracks the conversion of `SCYLLA_ASSERT` to the new `scylla_assert()` macro based on `on_internal_error()`. The new macro throws exceptions instead of crashing the process, preventing cluster-wide crashes and loss of availability.

## Status Summary

- **Total SCYLLA_ASSERT usages**: ~1307 (including tests)
- **Non-test usages**: ~886
- **Unsafe conversions (noexcept)**: ~187
- **Unsafe conversions (destructors)**: ~36
- **Safe conversions possible**: ~668
- **Converted so far**: 112

## Safe vs Unsafe Contexts

### Safe to Convert ✓
- Regular functions (non-noexcept)
- Coroutine functions (returning `future<T>`)
- Member functions without noexcept specifier
- Functions where exception propagation is acceptable

### Unsafe to Convert ✗
1. **noexcept functions** - throwing exceptions from noexcept causes `std::terminate()`
2. **Destructors** - destructors are implicitly noexcept
3. **noexcept lambdas and callbacks**
4. **Code with explicit exception-safety requirements** that cannot handle exceptions

## Files with Unsafe Conversions

### Files with SCYLLA_ASSERT in noexcept contexts (examples)

1. **reader_concurrency_semaphore.cc**
   - Lines with noexcept functions containing SCYLLA_ASSERT
   - Must remain as SCYLLA_ASSERT

2. **db/large_data_handler.cc**
   - Line 86: `maybe_delete_large_data_entries()` - marked noexcept but contains SCYLLA_ASSERT
   - Analysis shows this is actually safe (not truly noexcept)

3. **db/row_cache.cc**
   - Multiple SCYLLA_ASSERT usages in noexcept member functions

4. **db/schema_tables.cc**
   - SCYLLA_ASSERT in noexcept contexts

5. **raft/server.cc**
   - Multiple noexcept functions with SCYLLA_ASSERT

### Files with SCYLLA_ASSERT in destructors

1. **reader_concurrency_semaphore.cc**
   - Line 1116: SCYLLA_ASSERT in destructor

2. **api/column_family.cc**
   - Line 102: SCYLLA_ASSERT in destructor

3. **utils/logalloc.cc**
   - Line 1991: SCYLLA_ASSERT in destructor

4. **utils/file_lock.cc**
   - Lines 34, 36: SCYLLA_ASSERT in destructor

5. **utils/disk_space_monitor.cc**
   - Line 66: SCYLLA_ASSERT in destructor

## Conversion Strategy

### Phase 1: Infrastructure (Completed)
- Created `scylla_assert()` macro in `utils/assert.hh`
- Uses `on_internal_error()` for exception-based error handling
- Supports optional message parameters

### Phase 2: Safe Conversions
Convert SCYLLA_ASSERT to scylla_assert in contexts where:
- Function is not noexcept
- Not in a destructor
- Exception propagation is safe

### Phase 3: Document Remaining Uses
For contexts that cannot be converted:
- Add comments explaining why SCYLLA_ASSERT must remain
- Consider alternative approaches (e.g., using `on_fatal_internal_error()` in noexcept)

## Converted Files

### Completed Conversions

1. **db/large_data_handler.cc** (3 conversions)
   - Line 42: `maybe_record_large_partitions()`
   - Line 86: `maybe_delete_large_data_entries()`
   - Line 250: `delete_large_data_entries()`

2. **db/large_data_handler.hh** (2 conversions)
   - Line 83: `maybe_record_large_rows()`
   - Line 103: `maybe_record_large_cells()`

3. **db/schema_applier.cc** (1 conversion)
   - Line 1124: `commit()` coroutine

4. **db/system_distributed_keyspace.cc** (1 conversion)
   - Line 234: `get_updated_service_levels()`

5. **db/commitlog/commitlog_replayer.cc** (1 conversion)
   - Line 168: `recover()` coroutine

6. **db/view/row_locking.cc** (2 conversions)
   - Line 156: `unlock()` - partition lock check
   - Line 163: `unlock()` - row lock check

7. **db/size_estimates_virtual_reader.cc** (1 conversion)
   - Line 190: Lambda in `get_local_ranges()`

8. **db/corrupt_data_handler.cc** (2 conversions)
   - Line 78: `set_cell_raw` lambda
   - Line 85: `set_cell` lambda

9. **raft/tracker.cc** (2 conversions)
   - Line 49: Switch default case with descriptive error
   - Line 90: Switch default case with descriptive error

10. **service/topology_coordinator.cc** (11 conversions)
    - Line 363: Node lookup assertion in `retake_node()`
    - Line 2313: Bootstrapping state ring check
    - Line 2362: Replacing state ring check
    - Line 2365: Normal nodes lookup assertion
    - Line 2366: Node ring and state validation
    - Line 3025: Join request ring check
    - Line 3036: Leave request ring check
    - Line 3049: Remove request ring check
    - Line 3061: Replace request ring check
    - Line 3166: Transition nodes empty check
    - Line 4016: Barrier validation in `stop()`

11. **service/storage_service.cc** (28 conversions, 3 unsafe kept as SCYLLA_ASSERT)
    - Lines 603, 691, 857, 901, 969: Core service operations
    - Lines 1523, 1575, 1844, 2086, 2170, 2195: Bootstrap and join operations
    - Lines 2319, 2352, 2354: Replacement operations
    - Lines 3003, 3028, 3228: Cluster join and drain operations
    - Lines 3995, 4047, 4353: Decommission and removenode operations
    - Lines 4473, 5787, 5834, 5958: CDC and topology change operations
    - Lines 6490, 6491: Tablet streaming operations
    - Line 7512: Join node response handler
    - **Unsafe (kept as SCYLLA_ASSERT)**: Lines 3398, 5760, 5775 (noexcept functions)

12. **sstables/** (58 conversions across 22 files)
    - **sstables/trie/bti_node_reader.cc** (6): Node reading operations
    - **sstables/mx/writer.cc** (6): MX format writing
    - **sstables/sstable_set.cc** (5): SSTable set management
    - **sstables/compressor.cc** (5): Compression/decompression
    - **sstables/trie/trie_writer.hh** (4): Trie writing
    - **sstables/downsampling.hh** (4): Downsampling operations
    - **sstables/storage.{cc,hh}** (6): Storage operations
    - **sstables/sstables_manager.{cc,hh}** (6): SSTable lifecycle management
    - **sstables/trie/writer_node.{hh,impl.hh}** (4): Trie node writing
    - **sstables/trie/bti_key_translation.cc** (2): Key translation
    - **sstables/sstable_directory.cc** (2): Directory management
    - **sstables/trie/trie_writer.cc** (1): Trie writer implementation
    - **sstables/trie/trie_traversal.hh** (1): Trie traversal
    - **sstables/sstables.cc** (1): Core SSTable operations
    - **sstables/partition_index_cache.hh** (1): Index caching
    - **sstables/generation_type.hh** (1): Generation management
    - **sstables/compress.{cc,hh}** (2): Compression utilities
    - **sstables/exceptions.hh** (1): Comment update

## Testing

### Manual Testing
Created `test/manual/test_scylla_assert.cc` to verify:
- Passing assertions succeed
- Failing assertions throw exceptions
- Custom messages are properly formatted

### Integration Testing
- Run existing test suite with converted assertions
- Verify no regressions in error handling
- Confirm exception propagation works correctly

## Future Work

1. **Automated Analysis Tool**
   - Create tool to identify safe vs unsafe conversion contexts
   - Generate reports of remaining conversions

2. **Gradual Conversion**
   - Convert additional safe usages incrementally
   - Monitor for any unexpected issues

3. **noexcept Review**
   - Review functions marked noexcept that contain SCYLLA_ASSERT
   - Consider if they should use `on_fatal_internal_error()` instead

## References

- `utils/assert.hh` - Implementation of both SCYLLA_ASSERT and scylla_assert
- `utils/on_internal_error.hh` - Exception-based error handling infrastructure
- GitHub Issue: [Link to original issue tracking this work]
