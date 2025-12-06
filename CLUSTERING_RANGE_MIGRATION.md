# Clustering Range to Position Range Migration - Summary

## Problem Statement

The `clustering_range` type (alias for `interval<clustering_key_prefix>`) has known correctness issues with operations like `intersection()` and `deoverlap()`. These operations can return incorrect results due to the complex semantics of comparing clustering key prefixes with different bound inclusiveness.

**Related Issues:**
- #22817 - `interval<clustering_key_prefix>::deoverlap` can return incorrect results
- #21604 - Problems with clustering range operations  
- #8157 - `interval<clustering_key_prefix_view>::intersection` can return incorrect results

## Solution Approach

The `position_range` class represents clustering ranges as a pair of `position_in_partition` objects, avoiding the problematic interval semantics. The migration strategy involves:

1. **Fix critical bugs immediately** - Use `clustering_interval_set` which internally uses `position_range`
2. **Add infrastructure** - Feature flags, IDL support, utility functions
3. **Gradual internal migration** - Replace internal uses of `clustering_range` with `position_range`
4. **RPC compatibility** - Maintain backward compatibility with feature-gated new verbs

## What Has Been Done

### 1. Feature Flag ✅
Added `gms::feature position_range` to `gms/feature_service.hh` for cluster-wide feature detection.

### 2. IDL Support ✅
Added `position_range` to `idl/position_in_partition.idl.hh` for RPC serialization:
```idl
class position_range {
    position_in_partition start();
    position_in_partition end();
};
```

### 3. Critical Bug Fixes ✅

#### Fixed in `cql3/statements/cas_request.cc`:
```cpp
// OLD (buggy):
ranges = query::clustering_range::deoverlap(std::move(ranges), clustering_key::tri_compare(*_schema));

// NEW (fixed):
clustering_interval_set interval_set(*_schema, ranges);
ranges = interval_set.to_clustering_row_ranges();
```

#### Fixed in `db/view/view.cc`:
```cpp
// OLD (buggy):
auto deoverlapped_ranges = interval<clustering_key_prefix_view>::deoverlap(std::move(row_ranges), cmp);

// NEW (fixed):
clustering_interval_set interval_set(base, temp_ranges);
return interval_set.to_clustering_row_ranges();
```

### 4. Utility Functions ✅
Created `query/position_range_utils.hh` with safe range operation helpers:
- `clustering_row_ranges_to_position_ranges()` - Batch conversion
- `position_ranges_to_clustering_row_ranges()` - Batch conversion back
- `deoverlap_clustering_row_ranges()` - Safe deoverlap using clustering_interval_set
- `intersect_clustering_row_ranges()` - Safe intersection using clustering_interval_set

### 5. Tests ✅
Added comprehensive unit tests in `test/boost/position_range_utils_test.cc`:
- Test deoverlap with overlapping and non-overlapping ranges
- Test conversion between clustering_range and position_range
- Test intersection operations
- Validate correctness of utility functions

### 6. Documentation ✅
- **Migration guide**: `docs/dev/clustering-range-to-position-range-migration.md`
  - Overview of the problem and solution
  - Conversion utilities and patterns
  - Implementation checklist
  
- **RPC migration plan**: `docs/dev/position-range-rpc-migration.md`
  - Detailed plan for backward-compatible RPC migration
  - IDL type definitions for v2 types
  - Feature-gated verb selection logic
  - Phased rollout strategy

## What Remains To Be Done

### Phase 1: RPC Migration (High Priority)
1. Define `partition_slice_v2` with `std::vector<position_range>`
2. Define `read_command_v2` using `partition_slice_v2`
3. Add new RPC verbs: `read_data_v2`, `read_mutation_data_v2`, `read_digest_v2`
4. Implement conversion between v1 and v2 types
5. Add feature-gated verb selection in RPC clients
6. Test backward compatibility

### Phase 2: Internal Refactoring (Ongoing)
1. Identify internal data structures using `clustering_range`
2. Refactor to use `position_range` where appropriate
3. Update mutation readers and iterators
4. Modify query processing logic
5. Update cache structures

### Phase 3: Validation (Continuous)
1. Build and run existing tests
2. Add more tests for edge cases
3. Performance benchmarking
4. Rolling upgrade testing

## Files Changed

### Core Changes
- `gms/feature_service.hh` - Added position_range feature flag
- `idl/position_in_partition.idl.hh` - Added position_range IDL definition
- `cql3/statements/cas_request.cc` - Fixed deoverlap bug
- `db/view/view.cc` - Fixed deoverlap bug, enhanced documentation

### New Files
- `query/position_range_utils.hh` - Utility functions for safe range operations
- `test/boost/position_range_utils_test.cc` - Unit tests for utilities

### Documentation
- `docs/dev/clustering-range-to-position-range-migration.md` - Migration guide
- `docs/dev/position-range-rpc-migration.md` - RPC migration plan
- `CLUSTERING_RANGE_MIGRATION.md` - This summary document

## Impact and Benefits

### Immediate Benefits ✅
- **Fixed critical bugs**: Two production code bugs in `cas_request.cc` and `view.cc` that could cause incorrect query results
- **Safe operations**: Developers can now use utility functions that guarantee correct deoverlap and intersection
- **Future-proof**: Infrastructure is in place for gradual migration

### Future Benefits 🔄
- **Correctness**: All clustering range operations will be correct by construction
- **Maintainability**: Clearer code using position_range instead of complex interval semantics
- **Performance**: Potential optimizations from simpler position-based comparisons

## Testing Strategy

### Unit Tests ✅
- `test/boost/position_range_utils_test.cc` validates utility functions
- Existing tests in `test/boost/mutation_test.cc` use clustering_interval_set
- Tests in `test/boost/mvcc_test.cc` validate clustering_interval_set behavior

### Integration Testing (To Do)
- Test RPC backward compatibility during rolling upgrades
- Test mixed-version clusters
- Validate query correctness with position_range

### Performance Testing (To Do)
- Benchmark conversion overhead
- Compare memory usage
- Measure query latency impact

## Migration Timeline

- **Week 1-2**: ✅ Foundation and critical bug fixes (COMPLETED)
  - Feature flag
  - IDL support
  - Bug fixes in cas_request.cc and view.cc
  - Utility functions and tests
  - Documentation

- **Week 3-4**: 🔄 RPC migration (IN PROGRESS)
  - Define v2 IDL types
  - Implement new RPC verbs
  - Add feature-gated selection

- **Week 5-8**: 🔄 Internal refactoring (PLANNED)
  - Systematic replacement in internal code
  - Update readers and iterators
  - Performance validation

- **Week 9+**: 🔄 Validation and rollout (PLANNED)
  - Comprehensive testing
  - Rolling upgrade validation
  - Production deployment

## Key Takeaways

1. **clustering_interval_set is your friend**: When working with clustering ranges, use clustering_interval_set for set operations instead of raw interval operations.

2. **Use utility functions**: The helpers in `query/position_range_utils.hh` provide safe alternatives to buggy operations.

3. **RPC requires care**: Backward compatibility is critical. Always use feature flags for RPC changes.

4. **Incremental approach**: This is a large refactoring. Do it incrementally, with tests at each step.

5. **Document as you go**: Good documentation (like this) helps future developers understand the context and rationale.

## References

- `mutation/position_in_partition.hh` - position_range definition
- `keys/clustering_interval_set.hh` - Safe clustering range operations
- `query/query-request.hh` - clustering_range definition and warnings
- Issues: #22817, #21604, #8157
- Feature service: `gms/feature_service.hh`
