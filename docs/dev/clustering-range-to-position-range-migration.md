# Clustering Range to Position Range Migration

## Background

The `clustering_range` type (alias for `interval<clustering_key_prefix>`) has known issues with operations like `intersection()` and `deoverlap()` that can return incorrect results due to the complexity of comparing clustering key prefixes with different inclusiveness on bounds.

See issues:
- #22817 - `interval<clustering_key_prefix>::deoverlap` can return incorrect results  
- #21604 - Problems with clustering range operations
- #8157 - `interval<clustering_key_prefix_view>::intersection` can return incorrect results

The `position_range` class was introduced as a safer alternative that represents clustering ranges as a pair of `position_in_partition` objects, avoiding the problematic interval semantics.

## Migration Strategy

### 1. Feature Flag

A new `gms::feature` called `"POSITION_RANGE"` has been added to `gms/feature_service.hh`. This feature gates the use of position_range in RPC interfaces to ensure backward compatibility during rolling upgrades.

### 2. IDL Support

The `position_range` class has been added to `idl/position_in_partition.idl.hh` to support serialization in RPC verbs.

### 3. Internal Code Migration

Internal code should be migrated to use `position_range` instead of `clustering_range` wherever possible. This migration should be done incrementally:

#### Priority Areas

1. **Functions with known problematic operations**:
   - Any code using `clustering_range::intersection()`
   - Any code using `clustering_range::deoverlap()`
   - See marked locations in:
     - `db/view/view.cc` (lines 1687-1713)
     - `cql3/statements/cas_request.cc` (line 90-91)

2. **Internal data structures**:
   - Readers and iterators that track position ranges
   - Cache structures
   - Query processing internals

3. **Utility functions**:
   - Helper functions that operate on ranges
   - Range manipulation and transformation functions

#### Conversion Utilities

Existing converters:
- `position_range::from_range(const query::clustering_range&)` - Convert clustering_range to position_range
- `position_range_to_clustering_range(const position_range&, const schema&)` - Convert position_range to clustering_range (returns optional)

The `clustering_interval_set` class already demonstrates best practices - it uses `position_range` internally and provides conversion methods to/from `clustering_row_ranges`.

Helper utilities in `query/position_range_utils.hh`:
- `clustering_row_ranges_to_position_ranges()` - Batch convert clustering ranges to position ranges
- `position_ranges_to_clustering_row_ranges()` - Batch convert position ranges to clustering ranges
- `deoverlap_clustering_row_ranges()` - Safely deoverlap ranges using clustering_interval_set
- `intersect_clustering_row_ranges()` - Safely intersect ranges using clustering_interval_set

#### Migration Pattern

```cpp
// OLD CODE (problematic):
void process_ranges(const query::clustering_row_ranges& ranges) {
    auto deoverlapped = query::clustering_range::deoverlap(ranges, cmp);
    // ... use deoverlapped ranges
}

// NEW CODE (using position_range):
void process_ranges(const schema& s, const query::clustering_row_ranges& ranges) {
    clustering_interval_set interval_set(s, ranges);
    // interval_set handles deoverlapping correctly internally
    for (const position_range& r : interval_set) {
        // ... use position ranges
    }
    // Convert back if needed for compatibility
    auto result_ranges = interval_set.to_clustering_row_ranges();
}
```

### 4. RPC Interface Migration

RPC interfaces must maintain backward compatibility. The strategy is:

1. Keep existing RPC verbs that use `clustering_range` (in IDL: `std::vector<interval<clustering_key_prefix>>`)
2. Add new RPC verbs that use `position_range`
3. Use the new verbs when `feature_service.position_range` is enabled

#### Example RPC Migration

In `idl/storage_proxy.idl.hh`:

```cpp
// Existing verb (keep for compatibility)
verb [[with_client_info, with_timeout]] read_data (
    query::read_command cmd [[ref]], 
    ::compat::wrapping_partition_range pr, 
    ...
) -> query::result [[lw_shared_ptr]], ...;

// New verb using position_range (to be added)
verb [[with_client_info, with_timeout]] read_data_v2 (
    query::read_command_v2 cmd [[ref]], 
    ::compat::wrapping_partition_range pr, 
    ...
) -> query::result [[lw_shared_ptr]], ...;
```

Where `read_command_v2` would use a `partition_slice_v2` that contains position ranges instead of clustering ranges.

#### Feature-Gated RPC Selection

```cpp
future<query::result> storage_proxy::query_data(...) {
    if (_features.position_range) {
        return rpc_verb_read_data_v2(...);
    } else {
        return rpc_verb_read_data(...);
    }
}
```

### 5. Testing

Tests should verify:
1. Correct conversion between clustering_range and position_range
2. Correct behavior of position_range operations
3. RPC compatibility with both old and new verbs
4. Feature flag behavior during rolling upgrades

### 6. Known Limitations

- Not all clustering_range uses can be eliminated - some external interfaces may require them
- The conversion from position_range to clustering_range can return `nullopt` for empty ranges
- Performance implications should be measured for hot paths

## Implementation Checklist

- [x] Add `position_range` feature flag to `gms/feature_service.hh`
- [x] Add `position_range` IDL definition to `idl/position_in_partition.idl.hh`
- [ ] Create new RPC verbs using position_range
- [ ] Add feature-gated RPC selection logic
- [ ] Migrate high-priority problematic code paths:
  - [ ] Fix intersection in `db/view/view.cc:1687`
  - [ ] Fix deoverlap in `db/view/view.cc:1712`
  - [ ] Fix deoverlap in `cql3/statements/cas_request.cc:90`
- [ ] Migrate internal data structures systematically
- [ ] Add comprehensive tests
- [ ] Performance benchmarking
- [ ] Documentation updates

## References

- `mutation/position_in_partition.hh` - position_range definition
- `keys/clustering_interval_set.hh` - Example of correct position_range usage
- Issues: #22817, #21604, #8157
