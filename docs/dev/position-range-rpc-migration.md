# Position Range RPC Migration Plan

## Overview

This document outlines the plan for migrating RPC interfaces from `clustering_range` to `position_range` in a backward-compatible manner using feature flags.

## Background

The current RPC interfaces use `clustering_range` (defined as `interval<clustering_key_prefix>`) in structures like `partition_slice` and `read_command`. To enable the use of `position_range` internally while maintaining backward compatibility, we need to:

1. Create new RPC message types that use `position_range`
2. Add new RPC verbs that accept these new types
3. Feature-gate the use of these new verbs based on cluster capabilities

## Feature Flag

A new feature flag `position_range` has been added to `gms::feature_service`:

```cpp
gms::feature position_range { *this, "POSITION_RANGE"sv };
```

This feature will be enabled when all nodes in the cluster support the new RPC verbs.

## IDL Changes

### Already Added

The `position_range` class has been added to `idl/position_in_partition.idl.hh`:

```idl
class position_range {
    position_in_partition start();
    position_in_partition end();
};
```

### To Be Added

New IDL types need to be created for RPC migration:

#### 1. partition_slice_v2 (in `idl/read_command.idl.hh`)

```idl
namespace query {

class partition_slice_v2 {
    std::vector<position_range> default_row_ranges();
    utils::small_vector<uint32_t, 8> static_columns;
    utils::small_vector<uint32_t, 8> regular_columns;
    query::partition_slice::option_set options;
    std::unique_ptr<query::specific_ranges> get_specific_ranges();
    cql_serialization_format cql_format();
    uint32_t partition_row_limit_low_bits();
    uint32_t partition_row_limit_high_bits();
};

class read_command_v2 {
    table_id cf_id;
    table_schema_version schema_version;
    query::partition_slice_v2 slice;
    uint32_t row_limit_low_bits;
    std::chrono::time_point<gc_clock, gc_clock::duration> timestamp;
    std::optional<tracing::trace_info> trace_info;
    uint32_t partition_limit;
    query_id query_uuid;
    query::is_first_page is_first_page;
    std::optional<query::max_result_size> max_result_size;
    uint32_t row_limit_high_bits;
    uint64_t tombstone_limit;
};

}
```

#### 2. New RPC Verbs (in `idl/storage_proxy.idl.hh`)

```idl
// New verbs using position_range (to be used when position_range feature is enabled)
verb [[with_client_info, with_timeout]] read_data_v2 (
    query::read_command_v2 cmd [[ref]], 
    ::compat::wrapping_partition_range pr, 
    query::digest_algorithm digest,
    db::per_partition_rate_limit::info rate_limit_info,
    service::fencing_token fence
) -> query::result [[lw_shared_ptr]], 
     cache_temperature, 
     replica::exception_variant;

verb [[with_client_info, with_timeout]] read_mutation_data_v2 (
    query::read_command_v2 cmd [[ref]], 
    ::compat::wrapping_partition_range pr, 
    service::fencing_token fence
) -> reconcilable_result [[lw_shared_ptr]], 
     cache_temperature, 
     replica::exception_variant;

verb [[with_client_info, with_timeout]] read_digest_v2 (
    query::read_command_v2 cmd [[ref]], 
    ::compat::wrapping_partition_range pr, 
    query::digest_algorithm digest,
    db::per_partition_rate_limit::info rate_limit_info,
    service::fencing_token fence
) -> query::result_digest, 
     api::timestamp_type, 
     cache_temperature, 
     replica::exception_variant, 
     std::optional<full_position>;
```

## Implementation Changes

### 1. C++ Type Definitions

Create C++ implementations for the new IDL types:

```cpp
// In query/query-request.hh or a new header
namespace query {

class partition_slice_v2 {
    std::vector<position_range> _row_ranges;
    // ... other members same as partition_slice
    
public:
    // Constructors
    partition_slice_v2(std::vector<position_range> row_ranges, ...);
    
    // Conversion methods
    static partition_slice_v2 from_legacy(const partition_slice& legacy);
    partition_slice to_legacy(const schema& s) const;
    
    // Accessors
    const std::vector<position_range>& row_ranges() const { return _row_ranges; }
};

class read_command_v2 {
    partition_slice_v2 slice;
    // ... other members same as read_command
    
public:
    // Constructors
    read_command_v2(...);
    
    // Conversion methods
    static read_command_v2 from_legacy(const read_command& legacy);
    read_command to_legacy(const schema& s) const;
};

}
```

### 2. RPC Handler Implementation

In `service/storage_proxy.cc`, add handlers for the new verbs:

```cpp
future<rpc::tuple<query::result_lw_shared_ptr, cache_temperature, replica::exception_variant>>
storage_proxy::read_data_v2_handler(
    query::read_command_v2&& cmd,
    compat::wrapping_partition_range&& pr,
    query::digest_algorithm da,
    db::per_partition_rate_limit::info rate_limit_info,
    service::fencing_token fence) {
    
    // Convert to legacy format if needed internally
    // Or better: refactor internal implementation to work with position_range
    auto legacy_cmd = cmd.to_legacy(*get_schema(cmd.cf_id));
    
    // Call existing implementation
    return read_data_handler(std::move(legacy_cmd), std::move(pr), da, rate_limit_info, fence);
}
```

### 3. RPC Client Selection

In code that invokes RPCs (e.g., `storage_proxy::query_result`), add feature detection:

```cpp
future<query::result> storage_proxy::query_data(...) {
    if (_features.position_range) {
        // Use new verb with position_range
        auto cmd_v2 = read_command_v2::from_legacy(cmd);
        return rpc_verb_read_data_v2(std::move(cmd_v2), ...);
    } else {
        // Use legacy verb with clustering_range
        return rpc_verb_read_data(std::move(cmd), ...);
    }
}
```

## Migration Strategy

### Phase 1: Foundation (Complete)
- [x] Add `position_range` feature flag
- [x] Add `position_range` IDL definition
- [x] Fix critical clustering_range bugs using clustering_interval_set

### Phase 2: RPC Infrastructure (To Do)
- [ ] Add `partition_slice_v2` IDL definition
- [ ] Add `read_command_v2` IDL definition
- [ ] Add new RPC verbs (`read_data_v2`, etc.)
- [ ] Implement conversion methods between v1 and v2 types
- [ ] Add RPC handlers for new verbs

### Phase 3: Client Migration (To Do)
- [ ] Update RPC clients to check feature flag
- [ ] Add logic to select appropriate verb based on feature availability
- [ ] Test backward compatibility during rolling upgrades

### Phase 4: Internal Refactoring (To Do)
- [ ] Gradually refactor internal implementations to use position_range natively
- [ ] Remove conversion overhead once both versions are established
- [ ] Update documentation and examples

### Phase 5: Deprecation (Future)
- [ ] Once all production clusters are upgraded, consider deprecating v1 verbs
- [ ] Remove legacy code after sufficient time has passed

## Testing

### Unit Tests
- Test conversion between partition_slice and partition_slice_v2
- Test conversion between read_command and read_command_v2
- Verify that converted types produce equivalent results

### Integration Tests
- Test RPC calls using both old and new verbs
- Verify feature flag behavior during rolling upgrades
- Test mixed-version clusters

### Backward Compatibility Tests
- Ensure old clients can still communicate with new servers
- Ensure new clients fall back to old verbs when feature is disabled

## Performance Considerations

1. **Conversion Overhead**: During the transition period, conversions between v1 and v2 types add overhead. This should be measured and minimized.

2. **Memory Usage**: position_range may have different memory characteristics than clustering_range. Monitor memory usage after migration.

3. **Serialization Size**: Compare wire format sizes to ensure no significant increase in network traffic.

## Risks and Mitigation

### Risk: Conversion Bugs
**Mitigation**: Comprehensive unit tests for all conversion paths, particularly edge cases like empty ranges and open-ended ranges.

### Risk: Feature Flag Synchronization
**Mitigation**: Use standard Scylla feature propagation mechanisms. Ensure feature is only enabled when all nodes support it.

### Risk: Performance Regression
**Mitigation**: Performance benchmarks comparing old and new implementations. Have rollback plan if issues are discovered.

## Alternative Approaches Considered

### 1. Direct Migration Without Feature Flag
**Rejected**: Too risky for rolling upgrades. Would require all-at-once cluster upgrade.

### 2. Transparent Conversion in IDL Layer
**Rejected**: Would hide the distinction between old and new formats, making debugging harder.

### 3. Maintain Both Forever
**Rejected**: Increases maintenance burden without clear benefit once migration is complete.

## References

- Main migration guide: `docs/dev/clustering-range-to-position-range-migration.md`
- Issues: #22817, #21604, #8157
- Feature service: `gms/feature_service.hh`
- IDL definitions: `idl/position_in_partition.idl.hh`, `idl/read_command.idl.hh`
