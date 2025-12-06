# Analysis of unimplemented::cause Enum Values

This document provides an analysis of the `unimplemented::cause` enum values after cleanup.

## Removed Unused Enum Values (20 values removed)

The following enum values had **zero usages** in the codebase and have been removed:

- `LWT` - Lightweight transactions
- `PAGING` - Query result paging
- `AUTH` - Authentication
- `PERMISSIONS` - Permission checking
- `COUNTERS` - Counter columns
- `MIGRATIONS` - Schema migrations
- `GOSSIP` - Gossip protocol
- `TOKEN_RESTRICTION` - Token-based restrictions
- `LEGACY_COMPOSITE_KEYS` - Legacy composite key handling
- `COLLECTION_RANGE_TOMBSTONES` - Collection range tombstones
- `RANGE_DELETES` - Range deletion operations
- `COMPRESSION` - Compression features
- `NONATOMIC` - Non-atomic operations
- `CONSISTENCY` - Consistency level handling
- `WRAP_AROUND` - Token wrap-around handling
- `STORAGE_SERVICE` - Storage service operations
- `SCHEMA_CHANGE` - Schema change operations
- `MIXED_CF` - Mixed column family operations
- `SSTABLE_FORMAT_M` - SSTable format M

## Remaining Enum Values (8 values kept)

### 1. `API` (4 usages)
**Impact**: REST API features that are not fully implemented.

**Usages**:
- `api/column_family.cc:1052` - Fails when `split_output` parameter is used in major compaction
- `api/compaction_manager.cc:100,146,216` - Warns when force_user_defined_compaction or related operations are called

**User Impact**: Some REST API endpoints for compaction management are stubs and will warn or fail.

### 2. `INDEXES` (6 usages)
**Impact**: Secondary index features not fully supported.

**Usages**:
- `api/column_family.cc:433,440,449,456` - Warns about index-related operations
- `cql3/restrictions/statement_restrictions.cc:1158` - Fails when attempting filtering on collection columns without proper indexing
- `cql3/statements/update_statement.cc:149` - Warns about index operations

**User Impact**: Some advanced secondary index features (especially filtering on collections) are not available.

### 3. `TRIGGERS` (2 usages)
**Impact**: Trigger support is not implemented.

**Usages**:
- `db/schema_tables.cc:2017` - Warns when loading trigger metadata from schema tables
- `service/storage_proxy.cc:4166` - Warns when processing trigger-related operations

**User Impact**: Cassandra triggers (stored procedures that execute on data changes) are not supported.

### 4. `METRICS` (1 usage)
**Impact**: Some query processor metrics are not collected.

**Usages**:
- `cql3/query_processor.cc:585` - Warns about missing metrics implementation

**User Impact**: Minor - some internal metrics may not be available.

### 5. `VALIDATION` (4 usages)
**Impact**: Schema validation checks are partially implemented.

**Usages**:
- `cql3/functions/token_fct.hh:38` - Warns about validation in token functions
- `cql3/statements/drop_keyspace_statement.cc:40` - Warns when dropping keyspace
- `cql3/statements/truncate_statement.cc:87` - Warns when truncating table
- `service/migration_manager.cc:750` - Warns during schema migrations

**User Impact**: Some schema validation checks are skipped (with warnings logged).

### 6. `REVERSED` (1 usage)
**Impact**: Reversed type support in CQL protocol.

**Usages**:
- `transport/server.cc:2085` - Fails when trying to use reversed types in CQL protocol

**User Impact**: Reversed types are not supported in the CQL protocol implementation.

### 7. `HINT` (1 usage)
**Impact**: Hint replaying is not implemented.

**Usages**:
- `db/batchlog_manager.cc:251` - Warns when attempting to replay hints

**User Impact**: Cassandra hints (temporary storage of writes when nodes are down) are not supported.

### 8. `SUPER` (2 usages)
**Impact**: Super column families are not supported.

**Usages**:
- `db/legacy_schema_migrator.cc:157` - Fails when encountering super column family in legacy schema
- `db/schema_tables.cc:2288` - Fails when encountering super column family in schema tables

**User Impact**: Super column families (legacy Cassandra feature) will cause errors if encountered in legacy data or schema migrations.

## Summary

- **Removed**: 20 unused enum values (76% reduction)
- **Kept**: 8 actively used enum values (24% remaining)
- **Total lines removed**: ~40 lines from enum definition and switch statement

The remaining enum values represent actual unimplemented features that users may encounter, with varying impacts ranging from warnings (TRIGGERS, METRICS, VALIDATION, HINT) to failures (API split_output, INDEXES on collections, REVERSED types, SUPER tables).
