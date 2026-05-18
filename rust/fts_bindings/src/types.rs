// =========================================================================
// FTS Bindings — Type Definitions
// =========================================================================
//
// Core types shared across the schema, writer, and reader modules.
// Each `ShardIndex` is exclusively owned by one Seastar shard (or its
// alien-thread counterpart) — no cross-shard sharing occurs.

use std::collections::HashMap;

// ─── Field kind discriminant ─────────────────────────────────────────────────

/// Mirrors the Tantivy field type so the writer can dispatch correctly
/// without a dynamic trait object at every document insertion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldKind {
    /// Tokenized full-text (TEXT | STORED)
    Text,
    /// Untokenized exact match (STRING | STORED) — UUID, timeuuid, decimal, varint
    String,
    /// 64-bit signed integer (INDEXED | STORED | FAST)
    I64,
    /// 64-bit float (INDEXED | STORED | FAST)
    F64,
    /// Boolean (INDEXED | STORED | FAST)
    Bool,
    /// DateTime (INDEXED | STORED | FAST) — timestamp, date
    Date,
    /// IPv4/IPv6 address (INDEXED | STORED | FAST)
    IpAddr,
    /// Raw bytes (STORED only — not searchable)
    Bytes,
    /// JSON fallback for map<K,V> columns
    Json,
}

// ─── Document cache entry ─────────────────────────────────────────────────────

/// Tracks the last write timestamp for a document that has been staged but
/// not yet committed. Used for last-writer-wins conflict resolution so that
/// stale CDC events arriving out of order are silently discarded.
#[derive(Debug, Clone)]
pub struct CachedDoc {
    /// `writetime` of the most recent upsert for this `doc_id`.
    pub writetime: u64,
    /// Generation counter at the time of write — reserved for future use when
    /// determining safe cache pruning windows after multi-generational commits.
    // TODO: read this field once generational cache eviction is implemented.
    #[allow(dead_code)]
    pub generation: u64,
}

// ─── The shard-local Tantivy index ───────────────────────────────────────────

/// Owns the Tantivy index, writer, and reader for one ScyllaDB shard.
///
/// Thread-safety contract: each instance is accessed exclusively from a single
/// alien-thread runner attached to its owning Seastar shard. The `unsafe impl
/// Send` below is therefore safe — it matches the same pattern used in
/// `wasmtime_bindings/src/memory_creator.rs:73` for `ScyllaLinearMemory`.
pub struct ShardIndex {
    pub index: tantivy::Index,
    pub writer: tantivy::IndexWriter,
    pub reader: tantivy::IndexReader,
    pub schema: tantivy::schema::Schema,

    // ── System fields (always present in every document) ─────────────────
    /// Unique document ID: "<partition_key>:<clustering_key>" or just
    /// "<partition_key>" for tables without clustering columns.
    pub field_id: tantivy::schema::Field,
    /// Serialised partition key — returned in search hits so the C++ side
    /// can fetch the full row from the base table.
    pub field_partition_key: tantivy::schema::Field,
    /// Expiry time in microseconds since epoch (i64::MAX = no TTL).
    /// Used by the TTL filter in every search query.
    pub field_expires_at: tantivy::schema::Field,
    /// Write-timestamp in microseconds since epoch.
    /// Used for last-writer-wins conflict resolution.
    pub field_writetime: tantivy::schema::Field,

    // ── User fields (inferred from CQL schema at index creation time) ─────
    /// Maps CQL column name (dotted for UDT sub-fields) to a `(Field, FieldKind)` pair.
    pub user_fields: HashMap<String, (tantivy::schema::Field, FieldKind)>,
    /// All `FieldKind::Text` user fields — passed to `QueryParser::for_index`
    /// so that bare keyword queries search across every text column.
    pub default_text_fields: Vec<tantivy::schema::Field>,

    // ── Un-committed write cache ──────────────────────────────────────────
    /// Holds the latest writetime for each doc_id that has been staged
    /// (writer.add_document) but not yet flushed to a Tantivy segment.
    pub uncommitted: HashMap<String, CachedDoc>,
    /// Monotonically-increasing counter incremented on every successful
    /// `commit()`. Used to expire stale `uncommitted` entries.
    pub generation: u64,
}

// Safety: `ShardIndex` is always accessed from the single alien-thread
// runner attached to its owning Seastar shard; no concurrent access ever
// occurs. This matches the pattern used for `ScyllaLinearMemory` in
// `wasmtime_bindings/src/memory_creator.rs`.
unsafe impl Send for ShardIndex {}
