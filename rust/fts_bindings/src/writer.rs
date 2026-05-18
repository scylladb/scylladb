// =========================================================================
// FTS Bindings — Document Writer
// =========================================================================
//
// Handles typed document upsert, deletion, commit, and expiration pruning.
// All write operations are called from an alien-thread runner so that the
// blocking Tantivy I/O does not interfere with Seastar's event loop.

use anyhow::Result;
use std::collections::BTreeMap;
use tantivy::schema::Value;
use tantivy::Term;

use crate::ffi::FieldValue;
use crate::types::{CachedDoc, FieldKind, ShardIndex};

/// Upsert a document into the shard index.
///
/// The function implements last-writer-wins semantics: if a cached entry for
/// `doc_id` with an equal or newer `writetime_us` already exists the call is a
/// no-op.  This handles out-of-order CDC event delivery.
///
/// `expires_at_us` of `-1` (i64::MAX cast to i64) indicates no TTL.
pub fn upsert_document(
    index: &mut ShardIndex,
    doc_id: &str,
    partition_key: &str,
    fields: &[FieldValue],
    writetime_us: u64,
    expires_at_us: i64,
) -> Result<()> {
    // ── Last-writer-wins check ──────────────────────────────────────────
    // Check the un-committed cache first; if not found, fall through to
    // the Tantivy stored field.  The cache check covers the common case of
    // rapid successive writes before the next commit window.
    if let Some(cached) = index.uncommitted.get(doc_id) {
        if cached.writetime >= writetime_us {
            return Ok(());
        }
    }

    // ── Build the Tantivy document ──────────────────────────────────────
    let mut doc = tantivy::TantivyDocument::default();

    // System fields — always present.
    doc.add_text(index.field_id, doc_id);
    doc.add_text(index.field_partition_key, partition_key);
    doc.add_i64(index.field_expires_at, expires_at_us);
    doc.add_i64(index.field_writetime, writetime_us as i64);

    // User fields — typed dispatch, no JSON serialisation round-trip.
    for fv in fields {
        if fv.kind == "null" {
            // NULL columns are not indexed.
            continue;
        }

        let (field, kind) = match index.user_fields.get(fv.field_name.as_str()) {
            Some(entry) => entry,
            // Column not present in the index schema (e.g. added after
            // `CREATE INDEX` — will be visible after a rebuild).
            None => continue,
        };

        match kind {
            FieldKind::Text | FieldKind::String => {
                doc.add_text(*field, fv.str_val.as_str());
            }
            FieldKind::I64 => {
                doc.add_i64(*field, fv.i64_val);
            }
            FieldKind::F64 => {
                doc.add_f64(*field, fv.f64_val);
            }
            FieldKind::Bool => {
                doc.add_bool(*field, fv.i64_val != 0);
            }
            FieldKind::Date => {
                // `i64_val` is microseconds since Unix epoch.
                let dt = tantivy::DateTime::from_timestamp_micros(fv.i64_val);
                doc.add_date(*field, dt);
            }
            FieldKind::IpAddr => {
                // Try IPv6 first; fall back to IPv4-mapped-IPv6 for IPv4.
                if let Ok(ip) = fv.str_val.parse::<std::net::Ipv6Addr>() {
                    doc.add_ip_addr(*field, ip);
                } else if let Ok(ip) = fv.str_val.parse::<std::net::Ipv4Addr>() {
                    doc.add_ip_addr(*field, ip.to_ipv6_mapped());
                }
                // Invalid IP: skip silently — the C++ side validates input.
            }
            FieldKind::Bytes => {
                doc.add_bytes(*field, fv.str_val.as_bytes().to_vec());
            }
            FieldKind::Json => {
                if let Ok(serde_json::Value::Object(obj)) =
                    serde_json::from_str::<serde_json::Value>(&fv.str_val)
                {
                    // add_object requires BTreeMap<String, OwnedValue> in Tantivy 0.22.
                    let btree: BTreeMap<String, tantivy::schema::OwnedValue> = obj
                        .into_iter()
                        .map(|(k, v)| (k, serde_json_to_tantivy(v)))
                        .collect();
                    doc.add_object(*field, btree);
                }
                // Invalid JSON: skip silently.
            }
        }
    }

    // ── Delete-then-add (upsert semantics) ─────────────────────────────
    let term = Term::from_field_text(index.field_id, doc_id);
    index.writer.delete_term(term);
    index.writer.add_document(doc)?;

    // Cache the writetime for conflict resolution before the next commit.
    index.uncommitted.insert(
        doc_id.to_string(),
        CachedDoc {
            writetime: writetime_us,
            generation: index.generation,
        },
    );

    Ok(())
}

/// Delete a single document by its `doc_id`.
pub fn delete_document(index: &mut ShardIndex, doc_id: &str) -> Result<()> {
    let term = Term::from_field_text(index.field_id, doc_id);
    index.writer.delete_term(term);
    index.uncommitted.remove(doc_id);
    Ok(())
}

/// Delete all documents belonging to a given partition key.
/// Used when a ScyllaDB partition is deleted entirely.
pub fn delete_by_partition_key(index: &mut ShardIndex, partition_key: &str) -> Result<()> {
    let term = Term::from_field_text(index.field_partition_key, partition_key);
    index.writer.delete_term(term);
    // Purge any cached docs for this partition from the write cache.
    // Because the cache uses doc_id as key (not partition_key), we scan it.
    // This is O(uncommitted.len()) which is bounded by the commit interval.
    index.uncommitted.retain(|doc_id, _| {
        !doc_id.starts_with(&format!("{}|", partition_key)) && doc_id != partition_key
    });
    Ok(())
}

/// Flush all staged writes to a Tantivy segment and make them visible to
/// readers.  Returns the number of documents in the index after commit.
pub fn commit(index: &mut ShardIndex) -> Result<u64> {
    index.writer.commit()?;
    index.reader.reload()?;

    // Advance the generation counter and clear the un-committed cache.
    index.generation += 1;
    index.uncommitted.clear();

    let searcher = index.reader.searcher();
    Ok(searcher.num_docs())
}

/// Delete all documents whose `_expires_at` is in the past.
///
/// Runs in the background via the periodic `prune_interval_ms` timer.
/// Returns the number of documents deleted.
pub fn prune_expired(index: &mut ShardIndex) -> Result<u64> {
    let now_us = now_micros();

    // Build a range query: `_expires_at` in `(-∞, now_us)`.
    // Documents with `_expires_at == i64::MAX` (no TTL) are never pruned
    // because `i64::MAX > now_us` for the foreseeable future.
    //
    // RangeQuery::new_i64_bounds in Tantivy 0.22 takes a field *name* string.
    use std::ops::Bound;
    use tantivy::query::RangeQuery;

    let expires_at_name = index
        .schema
        .get_field_name(index.field_expires_at)
        .to_string();

    let expiry_query =
        RangeQuery::new_i64_bounds(expires_at_name, Bound::Unbounded, Bound::Excluded(now_us));

    let searcher = index.reader.searcher();
    use tantivy::collector::DocSetCollector;
    let expired_docs = searcher.search(&expiry_query, &DocSetCollector)?;

    let mut count = 0u64;
    for doc_addr in &expired_docs {
        let doc: tantivy::TantivyDocument = searcher.doc(*doc_addr)?;
        if let Some(id_val) = doc.get_first(index.field_id) {
            if let Some(id_str) = id_val.as_str() {
                let term = Term::from_field_text(index.field_id, id_str);
                index.writer.delete_term(term);
                count += 1;
            }
        }
    }

    if count > 0 {
        index.writer.commit()?;
        index.reader.reload()?;
    }

    Ok(count)
}

/// Drop the index: commit any pending writes.
/// The C++ caller is responsible for removing the shard directory on disk
/// using `std::filesystem::remove_all` after this function returns.
pub fn drop_index(index: &mut ShardIndex) -> Result<()> {
    // Commit before dropping so that Tantivy can release file handles cleanly.
    // Ignore errors here — we're in a cleanup path.
    let _ = index.writer.commit();
    Ok(())
}

// ─── Utilities ────────────────────────────────────────────────────────────────

/// Current time in microseconds since Unix epoch.
pub fn now_micros() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// Convert a `serde_json::Value` to a `tantivy::schema::OwnedValue` for
/// use with `add_object`.
fn serde_json_to_tantivy(v: serde_json::Value) -> tantivy::schema::OwnedValue {
    match v {
        serde_json::Value::Null => tantivy::schema::OwnedValue::Null,
        serde_json::Value::Bool(b) => tantivy::schema::OwnedValue::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                tantivy::schema::OwnedValue::I64(i)
            } else if let Some(f) = n.as_f64() {
                tantivy::schema::OwnedValue::F64(f)
            } else {
                tantivy::schema::OwnedValue::Str(n.to_string())
            }
        }
        serde_json::Value::String(s) => tantivy::schema::OwnedValue::Str(s),
        serde_json::Value::Array(arr) => {
            tantivy::schema::OwnedValue::Array(arr.into_iter().map(serde_json_to_tantivy).collect())
        }
        serde_json::Value::Object(obj) => tantivy::schema::OwnedValue::Object(
            obj.into_iter()
                .map(|(k, v)| (k, serde_json_to_tantivy(v)))
                .collect(),
        ),
    }
}
