// =========================================================================
// FTS Bindings — Search / Read Path
// =========================================================================
//
// Implements `search()`, `list_ids_by_partition_key()`, `doc_count()`,
// and facet aggregation.
//
// Key design decisions:
//   - Results carry only `(_id, _partition_key, score)` — no stored payload.
//     The C++ query path fetches full rows from the base table by primary key.
//   - `QueryParser` receives all TEXT-kind user fields as default fields so
//     bare queries (e.g. `wireless`) search across every text column.
//   - Every search query intersects with a TTL filter so expired documents
//     are never returned, even before `prune_expired()` runs.

use anyhow::Result;
use std::collections::HashMap;
use std::ops::Bound;
use tantivy::collector::{Count, DocSetCollector, TopDocs};
use tantivy::query::{BooleanQuery, Occur, QueryParser, RangeQuery};
use tantivy::schema::Value;

use crate::ffi::{FtsFacetBucket, FtsFacetResult, FtsSearchHit, FtsSearchResponse};
use crate::types::{FieldKind, ShardIndex};
use crate::writer::now_micros;

/// Execute a full-text search against a shard index.
///
/// `query` is a Tantivy query string (supports Lucene-style syntax).
/// `default_field` is the CQL column name to use as the sole default field
/// for the `QueryParser`.  If empty, all TEXT-kind user fields are used as
/// defaults (backwards-compatible behaviour for multi-field bare searches).
/// Returns a `FtsSearchResponse` box suitable for handing back to C++.
pub fn search(
    index: &ShardIndex,
    query: &str,
    default_field: &str,
    limit: u32,
    offset: u32,
    facet_fields: &[String],
    group_by_partition: bool,
) -> Result<Box<FtsSearchResponse>> {
    let start = std::time::Instant::now();
    let searcher = index.reader.searcher();

    // ── Query parsing ───────────────────────────────────────────────────
    // When `default_field` is non-empty, restrict the QueryParser to that
    // one field.  This mirrors the standalone Rust module pattern:
    //   QueryParser::for_index(&index, vec![self.field_doc])
    // and ensures that boolean/phrase expressions like `wonder OR builder`
    // are parsed correctly without the `field:(...)` wrapping that Tantivy
    // rejects for compound queries.
    //
    // When `default_field` is empty (e.g. a cross-column free-text search),
    // fall back to all TEXT-kind user fields so bare queries still search
    // across every text column.
    let qp_fields: Vec<tantivy::schema::Field> = if default_field.is_empty() {
        index.default_text_fields.clone()
    } else {
        match index.user_fields.get(default_field) {
            Some((field, _)) => vec![*field],
            None => index.default_text_fields.clone(),
        }
    };
    let qp = QueryParser::for_index(&index.index, qp_fields);
    let user_query = qp.parse_query(query)?;

    // ── TTL filter ──────────────────────────────────────────────────────
    // Intersect with `_expires_at > now_us`.  Documents without TTL have
    // `_expires_at == i64::MAX` so they always pass this filter.
    // RangeQuery::new_i64_bounds in Tantivy 0.22 takes a field *name* string,
    // not a Field handle — convert via schema.
    let now_us = now_micros();
    let expires_at_name = index
        .schema
        .get_field_name(index.field_expires_at)
        .to_string();
    let ttl_filter =
        RangeQuery::new_i64_bounds(expires_at_name, Bound::Excluded(now_us), Bound::Unbounded);
    let combined: Box<dyn tantivy::query::Query> = Box::new(BooleanQuery::new(vec![
        (Occur::Must, user_query),
        (Occur::Must, Box::new(ttl_filter)),
    ]));

    // ── Hit collection ──────────────────────────────────────────────────
    let (hits, total_hits) = if group_by_partition {
        collect_grouped(&searcher, &*combined, index, limit, offset)?
    } else {
        collect_top(&searcher, &*combined, index, limit, offset)?
    };

    // ── Facet aggregation ───────────────────────────────────────────────
    let facets = if !facet_fields.is_empty() {
        collect_facets(&searcher, &*combined, index, facet_fields)?
    } else {
        vec![]
    };

    let duration_us = start.elapsed().as_micros() as u64;

    Ok(Box::new(FtsSearchResponse {
        hits,
        total_hits,
        duration_us,
        facets,
    }))
}

/// Return all document IDs for a given partition key.
/// Used to enumerate rows belonging to a partition (e.g. during repair).
pub fn list_ids_by_partition_key(index: &ShardIndex, partition_key: &str) -> Result<Vec<String>> {
    let searcher = index.reader.searcher();
    use tantivy::query::TermQuery;
    use tantivy::schema::IndexRecordOption;
    let term = tantivy::Term::from_field_text(index.field_partition_key, partition_key);
    let query = TermQuery::new(term, IndexRecordOption::Basic);
    let docs = searcher.search(&query, &DocSetCollector)?;
    let mut ids = Vec::with_capacity(docs.len());
    for doc_addr in &docs {
        let doc: tantivy::TantivyDocument = searcher.doc(*doc_addr)?;
        if let Some(val) = doc.get_first(index.field_id) {
            if let Some(s) = val.as_str() {
                ids.push(s.to_string());
            }
        }
    }
    Ok(ids)
}

/// Total number of documents in the index (across all segments).
pub fn doc_count(index: &ShardIndex) -> Result<u64> {
    Ok(index.reader.searcher().num_docs())
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

/// Collect the top-N hits ordered by BM25 score descending.
fn collect_top(
    searcher: &tantivy::Searcher,
    query: &dyn tantivy::query::Query,
    index: &ShardIndex,
    limit: u32,
    offset: u32,
) -> Result<(Vec<FtsSearchHit>, u64)> {
    let top_collector = TopDocs::with_limit(limit as usize).and_offset(offset as usize);
    let count_collector = Count;
    let (total, top) = searcher.search(query, &(count_collector, top_collector))?;

    let mut hits = Vec::with_capacity(top.len());
    for (score, doc_addr) in top {
        let doc: tantivy::TantivyDocument = searcher.doc(doc_addr)?;
        let id = doc
            .get_first(index.field_id)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let partition_key = doc
            .get_first(index.field_partition_key)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        hits.push(FtsSearchHit {
            id,
            partition_key,
            score,
        });
    }

    Ok((hits, total as u64))
}

/// Collect hits grouped by partition key (returns one hit per partition,
/// the one with the highest BM25 score within that partition).
fn collect_grouped(
    searcher: &tantivy::Searcher,
    query: &dyn tantivy::query::Query,
    index: &ShardIndex,
    limit: u32,
    offset: u32,
) -> Result<(Vec<FtsSearchHit>, u64)> {
    // Fetch more results than `limit` to handle per-partition deduplication.
    // A factor of 10× is a conservative heuristic; in the worst case (one
    // document per partition) we still get `limit` unique partitions.
    let over_fetch = ((limit + offset) as usize).saturating_mul(10).max(1000);
    let top_collector = TopDocs::with_limit(over_fetch);
    let top = searcher.search(query, &top_collector)?;

    let mut seen: HashMap<String, FtsSearchHit> = HashMap::new();
    for (score, doc_addr) in top {
        let doc: tantivy::TantivyDocument = searcher.doc(doc_addr)?;
        let id = doc
            .get_first(index.field_id)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let partition_key = doc
            .get_first(index.field_partition_key)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // Keep the highest-scoring hit per partition.
        let entry = seen.entry(partition_key.clone()).or_insert(FtsSearchHit {
            id: id.clone(),
            partition_key: partition_key.clone(),
            score,
        });
        if score > entry.score {
            entry.id = id;
            entry.score = score;
        }
    }

    let total = seen.len() as u64;
    let mut result: Vec<FtsSearchHit> = seen.into_values().collect();
    result.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let paged: Vec<FtsSearchHit> = result
        .into_iter()
        .skip(offset as usize)
        .take(limit as usize)
        .collect();

    Ok((paged, total))
}

/// Aggregate facet counts for the requested fields over the result set.
///
/// Numeric/boolean fields use columnar fast-field access (O(1) per doc).
/// Text fields fall back to stored document retrieval (O(N) deserialization).
fn collect_facets(
    searcher: &tantivy::Searcher,
    query: &dyn tantivy::query::Query,
    index: &ShardIndex,
    facet_fields: &[String],
) -> Result<Vec<FtsFacetResult>> {
    let doc_set = searcher.search(query, &DocSetCollector)?;
    let mut results = Vec::new();

    for field_name in facet_fields {
        let (field, kind) = match index.user_fields.get(field_name.as_str()) {
            Some(f) => f,
            None => continue,
        };

        let mut buckets: HashMap<String, u64> = HashMap::new();

        // Fast-field names must be looked up as strings in Tantivy 0.22 —
        // the columnar API takes `&str` field names, not `Field` handles.
        let field_name_str = index.schema.get_field_name(*field);

        match kind {
            FieldKind::I64 => {
                // FAST field path — columnar access, no stored doc deserialization.
                // Enumerate segment readers to obtain the segment ordinal needed
                // for DocAddress construction; Tantivy 0.22 has no segment_ord()
                // method on SegmentReader — ordinal is the position in the slice.
                for (seg_ord, seg_reader) in searcher.segment_readers().iter().enumerate() {
                    let fast_reader = seg_reader.fast_fields().i64(field_name_str)?;
                    let alive = seg_reader.alive_bitset();
                    for doc_id in 0..seg_reader.num_docs() {
                        if alive.map_or(true, |bs| bs.is_alive(doc_id)) {
                            // Only count docs in our result set.
                            let doc_addr = tantivy::DocAddress::new(seg_ord as u32, doc_id);
                            if doc_set.contains(&doc_addr) {
                                if let Some(val) = fast_reader.first(doc_id) {
                                    *buckets.entry(val.to_string()).or_default() += 1;
                                }
                            }
                        }
                    }
                }
            }
            FieldKind::F64 => {
                for (seg_ord, seg_reader) in searcher.segment_readers().iter().enumerate() {
                    let fast_reader = seg_reader.fast_fields().f64(field_name_str)?;
                    let alive = seg_reader.alive_bitset();
                    for doc_id in 0..seg_reader.num_docs() {
                        if alive.map_or(true, |bs| bs.is_alive(doc_id)) {
                            let doc_addr = tantivy::DocAddress::new(seg_ord as u32, doc_id);
                            if doc_set.contains(&doc_addr) {
                                if let Some(val) = fast_reader.first(doc_id) {
                                    *buckets.entry(val.to_string()).or_default() += 1;
                                }
                            }
                        }
                    }
                }
            }
            FieldKind::Bool => {
                for (seg_ord, seg_reader) in searcher.segment_readers().iter().enumerate() {
                    let fast_reader = seg_reader.fast_fields().bool(field_name_str)?;
                    let alive = seg_reader.alive_bitset();
                    for doc_id in 0..seg_reader.num_docs() {
                        if alive.map_or(true, |bs| bs.is_alive(doc_id)) {
                            let doc_addr = tantivy::DocAddress::new(seg_ord as u32, doc_id);
                            if doc_set.contains(&doc_addr) {
                                if let Some(val) = fast_reader.first(doc_id) {
                                    *buckets.entry(val.to_string()).or_default() += 1;
                                }
                            }
                        }
                    }
                }
            }
            // Text/String fields must use stored document retrieval.
            FieldKind::Text | FieldKind::String => {
                for &doc_addr in &doc_set {
                    let doc: tantivy::TantivyDocument = searcher.doc(doc_addr)?;
                    // Direct value — no array unwrapping needed (per-column typed fields).
                    if let Some(val) = doc.get_first(*field) {
                        if let Some(s) = val.as_str() {
                            *buckets.entry(s.to_string()).or_default() += 1;
                        }
                    }
                }
            }
            // Date, IpAddr, Bytes, Json — not facetable; skip.
            _ => continue,
        }

        // Sort buckets by count descending (most-common first).
        let mut sorted: Vec<FtsFacetBucket> = buckets
            .into_iter()
            .map(|(value, count)| FtsFacetBucket { value, count })
            .collect();
        sorted.sort_by(|a, b| b.count.cmp(&a.count));

        results.push(FtsFacetResult {
            field: field_name.clone(),
            buckets: sorted,
        });
    }

    Ok(results)
}
