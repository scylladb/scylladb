// =========================================================================
// FTS Bindings — Schema Construction
// =========================================================================
//
// Translates a slice of `FieldMapping` descriptors (built by the C++ side
// from the ScyllaDB CQL schema) into a concrete Tantivy `Schema`.
//
// The C++ side iterates `schema.all_columns()`, inspects each column's
// `abstract_type::kind`, and serialises the mapping as a JSON array stored
// in the index OPTIONS map.  This module reconstructs the Tantivy schema
// from that descriptor at `CREATE INDEX` time.

use anyhow::{bail, Result};
use std::collections::HashMap;
use tantivy::schema::{
    BytesOptions, DateOptions, Field, IpAddrOptions, JsonObjectOptions, NumericOptions, Schema,
    SchemaBuilder, TextFieldIndexing, TextOptions, FAST, INDEXED, STORED, STRING,
};

use crate::ffi::FieldMapping;
use crate::types::{FieldKind, ShardIndex};

// ── Index writer memory budget ────────────────────────────────────────────────
// 50 MB per shard: conservative default that avoids large RSS spikes while
// still providing reasonable write throughput.  The value is tunable via an
// index option in a future release.
const WRITER_HEAP_BYTES: usize = 50 * 1024 * 1024;

/// Construct a new Tantivy index on disk at `<path>/shard-<shard_id>/`,
/// build its schema from `field_mappings`, and return an initialised
/// `ShardIndex` ready for writes.
pub fn build_shard_index(
    path: &str,
    shard_id: u32,
    field_mappings: &[FieldMapping],
) -> Result<ShardIndex> {
    let shard_path = std::path::Path::new(path).join(format!("shard-{}", shard_id));
    std::fs::create_dir_all(&shard_path)?;

    let (schema, field_id, field_pk, field_expires, field_writetime, user_fields, default_text) =
        build_schema(field_mappings)?;

    let index = tantivy::Index::create_in_dir(&shard_path, schema.clone())?;
    let writer = index.writer(WRITER_HEAP_BYTES)?;
    let reader = index
        .reader_builder()
        .reload_policy(tantivy::ReloadPolicy::OnCommitWithDelay)
        .try_into()?;

    Ok(ShardIndex {
        index,
        writer,
        reader,
        schema,
        field_id,
        field_partition_key: field_pk,
        field_expires_at: field_expires,
        field_writetime,
        user_fields,
        default_text_fields: default_text,
        uncommitted: HashMap::new(),
        generation: 0,
    })
}

/// Open an existing Tantivy index at `<path>/shard-<shard_id>/`.
///
/// The schema is reconstructed from the stored `meta.json` rather than from
/// `field_mappings`; the mappings are used only to repopulate `user_fields`
/// and `default_text_fields` so that the writer/reader can reference fields
/// by name.
pub fn open_shard_index(
    path: &str,
    shard_id: u32,
    field_mappings: &[FieldMapping],
) -> Result<ShardIndex> {
    let shard_path = std::path::Path::new(path).join(format!("shard-{}", shard_id));

    let index = tantivy::Index::open_in_dir(&shard_path)?;
    let schema = index.schema();

    // Re-derive system field handles from the persisted schema.
    let field_id = schema
        .get_field("_id")
        .map_err(|_| anyhow::anyhow!("missing system field _id in schema"))?;
    let field_pk = schema
        .get_field("_partition_key")
        .map_err(|_| anyhow::anyhow!("missing system field _partition_key in schema"))?;
    let field_expires = schema
        .get_field("_expires_at")
        .map_err(|_| anyhow::anyhow!("missing system field _expires_at in schema"))?;
    let field_writetime = schema
        .get_field("_writetime")
        .map_err(|_| anyhow::anyhow!("missing system field _writetime in schema"))?;

    // Rebuild user_fields map from the mappings descriptor.
    let (user_fields, default_text_fields) = rebuild_user_fields(&schema, field_mappings)?;

    let writer = index.writer(WRITER_HEAP_BYTES)?;
    let reader = index
        .reader_builder()
        .reload_policy(tantivy::ReloadPolicy::OnCommitWithDelay)
        .try_into()?;

    Ok(ShardIndex {
        index,
        writer,
        reader,
        schema,
        field_id,
        field_partition_key: field_pk,
        field_expires_at: field_expires,
        field_writetime,
        user_fields,
        default_text_fields,
        uncommitted: HashMap::new(),
        generation: 0,
    })
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

/// Build a fresh Tantivy `Schema` from the given field mappings.
///
/// Returns `(schema, field_id, field_pk, field_expires, field_writetime,
///          user_fields, default_text_fields)`.
#[allow(clippy::type_complexity)]
fn build_schema(
    field_mappings: &[FieldMapping],
) -> Result<(
    Schema,
    Field,
    Field,
    Field,
    Field,
    HashMap<String, (Field, FieldKind)>,
    Vec<Field>,
)> {
    let mut builder = SchemaBuilder::default();

    // ── System fields ────────────────────────────────────────────────────
    let field_id = builder.add_text_field("_id", STRING | STORED);
    let field_pk = builder.add_text_field("_partition_key", STRING | STORED);
    let field_expires = builder.add_i64_field("_expires_at", FAST | STORED);
    let field_writetime = builder.add_i64_field("_writetime", FAST | STORED);

    // ── User fields ──────────────────────────────────────────────────────
    let mut user_fields: HashMap<String, (Field, FieldKind)> = HashMap::new();
    let mut default_text_fields: Vec<Field> = Vec::new();

    for mapping in field_mappings {
        let (field, kind) = add_field(&mut builder, mapping)?;
        if kind == FieldKind::Text {
            default_text_fields.push(field);
        }
        user_fields.insert(mapping.name.clone(), (field, kind));
    }

    let schema = builder.build();
    Ok((
        schema,
        field_id,
        field_pk,
        field_expires,
        field_writetime,
        user_fields,
        default_text_fields,
    ))
}

/// Add a single user field to a `SchemaBuilder` according to the mapping
/// descriptor.  Returns `(Field, FieldKind)`.
fn add_field(builder: &mut SchemaBuilder, mapping: &FieldMapping) -> Result<(Field, FieldKind)> {
    match mapping.kind.as_str() {
        "text" => {
            let tokenizer = if mapping.tokenizer.is_empty() {
                "default"
            } else {
                mapping.tokenizer.as_str()
            };
            let indexing = TextFieldIndexing::default()
                .set_tokenizer(tokenizer)
                .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions);
            let opts = TextOptions::default()
                .set_stored()
                .set_indexing_options(indexing);
            Ok((builder.add_text_field(&mapping.name, opts), FieldKind::Text))
        }
        "string" => Ok((
            builder.add_text_field(&mapping.name, STRING | STORED),
            FieldKind::String,
        )),
        "i64" => Ok((
            builder.add_i64_field(
                &mapping.name,
                NumericOptions::default()
                    .set_indexed()
                    .set_stored()
                    .set_fast(),
            ),
            FieldKind::I64,
        )),
        "f64" => Ok((
            builder.add_f64_field(
                &mapping.name,
                NumericOptions::default()
                    .set_indexed()
                    .set_stored()
                    .set_fast(),
            ),
            FieldKind::F64,
        )),
        "bool" => Ok((
            builder.add_bool_field(&mapping.name, INDEXED | STORED | FAST),
            FieldKind::Bool,
        )),
        "date" => Ok((
            builder.add_date_field(
                &mapping.name,
                DateOptions::default().set_indexed().set_stored().set_fast(),
            ),
            FieldKind::Date,
        )),
        "ip_addr" => Ok((
            builder.add_ip_addr_field(
                &mapping.name,
                IpAddrOptions::default()
                    .set_indexed()
                    .set_stored()
                    .set_fast(),
            ),
            FieldKind::IpAddr,
        )),
        "bytes" => Ok((
            builder.add_bytes_field(&mapping.name, BytesOptions::default().set_stored()),
            FieldKind::Bytes,
        )),
        "json" => {
            let indexing = TextFieldIndexing::default()
                .set_tokenizer("default")
                .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions);
            let opts = JsonObjectOptions::default()
                .set_stored()
                .set_indexing_options(indexing);
            Ok((builder.add_json_field(&mapping.name, opts), FieldKind::Json))
        }
        other => bail!(
            "Unknown field kind '{}' for column '{}'",
            other,
            mapping.name
        ),
    }
}

/// Re-derive `user_fields` and `default_text_fields` from an already-open
/// schema.  Used when reopening an existing index where the schema is read
/// from disk rather than constructed anew.
fn rebuild_user_fields(
    schema: &Schema,
    field_mappings: &[FieldMapping],
) -> Result<(HashMap<String, (Field, FieldKind)>, Vec<Field>)> {
    let mut user_fields: HashMap<String, (Field, FieldKind)> = HashMap::new();
    let mut default_text_fields: Vec<Field> = Vec::new();

    for mapping in field_mappings {
        let field = schema
            .get_field(&mapping.name)
            .map_err(|_| anyhow::anyhow!("field '{}' not found in schema", mapping.name))?;
        let kind = kind_from_str(&mapping.kind, &mapping.name)?;
        if kind == FieldKind::Text {
            default_text_fields.push(field);
        }
        user_fields.insert(mapping.name.clone(), (field, kind));
    }

    Ok((user_fields, default_text_fields))
}

/// Parse a `kind` string from a `FieldMapping` back into a `FieldKind`.
fn kind_from_str(kind: &str, name: &str) -> Result<FieldKind> {
    match kind {
        "text" => Ok(FieldKind::Text),
        "string" => Ok(FieldKind::String),
        "i64" => Ok(FieldKind::I64),
        "f64" => Ok(FieldKind::F64),
        "bool" => Ok(FieldKind::Bool),
        "date" => Ok(FieldKind::Date),
        "ip_addr" => Ok(FieldKind::IpAddr),
        "bytes" => Ok(FieldKind::Bytes),
        "json" => Ok(FieldKind::Json),
        other => bail!("Unknown field kind '{}' for column '{}'", other, name),
    }
}
