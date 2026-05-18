/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "schema/schema.hh"
#include "data_dictionary/data_dictionary.hh"
#include "cql3/statements/index_target.hh"
#include "index/secondary_index_manager.hh"

#include <vector>

namespace db::index {

/// Native full-text search custom index backed by Tantivy (via fts_bindings).
///
/// One shard-local Tantivy index is created per ScyllaDB shard inside
/// `<data_dir>/fts_indexes/<ks>/<table>/<index_name>/shard-<N>/`.
/// Writes arrive via an implicit per-table CDC log, consumed by
/// `fts_cdc_consumer` running on a Seastar alien thread.
///
/// Coverage rule (intentional, see SESSION.md follow-up "FTS index coverage"):
/// the runtime indexer in `fts_cdc_consumer::open_indexes_for_table` opens a
/// Tantivy schema covering every FTS-indexable regular column of the base
/// table.  The CREATE INDEX targets list is used only to gate which columns
/// the MATCH operator may be applied to at query time (see
/// `has_fts_index_on_column`), not to restrict ingestion.
class fts_index : public ::secondary_index::custom_index {
public:
    fts_index() = default;
    ~fts_index() override = default;

    // ── custom_index interface ────────────────────────────────────────────

    std::optional<cql3::description> describe(
        const index_metadata& im,
        const schema& base_schema) const override;

    /// FTS indexes do not create a secondary view table.
    bool view_should_exist() const override;

    void validate(
        const schema& schema,
        const cql3::statements::index_specific_prop_defs& properties,
        const std::vector<::shared_ptr<cql3::statements::index_target>>& targets,
        const gms::feature_service& fs,
        const data_dictionary::database& db) const override;

    /// Returns `schema.version()` so that ALTER TABLE ADD column triggers
    /// an automatic index rebuild via `on_schema_change()`.
    table_schema_version index_version(const schema& schema) override;

    // ── Static helpers ───────────────────────────────────────────────────

    /// True if the schema has at least one FTS index.
    static bool has_fts_index(const schema& s);

    /// True if any FTS index on `s` lists `col` in its CREATE INDEX targets.
    ///
    /// The match is performed on a per-column-name basis after parsing the
    /// stored `target` option string; substring matches are explicitly
    /// avoided so that e.g. column `body` does not spuriously match an index
    /// on `body_text`.
    static bool has_fts_index_on_column(const schema& s, const sstring& col);

    // ── CQL type → Tantivy field kind ───────────────────────────────────

    /// Map a scalar CQL `abstract_type` to a Tantivy field kind string.
    /// Returns "skip" for types that cannot be indexed (duration, counter).
    static sstring map_cql_type_to_field_kind(const abstract_type& type);

    /// True if a column of this type can meaningfully be placed in a
    /// Tantivy FTS schema (i.e. `map_cql_type_to_field_kind` != "skip").
    static bool is_fts_indexable(const abstract_type& type);

    /// Class-name string registered in `secondary_index_manager::classes`.
    /// Exposed so callers can do a direct case-insensitive comparison and
    /// avoid the cost of constructing a throwaway `fts_index` instance just
    /// to feed `dynamic_cast`.
    static constexpr const char* class_name = "fts_index";

    /// True if `class_name_option` (the value of the `class_name` index
    /// option) names the FTS custom index class (case-insensitive).
    static bool is_fts_class(const sstring& class_name_option);

    /// Parse the stored `target` option string into a list of distinct
    /// column names.  Handles both the simple escaped form (`"col"`,
    /// `"col1, col2"`) and the JSON form (`'{"pk":[...]}'`) produced by
    /// `secondary_index::target_parser::serialize_targets`.
    static std::vector<sstring> parse_target_columns(const sstring& target);
};

std::unique_ptr<::secondary_index::custom_index> fts_index_factory();

} // namespace db::index
