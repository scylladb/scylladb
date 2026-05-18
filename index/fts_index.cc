/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "cdc/cdc_options.hh"
#include "cql3/statements/index_target.hh"
#include "cql3/util.hh"
#include "cql3/description.hh"
#include "exceptions/exceptions.hh"
#include "schema/schema.hh"
#include "index/fts_index.hh"
#include "index/secondary_index.hh"
#include "index/secondary_index_manager.hh"
#include "index/target_parser.hh"
#include "types/types.hh"
#include "utils/managed_string.hh"
#include "utils/rjson.hh"
#include <seastar/core/sstring.hh>
#include <boost/algorithm/string.hpp>

#include <algorithm>
#include <cctype>
#include <sstream>

namespace db::index {

// =========================================================================
// Supported FTS index OPTIONS keys
// =========================================================================
//
// Per-column tokenizer overrides use the key form `<column>.tokenizer`.
// Bulk-knob options control consumer write-path behaviour; the values are
// currently advisory (see SESSION.md follow-up "FTS index options plumbing").
static const std::unordered_set<sstring> fts_index_option_prefixes = {
    "commit_interval_ms",
    "prune_interval_ms",
};

// =========================================================================
// CQL type → Tantivy field kind
// =========================================================================
//
// Returns the Rust-side `FieldKind` string the schema builder understands.
// `"skip"` indicates the column type is not indexable (counters, durations,
// nested UDTs at depth > 1 are folded into JSON elsewhere).
sstring fts_index::map_cql_type_to_field_kind(const abstract_type& type) {
    switch (type.get_kind()) {
        case abstract_type::kind::utf8:
        case abstract_type::kind::ascii:
            return "text";

        case abstract_type::kind::int32:
        case abstract_type::kind::long_kind:
        case abstract_type::kind::short_kind:
        case abstract_type::kind::byte:
        case abstract_type::kind::time:    // nanos since midnight — store as i64
            return "i64";

        case abstract_type::kind::float_kind:
        case abstract_type::kind::double_kind:
            return "f64";

        case abstract_type::kind::boolean:
            return "bool";

        case abstract_type::kind::timestamp:
        case abstract_type::kind::simple_date:
            return "date";

        case abstract_type::kind::uuid:
        case abstract_type::kind::timeuuid:
        case abstract_type::kind::decimal:
        case abstract_type::kind::varint:
            // Stored as exact-match untokenized strings.
            return "string";

        case abstract_type::kind::inet:
            return "ip_addr";

        case abstract_type::kind::bytes:
            return "bytes";

        case abstract_type::kind::user:
        case abstract_type::kind::map:
            // Falls back to a Tantivy `json` field; the consumer serializes
            // the value as a hex-encoded byte string.
            return "json";

        case abstract_type::kind::list:
        case abstract_type::kind::set: {
            // Multi-valued field: element type determines the Tantivy type.
            const auto& elem_type = dynamic_cast<const listlike_collection_type_impl&>(type)
                .get_elements_type();
            auto kind = map_cql_type_to_field_kind(*elem_type);
            // If the element type is itself a UDT or map, fall back to JSON.
            if (kind == "json") {
                return "json";
            }
            return kind;
        }

        case abstract_type::kind::duration:
        case abstract_type::kind::counter:
            // ScyllaDB forbids indexing these types.
            return "skip";

        default:
            return "skip";
    }
}

bool fts_index::is_fts_indexable(const abstract_type& type) {
    return map_cql_type_to_field_kind(type) != "skip";
}

// =========================================================================
// Class-name / target-list parsing
// =========================================================================

bool fts_index::is_fts_class(const sstring& class_name_option) {
    // Match the case-folding rule used by
    // secondary_index_manager::get_custom_class_factory.
    if (class_name_option.size() != std::char_traits<char>::length(fts_index::class_name)) {
        return false;
    }
    for (size_t i = 0; i < class_name_option.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(class_name_option[i]))
                != fts_index::class_name[i]) {
            return false;
        }
    }
    return true;
}

std::vector<sstring> fts_index::parse_target_columns(const sstring& target) {
    std::vector<sstring> out;

    // Step 1: the multi-column / local-index serialised form is JSON
    // (`{"pk":["c1","c2"], "ck":["c3"]}`).  Try to parse it as JSON first.
    if (auto json = rjson::try_parse(target); json && json->IsObject()) {
        auto collect_array = [&out](const rjson::value* arr) {
            if (!arr || !arr->IsArray()) {
                return;
            }
            for (const auto& v : arr->GetArray()) {
                if (v.IsString()) {
                    out.emplace_back(rjson::to_string_view(v));
                }
            }
        };
        collect_array(rjson::find(*json, "pk"));
        collect_array(rjson::find(*json, "ck"));
        if (!out.empty()) {
            return out;
        }
    }

    // Step 2: comma-separated escaped column list (the form produced by
    // `serialize_targets` for the common single-column case, and a
    // best-effort split for any legacy comma-joined target string).
    sstring remaining = target;
    while (!remaining.empty()) {
        auto comma = remaining.find(',');
        sstring token = (comma == sstring::npos)
                ? remaining : remaining.substr(0, comma);
        remaining = (comma == sstring::npos)
                ? sstring{} : remaining.substr(comma + 1);

        // Trim ASCII whitespace from both ends — the CQL parser may emit
        // `"col1, col2"` with embedded spaces.
        auto is_ws = [](char c) { return c == ' ' || c == '\t'; };
        while (!token.empty() && is_ws(token.front())) {
            token.erase(token.begin());
        }
        while (!token.empty() && is_ws(token.back())) {
            token.pop_back();
        }
        if (token.empty()) {
            continue;
        }
        out.push_back(cql3::statements::index_target::unescape_target_column(token));
    }
    return out;
}

// =========================================================================
// custom_index interface implementation
// =========================================================================

bool fts_index::view_should_exist() const {
    return false;
}

std::optional<cql3::description> fts_index::describe(
    const index_metadata& im,
    const schema& base_schema) const
{
    fragmented_ostringstream os;
    os << "CREATE CUSTOM INDEX " << cql3::util::maybe_quote(im.name()) << " ON "
       << cql3::util::maybe_quote(base_schema.ks_name()) << "."
       << cql3::util::maybe_quote(base_schema.cf_name())
       << " (" << cql3::util::maybe_quote(
           im.options().count(cql3::statements::index_target::target_option_name)
               ? im.options().at(cql3::statements::index_target::target_option_name)
               : sstring{})
       << ") USING 'fts_index'";

    return cql3::description{
        .keyspace = base_schema.ks_name(),
        .type = "index",
        .name = im.name(),
        .create_statement = std::move(os).to_managed_string(),
    };
}

// Validates that the explicit `CREATE INDEX (col, ...)` targets reference
// columns of indexable type and (for now) live in the regular-column space.
//
// Static-column targets are rejected outright because the consumer's
// ingestion path only iterates `regular_columns()`; allowing static targets
// silently here would surface as "no rows ever indexed" later, which is
// strictly worse than a clear up-front error.  See SESSION.md follow-up
// "FTS static-column support".
void fts_index::validate(
    const schema& schema,
    const cql3::statements::index_specific_prop_defs& properties,
    const std::vector<::shared_ptr<cql3::statements::index_target>>& targets,
    const gms::feature_service& /* fs */,
    const data_dictionary::database& /* db */) const
{
    auto cdc_options = schema.cdc_options();

    // CDC must not be explicitly disabled — FTS relies on CDC for the write path.
    if (cdc_options.is_enabled_set() && !cdc_options.enabled()) {
        throw exceptions::invalid_request_exception(
            "Cannot create an FTS index when CDC is explicitly disabled on this table. "
            "Please enable CDC first, or remove the 'cdc = {enabled: false}' table option.");
    }

    // CDC postimage is required.  The consumer reads each CDC row and treats
    // the column set as the full new state of the row; delta-only UPDATE
    // events would otherwise erase non-updated columns from the Tantivy
    // document on every partial mutation.  Postimage rows replay the entire
    // row state and so are safe to index unconditionally.
    if (!cdc_options.postimage()) {
        throw exceptions::invalid_request_exception(
            "FTS index requires CDC postimage to be enabled on the base table. "
            "Add 'cdc = {\"enabled\": true, \"postimage\": true}' to the CREATE TABLE "
            "or ALTER TABLE statement before creating the FTS index.");
    }

    auto check_one_target = [&schema](const ::shared_ptr<cql3::statements::index_target>& target) {
        auto check_column = [&schema](const sstring& col_name, const auto& column_id) {
            const auto* cdef = schema.get_column_definition(column_id->name());
            if (!cdef) {
                throw exceptions::invalid_request_exception(
                    format("FTS index: column '{}' not found in schema", col_name));
            }
            if (cdef->is_static()) {
                throw exceptions::invalid_request_exception(format(
                    "FTS index: column '{}' is static; static-column FTS indexes are "
                    "not supported yet.  Use a regular column instead.", col_name));
            }
            if (cdef->is_primary_key()) {
                throw exceptions::invalid_request_exception(format(
                    "FTS index: column '{}' is part of the primary key; FTS indexes "
                    "only apply to regular columns.", col_name));
            }
            if (!is_fts_indexable(*cdef->type)) {
                throw exceptions::invalid_request_exception(format(
                    "FTS index: column '{}' has type '{}' which cannot be indexed",
                    col_name, cdef->type->name()));
            }
        };
        std::visit([&](auto&& value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, ::shared_ptr<cql3::column_identifier>>) {
                check_column(value->to_string(), value);
            } else if constexpr (std::is_same_v<T, std::vector<::shared_ptr<cql3::column_identifier>>>) {
                for (const auto& col_id : value) {
                    check_column(col_id->to_string(), col_id);
                }
            }
        }, target->value);
    };

    bool any_indexable_target = false;
    for (const auto& target : targets) {
        check_one_target(target);
        any_indexable_target = true;
    }

    // If no targets were given (CREATE CUSTOM INDEX ... USING 'fts_index'
    // without a column list), every regular column of the base table is
    // implicitly an index target.  Reject if not a single indexable column
    // exists, which would otherwise produce an empty Tantivy schema.
    if (targets.empty()) {
        for (const auto& cdef : schema.regular_columns()) {
            if (is_fts_indexable(*cdef.type)) {
                any_indexable_target = true;
                break;
            }
        }
    }
    if (!any_indexable_target) {
        throw exceptions::invalid_request_exception(
            "FTS index: base table has no FTS-indexable regular columns.");
    }

    // Per-column tokenizer overrides and bulk-knob options.
    for (const auto& [key, val] : properties.get_raw_options()) {
        if (key.ends_with(".tokenizer")) {
            static const std::unordered_set<sstring> valid_tokenizers = {
                "default", "en_stem", "keyword", "whitespace", "raw",
            };
            if (!valid_tokenizers.count(val)) {
                throw exceptions::invalid_request_exception(format(
                    "FTS index: invalid tokenizer '{}' for option '{}'. "
                    "Supported: default, en_stem, keyword, whitespace, raw",
                    val, key));
            }
            continue;
        }

        if (!fts_index_option_prefixes.count(key)) {
            throw exceptions::invalid_request_exception(format(
                "FTS index: unsupported option '{}'", key));
        }

        if (key == "commit_interval_ms" || key == "prune_interval_ms") {
            try {
                int v = std::stoi(val);
                if (v <= 0) {
                    throw exceptions::invalid_request_exception(format(
                        "FTS index: option '{}' must be a positive integer, got '{}'",
                        key, val));
                }
            } catch (const std::invalid_argument&) {
                throw exceptions::invalid_request_exception(format(
                    "FTS index: option '{}' must be a positive integer, got '{}'",
                    key, val));
            }
        }
    }
}

table_schema_version fts_index::index_version(const schema& schema) {
    // Tie the index version to the base table schema version so that any
    // ALTER TABLE (ADD column, DROP column, …) triggers a rebuild.
    return schema.version();
}

// =========================================================================
// Static detection helpers
// =========================================================================
//
// These predicates are called from the CQL prepare path (and from
// `cdc::cdc_enabled`), so the hot path is a string compare per index — no
// factory invocation, no `dynamic_cast`, no temporary allocations.

bool fts_index::has_fts_index(const schema& s) {
    for (const auto& index : s.indices()) {
        auto it = index.options().find(db::index::secondary_index::custom_class_option_name);
        if (it != index.options().end() && is_fts_class(it->second)) {
            return true;
        }
    }
    return false;
}

bool fts_index::has_fts_index_on_column(const schema& s, const sstring& col) {
    for (const auto& index : s.indices()) {
        auto class_it = index.options().find(db::index::secondary_index::custom_class_option_name);
        auto target_it = index.options().find(cql3::statements::index_target::target_option_name);
        if (class_it == index.options().end() || target_it == index.options().end()) {
            continue;
        }
        if (!is_fts_class(class_it->second)) {
            continue;
        }
        // Parse the target string into per-column tokens before comparing,
        // so that e.g. column `body` does not accidentally match an index
        // declared on `body_text`.
        for (const auto& tgt : parse_target_columns(target_it->second)) {
            if (tgt == col) {
                return true;
            }
        }
    }
    return false;
}

// =========================================================================
// Factory
// =========================================================================

std::unique_ptr<::secondary_index::custom_index> fts_index_factory() {
    return std::make_unique<fts_index>();
}

} // namespace db::index
