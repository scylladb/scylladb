/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/raw/insert_select_statement.hh"
#include "cql3/statements/insert_select_statement.hh"
#include "cql3/statements/select_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/column_specification.hh"
#include "cql3/column_identifier.hh"
#include "cql3/result_set.hh"
#include "exceptions/exceptions.hh"
#include "validation.hh"
#include "service/client_state.hh"

namespace cql3 {

namespace statements {

namespace raw {

insert_select_statement::insert_select_statement(cf_name target,
        std::unique_ptr<attributes::raw> attrs,
        std::vector<::shared_ptr<column_identifier::raw>> target_columns,
        std::unique_ptr<raw::select_statement> source)
    : raw::modification_statement(std::move(target), std::move(attrs), std::nullopt /* conditions */,
                                  false /* if_not_exists */, false /* if_exists */)
    , _target_columns(std::move(target_columns))
    , _source(std::move(source))
{ }

void insert_select_statement::prepare_keyspace(const service::client_state& state) {
    // Resolve the target table's keyspace (base class), then the inner SELECT's:
    // the processor only knows about this top-level statement, so without this
    // the nested source table's keyspace would stay unset and trip the
    // has_keyspace() assertion when the inner SELECT is prepared.
    raw::modification_statement::prepare_keyspace(state);
    _source->prepare_keyspace(state);
}

::shared_ptr<cql3::statements::modification_statement>
insert_select_statement::prepare_internal(data_dictionary::database, schema_ptr,
        prepare_context&, std::unique_ptr<attributes>, cql_stats&) const {
    // Reached only if this statement is prepared as part of a BATCH. INSERT ...
    // SELECT spans many partitions and is not atomic, so it cannot participate
    // in a batch.
    throw exceptions::invalid_request_exception(
            "INSERT INTO ... SELECT is not supported inside a BATCH");
}

std::unique_ptr<prepared_statement>
insert_select_statement::prepare(data_dictionary::database db, cql_stats& stats, const cql_config& cfg) {
    schema_ptr target_schema = validation::validate_column_family(db, keyspace(), column_family());

    if (target_schema->is_counter()) {
        throw exceptions::invalid_request_exception(
                "INSERT INTO ... SELECT is not allowed on counter tables");
    }
    if (target_schema->is_view()) {
        throw exceptions::invalid_request_exception(
                "INSERT INTO ... SELECT is not allowed with a materialized view as the target");
    }

    // Prepare the inner SELECT. Its prepared form drives the read side and
    // tells us the result-set column names/types we must map onto the target.
    std::unique_ptr<prepared_statement> prepared_source = _source->prepare(db, stats, cfg);
    auto source_stmt = ::dynamic_pointer_cast<cql3::statements::select_statement>(prepared_source->statement);
    if (!source_stmt) {
        // Should never happen: raw::select_statement always prepares into a
        // select_statement.
        throw exceptions::invalid_request_exception(
                "INSERT INTO ... SELECT: source is not a valid SELECT statement");
    }

    // Result-set columns produced by the SELECT, in order.
    auto source_meta = source_stmt->get_result_metadata();
    const auto& source_names = source_meta->get_names();

    // Resolve the ordered list of target columns to write.
    std::vector<const column_definition*> target_defs;
    if (_target_columns.empty()) {
        // No explicit target column list: match selected columns to target
        // columns by name. Every selected column must name a target column.
        target_defs.reserve(source_names.size());
        for (const auto& src : source_names) {
            const sstring& cname = src->name->text();
            const column_definition* def = target_schema->get_column_definition(to_bytes(cname));
            if (!def) {
                throw exceptions::invalid_request_exception(seastar::format(
                        "INSERT INTO ... SELECT: selected column '{}' has no matching column in target table {}.{}; "
                        "specify an explicit target column list",
                        cname, keyspace(), column_family()));
            }
            target_defs.push_back(def);
        }
    } else {
        if (_target_columns.size() != source_names.size()) {
            throw exceptions::invalid_request_exception(seastar::format(
                    "INSERT INTO ... SELECT: target column count ({}) does not match number of selected columns ({})",
                    _target_columns.size(), source_names.size()));
        }
        target_defs.reserve(_target_columns.size());
        for (const auto& raw_id : _target_columns) {
            auto id = raw_id->prepare_column_identifier(*target_schema);
            const column_definition* def = target_schema->get_column_definition(id->name());
            if (!def) {
                throw exceptions::invalid_request_exception(seastar::format(
                        "INSERT INTO ... SELECT: unknown target column '{}'", *id));
            }
            target_defs.push_back(def);
        }
    }

    // Build the mapping and validate type compatibility, rejecting duplicate
    // target columns.
    std::vector<cql3::statements::insert_select_statement::column_mapping_entry> mapping;
    mapping.reserve(target_defs.size());
    std::unordered_set<bytes> seen_target_cols;
    for (uint32_t i = 0; i < target_defs.size(); ++i) {
        const column_definition* def = target_defs[i];
        if (!seen_target_cols.insert(def->name()).second) {
            throw exceptions::invalid_request_exception(seastar::format(
                    "INSERT INTO ... SELECT: target column '{}' listed more than once", def->name_as_text()));
        }
        data_type source_type = source_names[i]->type;
        // The selected value must be assignable to the target column's type.
        if (!def->type->is_value_compatible_with(*source_type)) {
            throw exceptions::invalid_request_exception(seastar::format(
                    "INSERT INTO ... SELECT: type mismatch for target column '{}': cannot assign {} to {}",
                    def->name_as_text(), source_type->name(), def->type->name()));
        }
        mapping.push_back({i, def});
    }

    // Every primary-key column of the target must be supplied, otherwise we
    // cannot construct the partition/clustering key for the produced rows.
    std::vector<uint32_t> pk_indices;
    std::vector<uint32_t> ck_indices;
    auto find_mapping_index = [&] (const column_definition& col) -> std::optional<uint32_t> {
        for (uint32_t mi = 0; mi < mapping.size(); ++mi) {
            if (mapping[mi].target_column->name() == col.name()) {
                return mi;
            }
        }
        return std::nullopt;
    };
    for (const column_definition& col : target_schema->partition_key_columns()) {
        auto mi = find_mapping_index(col);
        if (!mi) {
            throw exceptions::invalid_request_exception(seastar::format(
                    "INSERT INTO ... SELECT: missing mandatory partition-key column '{}' in the SELECT result",
                    col.name_as_text()));
        }
        pk_indices.push_back(*mi);
    }
    for (const column_definition& col : target_schema->clustering_key_columns()) {
        auto mi = find_mapping_index(col);
        if (!mi) {
            throw exceptions::invalid_request_exception(seastar::format(
                    "INSERT INTO ... SELECT: missing mandatory clustering-key column '{}' in the SELECT result",
                    col.name_as_text()));
        }
        ck_indices.push_back(*mi);
    }

    auto prepared_attributes = _attrs->prepare(db, keyspace(), column_family());

    // The number of bind markers is whatever the inner SELECT (and USING
    // clause) declared.
    uint32_t bound_terms = source_stmt->get_bound_terms();

    auto stmt = ::make_shared<cql3::statements::insert_select_statement>(
            std::move(target_schema),
            std::move(source_stmt),
            std::move(mapping),
            std::move(pk_indices),
            std::move(ck_indices),
            std::move(prepared_attributes),
            bound_terms,
            stats);

    // Bind markers come from the inner SELECT's prepare context, carried on the
    // prepared source. Partition-key bind indices are not meaningful here (the
    // target partitions are computed per-row), so leave them empty.
    return std::make_unique<prepared_statement>(audit_info(), std::move(stmt),
            std::move(prepared_source->bound_names), std::vector<uint16_t>{},
            std::move(prepared_source->warnings));
}

}

}

}
