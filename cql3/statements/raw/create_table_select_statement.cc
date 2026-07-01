/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/raw/create_table_select_statement.hh"
#include "cql3/statements/create_table_select_statement.hh"
#include "cql3/statements/create_table_statement.hh"
#include "cql3/statements/select_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/cql3_type.hh"
#include "cql3/result_set.hh"
#include "exceptions/exceptions.hh"
#include "service/client_state.hh"

#include <unordered_set>

namespace cql3 {

namespace statements {

namespace raw {

create_table_select_statement::create_table_select_statement(cf_name target,
        bool if_not_exists,
        std::vector<::shared_ptr<column_identifier>> partition_keys,
        std::vector<::shared_ptr<column_identifier>> clustering_keys,
        std::unique_ptr<raw::select_statement> source)
    : raw::cf_statement(std::move(target))
    , _if_not_exists(if_not_exists)
    , _partition_keys(std::move(partition_keys))
    , _clustering_keys(std::move(clustering_keys))
    , _source(std::move(source))
{ }

void create_table_select_statement::prepare_keyspace(const service::client_state& state) {
    cf_statement::prepare_keyspace(state);
    _source->prepare_keyspace(state);
}

audit::statement_category create_table_select_statement::category() const {
    return audit::statement_category::DDL;
}

std::unique_ptr<prepared_statement>
create_table_select_statement::prepare(data_dictionary::database db, cql_stats& stats, const cql_config& cfg) {
    // Prepare the inner SELECT so we can read its result-set columns (names and
    // types) -- these become the target table's columns.
    std::unique_ptr<prepared_statement> prepared_source = _source->prepare(db, stats, cfg);
    auto source_stmt = ::dynamic_pointer_cast<cql3::statements::select_statement>(prepared_source->statement);
    if (!source_stmt) {
        throw exceptions::invalid_request_exception(
                "CREATE TABLE ... AS SELECT: source is not a valid SELECT statement");
    }
    const auto& names = source_stmt->get_result_metadata()->get_names();

    // Every primary-key column named by the user must be one of the selected
    // columns (that is where its type comes from).
    std::unordered_set<sstring> selected;
    selected.reserve(names.size());
    for (const auto& n : names) {
        selected.insert(n->name->text());
    }
    auto require_selected = [&] (const ::shared_ptr<column_identifier>& key) {
        if (!selected.contains(key->text())) {
            throw exceptions::invalid_request_exception(seastar::format(
                    "CREATE TABLE ... AS SELECT: primary-key column '{}' is not produced by the SELECT", key->text()));
        }
    };
    for (const auto& k : _partition_keys) {
        require_selected(k);
    }
    for (const auto& k : _clustering_keys) {
        require_selected(k);
    }

    // Build the equivalent CREATE TABLE: one column per selected column, typed
    // from the SELECT result metadata, with the requested primary key.
    auto create_raw = ::make_shared<create_table_statement::raw_statement>(*_cf_name, _if_not_exists);
    create_raw->prepare_keyspace(keyspace());
    for (const auto& n : names) {
        create_raw->add_definition(n->name, cql3_type::raw::from(n->type->as_cql3_type()), /*is_static=*/false, /*is_ttl=*/false);
    }
    create_raw->add_key_aliases(_partition_keys);
    for (const auto& c : _clustering_keys) {
        create_raw->add_column_alias(c);
    }
    auto prepared_create = create_raw->prepare(db, stats, cfg);
    auto create_stmt = ::dynamic_pointer_cast<cql3::statements::create_table_statement>(prepared_create->statement);
    if (!create_stmt) {
        throw exceptions::invalid_request_exception(
                "CREATE TABLE ... AS SELECT: failed to build target table definition");
    }

    const uint32_t bound_terms = source_stmt->get_bound_terms();
    auto stmt = ::make_shared<cql3::statements::create_table_select_statement>(
            *_cf_name, std::move(create_stmt), std::move(source_stmt), _if_not_exists,
            bound_terms, stats);

    return std::make_unique<prepared_statement>(audit_info(), std::move(stmt),
            std::move(prepared_source->bound_names), std::vector<uint16_t>{},
            std::move(prepared_source->warnings));
}

}

}

}
