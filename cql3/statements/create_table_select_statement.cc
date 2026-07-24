/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/create_table_select_statement.hh"
#include "cql3/statements/insert_select_statement.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "cql3/query_processor.hh"
#include "cql3/result_set.hh"
#include "cql3/attributes.hh"
#include "service/query_state.hh"
#include "service/client_state.hh"
#include "service/raft/raft_group0_client.hh"
#include "data_dictionary/data_dictionary.hh"
#include "transport/messages/result_message.hh"
#include "schema/schema.hh"
#include "exceptions/exceptions.hh"

namespace cql3 {

namespace statements {

static logging::logger ctaslog("create_table_select");

create_table_select_statement::create_table_select_statement(cf_name target,
        ::shared_ptr<create_table_statement> create_stmt,
        ::shared_ptr<select_statement> source,
        bool if_not_exists,
        uint32_t bound_terms,
        cql_stats& stats)
    : schema_altering_statement(std::move(target))
    , _create_stmt(std::move(create_stmt))
    , _source(std::move(source))
    , _if_not_exists(if_not_exists)
    , _bound_terms(bound_terms)
    , _stats(stats)
{ }

std::unique_ptr<prepared_statement> create_table_select_statement::prepare(data_dictionary::database, cql_stats&, const cql_config&) {
    // Already prepared by raw::create_table_select_statement::prepare(); the
    // schema_altering_statement base requires this override but it is never called.
    on_internal_error(ctaslog, "create_table_select_statement::prepare() should not be called on a prepared statement");
}

future<> create_table_select_statement::check_access(query_processor& qp, const service::client_state& state) const {
    // Creating the target requires CREATE on the keyspace; reading the source
    // requires SELECT on it. MODIFY on the target is implicit: the creator is
    // auto-granted full access to the table it creates.
    co_await state.has_keyspace_access(keyspace(), auth::permission::CREATE);
    co_await _source->check_access(qp, state);
}

future<::shared_ptr<cql_transport::messages::result_message>>
create_table_select_statement::execute(query_processor& qp, service::query_state& state,
        const query_options& options, std::optional<service::group0_guard> guard) const {
    const sstring ks = keyspace();
    const sstring cf = column_family();
    const bool existed_before = qp.db().has_schema(ks, cf);

    // Step 1: create the target table (schema change through group0).
    service::group0_batch mc{std::move(guard)};
    auto result = co_await qp.execute_schema_statement(*_create_stmt, state, options, mc);
    co_await qp.announce_schema_statement(*_create_stmt, mc);

    // With IF NOT EXISTS, an already-existing target makes the whole statement a
    // no-op: we neither (re)create it nor copy any rows into it.
    if (existed_before && _if_not_exists) {
        co_return std::move(result);
    }

    // Step 2: INSERT INTO target SELECT ... The target now exists with exactly
    // the SELECT's result columns (same names), so the mapping is by-name
    // identity; the user-chosen primary key columns are guaranteed present.
    schema_ptr target = qp.db().find_schema(ks, cf);
    const auto& names = _source->get_result_metadata()->get_names();

    std::vector<insert_select_statement::column_mapping_entry> mapping;
    mapping.reserve(names.size());
    for (uint32_t i = 0; i < names.size(); ++i) {
        const column_definition* def = target->get_column_definition(to_bytes(names[i]->name->text()));
        mapping.push_back({i, def});
    }
    auto find_mapping_index = [&] (const column_definition& col) -> uint32_t {
        for (uint32_t mi = 0; mi < mapping.size(); ++mi) {
            if (mapping[mi].target_column->name() == col.name()) {
                return mi;
            }
        }
        // Unreachable: every key column was a selected column used to build the table.
        on_internal_error(ctaslog,
                seastar::format("CREATE TABLE AS SELECT: key column {} missing from mapping", col.name_as_text()));
    };
    std::vector<uint32_t> pk_indices;
    for (const column_definition& col : target->partition_key_columns()) {
        pk_indices.push_back(find_mapping_index(col));
    }
    std::vector<uint32_t> ck_indices;
    for (const column_definition& col : target->clustering_key_columns()) {
        ck_indices.push_back(find_mapping_index(col));
    }

    auto attrs = cql3::attributes::raw().prepare(qp.db(), ks, cf);
    auto insert_select = ::make_shared<insert_select_statement>(target, _source,
            std::move(mapping), std::move(pk_indices), std::move(ck_indices),
            std::move(attrs), _bound_terms, _stats);
    co_await insert_select->execute(qp, state, options, std::nullopt);

    co_return std::move(result);
}

}

}
