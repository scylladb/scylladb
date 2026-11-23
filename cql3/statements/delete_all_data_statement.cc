/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/assert.hh"
#include "cql3/statements/raw/delete_all_data_statement.hh"
#include "cql3/statements/delete_all_data_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/statements/strong_consistency/statement_helpers.hh"
#include "cql3/cql_statement.hh"
#include "cql3/query_processor.hh"
#include "data_dictionary/data_dictionary.hh"
#include "service/storage_proxy.hh"
#include "service/storage_service.hh"
#include "service/raft/raft_group0_client.hh"
#include "exceptions/exceptions.hh"
#include "validation.hh"
#include "index/vector_index.hh"
#include <optional>
#include <seastar/core/with_timeout.hh>

namespace cql3 {

namespace statements {

namespace raw {

delete_all_data_statement::delete_all_data_statement(cf_name name, std::unique_ptr<attributes::raw> attrs)
        : cf_statement(std::move(name))
        , _attrs(std::move(attrs))
{
    // DELETE ALL DATA supports USING TIMESTAMP and USING TIMEOUT
    throwing_assert(!_attrs->time_to_live.has_value());
}

std::unique_ptr<prepared_statement> delete_all_data_statement::prepare(data_dictionary::database db, cql_stats& stats, const cql_config& cfg) {
    schema_ptr schema = validation::validate_column_family(db, keyspace(), column_family());
    auto prepared_attributes = _attrs->prepare(db, keyspace(), column_family());
    prepare_context& ctx = get_prepare_context();
    prepared_attributes->fill_prepare_context(ctx);
    auto stmt = ::make_shared<cql3::statements::delete_all_data_statement>(std::move(schema), std::move(prepared_attributes), ctx.bound_variables_size());
    // The statement has no restrictions, so no bind marker can stand for a partition key component.
    return std::make_unique<prepared_statement>(audit_info(), std::move(stmt), ctx, std::vector<uint16_t>());
}

audit::statement_category delete_all_data_statement::category() const {
    return audit::statement_category::DML;
}

} // namespace raw

delete_all_data_statement::delete_all_data_statement(schema_ptr schema, std::unique_ptr<attributes> prepared_attrs, uint32_t bound_terms)
    // The statement commits a group 0 command and waits for the live nodes to apply it, which a
    // write's timeout is too short for, so it has the timeout of TRUNCATE and of the DROP statements.
    : cql_statement(&timeout_config::truncate_timeout)
    , _schema{std::move(schema)}
    , _attrs(std::move(prepared_attrs))
    , _bound_terms(bound_terms)
{
}

delete_all_data_statement::delete_all_data_statement(const delete_all_data_statement& s)
    : cql_statement(s)
    , _schema(s._schema)
    , _attrs(std::make_unique<attributes>(*s._attrs))
    , _bound_terms(s._bound_terms)
{ }

const sstring& delete_all_data_statement::keyspace() const {
    return _schema->ks_name();
}

const sstring& delete_all_data_statement::column_family() const {
    return _schema->cf_name();
}

uint32_t delete_all_data_statement::get_bound_terms() const {
    return _bound_terms;
}

bool delete_all_data_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const {
    return keyspace() == ks_name && (!cf_name || column_family() == *cf_name);
}

future<> delete_all_data_statement::check_access(query_processor& qp, const service::client_state& state) const {
    return state.has_column_family_access(keyspace(), column_family(), auth::permission::MODIFY);
}

future<::shared_ptr<cql_transport::messages::result_message>>
delete_all_data_statement::execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const {
    auto schema = qp.db().find_schema(keyspace(), column_family());
    if (schema->is_view()) {
        throw exceptions::invalid_request_exception("Cannot DELETE ALL DATA FROM a materialized view directly; must delete from the base table instead");
    }
    if (!qp.proxy().features().eventually_consistent_delete_all_data) {
        throw exceptions::invalid_request_exception("DELETE ALL DATA is not supported until the EVENTUALLY_CONSISTENT_DELETE_ALL_DATA cluster feature is enabled");
    }
    // FIXME: logstor tables do not strip the data covered by the truncation tombstone yet.
    if (!qp.db().find_keyspace(keyspace()).uses_tablets()
            || strong_consistency::is_strongly_consistent(qp.db(), keyspace())
            || schema->storage_engine() == storage_engine_type::logstor) {
        throw exceptions::invalid_request_exception(format("DELETE ALL DATA is supported only for tables of tablet keyspaces, "
                "which are neither strongly consistent nor use the logstor storage engine; {}.{} is not one. Use TRUNCATE instead",
                keyspace(), column_family()));
    }

    // A vector index is maintained by Vector Store from the table's CDC log, where this deletion
    // is not written, so the index would keep the deleted rows.
    if (secondary_index::vector_index::has_index(*schema)) {
        throw exceptions::invalid_request_exception(format("DELETE ALL DATA is not supported for tables with a vector index; {}.{} has one",
                keyspace(), column_family()));
    }

    const auto timeout = get_timeout(state.get_client_state(), options);
    // USING TIMESTAMP, else the client-side timestamp, if any, else the coordinator's clock.
    const auto timestamp = _attrs->get_timestamp(options.get_timestamp(state), options);
    try {
        // On timeout, the deletion may still complete in the background.
        co_await seastar::with_timeout(db::timeout_clock::now() + timeout,
                qp.storage_service().truncate_with_timestamp(schema->id(), tombstone(timestamp, gc_clock::now())));
    } catch (const seastar::timed_out_error&) {
        throw exceptions::server_exception(format("DELETE ALL DATA FROM {}.{} timed out; it may still complete in the background", keyspace(), column_family()));
    }
    co_return ::shared_ptr<cql_transport::messages::result_message>{};
}

db::timeout_clock::duration delete_all_data_statement::get_timeout(const service::client_state& state, const query_options& options) const {
    return _attrs->is_timeout_set() ? _attrs->get_timeout(options) : state.get_timeout_config().truncate_timeout;
}

}

}
