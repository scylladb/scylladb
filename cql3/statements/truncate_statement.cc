/*
 * Copyright (C) 2014-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include "utils/assert.hh"
#include "cql3/statements/raw/truncate_statement.hh"
#include "cql3/statements/truncate_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/cql_statement.hh"
#include "data_dictionary/data_dictionary.hh"
#include "cql3/query_processor.hh"
#include "service/storage_proxy.hh"
#include "service/storage_service.hh"
#include "cql3/statements/strong_consistency/statement_helpers.hh"
#include <optional>
#include <seastar/core/with_timeout.hh>
#include "validation.hh"

namespace cql3 {

namespace statements {

namespace raw {

truncate_statement::truncate_statement(cf_name name, std::unique_ptr<attributes::raw> attrs)
        : cf_statement(std::move(name))
        , _attrs(std::move(attrs))
{
    // Validate the attributes.
    // TRUNCATE supports USING TIMESTAMP and USING TIMEOUT
    throwing_assert(!_attrs->time_to_live.has_value());
}

std::unique_ptr<prepared_statement> truncate_statement::prepare(data_dictionary::database db, cql_stats& stats, const cql_config& cfg) {
    schema_ptr schema = validation::validate_column_family(db, keyspace(), column_family());
    auto prepared_attributes = _attrs->prepare(db, keyspace(), column_family());
    prepare_context& ctx = get_prepare_context();
    prepared_attributes->fill_prepare_context(ctx);
    auto stmt = ::make_shared<cql3::statements::truncate_statement>(std::move(schema), std::move(prepared_attributes), ctx.bound_variables_size());
    // TRUNCATE has no restrictions, so no bind marker can stand for a partition key component.
    return std::make_unique<prepared_statement>(audit_info(), std::move(stmt), ctx, std::vector<uint16_t>());
}

} // namespace raw

truncate_statement::truncate_statement(schema_ptr schema, std::unique_ptr<attributes> prepared_attrs, uint32_t bound_terms)
    : cql_statement(&timeout_config::truncate_timeout)
    , _schema{std::move(schema)}
    , _attrs(std::move(prepared_attrs))
    , _bound_terms(bound_terms)
{
}

truncate_statement::truncate_statement(const truncate_statement& ts)
    : cql_statement(ts)
    , _schema(ts._schema)
    , _attrs(std::make_unique<attributes>(*ts._attrs))
    , _bound_terms(ts._bound_terms)
{ }

const sstring& truncate_statement::keyspace() const {
    return _schema->ks_name();
}

const sstring& truncate_statement::column_family() const {
    return _schema->cf_name();
}

uint32_t truncate_statement::get_bound_terms() const
{
    return _bound_terms;
}

bool truncate_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const
{
    return false;
}

future<> truncate_statement::check_access(query_processor& qp, const service::client_state& state) const
{
    return state.has_column_family_access(keyspace(), column_family(), auth::permission::MODIFY);
}

void truncate_statement::validate(query_processor&, const service::client_state& state) const
{
    warn(unimplemented::cause::VALIDATION);
}

future<::shared_ptr<cql_transport::messages::result_message>>
truncate_statement::execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const
{
    auto schema = qp.db().find_schema(keyspace(), column_family());
    if (schema->is_view()) {
        throw exceptions::invalid_request_exception("Cannot TRUNCATE materialized view directly; must truncate base table instead");
    }
    const auto timeout = get_timeout(state.get_client_state(), options);

    // TRUNCATE of eventually-consistent tablet tables is timestamped, like a DELETE of every partition
    // (see docs/dev/eventually-consistent-truncate.md). Until the cluster feature is enabled, and for other
    // tables, it removes the table's data physically.
    // FIXME: logstor tables do not strip the data covered by the truncation tombstone yet.
    const bool eventually_consistent = qp.db().find_keyspace(keyspace()).uses_tablets()
            && !strong_consistency::is_strongly_consistent(qp.db(), keyspace())
            && schema->storage_engine() != storage_engine_type::logstor
            && qp.proxy().features().eventually_consistent_truncate;
    if (eventually_consistent) {
        // USING TIMESTAMP, else the client-side timestamp, if any, else the coordinator's clock.
        const auto timestamp = _attrs->get_timestamp(options.get_timestamp(state), options);
        try {
            // On timeout, the truncation may still complete in the background.
            co_await seastar::with_timeout(db::timeout_clock::now() + timeout,
                    qp.storage_service().truncate_with_timestamp(schema->id(), tombstone(timestamp, gc_clock::now())));
        } catch (...) {
            throw exceptions::truncate_exception(std::current_exception());
        }
        co_return ::shared_ptr<cql_transport::messages::result_message>{};
    }
    if (_attrs->is_timestamp_set()) {
        throw exceptions::invalid_request_exception("TRUNCATE USING TIMESTAMP is supported only for tablet tables, which are not strongly consistent, "
                "once the EVENTUALLY_CONSISTENT_TRUNCATE cluster feature is enabled");
    }

    auto timeout_in_ms = std::chrono::duration_cast<std::chrono::milliseconds>(timeout);
    try {
        co_await qp.proxy().truncate_blocking(keyspace(), column_family(), timeout_in_ms);
    } catch (...) {
        throw exceptions::truncate_exception(std::current_exception());
    }
    co_return ::shared_ptr<cql_transport::messages::result_message>{};
}

audit::statement_category raw::truncate_statement::category() const {
    return audit::statement_category::DML;
}

db::timeout_clock::duration truncate_statement::get_timeout(const service::client_state& state, const query_options& options) const {
    return _attrs->is_timeout_set() ? _attrs->get_timeout(options) : state.get_timeout_config().truncate_timeout;
}

}

}
