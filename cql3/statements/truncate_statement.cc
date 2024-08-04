/*
 * Copyright (C) 2014-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "utils/assert.hh"
#include "cql3/statements/raw/truncate_statement.hh"
#include "cql3/statements/truncate_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/cql_statement.hh"
#include "data_dictionary/data_dictionary.hh"
#include "cql3/query_processor.hh"
#include "service/storage_proxy.hh"
#include <optional>
#include "validation.hh"

namespace cql3 {

namespace statements {

namespace raw {

truncate_statement::truncate_statement(cf_name name, std::unique_ptr<attributes::raw> attrs)
        : cf_statement(std::move(name))
        , _attrs(std::move(attrs))
{
    // Validate the attributes.
    // Currently, TRUNCATE supports only USING TIMEOUT
    SCYLLA_ASSERT(!_attrs->timestamp.has_value());
    SCYLLA_ASSERT(!_attrs->time_to_live.has_value());
}

std::unique_ptr<prepared_statement> truncate_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    schema_ptr schema = validation::validate_column_family(db, keyspace(), column_family());
    auto prepared_attributes = _attrs->prepare(db, keyspace(), column_family());
    auto ctx = get_prepare_context();
    prepared_attributes->fill_prepare_context(ctx);
    auto stmt = ::make_shared<cql3::statements::truncate_statement>(std::move(schema), std::move(prepared_attributes));
    return std::make_unique<prepared_statement>(std::move(stmt));
}

} // namespace raw

truncate_statement::truncate_statement(schema_ptr schema, std::unique_ptr<attributes> prepared_attrs)
    : cql_statement_no_metadata(&timeout_config::truncate_timeout)
    , _schema{std::move(schema)}
    , _attrs(std::move(prepared_attrs))
{
}

truncate_statement::truncate_statement(const truncate_statement& ts)
    : cql_statement_no_metadata(ts)
    , _schema(ts._schema)
    , _attrs(std::make_unique<attributes>(*ts._attrs))
{ }

const sstring& truncate_statement::keyspace() const {
    return _schema->ks_name();
}

const sstring& truncate_statement::column_family() const {
    return _schema->cf_name();
}

uint32_t truncate_statement::get_bound_terms() const
{
    return 0;
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
    if (qp.db().find_schema(keyspace(), column_family())->is_view()) {
        throw exceptions::invalid_request_exception("Cannot TRUNCATE materialized view directly; must truncate base table instead");
    }
    auto timeout_in_ms = std::chrono::duration_cast<std::chrono::milliseconds>(get_timeout(state.get_client_state(), options));
    return qp.proxy().truncate_blocking(keyspace(), column_family(), timeout_in_ms).handle_exception([](auto ep) {
        throw exceptions::truncate_exception(ep);
    }).then([] {
        return ::shared_ptr<cql_transport::messages::result_message>{};
    });
}

db::timeout_clock::duration truncate_statement::get_timeout(const service::client_state& state, const query_options& options) const {
    return _attrs->is_timeout_set() ? _attrs->get_timeout(options) : state.get_timeout_config().truncate_timeout;
}

}

}
