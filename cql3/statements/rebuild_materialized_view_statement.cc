/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "cql3/statements/rebuild_materialized_view_statement.hh"
#include "cql3/statements/raw/rebuild_materialized_view_statement.hh"
#include "cql3/statements/select_statement.hh"
#include "cql3/util.hh"
#include "validation.hh"
#include "view_info.hh"
#include "service/storage_proxy.hh"
namespace cql3 {

namespace statements {

namespace raw {

rebuild_materialized_view_statement::rebuild_materialized_view_statement(
        cf_name name,
        expr::expression where_clause,
        std::unique_ptr<attributes::raw> attrs)
    : cf_statement{std::move(name)}
    , _where_clause{where_clause}
    , _attrs{std::move(attrs)}
{
    // REBUILD MATERIALIZED VIEW doesn't support USING TIMESTAMP and USING TTL
    SCYLLA_ASSERT(!_attrs->timestamp.has_value());
    SCYLLA_ASSERT(!_attrs->time_to_live.has_value());
}

std::unique_ptr<prepared_statement> rebuild_materialized_view_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    schema_ptr schema = validation::validate_column_family(db, keyspace(), column_family());
    auto prepared_attributes = _attrs->prepare(db, keyspace(), column_family());
    auto ctx = get_prepare_context();
    prepared_attributes->fill_prepare_context(ctx);
    auto stmt = ::make_shared<cql3::statements::rebuild_materialized_view_statement>(std::move(schema), _where_clause, std::move(prepared_attributes));
    return std::make_unique<prepared_statement>(audit_info(), std::move(stmt));
}

audit::statement_category rebuild_materialized_view_statement::category() const {
    return audit::statement_category::DML;
}

}

rebuild_materialized_view_statement::rebuild_materialized_view_statement(
    schema_ptr view_schema,
    expr::expression where_clause,
    std::unique_ptr<attributes> prepared_attrs)
    : cql_statement_no_metadata(&timeout_config::other_timeout)
    , _view_schema(std::move(view_schema))
    , _where_clause(std::move(where_clause))
    , _attrs(std::move(prepared_attrs))
{
}

uint32_t rebuild_materialized_view_statement::get_bound_terms() const {
    return 0;
}

future<> rebuild_materialized_view_statement::check_access(query_processor& qp, const service::client_state& state) const {
    return state.has_column_family_access(_view_schema->ks_name(), _view_schema->cf_name(), auth::permission::MODIFY).then([this, &state] {
        return state.has_column_family_access(_view_schema->ks_name(), _view_schema->view_info()->base_name(), auth::permission::SELECT);
    });
}

void rebuild_materialized_view_statement::validate(query_processor& qp, const service::client_state& state) const {
    if (!_view_schema->is_view()) {
        throw exceptions::invalid_request_exception("You can only rebuild a Materialized View");
    }
    build_base_select_statement(qp.db());
}

future<::shared_ptr<cql_transport::messages::result_message>>
rebuild_materialized_view_statement::execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const {
    tracing::add_table_name(state.get_trace_state(), _view_schema->ks_name(), _view_schema->cf_name());

    auto select_stmt = build_base_select_statement(qp.db());
    dht::partition_range_vector partition_ranges = select_stmt->get_restrictions()->get_partition_key_ranges(options);
    query::partition_slice pslice = select_stmt->make_partition_slice(options);
    auto timeout_in_ms = std::chrono::duration_cast<std::chrono::milliseconds>(get_timeout(state.get_client_state(), options));

    query::rebuild_materialized_view_request request{
        .base_pranges = std::move(partition_ranges),
        .base_slice = pslice,
        .view_id = _view_schema->id(),
    };
    auto result = co_await qp.proxy().rebuild_materialized_view(request, timeout_in_ms);
    if (result.status != query::rebuild_materialized_view_result::command_status::success) {
        throw exceptions::invalid_request_exception("REBUILD MATERIALIZED VIEW failed on some replicas");
    }
    co_return ::make_shared<cql_transport::messages::result_message::void_message>();

}

bool rebuild_materialized_view_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const {
    return _view_schema->ks_name() == ks_name && (!cf_name || _view_schema->cf_name() == *cf_name || _view_schema->view_info()->base_name() == *cf_name);
}

shared_ptr<cql3::statements::select_statement> rebuild_materialized_view_statement::build_base_select_statement(data_dictionary::database db) const {
    std::unique_ptr<cql3::statements::raw::select_statement> raw;
    auto& view_info = *_view_schema->view_info();
    // FIXME: same legacy code as in view_info::select_statement()
    const column_definition* legacy_token_column = nullptr;
    if (db.find_column_family(view_info.base_id()).get_index_manager().is_global_index(*_view_schema)) {
        if (!_view_schema->clustering_key_columns().empty()) {
            legacy_token_column = &_view_schema->clustering_key_columns().front();
        }
    }

    if (legacy_token_column || std::ranges::any_of(_view_schema->all_columns(), std::mem_fn(&column_definition::is_computed))) {
        auto real_columns = _view_schema->all_columns() | std::views::filter([legacy_token_column] (const column_definition& cdef) {
            return &cdef != legacy_token_column && !cdef.is_computed();
        });
        schema::columns_type columns = std::ranges::to<schema::columns_type>(std::move(real_columns));
        raw = cql3::util::build_select_statement(view_info.base_name(), cql3::util::relations_to_where_clause(_where_clause), view_info.include_all_columns(), columns);
    } else {
        raw = cql3::util::build_select_statement(view_info.base_name(), cql3::util::relations_to_where_clause(_where_clause), view_info.include_all_columns(), _view_schema->all_columns());
    }
    raw->prepare_keyspace(_view_schema->ks_name());
    raw->set_bound_variables({});
    cql3::cql_stats ignored;
    auto prepared = raw->prepare(db, ignored, false);
    return static_pointer_cast<cql3::statements::select_statement>(prepared->statement);
}

db::timeout_clock::duration rebuild_materialized_view_statement::get_timeout(const service::client_state& state, const query_options& options) const {
    return _attrs->is_timeout_set() ? _attrs->get_timeout(options) : state.get_timeout_config().truncate_timeout;
}

}

}
