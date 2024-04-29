/*
 * Copyright 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <seastar/core/coroutine.hh>
#include "cql3/statements/alter_view_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "validation.hh"
#include "view_info.hh"
#include "data_dictionary/data_dictionary.hh"
#include "cql3/query_processor.hh"

namespace cql3 {

namespace statements {

alter_view_statement::alter_view_statement(cf_name view_name, std::optional<cf_prop_defs> properties)
        : schema_altering_statement{std::move(view_name)}
        , _properties{std::move(properties)}
{
}

future<> alter_view_statement::check_access(query_processor& qp, const service::client_state& state) const
{
    try {
        const data_dictionary::database db = qp.db();
        auto&& s = db.find_schema(keyspace(), column_family());
        if (s->is_view())  {
            return state.has_column_family_access(keyspace(), s->view_info()->base_name(), auth::permission::ALTER);
        }
    } catch (const data_dictionary::no_such_column_family& e) {
        // Will be validated afterwards.
    }
    return make_ready_future<>();
}

view_ptr alter_view_statement::prepare_view(data_dictionary::database db) const {
    schema_ptr schema = validation::validate_column_family(db, keyspace(), column_family());
    if (!schema->is_view()) {
        throw exceptions::invalid_request_exception("Cannot use ALTER MATERIALIZED VIEW on Table");
    }

    if (!_properties) {
        throw exceptions::invalid_request_exception("ALTER MATERIALIZED VIEW WITH invoked, but no parameters found");
    }

    auto schema_extensions = _properties->make_schema_extensions(db.extensions());
    _properties->validate(db, keyspace(), schema_extensions);

    auto builder = schema_builder(schema);
    _properties->apply_to_builder(builder, std::move(schema_extensions), db, keyspace());

    if (builder.get_gc_grace_seconds() == 0) {
        throw exceptions::invalid_request_exception(
                "Cannot alter gc_grace_seconds of a materialized view to 0, since this "
                "value is used to TTL undelivered updates. Setting gc_grace_seconds too "
                "low might cause undelivered updates to expire before being replayed.");
    }

    if (builder.default_time_to_live().count() > 0) {
        throw exceptions::invalid_request_exception(
                "Cannot set or alter default_time_to_live for a materialized view. "
                "Data in a materialized view always expire at the same time than "
                "the corresponding data in the parent table.");
    }

    return view_ptr(builder.build());
}

future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>> alter_view_statement::prepare_schema_mutations(query_processor& qp, const query_options&, api::timestamp_type ts) const {
    auto m = co_await service::prepare_view_update_announcement(qp.proxy(), prepare_view(qp.db()), ts);

    using namespace cql_transport;
    auto ret = ::make_shared<event::schema_change>(
            event::schema_change::change_type::UPDATED,
            event::schema_change::target_type::TABLE,
            keyspace(),
            column_family());

    co_return std::make_tuple(std::move(ret), std::move(m), std::vector<sstring>());
}

std::unique_ptr<cql3::statements::prepared_statement>
alter_view_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<alter_view_statement>(*this));
}

}

}
