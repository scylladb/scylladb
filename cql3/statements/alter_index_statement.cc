/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "cql3/statements/alter_index_statement.hh"

#include "cql3/query_processor.hh"
#include "cql3/statements/cf_prop_defs.hh"
#include "cql3/statements/view_prop_defs.hh"
#include "data_dictionary/data_dictionary.hh"
#include "exceptions/exceptions.hh"
#include "index/secondary_index_manager.hh"
#include "replica/database.hh"
#include "service/migration_manager.hh"
#include "validation.hh"

namespace cql3::statements {

namespace {

sstring get_view_name(const sstring& index_name) {
    return secondary_index::index_table_name(index_name);
}

// If this function returns, the result is guaranteed to be a non-null pointer and point to the underlying view.
// If there is no corresponding view, throws a proper exception.
schema_ptr get_view_for_index(const data_dictionary::database& db, const sstring& ks, const sstring& index_name) {
    if (!db.has_keyspace(ks)) {
        throw exceptions::invalid_request_exception(seastar::format(
                "Keyspace {} does not exist", ks));
    }

    const auto view_name = get_view_name(index_name);
    try {
        return db.find_schema(ks, view_name);
    } catch (...) {
        throw exceptions::invalid_request_exception(seastar::format(
                "Index {}.{} does not exist", ks, index_name));
    }
}

view_ptr prepare_view(data_dictionary::database db, const view_prop_defs& props, const sstring& ks, const sstring& index_name) {
    schema_ptr view_schema_ptr = get_view_for_index(db, ks, index_name);
    
    schema_builder builder{view_schema_ptr};
    const schema::extensions_map schema_exts = props.properties()->make_schema_extensions(db.extensions());
    props.apply_to_builder(view_prop_defs::op_type::alter, builder, schema_exts, db, ks);

    return view_ptr(builder.build());
}

} // anonymous namespace

alter_index_statement::alter_index_statement(cf_name name, view_prop_defs properties)
    // Note: `this->column_family()` returns the name of the index, i.e.
    //       if the index was created using
    //          CREATE INDEX i ON ks.t(v),
    //       then `this->column_family()` is supposed to return `"i"`.
    //
    //       This might not be in line with how the other INDEX statements are implemented,
    //       but I find it much easier to operate on this actual name than something more complex.
    : schema_altering_statement{std::move(name)}
    , _view_properties(std::move(properties))
{}

future<> alter_index_statement::check_access(query_processor& qp, const service::client_state& state) const {
    schema_ptr s = get_view_for_index(qp.db(), keyspace(), column_family());
    return state.has_column_family_access(s->ks_name(), s->cf_name(), auth::permission::ALTER);
}

void alter_index_statement::validate(query_processor& qp, const service::client_state& state) const {
    const auto view_schema_ptr = get_view_for_index(qp.db(), keyspace(), column_family());

    const schema::extensions_map exts = _view_properties.properties()->make_schema_extensions(qp.db().extensions());
    _view_properties.validate_raw(view_prop_defs::op_type::alter, qp.db(), keyspace(), exts);
}

future<std::tuple<seastar::shared_ptr<cql_transport::event::schema_change>, utils::chunked_vector<mutation>, cql_warnings_vec>>
alter_index_statement::prepare_schema_mutations(query_processor& qp, const query_options&, api::timestamp_type ts) const {
    view_ptr view_schema = prepare_view(qp.db(), _view_properties, keyspace(), column_family());
    utils::chunked_vector<mutation> m = co_await service::prepare_view_update_announcement(qp.proxy(), view_schema, ts);

    using namespace cql_transport;
    auto ret = ::make_shared<event::schema_change>(
            event::schema_change::change_type::UPDATED,
            event::schema_change::target_type::TABLE,
            keyspace(),
            get_view_name(column_family()));

    co_return std::make_tuple(std::move(ret), std::move(m), std::vector<sstring>());
}

std::unique_ptr<prepared_statement> alter_index_statement::prepare(data_dictionary::database db, cql_stats&) {
    // FIXME: Add CQL stats?
    return std::make_unique<prepared_statement>(audit_info(), seastar::make_shared<alter_index_statement>(*this));
}

} // namespace cql3::statements
