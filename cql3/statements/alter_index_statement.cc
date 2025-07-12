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
#include "utils/on_internal_error.hh"
#include "validation.hh"
#include "view_info.hh"

namespace cql3::statements {

namespace {

// The result is guaranteed to point to a valid schema object, which has an index of a given name.
schema_ptr get_base_table(const data_dictionary::database& db, std::string_view keyspace, std::string_view index_name) {
    if (!db.has_keyspace(keyspace)) {
        throw exceptions::invalid_request_exception(seastar::format(
                "Keyspace {} does not exist", keyspace));
    }

    schema_ptr result = db.find_indexed_table(keyspace, index_name);
    if (result == nullptr) {
        throw exceptions::invalid_request_exception(seastar::format(
                "There is no index of name {}.{}", keyspace, index_name));
    }
    return result;
}

sstring get_view_name(const sstring& index_name) {
    return secondary_index::index_table_name(index_name);
}

// No checks are performed.
view_ptr get_view_schema_for_index_unsafe(const data_dictionary::database& db, const sstring& ks, const sstring& index_name) {
    const auto view_name = get_view_name(index_name);
    return view_ptr(db.find_schema(ks, view_name));
}

// Precondition: `base_table` points to a valid schema object representing an existing table.
bool index_uses_view(const schema_ptr base_table, const sstring& index_name) {
    const sstring& ks = base_table->ks_name();
    const auto it = base_table->all_indices().find(index_name);
    if (it == base_table->all_indices().end()) {
        utils::on_internal_error(seastar::format(
                "The index {}.{} exists and has a base table {}.{}, but it's not listed in its indexes",
                ks, index_name, base_table->ks_name(), base_table->cf_name()));
    }

    const index_metadata& meta = it->second;
    const auto maybe_custom_class = base_table->table().get_index_manager().get_custom_class(meta);

    if (!maybe_custom_class.has_value()) {
        return true;
    }

    return maybe_custom_class->get()->view_should_exist();
}

// If this function returns, the result is guaranteed to be a non-null pointer and point to a valid
// materialized view. It does NOT guarantee that the view is the underlying view of a secondary index.
// If there is no corresponding view, throws a proper exception.
view_ptr get_view_for_index(const data_dictionary::database& db, const sstring& ks, const sstring& index_name) {
    // If this doesn't throw, we've confirmed that the index exists and is an index indeed.
    const schema_ptr base_table = get_base_table(db, ks, index_name);

    // If we don't enter this `if`, we've confirmed that the index uses a materialized view.
    // The view must exist then.
    if (!index_uses_view(base_table, index_name)) {
        throw exceptions::invalid_request_exception(seastar::format(
                "You cannot alter index {}.{} because its class does not use a materialized view",
                ks, index_name));
    }

    const auto view_name = get_view_name(index_name);
    schema_ptr result = nullptr;

    try {
        result = db.find_schema(ks, view_name);
    } catch (...) {
        utils::on_internal_error(seastar::format(
                "Violated precondition: the index {}.{} was supposed to have a materialized view, "
                "but there is no schema called {}.{}",
                ks, index_name, ks, view_name));
    }

    return view_ptr(std::move(result));
}

// Preconditions:
// * The index `ks.index_name` exists and is an index indeed.
// * The index `ks.index_name` uses a materialized view.
// * The underlying materialized view of the index exists.
view_ptr prepare_view(data_dictionary::database db, const view_prop_defs& props, const sstring& ks, const sstring& index_name) {
    schema_ptr view_schema_ptr = get_view_schema_for_index_unsafe(db, ks, index_name);
    
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
    const view_ptr view = get_view_for_index(qp.db(), keyspace(), column_family());

    // This is weird, I know. For some reason, `ALTER MATERIALIZED VIEW` requires that the role
    // performing the statement have the permission to alter the BASE TABLE, not the view itself.
    // We do the same here just to be consistent.
    return state.has_column_family_access(view->ks_name(), view->view_info()->base_name(), auth::permission::ALTER);
}

void alter_index_statement::validate(query_processor& qp, const service::client_state& state) const {
    const view_ptr view_schema_ptr = get_view_for_index(qp.db(), keyspace(), column_family());

    const schema::extensions_map exts = _view_properties.properties()->make_schema_extensions(qp.db().extensions());
    _view_properties.validate_raw(view_prop_defs::op_type::alter, qp.db(), keyspace(), exts);
}

future<std::tuple<seastar::shared_ptr<cql_transport::event::schema_change>, utils::chunked_vector<mutation>, cql_warnings_vec>>
alter_index_statement::prepare_schema_mutations(query_processor& qp, const query_options&, api::timestamp_type ts) const {
    view_ptr view_schema = prepare_view(qp.db(), _view_properties, keyspace(), column_family());
    utils::chunked_vector<mutation> m = co_await service::prepare_view_update_announcement(qp.proxy(), view_schema, ts);

    using namespace cql_transport;
    // It may be weird that we use the base table's name here, but we stay
    // consistent with how e.g. `DROP INDEX` is implemented.
    auto ret = ::make_shared<event::schema_change>(
            event::schema_change::change_type::UPDATED,
            event::schema_change::target_type::TABLE,
            keyspace(),
            view_schema->view_info()->base_name());

    co_return std::make_tuple(std::move(ret), std::move(m), std::vector<sstring>());
}

std::unique_ptr<prepared_statement> alter_index_statement::prepare(data_dictionary::database db, cql_stats&) {
    // FIXME: Add CQL stats?
    return std::make_unique<prepared_statement>(audit_info(), seastar::make_shared<alter_index_statement>(*this));
}

} // namespace cql3::statements
