/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <seastar/core/coroutine.hh>
#include "cql3/statements/alter_type_statement.hh"
#include "cql3/statements/create_type_statement.hh"
#include "cql3/query_processor.hh"
#include "cql3/column_identifier.hh"
#include "prepared_statement.hh"
#include "schema/schema_builder.hh"
#include "mutation/mutation.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "data_dictionary/data_dictionary.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include "boost/range/adaptor/map.hpp"
#include "data_dictionary/user_types_metadata.hh"

namespace cql3 {

namespace statements {

alter_type_statement::alter_type_statement(const ut_name& name)
    : _name{name}
{
}

void alter_type_statement::prepare_keyspace(const service::client_state& state)
{
    if (!_name.has_keyspace()) {
        _name.set_keyspace(state.get_keyspace());
    }
}

future<> alter_type_statement::check_access(query_processor& qp, const service::client_state& state) const
{
    return state.has_keyspace_access(keyspace(), auth::permission::ALTER);
}

const sstring& alter_type_statement::keyspace() const
{
    return _name.get_keyspace();
}

future<std::vector<mutation>> alter_type_statement::prepare_announcement_mutations(service::storage_proxy& sp, api::timestamp_type ts) const {
    std::vector<mutation> m;
    auto&& ks = sp.data_dictionary().find_keyspace(keyspace());
    auto&& all_types = ks.metadata()->user_types().get_all_types();
    auto to_update = all_types.find(_name.get_user_type_name());
    // Shouldn't happen, unless we race with a drop
    if (to_update == all_types.end()) {
        throw exceptions::invalid_request_exception(format("No user type named {} exists.", _name.to_cql_string()));
    }

    for (auto&& schema : ks.metadata()->cf_meta_data() | boost::adaptors::map_values) {
        for (auto&& column : schema->partition_key_columns()) {
            if (column.type->references_user_type(_name.get_keyspace(), _name.get_user_type_name())) {
                throw exceptions::invalid_request_exception(format("Cannot add new field to type {} because it is used in the partition key column {} of table {}.{}",
                    _name.to_cql_string(), column.name_as_text(), schema->ks_name(), schema->cf_name()));
            }
        }
    }

    auto&& updated = make_updated_type(sp.data_dictionary(), to_update->second);
    // Now, we need to announce the type update to basically change it for new tables using this type,
    // but we also need to find all existing user types and CF using it and change them.
    auto res = co_await service::prepare_update_type_announcement(sp, updated, ts);
    std::move(res.begin(), res.end(), std::back_inserter(m));

    for (auto&& schema : ks.metadata()->cf_meta_data() | boost::adaptors::map_values) {
        auto cfm = schema_builder(schema);
        bool modified = false;
        for (auto&& column : schema->all_columns()) {
            auto t_opt = column.type->update_user_type(updated);
            if (t_opt) {
                modified = true;
                // We need to update this column
                cfm.alter_column_type(column.name(), *t_opt);
            }
        }
        if (modified) {
            if (schema->is_view()) {
                auto res = co_await service::prepare_view_update_announcement(sp, view_ptr(cfm.build()), ts);
                std::move(res.begin(), res.end(), std::back_inserter(m));
            } else {
                auto res = co_await service::prepare_column_family_update_announcement(sp, cfm.build(), {}, ts);
                std::move(res.begin(), res.end(), std::back_inserter(m));
            }
        }
    }

    co_return m;
}

future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>>
alter_type_statement::prepare_schema_mutations(query_processor& qp, const query_options&, api::timestamp_type ts) const {
    try {
        auto m = co_await prepare_announcement_mutations(qp.proxy(), ts);

        using namespace cql_transport;
        auto ret = ::make_shared<event::schema_change>(
                event::schema_change::change_type::UPDATED,
                event::schema_change::target_type::TYPE,
                keyspace(),
                _name.get_string_type_name());

        co_return std::make_tuple(std::move(ret), std::move(m), std::vector<sstring>());
    } catch(data_dictionary::no_such_keyspace& e) {
        auto&& ex = std::make_exception_ptr(exceptions::invalid_request_exception(format("Cannot alter type in unknown keyspace {}", keyspace())));
        co_return coroutine::exception(std::move(ex));
    }
}

alter_type_statement::add_or_alter::add_or_alter(const ut_name& name, bool is_add, shared_ptr<column_identifier> field_name, shared_ptr<cql3_type::raw> field_type)
        : alter_type_statement(name)
        , _is_add(is_add)
        , _field_name(field_name)
        , _field_type(field_type)
{
}

user_type alter_type_statement::add_or_alter::do_add(data_dictionary::database db, user_type to_update) const
{
    if (to_update->idx_of_field(_field_name->name())) {
        throw exceptions::invalid_request_exception(format("Cannot add new field {} to type {}: a field of the same name already exists",
            _field_name->to_string(), _name.to_cql_string()));
    }

    if (to_update->size() == max_udt_fields) {
        throw exceptions::invalid_request_exception(format("Cannot add new field to type {}: maximum number of fields reached", _name));
    }

    if (_field_type->is_duration()) {
        auto&& ks = db.find_keyspace(keyspace());
        for (auto&& schema : ks.metadata()->cf_meta_data() | boost::adaptors::map_values) {
            for (auto&& column : schema->clustering_key_columns()) {
                if (column.type->references_user_type(_name.get_keyspace(), _name.get_user_type_name())) {
                    throw exceptions::invalid_request_exception(format("Cannot add new field to type {} because it is used in the clustering key column {} of table {}.{} where durations are not allowed",
                        _name.to_cql_string(), column.name_as_text(), schema->ks_name(), schema->cf_name()));
                }
            }
        }
    }

    std::vector<bytes> new_names(to_update->field_names());
    new_names.push_back(_field_name->name());
    std::vector<data_type> new_types(to_update->field_types());
    auto&& add_type = _field_type->prepare(db, keyspace()).get_type();
    if (add_type->references_user_type(to_update->_keyspace, to_update->_name)) {
        throw exceptions::invalid_request_exception(format("Cannot add new field {} of type {} to type {} as this would create a circular reference",
                    *_field_name, *_field_type, _name.to_cql_string()));
    }
    new_types.push_back(std::move(add_type));
    return user_type_impl::get_instance(to_update->_keyspace, to_update->_name, std::move(new_names), std::move(new_types), to_update->is_multi_cell());
}

user_type alter_type_statement::add_or_alter::do_alter(data_dictionary::database db, user_type to_update) const
{
    auto idx = to_update->idx_of_field(_field_name->name());
    if (!idx) {
        throw exceptions::invalid_request_exception(format("Unknown field {} in type {}", _field_name->to_string(), _name.to_cql_string()));
    }

    auto previous = to_update->field_types()[*idx];
    auto new_type = _field_type->prepare(db, keyspace()).get_type();
    if (!new_type->is_compatible_with(*previous)) {
        throw exceptions::invalid_request_exception(format("Type {} in incompatible with previous type {} of field {} in user type {}",
            *_field_type, previous->as_cql3_type(), *_field_name, _name));
    }

    std::vector<data_type> new_types(to_update->field_types());
    new_types[*idx] = new_type;
    return user_type_impl::get_instance(to_update->_keyspace, to_update->_name, to_update->field_names(), std::move(new_types), to_update->is_multi_cell());
}

user_type alter_type_statement::add_or_alter::make_updated_type(data_dictionary::database db, user_type to_update) const
{
    return _is_add ? do_add(db, to_update) : do_alter(db, to_update);
}

alter_type_statement::renames::renames(const ut_name& name)
        : alter_type_statement(name)
{
}

void alter_type_statement::renames::add_rename(shared_ptr<column_identifier> previous_name, shared_ptr<column_identifier> new_name)
{
    _renames.emplace_back(previous_name, new_name);
}

user_type alter_type_statement::renames::make_updated_type(data_dictionary::database db, user_type to_update) const
{
    std::vector<bytes> new_names(to_update->field_names());
    for (auto&& rename : _renames) {
        auto&& from = rename.first;
        auto idx = to_update->idx_of_field(from->name());
        if (!idx) {
            throw exceptions::invalid_request_exception(format("Unknown field {} in type {}", from->to_string(), _name.to_cql_string()));
        }
        new_names[*idx] = rename.second->name();
    }
    auto updated = user_type_impl::get_instance(to_update->_keyspace, to_update->_name, std::move(new_names), to_update->field_types(), to_update->is_multi_cell());
    create_type_statement::check_for_duplicate_names(updated);
    return updated;
}

std::unique_ptr<cql3::statements::prepared_statement>
alter_type_statement::add_or_alter::prepare(data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<alter_type_statement::add_or_alter>(*this));
}

std::unique_ptr<cql3::statements::prepared_statement>
alter_type_statement::renames::prepare(data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<alter_type_statement::renames>(*this));
}

}

}
