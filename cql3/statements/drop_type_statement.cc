/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <seastar/core/coroutine.hh>
#include "cql3/statements/drop_type_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/query_processor.hh"
#include "cql3/functions/functions.hh"

#include "boost/range/adaptor/map.hpp"

#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "data_dictionary/data_dictionary.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include "data_dictionary/user_types_metadata.hh"
#include "mutation/mutation.hh"

namespace cql3 {

namespace statements {

drop_type_statement::drop_type_statement(const ut_name& name, bool if_exists)
    : _name{name}
    , _if_exists{if_exists}
{
}

void drop_type_statement::prepare_keyspace(const service::client_state& state)
{
    if (!_name.has_keyspace()) {
        _name.set_keyspace(state.get_keyspace());
    }
}

future<> drop_type_statement::check_access(query_processor& qp, const service::client_state& state) const
{
    return state.has_keyspace_access(keyspace(), auth::permission::DROP);
}

// function returns false when there is no type or keyspace found and flag _if_exists is set;
// generate exception if anything is wrong, and returns true when we have a type to delete.
bool drop_type_statement::validate_while_executing(query_processor& qp) const {
    try {
        auto&& ks = qp.db().find_keyspace(keyspace());
        auto&& all_types = ks.metadata()->user_types().get_all_types();
        auto old = all_types.find(_name.get_user_type_name());
        if (old == all_types.end()) {
            if (_if_exists) {
                return false;
            } else {
                throw exceptions::invalid_request_exception(format("No user type named {} exists.", _name.to_cql_string()));
            }
        }

        // We don't want to drop a type unless it's not used anymore (mainly because
        // if someone drops a type and recreates one with the same name but different
        // definition with the previous name still in use, things can get messy).
        // We have two places to check: 1) other user type that can nest the one
        // we drop and 2) existing tables referencing the type (maybe in a nested
        // way).

        // This code is moved from schema_keyspace (akin to origin) because we cannot
        // delay this check to until after we've applied the mutations. If a type or
        // table references the type we're dropping, we will a.) get exceptions parsing
        // (can be translated to invalid_request, but...) and more importantly b.)
        // we will leave those types/tables in a broken state.
        // We managed to get through this before because we neither enforced hard
        // cross reference between types when loading them, nor did we in fact
        // probably ever run the scenario of dropping a referenced type and then
        // actually using the referee.
        //
        // Now, this has a giant flaw. We are succeptible to race conditions here,
        // since we could have a drop at the same time as a create type that references
        // the dropped one, but we complete the check before the create is done,
        // yet apply the drop mutations after -> inconsistent data!
        // This problem is the same in origin, and I see no good way around it
        // as long as the atomicity of schema modifications are based on
        // actual apply of mutations, because unlike other drops, this one isn't
        // benevolent.
        // I guess this is one case where user need beware, and don't mess with types
        // concurrently!

        auto&& type = old->second;
        auto&& keyspace = type->_keyspace;
        auto&& name = type->_name;

        for (auto&& ut : all_types | boost::adaptors::map_values) {
            if (ut->_keyspace == keyspace && ut->_name == name) {
                continue;
            }

            if (ut->references_user_type(keyspace, name)) {
                throw exceptions::invalid_request_exception(format("Cannot drop user type {}.{} as it is still used by user type {}", keyspace, type->get_name_as_string(), ut->get_name_as_string()));
            }
        }

        for (auto&& cfm : ks.metadata()->cf_meta_data() | boost::adaptors::map_values) {
            for (auto&& col : cfm->all_columns()) {
                if (col.type->references_user_type(keyspace, name)) {
                    throw exceptions::invalid_request_exception(format("Cannot drop user type {}.{} as it is still used by table {}.{}", keyspace, type->get_name_as_string(), cfm->ks_name(), cfm->cf_name()));
                }
            }
        }

        if (auto&& fun_name = functions::instance().used_by_user_function(_name)) {
            throw exceptions::invalid_request_exception(format("Cannot drop user type {}.{} as it is still used by function {}", keyspace, type->get_name_as_string(), *fun_name));
        }
        return true;
    } catch (data_dictionary::no_such_keyspace& e) {
        if (!_if_exists) {
            throw exceptions::invalid_request_exception(format("Cannot drop type in unknown keyspace {}", keyspace()));
        }
        return false;
    }
}


const sstring& drop_type_statement::keyspace() const
{
    return _name.get_keyspace();
}

future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>>
drop_type_statement::prepare_schema_mutations(query_processor& qp, const query_options&, api::timestamp_type ts) const {
    ::shared_ptr<cql_transport::event::schema_change> ret;
    std::vector<mutation> m;

    if (validate_while_executing(qp)) {
        data_dictionary::database db = qp.db();

        // Keyspace exists or we wouldn't have validated otherwise
        auto&& ks = db.find_keyspace(keyspace());

        const auto& all_types = ks.metadata()->user_types().get_all_types();
        auto to_drop = all_types.find(_name.get_user_type_name());

        m = co_await service::prepare_type_drop_announcement(qp.proxy(), to_drop->second, ts);

        using namespace cql_transport;
        ret = ::make_shared<event::schema_change>(
                event::schema_change::change_type::DROPPED,
                event::schema_change::target_type::TYPE,
                keyspace(),
                _name.get_string_type_name());
    }
    co_return std::make_tuple(std::move(ret), std::move(m), std::vector<sstring>());
}

std::unique_ptr<cql3::statements::prepared_statement>
drop_type_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<drop_type_statement>(*this));
}

}

}
