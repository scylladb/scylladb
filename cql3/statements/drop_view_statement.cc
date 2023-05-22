/*
 * Copyright 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <seastar/core/coroutine.hh>
#include "cql3/statements/drop_view_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/query_backend.hh"
#include "service/migration_manager.hh"
#include "service/client_state.hh"
#include "view_info.hh"
#include "data_dictionary/data_dictionary.hh"

namespace cql3 {

namespace statements {

drop_view_statement::drop_view_statement(cf_name view_name, bool if_exists)
    : schema_altering_statement{std::move(view_name), &timeout_config::truncate_timeout}
    , _if_exists{if_exists}
{
}

future<> drop_view_statement::check_access(query_backend& qb, const service::client_state& state) const
{
    try {
        const data_dictionary::database db = qb.db();
        auto&& s = db.find_schema(keyspace(), column_family());
        if (s->is_view()) {
            return state.has_column_family_access(db, keyspace(), s->view_info()->base_name(), auth::permission::ALTER);
        }
    } catch (const data_dictionary::no_such_column_family& e) {
        // Will be validated afterwards.
    }
    return make_ready_future<>();
}

void drop_view_statement::validate(query_backend&, const service::client_state& state) const
{
    // validated in migration_manager::announce_view_drop()
}

future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>>
drop_view_statement::prepare_schema_mutations(query_backend& qb, api::timestamp_type ts) const {
    ::shared_ptr<cql_transport::event::schema_change> ret;
    std::vector<mutation> m;

    try {
        m = co_await qb.get_migration_manager().prepare_view_drop_announcement(keyspace(), column_family(), ts);

        using namespace cql_transport;
        ret = ::make_shared<event::schema_change>(
            event::schema_change::change_type::DROPPED,
            event::schema_change::target_type::TABLE,
            keyspace(),
            column_family());
    } catch (const exceptions::configuration_exception& e) {
        if (!_if_exists) {
            co_return coroutine::exception(std::current_exception());
        }
    }

    co_return std::make_pair(std::move(ret), std::move(m));
}

std::unique_ptr<cql3::statements::prepared_statement>
drop_view_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<drop_view_statement>(*this));
}

}

}
