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
#include "cql3/query_processor.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "view_info.hh"
#include "data_dictionary/data_dictionary.hh"

namespace cql3 {

namespace statements {

drop_view_statement::drop_view_statement(cf_name view_name, bool if_exists)
    : schema_altering_statement{std::move(view_name), &timeout_config::truncate_timeout}
    , _if_exists{if_exists}
{
}

future<> drop_view_statement::check_access(query_processor& qp, const service::client_state& state) const
{
    try {
        const data_dictionary::database db = qp.db();
        auto&& s = db.find_schema(keyspace(), column_family());
        if (s->is_view()) {
            return state.has_column_family_access(keyspace(), s->view_info()->base_name(), auth::permission::ALTER);
        }
    } catch (const data_dictionary::no_such_column_family& e) {
        // Will be validated afterwards.
    }
    return make_ready_future<>();
}

future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>>
drop_view_statement::prepare_schema_mutations(query_processor& qp, const query_options&, api::timestamp_type ts) const {
    ::shared_ptr<cql_transport::event::schema_change> ret;
    std::vector<mutation> m;

    try {
        m = co_await service::prepare_view_drop_announcement(qp.proxy(), keyspace(), column_family(), ts);

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

    co_return std::make_tuple(std::move(ret), std::move(m), std::vector<sstring>());
}

std::unique_ptr<cql3::statements::prepared_statement>
drop_view_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<drop_view_statement>(*this));
}

}

}
