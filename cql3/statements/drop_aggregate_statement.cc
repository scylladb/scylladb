/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>
#include "cql3/statements/drop_aggregate_statement.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/user_aggregate.hh"
#include "prepared_statement.hh"
#include "service/migration_manager.hh"
#include "cql3/query_backend.hh"
#include "mutation/mutation.hh"

namespace cql3 {

namespace statements {

std::unique_ptr<prepared_statement> drop_aggregate_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<drop_aggregate_statement>(*this));
}

future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>>
drop_aggregate_statement::prepare_schema_mutations(query_backend& qb, api::timestamp_type ts) const {
    ::shared_ptr<cql_transport::event::schema_change> ret;
    std::vector<mutation> m;

    auto func = co_await validate_while_executing(qb);
    if (func) {
        auto user_aggr = dynamic_pointer_cast<functions::user_aggregate>(func);
        if (!user_aggr) {
            throw exceptions::invalid_request_exception(format("'{}' is not a user defined aggregate", func));
        }
        m = co_await qb.get_migration_manager().prepare_aggregate_drop_announcement(user_aggr, ts);
        ret = create_schema_change(*func, false);
    }

    co_return std::make_pair(std::move(ret), std::move(m));
}

drop_aggregate_statement::drop_aggregate_statement(functions::function_name name,
        std::vector<shared_ptr<cql3_type::raw>> arg_types, bool args_present, bool if_exists)
    : drop_function_statement_base(std::move(name), std::move(arg_types), args_present, if_exists) {}

}
}
