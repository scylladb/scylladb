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
#include "cql3/query_processor.hh"
#include "mutation/mutation.hh"

namespace cql3 {

namespace statements {

std::unique_ptr<prepared_statement> drop_aggregate_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<drop_aggregate_statement>(*this));
}

future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, cql3::cql_warnings_vec>> drop_aggregate_statement::prepare_schema_mutations(query_processor& qp, service::query_state& state, const query_options& options, service::group0_batch& mc) const {
    ::shared_ptr<cql_transport::event::schema_change> ret;

    auto func = co_await validate_while_executing(qp);
    if (func) {
        auto user_aggr = dynamic_pointer_cast<functions::user_aggregate>(func);
        if (!user_aggr) {
            throw exceptions::invalid_request_exception(format("'{}' is not a user defined aggregate", func));
        }
        auto muts = co_await service::prepare_aggregate_drop_announcement(qp.proxy(), user_aggr, mc.write_timestamp());
        mc.add_mutations(std::move(muts), "CQL drop aggregate");

        const auto& as = *state.get_client_state().get_auth_service();
        co_await auth::revoke_all(as, auth::make_functions_resource(*user_aggr), mc);

        ret = create_schema_change(*func, false);
    }

    co_return std::make_tuple(std::move(ret), std::vector<sstring>());
}

drop_aggregate_statement::drop_aggregate_statement(functions::function_name name,
        std::vector<shared_ptr<cql3_type::raw>> arg_types, bool args_present, bool if_exists)
    : drop_function_statement_base(std::move(name), std::move(arg_types), args_present, if_exists) {}

}
}
