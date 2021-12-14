/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <seastar/core/coroutine.hh>
#include "cql3/statements/drop_aggregate_statement.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/user_aggregate.hh"
#include "prepared_statement.hh"
#include "service/migration_manager.hh"
#include "cql3/query_processor.hh"
#include "mutation.hh"

namespace cql3 {

namespace statements {

std::unique_ptr<prepared_statement> drop_aggregate_statement::prepare(database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<drop_aggregate_statement>(*this));
}

future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>>
drop_aggregate_statement::prepare_schema_mutations(query_processor& qp) const {
    ::shared_ptr<cql_transport::event::schema_change> ret;
    std::vector<mutation> m;

    auto func = validate_while_executing(qp.proxy());
    if (func) {
        auto user_aggr = dynamic_pointer_cast<functions::user_aggregate>(func);
        if (!user_aggr) {
            throw exceptions::invalid_request_exception(format("'{}' is not a user defined aggregate", func));
        }
        m = co_await qp.get_migration_manager().prepare_aggregate_drop_announcement(user_aggr);
        ret = create_schema_change(*func, false);
    }

    co_return std::make_pair(std::move(ret), std::move(m));
}

drop_aggregate_statement::drop_aggregate_statement(functions::function_name name,
        std::vector<shared_ptr<cql3_type::raw>> arg_types, bool args_present, bool if_exists)
    : drop_function_statement_base(std::move(name), std::move(arg_types), args_present, if_exists) {}

}
}
