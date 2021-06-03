/*
 * Copyright (C) 2019-present ScyllaDB
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

#include "cql3/statements/drop_function_statement.hh"
#include "cql3/functions/functions.hh"
#include "prepared_statement.hh"
#include "service/migration_manager.hh"
#include "cql3/query_processor.hh"

namespace cql3 {

namespace statements {

std::unique_ptr<prepared_statement> drop_function_statement::prepare(database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<drop_function_statement>(*this));
}

future<shared_ptr<cql_transport::event::schema_change>> drop_function_statement::announce_migration(
        query_processor& qp) const {
    if (!_func) {
        return make_ready_future<shared_ptr<cql_transport::event::schema_change>>();
    }
    auto user_func = dynamic_pointer_cast<functions::user_function>(_func);
    if (!user_func) {
        throw exceptions::invalid_request_exception(format("'{}' is not a user defined function", _func));
    }
    return qp.get_migration_manager().announce_function_drop(user_func).then([this] {
        return create_schema_change(*_func, false);
    });
}

drop_function_statement::drop_function_statement(functions::function_name name,
        std::vector<shared_ptr<cql3_type::raw>> arg_types, bool args_present, bool if_exists)
    : drop_function_statement_base(std::move(name), std::move(arg_types), args_present, if_exists) {}

}
}
