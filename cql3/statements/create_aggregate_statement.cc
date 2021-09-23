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

#include "cql3/statements/create_aggregate_statement.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/user_aggregate.hh"
#include "prepared_statement.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "database.hh"
#include "cql3/query_processor.hh"
#include "gms/feature_service.hh"

namespace cql3 {

namespace statements {

void create_aggregate_statement::create(service::storage_proxy& proxy, functions::function* old) const {
    if (!proxy.features().cluster_supports_user_defined_aggregates()) {
        throw exceptions::invalid_request_exception("Cluster does not support user-defined aggregates, upgrade the whole cluster in order to use UDA");
    }
    if (old && !dynamic_cast<functions::user_aggregate*>(old)) {
        throw exceptions::invalid_request_exception(format("Cannot replace '{}' which is not a user defined aggregate", *old));
    }
    data_type state_type = prepare_type(proxy, *_stype);

    auto&& db = proxy.get_db().local();
    std::vector<data_type> acc_types{state_type};
    acc_types.insert(acc_types.end(), _arg_types.begin(), _arg_types.end());
    auto state_func = dynamic_pointer_cast<functions::scalar_function>(functions::functions::find(functions::function_name{_name.keyspace, _sfunc}, acc_types));
    auto final_func = dynamic_pointer_cast<functions::scalar_function>(functions::functions::find(functions::function_name{_name.keyspace, _ffunc}, {state_type}));
 
    if (!state_func) {
        throw exceptions::invalid_request_exception(format("State function not found: {}", _sfunc));
    }
    if (!final_func) {
        throw exceptions::invalid_request_exception(format("Final function not found: {}", _ffunc));
    }

    auto dummy_ident = ::make_shared<column_identifier>("", true);
    auto column_spec = make_lw_shared<column_specification>("", "", dummy_ident, state_type);
    auto initcond_term = prepare_term(_ival, db, _name.keyspace, {column_spec});
    bytes_opt initcond = to_bytes(*to_managed_bytes_opt(expr::evaluate_to_raw_view(initcond_term, cql3::query_options::DEFAULT)));

    _aggregate = ::make_shared<functions::user_aggregate>(_name, initcond, std::move(state_func), std::move(final_func));
    return;
}

std::unique_ptr<prepared_statement> create_aggregate_statement::prepare(database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<create_aggregate_statement>(*this));
}

future<shared_ptr<cql_transport::event::schema_change>> create_aggregate_statement::announce_migration(
        query_processor& qp) const {
    if (!_aggregate) {
        return make_ready_future<::shared_ptr<cql_transport::event::schema_change>>();
    }
    return qp.get_migration_manager().announce_new_aggregate(_aggregate).then([this] {
        return create_schema_change(*_aggregate, true);
    });
}

create_aggregate_statement::create_aggregate_statement(functions::function_name name, std::vector<shared_ptr<cql3_type::raw>> arg_types,
            sstring sfunc, shared_ptr<cql3_type::raw> stype, sstring ffunc, expr::expression ival, bool or_replace, bool if_not_exists)
        : create_function_statement_base(std::move(name), std::move(arg_types), or_replace, if_not_exists)
        , _sfunc(std::move(sfunc))
        , _stype(std::move(stype))
        , _ffunc(std::move(ffunc))
        , _ival(std::move(ival))
    {}
}
}
