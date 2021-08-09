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

#pragma once

#include "cql3/statements/function_statement.hh"
#include "cql3/cql3_type.hh"
#include "cql3/expr/expression.hh"

namespace cql3 {

class query_processor;

namespace functions {
    class user_aggregate;
}

namespace statements {

class create_aggregate_statement final : public create_function_statement_base {
    virtual std::unique_ptr<prepared_statement> prepare(database& db, cql_stats& stats) override;
    virtual future<shared_ptr<cql_transport::event::schema_change>> announce_migration(
            query_processor& qp) const override;
    virtual void create(service::storage_proxy& proxy, functions::function* old) const override;

    sstring _sfunc;
    shared_ptr<cql3_type::raw> _stype;
    sstring _ffunc;
    expr::expression _ival;

    mutable shared_ptr<functions::user_aggregate> _aggregate{};

public:
    create_aggregate_statement(functions::function_name name, std::vector<shared_ptr<cql3_type::raw>> arg_types,
            sstring sfunc, shared_ptr<cql3_type::raw> stype, sstring ffunc, expr::expression ival, bool or_replace, bool if_not_exists);
};
}
}
