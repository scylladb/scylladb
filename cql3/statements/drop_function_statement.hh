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

#pragma once

#include "cql3/statements/function_statement.hh"

namespace cql3 {
class query_processor;
namespace statements {
class drop_function_statement final : public drop_function_statement_base {
    virtual std::unique_ptr<prepared_statement> prepare(database& db, cql_stats& stats) override;
    virtual future<shared_ptr<cql_transport::event::schema_change>> announce_migration(
            query_processor& qp) const override;

public:
    drop_function_statement(functions::function_name name, std::vector<shared_ptr<cql3_type::raw>> arg_types,
            bool args_present, bool if_exists);
};
}
}
