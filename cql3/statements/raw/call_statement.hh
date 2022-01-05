/*
 * Copyright (C) 2020 ScyllaDB
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

#include "cql3/statements/raw/cf_statement.hh"
#include "cql3/column_identifier.hh"
#include "cql3/cql_statement.hh"
#include "cql3/attributes.hh"

namespace cql3::statements {

class call_statement;

} // end of namespace cql3::statements

namespace cql3::statements::raw {

class call_statement : public cf_statement {
private:
    std::unordered_map<sstring, sstring> _params;

public:
    call_statement(cf_name name, std::unordered_map<sstring, sstring> params);

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
};

} // end of namespace cql3::statements::raw
