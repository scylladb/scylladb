/*
 * Copyright (C) 2017 ScyllaDB
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

#include <experimental/string_view>
#include <functional>

#include <seastar/core/future.hh>

#include "seastarx.hh"
#include "stdx.hh"

namespace cql3 {
class query_processor;
class untyped_result_set_row;
}

namespace auth {

namespace meta {

namespace roles_table {

stdx::string_view creation_query();

constexpr stdx::string_view name{"roles", 5};

stdx::string_view qualified_name() noexcept;

constexpr stdx::string_view role_col_name{"role", 4};

}

}

///
/// Check that the default role satisfies a predicate, or `false` if the default role does not exist.
///
future<bool> default_role_row_satisfies(
        cql3::query_processor&,
        std::function<bool(const cql3::untyped_result_set_row&)>);

///
/// Check that any nondefault role satisfies a predicate. `false` if no nondefault roles exist.
///
future<bool> any_nondefault_role_row_satisfies(
        cql3::query_processor&,
        std::function<bool(const cql3::untyped_result_set_row&)>);

}
