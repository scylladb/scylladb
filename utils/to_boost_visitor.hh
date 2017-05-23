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

template <typename ResultType, typename Visitor>
struct boost_visitor_adapter : Visitor {
    using result_type = ResultType;
    boost_visitor_adapter(Visitor&& v) : Visitor(std::move(v)) {}
};

// Boost 1.55 requires that visitors expose a `result_type` member. This
// function adds it.
template <typename ResultType = void, typename Visitor>
auto
to_boost_visitor(Visitor&& v) {
    return boost_visitor_adapter<ResultType, Visitor>(std::move(v));
}
