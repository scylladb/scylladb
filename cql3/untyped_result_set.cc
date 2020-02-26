/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
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
#include <algorithm>
#include <iterator>
#include <utility>
#include <stdexcept>
#include "untyped_result_set.hh"
#include "result_set.hh"
#include "transport/messages/result_message.hh"

cql3::untyped_result_set_row::untyped_result_set_row(const map_t& data)
    : _data(data)
{}

cql3::untyped_result_set_row::untyped_result_set_row(const std::vector<lw_shared_ptr<column_specification>>& columns, std::vector<bytes_opt> data)
: _columns(columns)
, _data([&columns, data = std::move(data)] () mutable {
    map_t tmp;
    std::transform(columns.begin(), columns.end(), data.begin(), std::inserter(tmp, tmp.end()), [](lw_shared_ptr<column_specification> c, bytes_opt& d) {
       return std::make_pair<sstring, bytes_opt>(c->name->to_string(), std::move(d));
    });
    return tmp;
}())
{}

bool cql3::untyped_result_set_row::has(std::string_view name) const {
    auto i = _data.find(name);
    return i != _data.end() && i->second;
}

using cql_transport::messages::result_message;

cql3::untyped_result_set::untyped_result_set(const cql3::result_set& rs) {
    auto& cn = rs.get_metadata().get_names();
    for (auto& r : rs.rows()) {
        // r is const ref. TODO: make this more efficient by either wrapping result set
        // or adding modifying accessors to it.
        _rows.emplace_back(cn, r);
    }
}

cql3::untyped_result_set::untyped_result_set(::shared_ptr<result_message> msg)
    : _rows([msg]{
    class visitor : public result_message::visitor_base {
    public:
        std::optional<untyped_result_set> res;
        void visit(const result_message::rows& rmrs) override {
            const auto& rs = rmrs.rs();
            const auto& set = rs.result_set();
            res.emplace(set); // construct untyped_result_set by const ref.
        }
    };
    visitor v;
    if (msg != nullptr) {
        msg->accept(v);
    }
    if (v.res) {
        return std::move(v.res->_rows);
    }
    return rows_type{};
}())
{}

const cql3::untyped_result_set_row& cql3::untyped_result_set::one() const {
    if (_rows.size() != 1) {
        throw std::runtime_error("One row required, " + std::to_string(_rows.size()) + " found");
    }
    return at(0);
}
