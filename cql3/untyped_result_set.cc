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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */
#include <algorithm>
#include <iterator>
#include <utility>
#include <stdexcept>
#include "untyped_result_set.hh"
#include "result_set.hh"
#include "transport/messages/result_message.hh"

cql3::untyped_result_set::row::row(const std::unordered_map<sstring, bytes_opt>& data)
    : _data(data)
{}

cql3::untyped_result_set::row::row(const std::vector<::shared_ptr<column_specification>>& columns, std::vector<bytes_opt> data)
: _columns(columns)
, _data([&columns, data = std::move(data)] () mutable {
    std::unordered_map<sstring, bytes_opt> tmp;
    std::transform(columns.begin(), columns.end(), data.begin(), std::inserter(tmp, tmp.end()), [](::shared_ptr<column_specification> c, bytes_opt& d) {
       return std::make_pair<sstring, bytes_opt>(c->name->to_string(), std::move(d));
    });
    return tmp;
}())
{}

bool cql3::untyped_result_set::row::has(const sstring& name) const {
    auto i = _data.find(name);
    return i != _data.end() && i->second;
}

using transport::messages::result_message;

cql3::untyped_result_set::untyped_result_set(::shared_ptr<result_message> msg)
    : _rows([msg]{
    class visitor : public result_message::visitor_base {
    public:
        rows_type rows;
        void visit(const result_message::rows& rmrs) override {
            auto& rs = rmrs.rs();
            auto& cn = rs.get_metadata().get_names();
            for (auto& r : rs.rows()) {
                rows.emplace_back(cn, r);
            }
        }
    };
    visitor v;
    if (msg != nullptr) {
        msg->accept(v);
    }
    return std::move(v.rows);
}())
{}

const cql3::untyped_result_set::row& cql3::untyped_result_set::one() const {
    if (_rows.size() != 1) {
        throw std::runtime_error("One row required, " + std::to_string(_rows.size()) + " found");
    }
    return at(0);
}
