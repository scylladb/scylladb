/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2017 ScyllaDB
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

#include "index/secondary_index_manager.hh"

#include "cql3/statements/index_target.hh"
#include "index/target_parser.hh"
#include "db/query_context.hh"
#include "schema_builder.hh"
#include "database.hh"

#include <boost/range/adaptor/map.hpp>

namespace secondary_index {

index::index(const sstring& target_column, const index_metadata& im)
    : _target_column{target_column}
    , _im{im}
{}

bool index::depends_on(const column_definition& cdef) const {
    return cdef.name_as_text() == _target_column;
}

bool index::supports_expression(const column_definition& cdef, const cql3::operator_type& op) const {
    return cdef.name_as_text() == _target_column && op == cql3::operator_type::EQ;
}

const index_metadata& index::metadata() const {
    return _im;
}

secondary_index_manager::secondary_index_manager(column_family& cf)
    : _cf{cf}
{}

void secondary_index_manager::reload() {
    const auto& table_indices = _cf.schema()->all_indices();
    auto it = _indices.begin();
    while (it != _indices.end()) {
        auto index_name = it->first;
        if (table_indices.count(index_name) == 0) {
            it = _indices.erase(it);
        } else {
            ++it;
        }
    }
    for (const auto& index : _cf.schema()->all_indices()) {
        add_index(index.second);
    }
}

void secondary_index_manager::add_index(const index_metadata& im) {
    sstring index_target_name = im.options().at(cql3::statements::index_target::target_option_name);
    _indices.emplace(im.name(), index{index_target_name, im});
}

view_ptr secondary_index_manager::create_view_for_index(const index_metadata& im) const {
    auto schema = _cf.schema();
    sstring index_table_name = sprint("%s_index", im.name());
    sstring index_target_name = im.options().at(cql3::statements::index_target::target_option_name);
    schema_builder builder{schema->ks_name(), index_table_name};
    auto target = target_parser::parse(schema, im);
    const auto* index_target = std::get<const column_definition*>(target);
    auto target_type = std::get<cql3::statements::index_target::target_type>(target);
    if (target_type != cql3::statements::index_target::target_type::values) {
        throw std::runtime_error(sprint("Unsupported index target type: %s", to_sstring(target_type)));
    }
    builder.with_column(index_target->name(), index_target->type, column_kind::partition_key);
    for (auto& col : schema->partition_key_columns()) {
        builder.with_column(col.name(), col.type, column_kind::clustering_key);
    }
    for (auto& col : schema->clustering_key_columns()) {
        if (col == *index_target) {
            continue;
        }
        builder.with_column(col.name(), col.type, column_kind::clustering_key);
    }
    const sstring where_clause = sprint("%s IS NOT NULL", index_target_name);
    builder.with_view_info(*schema, false, where_clause);
    return view_ptr{builder.build()};
}

std::vector<index_metadata> secondary_index_manager::get_dependent_indices(const column_definition& cdef) const {
    return boost::copy_range<std::vector<index_metadata>>(_indices
           | boost::adaptors::map_values
           | boost::adaptors::filtered([&] (auto& index) { return index.depends_on(cdef); })
           | boost::adaptors::transformed([&] (auto& index) { return index.metadata(); }));
}

std::vector<index> secondary_index_manager::list_indexes() const {
    return boost::copy_range<std::vector<index>>(_indices | boost::adaptors::map_values);
}
}
