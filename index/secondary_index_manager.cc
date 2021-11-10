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
 * Copyright (C) 2017-present ScyllaDB
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
#include "cql3/util.hh"
#include "cql3/expr/expression.hh"
#include "index/target_parser.hh"
#include "db/query_context.hh"
#include "schema_builder.hh"
#include "database.hh"
#include "db/view/view.hh"

#include <boost/range/adaptor/map.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/algorithm/string/predicate.hpp>

namespace secondary_index {

index::index(const sstring& target_column, const index_metadata& im)
    : _target_column{target_column}
    , _im{im}
{}

bool index::depends_on(const column_definition& cdef) const {
    return cdef.name_as_text() == _target_column;
}

bool index::supports_expression(const column_definition& cdef, const cql3::expr::oper_t op) const {
    return cdef.name_as_text() == _target_column && op == cql3::expr::oper_t::EQ;
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
        if (!table_indices.contains(index_name)) {
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
    sstring index_target = im.options().at(cql3::statements::index_target::target_option_name);
    sstring index_target_name = target_parser::get_target_column_name_from_string(index_target);
    _indices.emplace(im.name(), index{index_target_name, im});
}

sstring index_table_name(const sstring& index_name) {
    return format("{}_index", index_name);
}

sstring index_name_from_table_name(const sstring& table_name) {
    if (table_name.size() < 7 || !boost::algorithm::ends_with(table_name, "_index")) {
        throw std::runtime_error(format("Table {} does not have _index suffix", table_name));
    }
    return table_name.substr(0, table_name.size() - 6); // remove the _index suffix from an index name;
}

static bytes get_available_token_column_name(const schema& schema) {
    bytes base_name = "idx_token";
    bytes accepted_name = base_name;
    int i = 0;
    while (schema.get_column_definition(accepted_name)) {
        accepted_name = base_name + to_bytes("_")+ to_bytes(std::to_string(++i));
    }
    return accepted_name;
}

view_ptr secondary_index_manager::create_view_for_index(const index_metadata& im, bool new_token_column_computation) const {
    auto schema = _cf.schema();
    sstring index_target_name = im.options().at(cql3::statements::index_target::target_option_name);
    schema_builder builder{schema->registry(), schema->ks_name(), index_table_name(im.name())};
    auto target_info = target_parser::parse(schema, im);
    const auto* index_target = im.local() ? target_info.ck_columns.front() : target_info.pk_columns.front();
    auto target_type = target_info.type;
    if (target_type != cql3::statements::index_target::target_type::values) {
        throw std::runtime_error(format("Unsupported index target type: {}", to_sstring(target_type)));
    }

    // For local indexing, start with base partition key
    if (im.local()) {
        if (index_target->is_partition_key()) {
            throw exceptions::invalid_request_exception("Local indexing based on partition key column is not allowed,"
                    " since whole base partition key must be used in queries anyway. Use global indexing instead.");
        }
        for (auto& col : schema->partition_key_columns()) {
            builder.with_column(col.name(), col.type, column_kind::partition_key);
        }
        builder.with_column(index_target->name(), index_target->type, column_kind::clustering_key);
    } else {
        builder.with_column(index_target->name(), index_target->type, column_kind::partition_key);
        // Additional token column is added to ensure token order on secondary index queries
        bytes token_column_name = get_available_token_column_name(*schema);
        if (new_token_column_computation) {
            builder.with_computed_column(token_column_name, long_type, column_kind::clustering_key, std::make_unique<token_column_computation>());
        } else {
            // FIXME(pgrabowski): this legacy code is here for backward compatibility and should be removed
            // once "supports_correct_idx_token_in_secondary_index" is supported by every node
            builder.with_computed_column(token_column_name, bytes_type, column_kind::clustering_key, std::make_unique<legacy_token_column_computation>());            
        }
        for (auto& col : schema->partition_key_columns()) {
            if (col == *index_target) {
                continue;
            }
            builder.with_column(col.name(), col.type, column_kind::clustering_key);
        }
    }

    for (auto& col : schema->clustering_key_columns()) {
        if (col == *index_target) {
            continue;
        }
        builder.with_column(col.name(), col.type, column_kind::clustering_key);
    }
    if (index_target->is_primary_key()) {
        for (auto& def : schema->regular_columns()) {
            db::view::create_virtual_column(builder, def.name(), def.type);
        }
    }
    const sstring where_clause = format("{} IS NOT NULL", index_target->name_as_cql_string());
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

bool secondary_index_manager::is_index(view_ptr view) const {
    return is_index(*view);
}

bool secondary_index_manager::is_index(const schema& s) const {
    return boost::algorithm::any_of(_indices | boost::adaptors::map_values, [&s] (const index& i) {
        return s.cf_name() == index_table_name(i.metadata().name());
    });
}

bool secondary_index_manager::is_global_index(const schema& s) const {
    return boost::algorithm::any_of(_indices | boost::adaptors::map_values, [&s] (const index& i) {
        return !i.metadata().local() && s.cf_name() == index_table_name(i.metadata().name());
    });
}

}
