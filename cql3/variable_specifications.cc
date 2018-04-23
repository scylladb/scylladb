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

#include "cql3/variable_specifications.hh"

namespace cql3 {

variable_specifications::variable_specifications(const std::vector<::shared_ptr<column_identifier>>& variable_names)
    : _variable_names{variable_names}
    , _specs{variable_names.size()}
    , _target_columns{variable_names.size()}
{ }

::shared_ptr<variable_specifications> variable_specifications::empty() {
    return ::make_shared<variable_specifications>(std::vector<::shared_ptr<column_identifier>>{});
}

size_t variable_specifications::size() const {
    return _variable_names.size();
}

std::vector<::shared_ptr<column_specification>> variable_specifications::get_specifications() const & {
    return std::vector<::shared_ptr<column_specification>>(_specs.begin(), _specs.end());
}

std::vector<::shared_ptr<column_specification>> variable_specifications::get_specifications() && {
    return std::move(_specs);
}

std::vector<uint16_t> variable_specifications::get_partition_key_bind_indexes(schema_ptr schema) const {
    auto count = schema->partition_key_columns().size();
    std::vector<uint16_t> partition_key_positions(count, uint16_t(0));
    std::vector<bool> set(count, false);
    for (size_t i = 0; i < _target_columns.size(); i++) {
        auto& target_column = _target_columns[i];
        const auto* cdef = target_column ? schema->get_column_definition(target_column->name->name()) : nullptr;
        if (cdef && cdef->is_partition_key()) {
            partition_key_positions[cdef->position()] = i;
            set[cdef->position()] = true;
        }
    }
    for (bool b : set) {
        if (!b) {
            return {};
        }
    }
    return partition_key_positions;
}

void variable_specifications::add(int32_t bind_index, ::shared_ptr<column_specification> spec) {
    _target_columns[bind_index] = spec;
    auto name = _variable_names[bind_index];
    // Use the user name, if there is one
    if (name) {
        spec = ::make_shared<column_specification>(spec->ks_name, spec->cf_name, name, spec->type);
    }
    _specs[bind_index] = spec;
}

}
