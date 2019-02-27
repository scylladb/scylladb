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

#include "secondary_index.hh"
#include "index/target_parser.hh"

#include <regex>

const sstring db::index::secondary_index::custom_index_option_name = "class_name";
const sstring db::index::secondary_index::index_keys_option_name = "index_keys";
const sstring db::index::secondary_index::index_values_option_name = "index_values";
const sstring db::index::secondary_index::index_entries_option_name = "index_keys_and_values";

namespace secondary_index {

static const std::regex target_regex("^(keys|entries|values|full)\\((.+)\\)$");

std::pair<const column_definition*, cql3::statements::index_target::target_type>
target_parser::parse(schema_ptr schema, const index_metadata& im)
{
    sstring target = im.options().at(cql3::statements::index_target::target_option_name);
    auto result = parse(schema, target);
    if (!result) {
        throw exceptions::configuration_exception(format("Unable to parse targets for index {} ({})", im.name(), target));
    }
    return *result;
}

std::optional<std::pair<const column_definition*, cql3::statements::index_target::target_type>>
target_parser::parse(schema_ptr schema, const sstring& target)
{
    using namespace cql3::statements;
    // if the regex matches then the target is in the form "keys(foo)", "entries(bar)" etc
    // if not, then it must be a simple column name and implictly its type is VALUES
    sstring column_name;
    index_target::target_type target_type;

    std::cmatch match;
    if (std::regex_match(target.data(), match, target_regex)) {
        target_type = index_target::from_sstring(match[1].str());
        column_name = match[2].str();
    } else {
        column_name = target;
        target_type = index_target::target_type::values;
    }

    auto column = schema->get_column_definition(utf8_type->decompose(column_name));
    if (!column) {
        return std::nullopt;
    }
    return std::make_pair(column, target_type);
}

}
