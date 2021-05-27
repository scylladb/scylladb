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
#include "cql3/statements/index_target.hh"

#include <regex>
#include <boost/range/algorithm/find_if.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptors.hpp>

#include "utils/rjson.hh"
#include "log.hh"

const sstring db::index::secondary_index::custom_index_option_name = "class_name";
const sstring db::index::secondary_index::index_keys_option_name = "index_keys";
const sstring db::index::secondary_index::index_values_option_name = "index_values";
const sstring db::index::secondary_index::index_entries_option_name = "index_keys_and_values";

namespace secondary_index {

static constexpr auto PK_TARGET_KEY = "pk";
static constexpr auto CK_TARGET_KEY = "ck";

static const std::regex target_regex("^(keys|entries|values|full)\\((.+)\\)$");

target_parser::target_info target_parser::parse(schema_ptr schema, const index_metadata& im) {
    sstring target = im.options().at(cql3::statements::index_target::target_option_name);
    try {
        return parse(schema, target);
    } catch (...) {
        throw exceptions::configuration_exception(format("Unable to parse targets for index {} ({}): {}", im.name(), target, std::current_exception()));
    }
}

target_parser::target_info target_parser::parse(schema_ptr schema, const sstring& target) {
    using namespace cql3::statements;
    target_info info;

    auto get_column = [&schema] (const sstring& name) -> const column_definition* {
        const column_definition* cdef = schema->get_column_definition(utf8_type->decompose(name));
        if (!cdef) {
            throw std::runtime_error(format("Column {} not found", name));
        }
        return cdef;
    };

    std::cmatch match;
    if (std::regex_match(target.data(), match, target_regex)) {
        info.type = index_target::from_sstring(match[1].str());
        info.pk_columns.push_back(get_column(sstring(match[2].str())));
        return info;
    }

    std::optional<rjson::value> json_value = rjson::try_parse(target);
    if (json_value && json_value->IsObject()) {
        rjson::value* pk = rjson::find(*json_value, PK_TARGET_KEY);
        rjson::value* ck = rjson::find(*json_value, CK_TARGET_KEY);
        if (!pk || !ck || !pk->IsArray() || !ck->IsArray()) {
            throw std::runtime_error("pk and ck fields of JSON definition must be arrays");
        }
        for (const rjson::value& v : pk->GetArray()) {
            info.pk_columns.push_back(get_column(sstring(rjson::to_string_view(v))));
        }
        for (const rjson::value& v : ck->GetArray()) {
            info.ck_columns.push_back(get_column(sstring(rjson::to_string_view(v))));
        }
        info.type = index_target::target_type::values;
        return info;
    }

    // Fallback and treat the whole string as a single target
    return target_info{{get_column(target)}, {}, index_target::target_type::values};
}

bool target_parser::is_local(sstring target_string) {
    std::optional<rjson::value> json_value = rjson::try_parse(target_string);
    if (!json_value) {
        return false;
    }
    rjson::value* pk = rjson::find(*json_value, PK_TARGET_KEY);
    rjson::value* ck = rjson::find(*json_value, CK_TARGET_KEY);
    return pk && ck && pk->IsArray() && ck->IsArray() && !pk->Empty() && !ck->Empty();
}

sstring target_parser::get_target_column_name_from_string(const sstring& targets) {
    std::optional<rjson::value> json_value = rjson::try_parse(targets);
    if (!json_value) {
        return targets;
    }

    rjson::value* pk = rjson::find(*json_value, "pk");
    rjson::value* ck = rjson::find(*json_value, "ck");
    if (ck && ck->IsArray() && !ck->Empty()) {
        return sstring(rjson::to_string_view(ck->GetArray()[0]));
    }
    if (pk && pk->IsArray() && !pk->Empty()) {
        return sstring(rjson::to_string_view(pk->GetArray()[0]));
    }
    return targets;
}

sstring target_parser::serialize_targets(const std::vector<::shared_ptr<cql3::statements::index_target>>& targets) {
    using cql3::statements::index_target;

    struct as_json_visitor {
        rjson::value operator()(const index_target::multiple_columns& columns) const {
            rjson::value json_array = rjson::empty_array();
            for (const auto& column : columns) {
                rjson::push_back(json_array, rjson::from_string(column->to_string()));
            }
            return json_array;
        }

        rjson::value operator()(const index_target::single_column& column) const {
            return rjson::from_string(column->to_string());
        }
    };

    if (targets.size() == 1 && std::holds_alternative<index_target::single_column>(targets.front()->value)) {
        return std::get<index_target::single_column>(targets.front()->value)->to_string();
    }

    rjson::value json_map = rjson::empty_object();
    rjson::value pk_json = std::visit(as_json_visitor(), targets.front()->value);
    if (!pk_json.IsArray()) {
        rjson::value pk_array = rjson::empty_array();
        rjson::push_back(pk_array, std::move(pk_json));
        pk_json = std::move(pk_array);
    }
    rjson::set_with_string_name(json_map, PK_TARGET_KEY, std::move(pk_json));
    if (targets.size() > 1) {
        rjson::value ck_json = rjson::empty_array();
        for (unsigned i = 1; i < targets.size(); ++i) {
            rjson::push_back(ck_json, std::visit(as_json_visitor(), targets.at(i)->value));
        }
        rjson::set_with_string_name(json_map, CK_TARGET_KEY, std::move(ck_json));
    }
    return rjson::print(json_map);
}

}
