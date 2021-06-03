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
 * Copyright (C) 2015-present ScyllaDB
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

#include <set>
#include "index_prop_defs.hh"
#include "index/secondary_index.hh"
#include "exceptions/exceptions.hh"

void cql3::statements::index_prop_defs::validate() {
    static std::set<sstring> keywords({ sstring(KW_OPTIONS) });

    property_definitions::validate(keywords);

    if (is_custom && !custom_class) {
        throw exceptions::invalid_request_exception("CUSTOM index requires specifying the index class");
    }

    if (!is_custom && custom_class) {
        throw exceptions::invalid_request_exception("Cannot specify index class for a non-CUSTOM index");
    }
    if (!is_custom && !_properties.empty()) {
        throw exceptions::invalid_request_exception("Cannot specify options for a non-CUSTOM index");
    }
    if (get_raw_options().count(
            db::index::secondary_index::custom_index_option_name)) {
        throw exceptions::invalid_request_exception(
                format("Cannot specify {} as a CUSTOM option",
                        db::index::secondary_index::custom_index_option_name));
    }

    // Currently, Scylla does not support *any* class of custom index
    // implementation. If in the future we do (e.g., SASI, or something
    // new), we'll need to check for valid values here.
    if (is_custom && custom_class) {
        throw exceptions::invalid_request_exception(
                format("Unsupported CUSTOM INDEX class {}. Note that currently, Scylla does not support SASI or any other CUSTOM INDEX class.",
                        *custom_class));

    }
}

index_options_map
cql3::statements::index_prop_defs::get_raw_options() {
    auto options = get_map(KW_OPTIONS);
    return !options ? std::unordered_map<sstring, sstring>() : std::unordered_map<sstring, sstring>(options->begin(), options->end());
}

index_options_map
cql3::statements::index_prop_defs::get_options() {
    auto options = get_raw_options();
    options.emplace(db::index::secondary_index::custom_index_option_name, *custom_class);
    return options;
}
