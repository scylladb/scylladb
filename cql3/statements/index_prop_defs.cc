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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#include <set>
#include "index_prop_defs.hh"
#include "db/index/secondary_index.hh"

void cql3::statements::index_prop_defs::validate() {
    static std::set<sstring> keywords({ sstring(KW_OPTIONS) });

    property_definitions::validate(keywords, std::set<sstring>());

    if (is_custom && !custom_class) {
        throw exceptions::invalid_request_exception("CUSTOM index requires specifiying the index class");
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
                sprint("Cannot specify %s as a CUSTOM option",
                        db::index::secondary_index::custom_index_option_name));
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
