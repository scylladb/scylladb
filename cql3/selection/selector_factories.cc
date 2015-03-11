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

#include "cql3/selection/selector_factories.hh"
#include "cql3/selection/simple_selector.hh"
#include "cql3/selection/selectable.hh"

namespace cql3 {

namespace selection {

selector_factories::selector_factories(std::vector<::shared_ptr<selectable>> selectables, schema_ptr schema,
        std::vector<const column_definition*>& defs)
    : _contains_write_time_factory(false)
    , _contains_ttl_factory(false)
    , _number_of_aggregate_factories(0)
{
    _factories.reserve(selectables.size());

    for (auto&& selectable : selectables) {
        auto factory = selectable->new_selector_factory(schema, defs);
        _contains_write_time_factory |= factory->is_write_time_selector_factory();
        _contains_ttl_factory |= factory->is_ttl_selector_factory();
        if (factory->is_aggregate_selector_factory()) {
            ++_number_of_aggregate_factories;
        }
        _factories.emplace_back(std::move(factory));
    }
}

bool selector_factories::uses_function(const sstring& ks_name, const sstring& function_name) const {
    for (auto&& f : _factories) {
        if (f && f->uses_function(ks_name, function_name)) {
            return true;
        }
    }
    return false;
}

void selector_factories::add_selector_for_ordering(const column_definition& def, uint32_t index) {
    _factories.emplace_back(simple_selector::new_factory(def.name_as_text(), index, def.type));
}

std::vector<::shared_ptr<selector>> selector_factories::new_instances() const {
    std::vector<::shared_ptr<selector>> r;
    r.reserve(_factories.size());
    for (auto&& f : _factories) {
        r.emplace_back(f->new_instance());
    }
    return r;
}

std::vector<sstring> selector_factories::get_column_names() const {
    std::vector<sstring> r;
    r.reserve(_factories.size());
    std::transform(_factories.begin(), _factories.end(), std::back_inserter(r), [] (auto&& f) {
        return f->column_name();
    });
    return r;
}

}

}
