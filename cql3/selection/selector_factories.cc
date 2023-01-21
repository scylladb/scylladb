/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "cql3/selection/selector_factories.hh"
#include "cql3/selection/simple_selector.hh"
#include "cql3/selection/selectable.hh"
#include "cql3/query_options.hh"

namespace cql3 {

namespace selection {

selector_factories::selector_factories(std::vector<::shared_ptr<selectable>> selectables,
        data_dictionary::database db, schema_ptr schema,
        std::vector<const column_definition*>& defs)
    : _contains_write_time_factory(false)
    , _contains_ttl_factory(false)
    , _number_of_simple_factories(0)
    , _number_of_aggregate_factories(0)
    , _number_of_factories_for_post_processing(0)
{
    _factories.reserve(selectables.size());

    for (auto&& selectable : selectables) {
        auto factory = selectable->new_selector_factory(db, schema, defs);
        _contains_write_time_factory |= factory->is_write_time_selector_factory();
        _contains_ttl_factory |= factory->is_ttl_selector_factory();
        if (factory->is_aggregate_selector_factory()) {
            ++_number_of_aggregate_factories;
        } else if (factory->is_simple_selector_factory()) {
            ++_number_of_simple_factories;
        }
        _factories.emplace_back(std::move(factory));
    }
}

void selector_factories::add_selector_for_post_processing(const column_definition& def, uint32_t index) {
    _factories.emplace_back(simple_selector::new_factory(def.name_as_text(), index, def.type));
    ++_number_of_factories_for_post_processing;
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
