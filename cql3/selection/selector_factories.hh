/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <algorithm>
#include <stdexcept>
#include <vector>
#include "cql3/selection/selector.hh"
#include "schema.hh"
#include "query-request.hh"

namespace cql3 {

namespace selection {

class selectable;

/**
 * A set of <code>selector</code> factories.
 */
class selector_factories {
private:
    /**
     * The <code>Selector</code> factories.
     */
    std::vector<::shared_ptr<selector::factory>> _factories;

    /**
     * <code>true</code> if one of the factory creates writetime selectors.
     */
    bool _contains_write_time_factory;

    /**
     * <code>true</code> if one of the factory creates TTL selectors.
     */
    bool _contains_ttl_factory;

    /**
     * The number of factories creating simple selectors.
     */
    uint32_t _number_of_simple_factories;

    /**
     * The number of factories creating aggregates.
     */
    uint32_t _number_of_aggregate_factories;

    /**
     * The number of factories that are only for post processing.
     */
    uint32_t _number_of_factories_for_post_processing;

public:
    /**
     * Creates a new <code>SelectorFactories</code> instance and collect the column definitions.
     *
     * @param selectables the <code>Selectable</code>s for which the factories must be created
     * @param cfm the Column Family Definition
     * @param defs the collector parameter for the column definitions
     * @return a new <code>SelectorFactories</code> instance
     * @throws InvalidRequestException if a problem occurs while creating the factories
     */
    static ::shared_ptr<selector_factories> create_factories_and_collect_column_definitions(
                std::vector<::shared_ptr<selectable>> selectables,
                data_dictionary::database db, schema_ptr schema,
                std::vector<const column_definition*>& defs) {
        return ::make_shared<selector_factories>(std::move(selectables), db, std::move(schema), defs);
    }

    selector_factories(std::vector<::shared_ptr<selectable>> selectables,
            data_dictionary::database db, schema_ptr schema, std::vector<const column_definition*>& defs);
public:
    /**
     * Adds a new <code>Selector.Factory</code> for a column that is needed only for ORDER BY or post
     * processing purposes.
     * @param def the column that is needed for ordering
     * @param index the index of the column definition in the Selection's list of columns
     */
    void add_selector_for_post_processing(const column_definition& def, uint32_t index);

    /**
     * Checks if this <code>SelectorFactories</code> contains only factories for simple selectors.
     *
     * @return <code>true</code> if this <code>SelectorFactories</code> contains only factories for simple selectors,
     * <code>false</code> otherwise.
     */
    bool contains_only_simple_selection() const {
        auto size = _factories.size();
        return _number_of_simple_factories == (size - _number_of_factories_for_post_processing);
    }

    /**
     * Checks if this <code>SelectorFactories</code> contains only factories for aggregates.
     *
     * @return <code>true</code> if this <code>SelectorFactories</code> contains only factories for aggregates,
     * <code>false</code> otherwise.
     */
    bool contains_only_aggregate_functions() const {
        auto size = _factories.size();
        return size != 0 && _number_of_aggregate_factories  == (size - _number_of_factories_for_post_processing);
    }

    /**
     * Whether the selector built by this factory does aggregation or not (either directly or in a sub-selector).
     *
     * @return <code>true</code> if the selector built by this factor does aggregation, <code>false</code> otherwise.
     */
    bool does_aggregation() const {
        return _number_of_aggregate_factories > 0;
    }

    bool does_count() const {
        if (_factories.size() != 1) {
            return false;
        }
        return _factories[0]->is_count_selector_factory();
    }

    bool does_reduction() const {
        return std::all_of(_factories.cbegin(), _factories.cend(), [](const ::shared_ptr<selector::factory>& factory) {
            return factory->is_reducible_selector_factory() && factory->contains_only_simple_arguments();
        });
    }

    query::forward_request::reductions_info get_reductions() const {
        std::vector<query::forward_request::reduction_type> types;
        std::vector<query::forward_request::aggregation_info> infos;
        for (const auto& factory: _factories) {
            auto r = factory->get_reduction();
            if (!r) {
                throw std::runtime_error(format("Column {} doesn't have reduction type", factory->column_name()));
            }

            types.push_back(r->first);
            infos.push_back(r->second);
        }
        return {types, infos};
    }

    /**
     * Checks if this <code>SelectorFactories</code> contains at least one factory for writetime selectors.
     *
     * @return <code>true</code> if this <code>SelectorFactories</code> contains at least one factory for writetime
     * selectors, <code>false</code> otherwise.
     */
    bool contains_write_time_selector_factory() const {
        return _contains_write_time_factory;
    }

    /**
     * Checks if this <code>SelectorFactories</code> contains at least one factory for TTL selectors.
     *
     * @return <code>true</code> if this <code>SelectorFactories</code> contains at least one factory for TTL
     * selectors, <code>false</code> otherwise.
     */
    bool contains_ttl_selector_factory() const {
        return _contains_ttl_factory;
    }

    /**
     * Creates a list of new <code>selector</code> instances.
     * @return a list of new <code>selector</code> instances.
     */
    std::vector<::shared_ptr<selector>> new_instances() const;

    auto begin() const {
        return _factories.begin();
    }

    auto end() const {
        return _factories.end();
    }

    /**
     * Returns the names of the columns corresponding to the output values of the selector instances created by
     * these factories.
     *
     * @return a list of column names
     */
    std::vector<sstring> get_column_names() const;
};

}

}
