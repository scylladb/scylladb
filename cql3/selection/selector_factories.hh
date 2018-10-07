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

#pragma once

#include <vector>
#include "cql3/selection/selector.hh"
#include "cql3/selection/selectable.hh"

namespace cql3 {

namespace selection {

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
                database& db, schema_ptr schema,
                std::vector<const column_definition*>& defs) {
        return ::make_shared<selector_factories>(std::move(selectables), db, std::move(schema), defs);
    }

    selector_factories(std::vector<::shared_ptr<selectable>> selectables,
            database& db, schema_ptr schema, std::vector<const column_definition*>& defs);
public:
    bool uses_function(const sstring& ks_name, const sstring& function_name) const;

    /**
     * Adds a new <code>Selector.Factory</code> for a column that is needed only for ORDER BY or post
     * processing purposes.
     * @param def the column that is needed for ordering
     * @param index the index of the column definition in the Selection's list of columns
     */
    void add_selector_for_post_processing(const column_definition& def, uint32_t index);

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
