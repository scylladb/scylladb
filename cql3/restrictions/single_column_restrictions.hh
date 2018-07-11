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

#include "cql3/restrictions/restrictions.hh"
#include "cql3/restrictions/single_column_restriction.hh"
#include "schema.hh"
#include "types.hh"

namespace cql3 {

namespace restrictions {

/**
 * Sets of single column _restrictions.
 */
class single_column_restrictions : public restrictions {
private:
    /**
     * The comparator used to sort the <code>restriction</code>s.
     */
    struct column_definition_comparator {
        schema_ptr _schema;
        bool operator()(const column_definition* def1, const column_definition* def2) const {
            auto pos1 = _schema->position(*def1);
            auto pos2 = _schema->position(*def2);
            if (pos1 != pos2) {
                return pos1 < pos2;
            }
            // FIXME: shouldn't we use regular column name comparator here? Origin does not...
            return less_unsigned(def1->name(), def2->name());
        }
    };

    /**
     * The _restrictions per column.
     */
public:
    using restrictions_map = std::map<const column_definition*, ::shared_ptr<single_column_restriction>, column_definition_comparator>;
private:
    restrictions_map _restrictions;
    bool _is_all_eq = true;
public:
    single_column_restrictions(schema_ptr schema)
        : _restrictions(column_definition_comparator{std::move(schema)})
    { }

#if 0
    @Override
    public final void addIndexExpressionTo(List<IndexExpression> expressions,
                                           QueryOptions options) throws InvalidRequestException
    {
        for (Restriction restriction : _restrictions.values())
            restriction.addIndexExpressionTo(expressions, options);
    }
#endif

    virtual std::vector<const column_definition*> get_column_defs() const override {
        std::vector<const column_definition*> r;
        for (auto&& e : _restrictions) {
            r.push_back(e.first);
        }
        return r;
    }

    virtual std::vector<bytes_opt> values(const query_options& options) const override {
        std::vector<bytes_opt> r;
        for (auto&& e : _restrictions) {
            auto&& value = e.second->value(options);
            r.emplace_back(value);
        }
        return r;
    }

    virtual bytes_opt value_for(const column_definition& cdef, const query_options& options) const override {
        auto it = _restrictions.find(std::addressof(cdef));
        return (it != _restrictions.end()) ? it->second->value(options) : bytes_opt{};
    }

    /**
     * Returns the restriction associated to the specified column.
     *
     * @param column_def the column definition
     * @return the restriction associated to the specified column
     */
    ::shared_ptr<restriction> get_restriction(const column_definition& column_def) const {
        auto i = _restrictions.find(&column_def);
        if (i == _restrictions.end()) {
            return {};
        }
        return i->second;
    }

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override {
        for (auto&& e : _restrictions) {
            if (e.second->uses_function(ks_name, function_name)) {
                return true;
            }
        }
        return false;
    }

    virtual bool empty() const override {
        return _restrictions.empty();
    }

    virtual uint32_t size() const override {
        return _restrictions.size();
    }

    /**
     * Adds the specified restriction to this set of _restrictions.
     *
     * @param restriction the restriction to add
     * @throws InvalidRequestException if the new restriction cannot be added
     */
    void add_restriction(::shared_ptr<single_column_restriction> restriction) {
        _is_all_eq &= restriction->is_EQ();

        auto i = _restrictions.find(&restriction->get_column_def());
        if (i == _restrictions.end()) {
            _restrictions.emplace_hint(i, &restriction->get_column_def(), std::move(restriction));
        } else {
            i->second->merge_with(restriction);
        }
    }

    virtual bool has_supporting_index(const secondary_index::secondary_index_manager& index_manager) const override {
        for (auto&& e : _restrictions) {
            if (e.second->has_supporting_index(index_manager)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the column after the specified one.
     *
     * @param column_def the column for which the next one need to be found
     * @return the column after the specified one.
     */
    const column_definition* next_column(const column_definition& column_def) const {
        auto i = _restrictions.find(&column_def);
        if (i == _restrictions.end()) {
            return nullptr;
        }
        ++i;
        if (i == _restrictions.end()) {
            return nullptr;
        }
        return i->first;
    }

    /**
     * Returns the definition of the last column.
     *
     * @return the definition of the last column.
     */
    const column_definition* last_column() const {
        if (_restrictions.empty()) {
            return nullptr;
        }
        auto i = _restrictions.end();
        --i;
        return i->first;
    }

    /**
     * Returns the last restriction.
     *
     * @return the last restriction.
     */
    ::shared_ptr<restriction> last_restriction() const {
        if (_restrictions.empty()) {
            return {};
        }
        auto i = _restrictions.end();
        --i;
        return i->second;
    }

    const restrictions_map& restrictions() const {
        return _restrictions;
    }

    /**
     * Checks if the _restrictions contains multiple contains, contains key, or map[key] = value.
     *
     * @return <code>true</code> if the _restrictions contains multiple contains, contains key, or ,
     * map[key] = value; <code>false</code> otherwise
     */
    bool has_multiple_contains() const {
        uint32_t number_of_contains = 0;
        for (auto&& e : _restrictions) {
            if (e.second->is_contains()) {
                auto contains_ = static_pointer_cast<single_column_restriction::contains>(e.second);
                number_of_contains += contains_->number_of_values();
                number_of_contains += contains_->number_of_keys();
                number_of_contains += contains_->number_of_entries();
            }
        }
        return number_of_contains > 1;
    }

    bool is_all_eq() const {
        return _is_all_eq;
    }
};

}
}
