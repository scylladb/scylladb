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

#include "cql3/restrictions/primary_key_restrictions.hh"

namespace cql3 {

namespace restrictions {

/**
 * A <code>primary_key_restrictions</code> which forwards all its method calls to another
 * <code>primary_key_restrictions</code>. Subclasses should override one or more methods to modify the behavior
 * of the backing <code>primary_key_restrictions</code> as desired per the decorator pattern.
 */
template <typename ValueType>
class forwarding_primary_key_restrictions : public primary_key_restrictions<ValueType> {
    using bounds_range_type = typename primary_key_restrictions<ValueType>::bounds_range_type;
protected:
    /**
     * Returns the backing delegate instance that methods are forwarded to.
     */
    virtual ::shared_ptr<primary_key_restrictions<ValueType>> get_delegate() const = 0;

public:
    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override {
        return get_delegate()->uses_function(ks_name, function_name);
    }

    virtual std::vector<const column_definition*> get_column_defs() const override {
        return get_delegate()->get_column_defs();
    }

    virtual void merge_with(::shared_ptr<restriction> restriction) override {
        get_delegate()->merge_with(restriction);
    }

    virtual bool has_supporting_index(const secondary_index::secondary_index_manager& index_manager) const override {
        return get_delegate()->has_supporting_index(index_manager);
    }

    virtual std::vector<bytes_opt> values(const query_options& options) const override {
        return get_delegate()->values(options);
    }

    virtual std::vector<bytes_opt> bounds(statements::bound b, const query_options& options) const override {
        return get_delegate()->bounds(b, options);
    }

    virtual std::vector<ValueType> values_as_keys(const query_options& options) const override {
        return get_delegate()->values_as_keys(options);
    }

    virtual std::vector<bounds_range_type> bounds_ranges(const query_options& options) const override {
        return get_delegate()->bounds_ranges(options);
    }

    virtual bool is_on_token() const override {
        return get_delegate()->is_on_token();
    }

    virtual bool is_multi_column() const override {
        return get_delegate()->is_multi_column();
    }

    virtual bool is_slice() const override {
        return get_delegate()->is_slice();
    }

    virtual bool is_contains() const override {
        return get_delegate()->is_contains();
    }

    virtual bool is_IN() const override {
        return get_delegate()->is_IN();
    }

    virtual bool empty() const override {
        return get_delegate()->empty();
    }

    virtual uint32_t size() const override {
        return get_delegate()->size();
    }

#if 0
    virtual void addIndexExpressionTo(List<IndexExpression> expressions, QueryOptions options) {
        get_delegate()->addIndexExpressionTo(expressions, options);
    }
#endif

    sstring to_string() const override {
        return get_delegate()->to_string();
    }
};

}
}
