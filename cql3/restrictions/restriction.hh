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

#include "index/secondary_index_manager.hh"
#include "cql3/query_options.hh"
#include "cql3/statements/bound.hh"
#include "types.hh"

namespace cql3 {

namespace restrictions {

/**
 * A restriction/clause on a column.
 * The goal of this class being to group all conditions for a column in a SELECT.
 */
class restriction {
public:
    virtual ~restriction() {}
    virtual bool is_on_token() const = 0;
    virtual bool is_slice() const = 0;
    virtual bool is_EQ() const = 0;
    virtual bool is_IN() const = 0;
    virtual bool is_contains() const = 0;
    virtual bool is_multi_column() const = 0;

    virtual std::vector<bytes_opt> values(const query_options& options) const = 0;

    virtual bytes_opt value(const query_options& options) const {
        auto vec = values(options);
        assert(vec.size() == 1);
        return std::move(vec[0]);
    }

    /**
     * Returns <code>true</code> if one of the restrictions use the specified function.
     *
     * @param ks_name the keyspace name
     * @param function_name the function name
     * @return <code>true</code> if one of the restrictions use the specified function, <code>false</code> otherwise.
     */
    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const = 0;

    /**
     * Checks if the specified bound is set or not.
     * @param b the bound type
     * @return <code>true</code> if the specified bound is set, <code>false</code> otherwise
     */
    virtual bool has_bound(statements::bound b) const = 0;

    virtual std::vector<bytes_opt> bounds(statements::bound b, const query_options& options) const = 0;

    /**
     * Checks if the specified bound is inclusive or not.
     * @param b the bound type
     * @return <code>true</code> if the specified bound is inclusive, <code>false</code> otherwise
     */
    virtual bool is_inclusive(statements::bound b) const = 0;

    /**
     * Merges this restriction with the specified one.
     *
     * @param otherRestriction the restriction to merge into this one
     * @return the restriction resulting of the merge
     * @throws InvalidRequestException if the restrictions cannot be merged
     */
    virtual void merge_with(::shared_ptr<restriction> other) = 0;

    /**
     * Check if the restriction is on indexed columns.
     *
     * @param indexManager the index manager
     * @return <code>true</code> if the restriction is on indexed columns, <code>false</code>
     */
    virtual bool has_supporting_index(const secondary_index::secondary_index_manager& index_manager) const = 0;

#if 0
    /**
     * Adds to the specified list the <code>IndexExpression</code>s corresponding to this <code>Restriction</code>.
     *
     * @param expressions the list to add the <code>IndexExpression</code>s to
     * @param options the query options
     * @throws InvalidRequestException if this <code>Restriction</code> cannot be converted into 
     * <code>IndexExpression</code>s
     */
    public void addIndexExpressionTo(List<IndexExpression> expressions,
                                     QueryOptions options)
                                     throws InvalidRequestException;
#endif

    virtual sstring to_string() const = 0;
};

}

}
