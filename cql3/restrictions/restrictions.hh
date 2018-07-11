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

#include "cql3/query_options.hh"
#include "types.hh"
#include "schema.hh"
#include "index/secondary_index_manager.hh"

namespace cql3 {

namespace restrictions {

/**
 * Sets of restrictions
 */
class restrictions {
public:
    virtual ~restrictions() {}

    /**
     * Returns the column definitions in position order.
     * @return the column definitions in position order.
     */
    virtual std::vector<const column_definition*> get_column_defs() const = 0;

    virtual std::vector<bytes_opt> values(const query_options& options) const = 0;

    virtual bytes_opt value_for(const column_definition& cdef, const query_options& options) const {
        throw exceptions::invalid_request_exception("Single value can be obtained from single-column restrictions only");
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
     * Check if the restriction is on indexed columns.
     *
     * @param index_manager the index manager
     * @return <code>true</code> if the restriction is on indexed columns, <code>false</code>
     */
    virtual bool has_supporting_index(const secondary_index::secondary_index_manager& index_manager) const = 0;

#if 0
    /**
     * Adds to the specified list the <code>index_expression</code>s corresponding to this <code>Restriction</code>.
     *
     * @param expressions the list to add the <code>index_expression</code>s to
     * @param options the query options
     * @throws InvalidRequestException if this <code>Restriction</code> cannot be converted into
     * <code>index_expression</code>s
     */
    virtual void add_index_expression_to(std::vector<::shared_ptr<index_expression>>& expressions,
                                         const query_options& options) = 0;
#endif

    /**
     * Checks if this <code>SingleColumnprimary_key_restrictions</code> is empty or not.
     *
     * @return <code>true</code> if this <code>SingleColumnprimary_key_restrictions</code> is empty, <code>false</code> otherwise.
     */
    virtual bool empty() const = 0;

    /**
     * Returns the number of columns that have a restriction.
     *
     * @return the number of columns that have a restriction.
     */
    virtual uint32_t size() const = 0;
};

}

}
