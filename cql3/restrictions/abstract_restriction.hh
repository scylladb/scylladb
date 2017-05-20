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

#include "core/shared_ptr.hh"
#include "core/sstring.hh"
#include "cql3/restrictions/restriction.hh"
#include "cql3/term.hh"
#include "types.hh"

namespace cql3 {

namespace restrictions {

/**
 * Base class for <code>Restriction</code>s
 */
class abstract_restriction : public restriction {
public:
    virtual bool is_on_token() const override {
        return false;
    }

    virtual bool is_multi_column() const override {
        return false;
    }

    virtual bool is_slice() const override {
        return false;
    }

    virtual bool is_EQ() const override {
        return false;
    }

    virtual bool is_IN() const override {
        return false;
    }

    virtual bool is_contains() const override {
        return false;
    }

    virtual bool has_bound(statements::bound b) const override {
        return true;
    }

    virtual std::vector<bytes_opt> bounds(statements::bound b, const query_options& options) const override {
        return values(options);
    }

    virtual bool is_inclusive(statements::bound b) const override {
        return true;
    }

    /**
     * Whether the specified row satisfied this restriction.
     * Assumes the row is live, but not all cells. If a cell
     * isn't live and there's a restriction on its column,
     * then the function returns false.
     *
     * @param schema the schema the row belongs to
     * @param key the partition key
     * @param ckey the clustering key
     * @param cells the remaining row columns
     * @return the restriction resulting of the merge
     * @throws InvalidRequestException if the restrictions cannot be merged
     */
    virtual bool is_satisfied_by(const schema& schema,
                                 const partition_key& key,
                                 const clustering_key_prefix& ckey,
                                 const row& cells,
                                 const query_options& options,
                                 gc_clock::time_point now) const = 0;

protected:
#if 0
    protected static ByteBuffer validateIndexedValue(ColumnSpecification columnSpec,
                                                     ByteBuffer value)
                                                     throws InvalidRequestException
    {
        checkNotNull(value, "Unsupported null value for indexed column %s", columnSpec.name);
        checkFalse(value.remaining() > 0xFFFF, "Index expression values may not be larger than 64K");
        return value;
    }
#endif
    /**
     * Checks if the specified term is using the specified function.
     *
     * @param term the term to check
     * @param ks_name the function keyspace name
     * @param function_name the function name
     * @return <code>true</code> if the specified term is using the specified function, <code>false</code> otherwise.
     */
    static bool term_uses_function(::shared_ptr<term> term, const sstring& ks_name, const sstring& function_name) {
        return bool(term) && term->uses_function(ks_name, function_name);
    }

    /**
     * Checks if one of the specified term is using the specified function.
     *
     * @param terms the terms to check
     * @param ks_name the function keyspace name
     * @param function_name the function name
     * @return <code>true</code> if one of the specified term is using the specified function, <code>false</code> otherwise.
     */
    static bool term_uses_function(const std::vector<::shared_ptr<term>>& terms, const sstring& ks_name, const sstring& function_name) {
        for (auto&& value : terms) {
            if (term_uses_function(value, ks_name, function_name)) {
                return true;
            }
        }
        return false;
    }
};

}

}
