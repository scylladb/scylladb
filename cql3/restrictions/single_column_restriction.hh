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
 * Copyright (C) 2015-present ScyllaDB
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

#include <optional>

#include "cql3/restrictions/restriction.hh"
#include "cql3/restrictions/term_slice.hh"
#include "cql3/term.hh"
#include "cql3/abstract_marker.hh"
#include <seastar/core/shared_ptr.hh>
#include "schema_fwd.hh"
#include "to_string.hh"
#include "exceptions/exceptions.hh"
#include "keys.hh"
#include "utils/like_matcher.hh"

namespace cql3 {

namespace restrictions {

class single_column_restriction : public restriction {
protected:
    /**
     * The definition of the column to which apply the restriction.
     */
    const column_definition& _column_def;
public:
    single_column_restriction(const column_definition& column_def) : _column_def(column_def) {}

    const column_definition& get_column_def() const {
        return _column_def;
    }

#if 0
    @Override
    public void addIndexExpressionTo(List<IndexExpression> expressions,
                                     QueryOptions options) throws InvalidRequestException
    {
        List<ByteBuffer> values = values(options);
        checkTrue(values.size() == 1, "IN restrictions are not supported on indexed columns");

        ByteBuffer value = validateIndexedValue(columnDef, values.get(0));
        expressions.add(new IndexExpression(columnDef.name.bytes, Operator.EQ, value));
    }

    /**
     * Check if this type of restriction is supported by the specified index.
     *
     * @param index the Secondary index
     * @return <code>true</code> this type of restriction is supported by the specified index,
     * <code>false</code> otherwise.
     */
    protected abstract boolean isSupportedBy(SecondaryIndex index);
#endif
};

}

}
