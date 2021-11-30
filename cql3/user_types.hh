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
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015-present ScyllaDB
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

#include "cql3/abstract_marker.hh"
#include "column_specification.hh"
#include "column_identifier.hh"
#include "operation.hh"
#include "to_string.hh"

namespace cql3 {

/**
 * Static helper methods and classes for user types.
 */
class user_types {
    user_types() = delete;
public:
    static lw_shared_ptr<column_specification> field_spec_of(const column_specification& column, size_t field);

    class setter : public operation {
    public:
        using operation::operation;

        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
        static void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const column_definition& column, const expr::constant& value);
    };

    class setter_by_field : public operation {
        size_t _field_idx;
    public:
        setter_by_field(const column_definition& column, size_t field_idx, expr::expression e)
            : operation(column, std::move(e)), _field_idx(field_idx) {
        }

        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
    };

    class deleter_by_field : public operation {
        size_t _field_idx;
    public:
        deleter_by_field(const column_definition& column, size_t field_idx)
            : operation(column, std::nullopt), _field_idx(field_idx) {
        }

        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
    };
};

}
