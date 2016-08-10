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

#include "core/sstring.hh"
#include "schema.hh"
#include "compound_compat.hh"
#include <cmath>
#include <algorithm>
#include <vector>

class column_name_helper {
private:
    static inline void may_grow(std::vector<bytes_opt>& v, size_t target_size) {
        if (target_size > v.size()) {
            v.resize(target_size);
        }
    }
public:
    template <typename T>
    static void min_max_components(const schema& schema, std::vector<bytes_opt>& min_seen, std::vector<bytes_opt>& max_seen, T components) {
        may_grow(min_seen, schema.clustering_key_size());
        may_grow(max_seen, schema.clustering_key_size());

        auto& types = schema.clustering_key_type()->types();
        auto i = 0U;
        for (auto& value : components) {
            auto& type = types[i];

            if (!max_seen[i] || type->compare(value, max_seen[i].value()) > 0) {
                max_seen[i] = bytes(value.data(), value.size());
            }
            if (!min_seen[i] || type->compare(value, min_seen[i].value()) < 0) {
                min_seen[i] = bytes(value.data(), value.size());
            }
            i++;
        }
    }
};
