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
 * Copyright 2015 Cloudius Systems
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
#include <cmath>
#include <algorithm>
#include <vector>

class column_name_helper {
private:
    static void may_grow(std::vector<bytes>& v, size_t target_size) {
        if (target_size > v.size()) {
            v.resize(target_size);
        }
    }
public:
    static void min_max_components(std::vector<bytes>& min_seen, std::vector<bytes>& max_seen, const std::vector<bytes_view>& column_names) {
        may_grow(min_seen, column_names.size());
        may_grow(max_seen, column_names.size());

        for (auto i = 0U; i < column_names.size(); i++) {
            auto& name = column_names[i];
            if (max_seen[i].size() == 0 || name > bytes_view(max_seen[i])) {
                max_seen[i] = bytes(name.data(), name.size());
            }
            if (min_seen[i].size() == 0 || name < bytes_view(min_seen[i])) {
                min_seen[i] = bytes(name.data(), name.size());
            }
        }
    }

    static void merge_max_components(std::vector<bytes>& to, std::vector<bytes>&& from) {
        if (to.empty()) {
            to = std::move(from);
            return;
        }

        if (from.empty()) {
            return;
        }

        may_grow(to, from.size());

        for (auto i = 0U; i < from.size(); i++) {
            if (to[i].size() == 0 || bytes_view(from[i]) > bytes_view(to[i])) {
                to[i] = std::move(from[i]);
            }
        }
    }

    static void merge_min_components(std::vector<bytes>& to, std::vector<bytes>&& from) {
        if (to.empty()) {
            to = std::move(from);
        }

        if (from.empty()) {
            return;
        }

        may_grow(to, from.size());

        for (auto i = 0U; i < from.size(); i++) {
            if (to[i].size() == 0 || bytes_view(from[i]) < bytes_view(to[i])) {
                to[i] = std::move(from[i]);
            }
        }
    }
};
