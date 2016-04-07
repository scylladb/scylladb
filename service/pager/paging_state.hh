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

#include <experimental/optional>

#include "bytes.hh"
#include "keys.hh"

namespace service {

namespace pager {

class paging_state final {
    partition_key _partition_key;
    std::experimental::optional<clustering_key> _clustering_key;
    uint32_t _remaining;

public:
    paging_state(partition_key pk, std::experimental::optional<clustering_key> ck, uint32_t rem);

    /**
     * Last processed key, i.e. where to start from in next paging round
     */
    const partition_key& get_partition_key() const {
        return _partition_key;
    }
    /**
     * Clustering key in last partition. I.e. first, next, row
     */
    const std::experimental::optional<clustering_key>& get_clustering_key() const {
        return _clustering_key;
    }
    /**
     * Max remaining rows to fetch in total.
     * I.e. initial row_limit - #rows returned so far.
     */
    uint32_t get_remaining() const {
        return _remaining;
    }

    static ::shared_ptr<paging_state> deserialize(bytes_opt bytes);
    bytes_opt serialize() const;
};

}

}
