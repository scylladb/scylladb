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
 *
 * Modified by ScyllaDB
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

#include <seastar/core/sstring.hh>
#include "range.hh"
#include "dht/i_partitioner.hh"
#include "partition_range_compat.hh"
#include <vector>

namespace streaming {

class stream_request {
public:
    using token = dht::token;
    sstring keyspace;
    dht::token_range_vector ranges;
    // For compatibility with <= 1.5, we send wrapping ranges (though they will never wrap).
    std::vector<wrapping_range<token>> ranges_compat() const {
        return ::compat::wrap(ranges);
    }
    std::vector<sstring> column_families;
    stream_request() = default;
    stream_request(sstring _keyspace, dht::token_range_vector _ranges, std::vector<sstring> _column_families)
        : keyspace(std::move(_keyspace))
        , ranges(std::move(_ranges))
        , column_families(std::move(_column_families)) {
    }
    stream_request(sstring _keyspace, std::vector<wrapping_range<token>> _ranges, std::vector<sstring> _column_families)
        : stream_request(std::move(_keyspace), ::compat::unwrap(std::move(_ranges)), std::move(_column_families)) {
    }
    friend std::ostream& operator<<(std::ostream& os, const stream_request& r);
};

} // namespace streaming
