/*
 * Copyright (C) 2018 ScyllaDB
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

#include "mp_row_consumer.hh"

namespace sstables {

atomic_cell make_counter_cell(api::timestamp_type timestamp, bytes_view value) {
    static constexpr size_t shard_size = 32;

    data_input in(value);

    auto header_size = in.read<int16_t>();
    for (auto i = 0; i < header_size; i++) {
        auto idx = in.read<int16_t>();
        if (idx >= 0) {
            throw marshal_exception("encountered a local shard in a counter cell");
        }
    }
    auto header_length = (size_t(header_size) + 1) * sizeof(int16_t);
    auto shard_count = (value.size() - header_length) / shard_size;
    if (shard_count != size_t(header_size)) {
        throw marshal_exception("encountered remote shards in a counter cell");
    }

    std::vector<counter_shard> shards;
    shards.reserve(shard_count);
    counter_cell_builder ccb(shard_count);
    for (auto i = 0u; i < shard_count; i++) {
        auto id_hi = in.read<int64_t>();
        auto id_lo = in.read<int64_t>();
        auto clock = in.read<int64_t>();
        auto value = in.read<int64_t>();
        ccb.add_maybe_unsorted_shard(counter_shard(counter_id(utils::UUID(id_hi, id_lo)), value, clock));
    }
    ccb.sort_and_remove_duplicates();
    return ccb.build(timestamp);
}

}
