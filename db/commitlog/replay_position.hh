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
 * Modified by Cloudius Systems
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <stdint.h>

namespace db {

using segment_id_type = uint64_t;
using position_type = uint32_t;

struct replay_position {
    static const constexpr size_t max_cpu_bits = 10; // 1024 cpus. should be enough for anyone
    static const constexpr size_t max_ts_bits = 8 * sizeof(segment_id_type) - max_cpu_bits;
    static const constexpr segment_id_type ts_mask = (segment_id_type(1) << max_ts_bits) - 1;
    static const constexpr segment_id_type cpu_mask = ~ts_mask;

    segment_id_type id;
    position_type pos;

    replay_position(segment_id_type i = 0, position_type p = 0)
        : id(i), pos(p)
    {}

    replay_position(unsigned shard, segment_id_type i, position_type p = 0)
            : id((segment_id_type(shard) << max_ts_bits) | i), pos(p)
    {
        if (i & cpu_mask) {
            throw std::invalid_argument("base id overflow: " + std::to_string(i));
        }
    }

    bool operator<(const replay_position & r) const {
        return id < r.id ? true : (r.id < id ? false : pos < r.pos);
    }
    bool operator==(const replay_position & r) const {
        return id == r.id && pos == r.pos;
    }
    bool operator!=(const replay_position & r) const {
        return !(*this == r);
    }

    unsigned shard_id() const {
        return unsigned(id >> max_ts_bits);
    }
    segment_id_type base_id() const {
        return id & ts_mask;
    }
    replay_position base() const {
        return replay_position(base_id(), pos);
    }

    template <typename Describer>
    auto describe_type(Describer f) { return f(id, pos); }
};

std::ostream& operator<<(std::ostream& out, const replay_position& s);

}
