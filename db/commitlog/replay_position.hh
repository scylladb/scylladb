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

#include <stdint.h>
#include "utils/UUID.hh"
#include "utils/hash.hh"
#include "sstables/version.hh"


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
    bool operator<=(const replay_position & r) const {
        return !(r < *this);
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
    auto describe_type(sstables::sstable_version_types v, Describer f) { return f(id, pos); }
};

class commitlog;
class cf_holder;

using cf_id_type = utils::UUID;

class rp_handle {
public:
    rp_handle() noexcept;
    rp_handle(rp_handle&&) noexcept;
    rp_handle& operator=(rp_handle&&) noexcept;
    ~rp_handle();

    replay_position release();

    operator bool() const {
        return _h && _rp != replay_position();
    }
    operator const replay_position&() const {
        return _rp;
    }
    const replay_position& rp() const {
        return _rp;
    }
private:
    friend class commitlog;

    rp_handle(shared_ptr<cf_holder>, cf_id_type, replay_position) noexcept;

    ::shared_ptr<cf_holder> _h;
    cf_id_type _cf;
    replay_position _rp;
};


std::ostream& operator<<(std::ostream& out, const replay_position& s);

}

namespace std {
template <>
struct hash<db::replay_position> {
    size_t operator()(const db::replay_position& v) const {
        return utils::tuple_hash()(v.id, v.pos);
    }
};
}
