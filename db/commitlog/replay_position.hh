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
    segment_id_type id;
    position_type pos;

    replay_position(segment_id_type i = 0, position_type p = 0)
        : id(i), pos(p)
    { }

    bool operator<(const replay_position & r) const {
        return id < r.id ? true : (r.id < id ? false : pos < r.pos);
    }
    bool operator==(const replay_position & r) const {
        return id == r.id && pos == r.pos;
    }

    template <typename Describer>
    auto describe_type(Describer f) { return f(id, pos); }
};

std::ostream& operator<<(std::ostream& out, const replay_position& s);

}
