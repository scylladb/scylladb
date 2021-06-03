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

#include "gms/endpoint_state.hh"
#include <optional>
#include <ostream>

namespace gms {

static_assert(!std::is_default_constructible_v<heart_beat_state>);
static_assert(std::is_nothrow_copy_constructible_v<heart_beat_state>);
static_assert(std::is_nothrow_move_constructible_v<heart_beat_state>);

static_assert(std::is_nothrow_default_constructible_v<std::map<application_state, versioned_value>>);

// Note: although std::map::find is not guaranteed to be noexcept
// it depends on the comperator used and in this case comparing application_state
// is noexcept.  Therefore, we can safely mark this method noexcept.
const versioned_value* endpoint_state::get_application_state_ptr(application_state key) const noexcept {
    auto it = _application_state.find(key);
    if (it == _application_state.end()) {
        return nullptr;
    } else {
        return &it->second;
    }
}

std::ostream& operator<<(std::ostream& os, const endpoint_state& x) {
    os << "HeartBeatState = " << x._heart_beat_state << ", AppStateMap =";
    for (auto&entry : x._application_state) {
        const application_state& state = entry.first;
        const versioned_value& value = entry.second;
        os << " { " << state << " : " << value << " } ";
    }
    return os;
}

bool endpoint_state::is_cql_ready() const noexcept {
    auto* app_state = get_application_state_ptr(application_state::RPC_READY);
    if (!app_state) {
        return false;
    }
    try {
        return boost::lexical_cast<int>(app_state->value);
    } catch (...) {
        return false;
    }
}

}
