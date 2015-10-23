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
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
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
#include <experimental/optional>
#include <ostream>

namespace gms {

std::experimental::optional<versioned_value> endpoint_state::get_application_state(application_state key) const {
    auto it = _application_state.find(key);
    if (it == _application_state.end()) {
        return {};
    } else {
        return _application_state.at(key);
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

void endpoint_state::serialize(bytes::iterator& out) const {
    /* serialize the HeartBeatState */
    _heart_beat_state.serialize(out);

    /* serialize the map of ApplicationState objects */
    int32_t app_state_size = _application_state.size();
    serialize_int32(out, app_state_size);
    for (auto& entry : _application_state) {
        const application_state& state = entry.first;
        const versioned_value& value = entry.second;
        serialize_int32(out, int32_t(state));
        value.serialize(out);
    }
}

endpoint_state endpoint_state::deserialize(bytes_view& v) {
    heart_beat_state hbs = heart_beat_state::deserialize(v);
    endpoint_state es = endpoint_state(hbs);
    int32_t app_state_size = read_simple<int32_t>(v);
    for (int32_t i = 0; i < app_state_size; ++i) {
        auto state = static_cast<application_state>(read_simple<int32_t>(v));
        auto value = versioned_value::deserialize(v);
        es.add_application_state(state, value);
    }
    return es;
}

size_t endpoint_state::serialized_size() const {
    long size = _heart_beat_state.serialized_size();
    size += serialize_int32_size;
    for (auto& entry : _application_state) {
        const versioned_value& value = entry.second;
        size += serialize_int32_size;
        size += value.serialized_size();
    }
    return size;
}

}
