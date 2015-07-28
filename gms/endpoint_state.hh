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

#pragma once

#include "types.hh"
#include "utils/serialization.hh"
#include "gms/heart_beat_state.hh"
#include "gms/application_state.hh"
#include "gms/versioned_value.hh"
#include <experimental/optional>
#include <chrono>

namespace gms {

/**
 * This abstraction represents both the HeartBeatState and the ApplicationState in an EndpointState
 * instance. Any state for a given endpoint can be retrieved from this instance.
 */
class endpoint_state {
public:
    using clk = std::chrono::high_resolution_clock;
private:
    heart_beat_state _heart_beat_state;
    std::map<application_state, versioned_value> _application_state;
    /* fields below do not get serialized */
    clk::time_point _update_timestamp;
    bool _is_alive;
public:
    bool operator==(const endpoint_state& other) const {
        return _heart_beat_state  == other._heart_beat_state &&
               _application_state == other._application_state &&
               _update_timestamp  == other._update_timestamp &&
               _is_alive          == other._is_alive;
    }

    endpoint_state()
        : _heart_beat_state(0)
        , _update_timestamp(clk::now())
        , _is_alive(true) {
    }

    endpoint_state(heart_beat_state initial_hb_state)
        : _heart_beat_state(initial_hb_state)
        , _update_timestamp(clk::now())
        , _is_alive(true) {
    }

    heart_beat_state& get_heart_beat_state() {
        return _heart_beat_state;
    }

    void set_heart_beat_state(heart_beat_state hbs) {
        update_timestamp();
        _heart_beat_state = hbs;
    }

    std::experimental::optional<versioned_value> get_application_state(application_state key) const {
        auto it = _application_state.find(key);
        if (it == _application_state.end()) {
            return {};
        } else {
            return _application_state.at(key);
        }
    }

    /**
     * TODO replace this with operations that don't expose private state
     */
    // @Deprecated
    std::map<application_state, versioned_value>& get_application_state_map() {
        return _application_state;
    }

    void add_application_state(application_state key, versioned_value value) {
        _application_state[key] = value;
    }

    /* getters and setters */
    /**
     * @return System.nanoTime() when state was updated last time.
     */
    clk::time_point get_update_timestamp() {
        return _update_timestamp;
    }

    void update_timestamp() {
        _update_timestamp = clk::now();
    }

    bool is_alive() {
        return _is_alive;
    }

    void mark_alive() {
        _is_alive = true;
    }

    void mark_dead() {
        _is_alive = false;
    }

    friend inline std::ostream& operator<<(std::ostream& os, const endpoint_state& x) {
        os << "EndpointState: HeartBeatState = " << x._heart_beat_state << ", AppStateMap = ";
        for (auto&entry : x._application_state) {
            const application_state& state = entry.first;
            const versioned_value& value = entry.second;
            os << " { " << int32_t(state) << " : " << value << " } ";
        }
        return os;
    }

    // The following replaces EndpointStateSerializer from the Java code
    void serialize(bytes::iterator& out) const {
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

    static endpoint_state deserialize(bytes_view& v) {
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

    size_t serialized_size() const {
        long size = _heart_beat_state.serialized_size();
        size += serialize_int32_size;
        for (auto& entry : _application_state) {
            const versioned_value& value = entry.second;
            size += serialize_int32_size;
            size += value.serialized_size();
        }
        return size;
    }
};

} // gms
