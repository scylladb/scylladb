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

#include "gms/version_generator.hh"
#include "utils/serialization.hh"
#include <ostream>
#include <limits>

namespace gms {
/**
 * HeartBeat State associated with any given endpoint.
 */
class heart_beat_state {
private:
    int32_t _generation;
    int32_t _version;
public:
    bool operator==(const heart_beat_state& other) const {
        return _generation == other._generation && _version == other._version;
    }

    heart_beat_state(int32_t gen)
        : _generation(gen)
        , _version(0) {
    }

    heart_beat_state(int32_t gen, int32_t ver)
        : _generation(gen)
        , _version(ver) {
    }

    int32_t get_generation() const {
        return _generation;
    }

    void update_heart_beat() {
        _version = version_generator::get_next_version();
    }

    int32_t get_heart_beat_version() const {
        return _version;
    }

    void force_newer_generation_unsafe() {
        _generation += 1;
    }

    void force_highest_possible_version_unsafe() {
        _version = std::numeric_limits<int32_t>::max();
    }

    friend inline std::ostream& operator<<(std::ostream& os, const heart_beat_state& h) {
        return os << "{ generation = " << h._generation << ", version = " << h._version << " }";
    }
};

} // gms
