/*
 * Copyright 2018-present ScyllaDB
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

namespace ser {

template <typename T>
class serializer;

};

class cache_temperature {
    float hit_rate;
    explicit cache_temperature(uint8_t hr) : hit_rate(hr/255.0f) {}
public:
    uint8_t get_serialized_temperature() const {
        return hit_rate * 255;
    }
    cache_temperature() : hit_rate(0) {}
    explicit cache_temperature(float hr) : hit_rate(hr) {}
    explicit operator float() const { return hit_rate; }
    static cache_temperature invalid() { return cache_temperature(-1.0f); }
    friend struct ser::serializer<cache_temperature>;
};
