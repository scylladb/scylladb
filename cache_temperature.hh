/*
 * Copyright 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
