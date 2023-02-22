/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
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
    bool operator==(const heart_beat_state& other) const noexcept {
        return _generation == other._generation && _version == other._version;
    }

    heart_beat_state(int32_t gen) noexcept
        : _generation(gen)
        , _version(0) {
    }

    heart_beat_state(int32_t gen, int32_t ver) noexcept
        : _generation(gen)
        , _version(ver) {
    }

    int32_t get_generation() const noexcept {
        return _generation;
    }

    void update_heart_beat() noexcept {
        _version = version_generator::get_next_version().value();
    }

    int32_t get_heart_beat_version() const noexcept {
        return _version;
    }

    void force_newer_generation_unsafe() noexcept {
        _generation += 1;
    }

    void force_highest_possible_version_unsafe() noexcept {
        _version = std::numeric_limits<int32_t>::max();
    }

    friend inline std::ostream& operator<<(std::ostream& os, const heart_beat_state& h) {
        return os << "{ generation = " << h._generation << ", version = " << h._version << " }";
    }
};

} // gms
