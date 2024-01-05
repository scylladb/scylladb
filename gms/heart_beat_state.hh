/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "gms/generation-number.hh"
#include "gms/version_generator.hh"
#include <fmt/core.h>
#include <limits>

namespace gms {
/**
 * HeartBeat State associated with any given endpoint.
 */
class heart_beat_state {
private:
    generation_type _generation;
    version_type _version;
public:
    bool operator==(const heart_beat_state& other) const noexcept {
        return _generation == other._generation && _version == other._version;
    }

    heart_beat_state() noexcept : heart_beat_state(generation_type(0)) {}

    explicit heart_beat_state(generation_type gen) noexcept
        : _generation(gen)
    {
    }

    heart_beat_state(generation_type gen, version_type ver) noexcept
        : _generation(gen)
        , _version(ver) {
    }

    generation_type get_generation() const noexcept {
        return _generation;
    }

    void update_heart_beat() noexcept {
        _version = version_generator::get_next_version();
    }

    version_type get_heart_beat_version() const noexcept {
        return _version;
    }

    void force_newer_generation_unsafe() noexcept {
        ++_generation;
    }

    void force_highest_possible_version_unsafe() noexcept {
        static_assert(std::numeric_limits<version_type>::is_bounded);
        _version = std::numeric_limits<version_type>::max();
    }

    friend fmt::formatter<heart_beat_state>;
};

} // gms

template<>
struct fmt::formatter<gms::heart_beat_state> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const gms::heart_beat_state& h, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{{ generation = {}, version = {} }}",
                              h._generation, h._version);
    }
};
