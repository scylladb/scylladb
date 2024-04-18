/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <map>
#include <chrono>
#include <fmt/core.h>
#include <seastar/core/sstring.hh>

enum class tombstone_gc_mode : uint8_t { timeout, disabled, immediate, repair };

class tombstone_gc_options {
private:
    tombstone_gc_mode _mode = tombstone_gc_mode::timeout;
    std::chrono::seconds _propagation_delay_in_seconds = std::chrono::seconds(3600);
public:
    tombstone_gc_options() = default;
    const tombstone_gc_mode& mode() const { return _mode; }
    explicit tombstone_gc_options(const std::map<seastar::sstring, seastar::sstring>& map);
    const std::chrono::seconds& propagation_delay_in_seconds() const {
        return _propagation_delay_in_seconds;
    }
    std::map<seastar::sstring, seastar::sstring> to_map() const;
    seastar::sstring to_sstring() const;
    bool operator==(const tombstone_gc_options&) const = default;
};

template <> struct fmt::formatter<tombstone_gc_mode> : fmt::formatter<string_view> {
    auto format(tombstone_gc_mode mode, fmt::format_context& ctx) const -> decltype(ctx.out());
};
