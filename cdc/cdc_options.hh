/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <map>
#include <optional>
#include <seastar/core/sstring.hh>
#include "seastarx.hh"

namespace cdc {

enum class delta_mode : uint8_t {
    keys,
    full,
};

/**
 * (for now only pre-) image collection mode.
 * Stating how much info to record.
 * off == none
 * on == changed columns
 * full == all (changed and unmodified columns)
 */
enum class image_mode : uint8_t {
    off, 
    on,
    full,
};

class options final {
    std::optional<bool> _enabled;
    image_mode _preimage = image_mode::off;
    bool _postimage = false;
    delta_mode _delta_mode = delta_mode::full;
    int _ttl = 86400; // 24h in seconds
public:
    options() = default;
    options(const std::map<sstring, sstring>& map);

    std::map<sstring, sstring> to_map() const;
    sstring to_sstring() const;

    bool enabled() const { return _enabled.value_or(false); }
    bool is_enabled_set() const { return _enabled.has_value(); }
    bool preimage() const { return _preimage != image_mode::off; }
    bool full_preimage() const { return _preimage == image_mode::full; }
    bool postimage() const { return _postimage; }
    delta_mode get_delta_mode() const { return _delta_mode; }
    void set_delta_mode(delta_mode m) { _delta_mode = m; }
    int ttl() const { return _ttl; }

    void enabled(bool b) { _enabled = b; }
    void preimage(bool b) { preimage(b ? image_mode::on : image_mode::off); }
    void preimage(image_mode m) { _preimage = m; }
    void postimage(bool b) { _postimage = b; }
    void ttl(int v) { _ttl = v; }

    bool operator==(const options& o) const;
};

} // namespace cdc

template <> struct fmt::formatter<cdc::image_mode> : fmt::formatter<string_view> {
    auto format(cdc::image_mode, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <> struct fmt::formatter<cdc::delta_mode> : fmt::formatter<string_view> {
    auto format(cdc::delta_mode, fmt::format_context& ctx) const -> decltype(ctx.out());
};
