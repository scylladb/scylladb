/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <unordered_set>
#include <stdexcept>
#include <iosfwd>
#include <string_view>

#include <seastar/core/sstring.hh>
#include "seastarx.hh"
#include "db/hints/internal/common.hh"

namespace gms {
    class inet_address;
} // namespace gms

namespace locator { class topology; }

namespace db {
namespace hints {

// host_filter tells hints_manager towards which endpoints it is allowed to generate hints.
class host_filter final {
private:
    using endpoint_id = internal::endpoint_id;
    
    enum class enabled_kind {
        enabled_for_all,
        enabled_selectively,
        disabled_for_all,
    };

    enabled_kind _enabled_kind;
    std::unordered_set<sstring> _dcs;

    static std::string_view enabled_kind_to_string(host_filter::enabled_kind ek);

public:
    struct enabled_for_all_tag {};
    struct disabled_for_all_tag {};

    // Creates a filter that allows hints to all endpoints (default)
    host_filter(enabled_for_all_tag tag = {});

    // Creates a filter that does not allow any hints.
    host_filter(disabled_for_all_tag);

    // Creates a filter that allows sending hints to specified DCs.
    explicit host_filter(std::unordered_set<sstring> allowed_dcs);

    // Parses hint filtering configuration from the hinted_handoff_enabled option.
    static host_filter parse_from_config_string(sstring opt);

    // Parses hint filtering configuration from a list of DCs.
    static host_filter parse_from_dc_list(sstring opt);

    bool can_hint_for(const locator::topology& topo, endpoint_id ep) const;

    inline const std::unordered_set<sstring>& get_dcs() const {
        return _dcs;
    }

    bool operator==(const host_filter& other) const noexcept {
        return _enabled_kind == other._enabled_kind
                && _dcs == other._dcs;
    }

    inline bool is_enabled_for_all() const noexcept {
        return _enabled_kind == enabled_kind::enabled_for_all;
    }

    inline bool is_disabled_for_all() const noexcept {
        return _enabled_kind == enabled_kind::disabled_for_all;
    }

    sstring to_configuration_string() const;

    friend fmt::formatter<host_filter>;
};

std::istream& operator>>(std::istream& is, host_filter& f);

class hints_configuration_parse_error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

}
}

template <>
struct fmt::formatter<db::hints::host_filter> : fmt::formatter<string_view> {
    auto format(const db::hints::host_filter&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
