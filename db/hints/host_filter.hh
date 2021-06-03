/*
 * Copyright (C) 2020-present ScyllaDB
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

#include <functional>
#include <unordered_set>
#include <exception>
#include <iostream>
#include <string_view>

#include <seastar/core/sstring.hh>
#include "gms/inet_address.hh"
#include "locator/snitch_base.hh"
#include "seastarx.hh"

namespace db {
namespace hints {

// host_filter tells hints_manager towards which endpoints it is allowed to generate hints.
class host_filter final {
private:
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

    bool can_hint_for(locator::snitch_ptr& snitch, gms::inet_address ep) const;

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

    friend std::ostream& operator<<(std::ostream& os, const host_filter& f);
};

std::istream& operator>>(std::istream& is, host_filter& f);

class hints_configuration_parse_error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

}
}
