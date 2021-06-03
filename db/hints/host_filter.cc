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

#include <string_view>
#include <boost/algorithm/string.hpp>
#include "to_string.hh"
#include "host_filter.hh"

namespace db {
namespace hints {

host_filter::host_filter(host_filter::enabled_for_all_tag)
        : _enabled_kind(host_filter::enabled_kind::enabled_for_all) {
}

host_filter::host_filter(host_filter::disabled_for_all_tag)
        : _enabled_kind(host_filter::enabled_kind::disabled_for_all) {
}

host_filter::host_filter(std::unordered_set<sstring> allowed_dcs)
        : _enabled_kind(allowed_dcs.empty() ? enabled_kind::disabled_for_all : enabled_kind::enabled_selectively)
        , _dcs(std::move(allowed_dcs)) {
}

bool host_filter::can_hint_for(locator::snitch_ptr& snitch, gms::inet_address ep) const {
    switch (_enabled_kind) {
    case enabled_kind::enabled_for_all:
        return true;
    case enabled_kind::enabled_selectively:
        return _dcs.contains(snitch->get_datacenter(ep));
    case enabled_kind::disabled_for_all:
        return false;
    }
    throw std::logic_error("Uncovered variant of enabled_kind");
}

host_filter host_filter::parse_from_config_string(sstring opt) {
    if (boost::iequals(opt, "false") || opt == "0") {
        return host_filter(disabled_for_all_tag());
    } else if (boost::iequals(opt, "true") || opt == "1") {
        return host_filter(enabled_for_all_tag());
    }

    return parse_from_dc_list(std::move(opt));
}

host_filter host_filter::parse_from_dc_list(sstring opt) {
    using namespace boost::algorithm;

    std::vector<sstring> dcs;
    split(dcs, opt, is_any_of(","));

    std::for_each(dcs.begin(), dcs.end(), [] (sstring& dc) {
        trim(dc);
        if (dc.empty()) {
            throw hints_configuration_parse_error("hinted_handoff_enabled: DC name may not be an empty string");
        }
    });

    return host_filter(std::unordered_set<sstring>(dcs.begin(), dcs.end()));
}

std::istream& operator>>(std::istream& is, host_filter& f) {
    sstring tmp;
    is >> tmp;
    f = host_filter::parse_from_config_string(std::move(tmp));
    return is;
}

sstring host_filter::to_configuration_string() const {
    switch (_enabled_kind) {
    case enabled_kind::enabled_for_all:
        return "true";
    case enabled_kind::enabled_selectively:
        return ::join(",", _dcs);
    case enabled_kind::disabled_for_all:
        return "false";
    }
    throw std::logic_error("Uncovered variant of enabled_kind");
}


std::string_view host_filter::enabled_kind_to_string(host_filter::enabled_kind ek) {
    switch (ek) {
    case host_filter::enabled_kind::enabled_for_all:
        return "enabled_for_all";
    case host_filter::enabled_kind::enabled_selectively:
        return "enabled_selectively";
    case host_filter::enabled_kind::disabled_for_all:
        return "disabled_for_all";
    }
    throw std::logic_error("Uncovered variant of enabled_kind");
}

std::ostream& operator<<(std::ostream& os, const host_filter& f) {
    os << "host_filter{enabled_kind="
        << host_filter::enabled_kind_to_string(f._enabled_kind);
    if (f._enabled_kind == host_filter::enabled_kind::enabled_selectively) {
        os << ", dcs={" << ::join(",", f._dcs);
    }
    os << "}";
    return os;
}

}
}

