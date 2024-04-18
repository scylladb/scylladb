/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include "tombstone_gc_options.hh"
#include "exceptions/exceptions.hh"
#include <boost/lexical_cast.hpp>
#include <seastar/core/sstring.hh>
#include <map>
#include "utils/rjson.hh"

tombstone_gc_options::tombstone_gc_options(const std::map<seastar::sstring, seastar::sstring>& map) {
    for (const auto& x : map) {
        if (x.first == "mode") {
            if (x.second == "disabled") {
                _mode = tombstone_gc_mode::disabled;
            } else if (x.second == "repair") {
                _mode = tombstone_gc_mode::repair;
            } else if (x.second == "timeout") {
                _mode = tombstone_gc_mode::timeout;
            } else if (x.second == "immediate") {
                _mode = tombstone_gc_mode::immediate;
            } else  {
                throw exceptions::configuration_exception(format("Invalid value for tombstone_gc option mode: {}", x.second));
            }
        } else if (x.first == "propagation_delay_in_seconds") {
            try {
                auto seconds = boost::lexical_cast<int64_t>(x.second);
                if (seconds < 0) {
                    throw exceptions::configuration_exception(format("Invalid value for tombstone_gc option propagation_delay_in_seconds: {}", x.second));
                }
                _propagation_delay_in_seconds = std::chrono::seconds(seconds);
            } catch (...) {
                throw exceptions::configuration_exception(format("Invalid value for tombstone_gc option propagation_delay_in_seconds: {}", x.second));
            }
        } else {
            throw exceptions::configuration_exception(format("Invalid tombstone_gc option: {}", x.first));
        }
    }
}

std::map<seastar::sstring, seastar::sstring> tombstone_gc_options::to_map() const {
    std::map<seastar::sstring, seastar::sstring> res = {
        {"mode", format("{}", _mode)},
        {"propagation_delay_in_seconds", format("{}", _propagation_delay_in_seconds.count())},
    };
    return res;
}

seastar::sstring tombstone_gc_options::to_sstring() const {
    return rjson::print(rjson::from_string_map(to_map()));
}

auto fmt::formatter<tombstone_gc_mode>::format(tombstone_gc_mode mode, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    std::string_view name = "unknown";
    switch (mode) {
    case tombstone_gc_mode::timeout:     name = "timeout"; break;
    case tombstone_gc_mode::disabled:    name = "disabled"; break;
    case tombstone_gc_mode::immediate:   name = "immediate"; break;
    case tombstone_gc_mode::repair:      name = "repair"; break;
    }
    return formatter<string_view>::format(name, ctx);
}
