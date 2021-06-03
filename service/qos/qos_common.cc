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

#include "qos_common.hh"
#include "utils/overloaded_functor.hh"

namespace qos {

service_level_options service_level_options::replace_defaults(const service_level_options& default_values) const {
    service_level_options ret = *this;
    std::visit(overloaded_functor {
        [&] (const unset_marker& um) {
            // reset the value to the default one
            ret.timeout = default_values.timeout;
        },
        [&] (const delete_marker& dm) {
            // remove the value
            ret.timeout = unset_marker{};
        },
        [&] (const lowres_clock::duration&) {
            // leave the value as is
        },
    }, ret.timeout);
    switch (ret.workload) {
    case workload_type::unspecified:
        ret.workload = default_values.workload;
        break;
    case workload_type::delete_marker:
        ret.workload = workload_type::unspecified;
        break;
    default:
        // no-op
        break;
    }
    return ret;
}

service_level_options service_level_options::merge_with(const service_level_options& other) const {
    service_level_options ret = *this;
    std::visit(overloaded_functor {
        [&] (const unset_marker& um) {
            ret.timeout = other.timeout;
        },
        [&] (const delete_marker& dm) {
            ret.timeout = other.timeout;
        },
        [&] (const lowres_clock::duration& d) {
            if (auto* other_timeout = std::get_if<lowres_clock::duration>(&other.timeout)) {
                ret.timeout = std::min(d, *other_timeout);
            }
        },
    }, ret.timeout);
    // Specified workloads should be preferred over unspecified ones
    if (ret.workload == workload_type::unspecified || other.workload == workload_type::unspecified) {
        ret.workload = std::max(ret.workload, other.workload);
    } else {
        ret.workload = std::min(ret.workload, other.workload);
    }
    return ret;
}

std::string_view service_level_options::to_string(const workload_type& wt) {
    switch (wt) {
    case workload_type::unspecified: return "unspecified";
    case workload_type::batch: return "batch";
    case workload_type::interactive: return "interactive";
    case workload_type::delete_marker: return "delete_marker";
    }
    abort();
}

std::ostream& operator<<(std::ostream& os, const service_level_options::workload_type& wt) {
    return os << service_level_options::to_string(wt);
}

std::optional<service_level_options::workload_type> service_level_options::parse_workload_type(std::string_view sv) {
    if (sv == "null") {
        return workload_type::unspecified;
    } else if (sv == "interactive") {
        return workload_type::interactive;
    } else if (sv == "batch") {
        return workload_type::batch;
    }
    return std::nullopt;
}

}
