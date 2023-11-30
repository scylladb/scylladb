/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
    auto maybe_update_timeout_name = [] (service_level_options& slo, const service_level_options& other) {
        if (slo.effective_names && other.effective_names) {
            slo.effective_names->timeout = other.effective_names->timeout;
        }
    };
    auto maybe_update_workload_name = [] (service_level_options& slo, const service_level_options& other) {
        if (slo.effective_names && other.effective_names) {
            slo.effective_names->workload = other.effective_names->workload;
        }
    };

    service_level_options ret = *this;
    std::visit(overloaded_functor {
        [&] (const unset_marker& um) {
            ret.timeout = other.timeout;
            maybe_update_timeout_name(ret, other);
        },
        [&] (const delete_marker& dm) {
            ret.timeout = other.timeout;
            maybe_update_timeout_name(ret, other);
        },
        [&] (const lowres_clock::duration& d) {
            if (auto* other_timeout = std::get_if<lowres_clock::duration>(&other.timeout)) {
                auto prev_timeout = ret.timeout;
                ret.timeout = std::min(d, *other_timeout);

                if (prev_timeout != ret.timeout) {
                    maybe_update_timeout_name(ret, other);
                }
            }
        },
    }, ret.timeout);

    // Specified workloads should be preferred over unspecified ones
    auto prev_workload = ret.workload;
    if (ret.workload == workload_type::unspecified || other.workload == workload_type::unspecified) {
        ret.workload = std::max(ret.workload, other.workload);
    } else {
        ret.workload = std::min(ret.workload, other.workload);
    }
    if (prev_workload != ret.workload) {
        maybe_update_workload_name(ret, other);
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

void service_level_options::init_effective_names(sstring& service_level_name) {
    effective_names = service_level_options::slo_effective_names {
        .timeout = service_level_name,
        .workload = service_level_name
    };
}

}
