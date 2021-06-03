/*
 * Copyright (C) 2021-present ScyllaDB
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

#include "cql3/statements/sl_prop_defs.hh"
#include "database.hh"
#include "duration.hh"
#include "concrete_types.hh"
#include <boost/algorithm/string/predicate.hpp>

namespace cql3 {

namespace statements {

void sl_prop_defs::validate() {
    static std::set<sstring> timeout_props {
        "timeout", "workload_type"
    };
    auto get_duration = [&] (const std::optional<sstring>& repr) -> qos::service_level_options::timeout_type {
        if (!repr) {
            return qos::service_level_options::unset_marker{};
        }
        if (boost::algorithm::iequals(*repr, "null")) {
            return qos::service_level_options::delete_marker{};
        }
        data_value v = duration_type->deserialize(duration_type->from_string(*repr));
        cql_duration duration = static_pointer_cast<const duration_type_impl>(duration_type)->from_value(v);
        if (duration.months || duration.days) {
            throw exceptions::invalid_request_exception("Timeout values cannot be longer than 24h");
        }
        if (duration.nanoseconds % 1'000'000 != 0) {
            throw exceptions::invalid_request_exception("Timeout values must be expressed in millisecond granularity");
        }
        if (duration.nanoseconds < 0) {
            throw exceptions::invalid_request_exception("Timeout values must be nonnegative");
        }
        return std::chrono::duration_cast<lowres_clock::duration>(std::chrono::nanoseconds(duration.nanoseconds));
    };

    property_definitions::validate(timeout_props);
    _slo.timeout = get_duration(get_simple("timeout"));
    auto workload_string_opt = get_simple("workload_type");
    if (workload_string_opt) {
        auto workload = qos::service_level_options::parse_workload_type(*workload_string_opt);
        if (!workload) {
            throw exceptions::invalid_request_exception(format("Invalid workload type: {}", *workload_string_opt));
        }
        _slo.workload = *workload;
        // Explicitly setting a workload type to 'unspecified' should result in resetting
        // the previous value to 'unspecified, not just keeping it as is
        if (_slo.workload == qos::service_level_options::workload_type::unspecified) {
            _slo.workload = qos::service_level_options::workload_type::delete_marker;
        }
    }
}

qos::service_level_options sl_prop_defs::get_service_level_options() const {
    return _slo;
}

}

}
