/*
 * Copyright (C) 2021 ScyllaDB
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

namespace cql3 {

namespace statements {

void sl_prop_defs::validate() {
    static std::set<sstring> timeout_props {
        "timeout"
    };
    auto get_duration = [&] (const std::optional<sstring>& repr) -> qos::service_level_options::timeout_type {
        if (!repr) {
            return qos::service_level_options::unset_marker{};
        }
        data_value v = duration_type->deserialize(duration_type->from_string(*repr));
        cql_duration duration = static_pointer_cast<const duration_type_impl>(duration_type)->from_value(v);
        return std::chrono::duration_cast<lowres_clock::duration>(std::chrono::nanoseconds(duration.nanoseconds));
    };

    property_definitions::validate(timeout_props);
    _slo.timeout = get_duration(get_simple("timeout"));
}

qos::service_level_options sl_prop_defs::get_service_level_options() const {
    return _slo;
}

}

}
