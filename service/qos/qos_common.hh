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

#include "seastarx.hh"
#include <seastar/core/sstring.hh>
#include <seastar/core/print.hh>
#include <map>
#include <stdexcept>
#include <variant>
#include <seastar/core/lowres_clock.hh>

namespace qos {

/**
 *  a structure that holds the configuration for
 *  a service level.
 */
struct service_level_options {
    struct unset_marker {
        bool operator==(const unset_marker&) const { return true; };
        bool operator!=(const unset_marker&) const { return false; };
    };
    struct delete_marker {
        bool operator==(const delete_marker&) const { return true; };
        bool operator!=(const delete_marker&) const { return false; };
    };

    enum class workload_type {
        unspecified, batch, interactive, delete_marker
    };

    using timeout_type = std::variant<unset_marker, delete_marker, lowres_clock::duration>;
    timeout_type timeout = unset_marker{};
    workload_type workload = workload_type::unspecified;

    service_level_options replace_defaults(const service_level_options& other) const;
    // Merges the values of two service level options. The semantics depends
    // on the type of the parameter - e.g. for timeouts, a min value is preferred.
    service_level_options merge_with(const service_level_options& other) const;

    bool operator==(const service_level_options& other) const = default;
    bool operator!=(const service_level_options& other) const = default;

    static std::string_view to_string(const workload_type& wt);
    static std::optional<workload_type> parse_workload_type(std::string_view sv);
};

std::ostream& operator<<(std::ostream& os, const service_level_options::workload_type&);

using service_levels_info = std::map<sstring, service_level_options>;

///
/// A logical argument error for a service_level statement operation.
///
class service_level_argument_exception : public std::invalid_argument {
public:
    using std::invalid_argument::invalid_argument;
};

///
/// An exception to indicate that the service level given as parameter doesn't exist.
///
class nonexistant_service_level_exception : public service_level_argument_exception {
public:
    nonexistant_service_level_exception(sstring service_level_name)
            : service_level_argument_exception(format("Service Level {} doesn't exists.", service_level_name)) {
    }
};

}
