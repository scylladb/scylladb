/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "db/consistency_level_type.hh"
#include "seastarx.hh"
#include <seastar/core/sstring.hh>
#include <seastar/core/print.hh>
#include <map>
#include <stdexcept>
#include <string_view>
#include <variant>
#include <seastar/core/lowres_clock.hh>

namespace cql3 {
class query_processor;
}

namespace service {
class query_state;
}

namespace qos {

enum class include_effective_names { yes, no };

/**
 *  a structure that holds the configuration for
 *  a service level.
 */
struct service_level_options {
    struct unset_marker {
        bool operator==(const unset_marker&) const { return true; };
    };
    struct delete_marker {
        bool operator==(const delete_marker&) const { return true; };
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

    static std::string_view to_string(const workload_type& wt);
    static std::optional<workload_type> parse_workload_type(std::string_view sv);

    struct slo_effective_names {
        sstring timeout;
        sstring workload;

        bool operator==(const slo_effective_names& other) const = default;
        bool operator!=(const slo_effective_names& other) const = default;
    };
    std::optional<slo_effective_names> effective_names = std::nullopt;

    void init_effective_names(std::string_view service_level_name);
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

service::query_state& qos_query_state();

future<service_levels_info> get_service_levels(cql3::query_processor& qp, std::string_view ks_name, std::string_view cf_name, db::consistency_level cl);
future<service_levels_info> get_service_level(cql3::query_processor& qp, std::string_view ks_name, std::string_view cf_name, sstring service_level_name, db::consistency_level cl);

}

template <> struct fmt::formatter<qos::service_level_options::workload_type> : fmt::formatter<string_view> {
    auto format(qos::service_level_options::workload_type wt, fmt::format_context& ctx) const {
        return formatter<string_view>::format(qos::service_level_options::to_string(wt), ctx);
    }
};
