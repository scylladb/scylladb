/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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
#include <optional>
#include "exceptions/exceptions.hh"

namespace cql3 {
class query_processor;
}

namespace service {
class query_state;
}

namespace qos {

enum class include_effective_names { yes, no };

/*
 * for functions that execute queries, this is used to determine whether to execute
 * the query with internal client_state with an 'infinite' timeout, or a default
 * state with short timeout.
 * for queries that are executed in context of group0 operations it is important to have
 * a long timeout so the query doesn't fail the group0 client spuriously. in the context
 * of user commands, however, a shorter timeout is preferred. in other cases, the default
 * unspecified behavior may be sufficient.
 */
enum class query_context { group0, user, unspecified };

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

    using shares_type = std::variant<unset_marker, delete_marker, int32_t>;
    shares_type shares = unset_marker{};

    std::optional<sstring> shares_name; // service level name, if shares is set

    service_level_options replace_defaults(const service_level_options& other) const;
    // Merges the values of two service level options. The semantics depends
    // on the type of the parameter - e.g. for timeouts, a min value is preferred.
    service_level_options merge_with(const service_level_options& other) const;

    bool operator==(const service_level_options& other) const = default;

    static sstring to_string(timeout_type);

    static std::string_view to_string(const workload_type& wt);
    static std::optional<workload_type> parse_workload_type(std::string_view sv);

    static sstring to_string(shares_type);

    struct slo_effective_names {
        sstring timeout;
        sstring workload;
        sstring shares;

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
class service_level_argument_exception : public exceptions::invalid_request_exception {
public:
    using exceptions::invalid_request_exception::invalid_request_exception;
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

service::query_state& qos_query_state(qos::query_context ctx = qos::query_context::unspecified);

future<service_levels_info> get_service_levels(cql3::query_processor& qp, std::string_view ks_name, std::string_view cf_name, db::consistency_level cl, qos::query_context ctx);
future<service_levels_info> get_service_level(cql3::query_processor& qp, std::string_view ks_name, std::string_view cf_name, sstring service_level_name, db::consistency_level cl);

class service_level_scheduling_groups_exhausted : public std::runtime_error {
public:
   static constexpr const char* msg = "Can't create scheduling group for {}, consider removing this service level or some other service level";
   service_level_scheduling_groups_exhausted(sstring name) : std::runtime_error(format(msg, name)) {
   }
};

}

template <> struct fmt::formatter<qos::service_level_options::timeout_type> : fmt::formatter<sstring> {
    auto format(qos::service_level_options::timeout_type tt, fmt::format_context& ctx) const {
        return formatter<sstring>::format(qos::service_level_options::to_string(tt), ctx);
    }
};

template <> struct fmt::formatter<qos::service_level_options::workload_type> : fmt::formatter<string_view> {
    auto format(qos::service_level_options::workload_type wt, fmt::format_context& ctx) const {
        return formatter<string_view>::format(qos::service_level_options::to_string(wt), ctx);
    }
};

template <> struct fmt::formatter<qos::service_level_options::shares_type> : fmt::formatter<sstring> {
    auto format(qos::service_level_options::shares_type st, fmt::format_context& ctx) const {
        return formatter<sstring>::format(qos::service_level_options::to_string(st), ctx);
    }
};
