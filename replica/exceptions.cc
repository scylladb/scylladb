/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <stdexcept>
#include <type_traits>

#include "replica/exceptions.hh"
#include "utils/exceptions.hh"


namespace replica {

exception_variant try_encode_replica_exception(std::exception_ptr eptr, encode_timeouts enc_timeouts) {
    if (const auto* e = try_catch<const rate_limit_exception>(eptr)) return *e;
    if (const auto* e = try_catch<const stale_topology_exception>(eptr)) return *e;
    if (const auto* e = try_catch<const abort_requested_exception>(eptr)) return *e;
    if (const auto* e = try_catch<const critical_disk_utilization_exception>(eptr)) return *e;
    if (const auto* e = try_catch<const large_data_exception>(eptr)) return *e;
    if (enc_timeouts && is_timeout_exception(eptr)) return timed_out_error();
    return no_exception{};
}

std::exception_ptr exception_variant::into_exception_ptr() noexcept {
    return std::visit([] <typename Ex> (Ex&& ex) {
        if constexpr (std::is_same_v<Ex, unknown_exception>) {
            return std::make_exception_ptr(std::runtime_error("unknown exception"));
        } else {
            return std::make_exception_ptr(std::move(ex));
        }
    }, std::move(reason));
}

}
