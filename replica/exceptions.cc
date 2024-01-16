/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <stdexcept>
#include <type_traits>

#include "replica/exceptions.hh"


namespace replica {

exception_variant try_encode_replica_exception(std::exception_ptr eptr) {
    try {
        std::rethrow_exception(std::move(eptr));
    } catch (rate_limit_exception&) {
        return rate_limit_exception();
    } catch (const stale_topology_exception& e) {
        return e;
    } catch (abort_requested_exception&) {
        return abort_requested_exception();
    } catch (...) {
        return no_exception{};
    }
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
