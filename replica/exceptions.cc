/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <concepts>
#include <sstream>

#include <seastar/util/log.hh>

#include "replica/exceptions.hh"
#include "utils/exceptions.hh"
#include "utils/result_try.hh"


namespace replica {

static exception_variant encode_replica_exception_or_fail(std::exception& ex) {
    if (auto* e = dynamic_cast<unknown_exception*>(&ex)) {
        return std::move(*e);
    } else if (auto* e = dynamic_cast<timeout_exception*>(&ex)) {
        return std::move(*e);
    } else if (auto* e = dynamic_cast<forward_exception*>(&ex)) {
        return std::move(*e);
    } else if (auto* e = dynamic_cast<virtual_table_update_exception*>(&ex)) {
        return std::move(*e);
    } else {
        if (is_timeout_exception(ex)) {
            return timeout_exception();
        }
    }
    return encode_unknown_replica_exception(std::current_exception());
}

exception_variant encode_replica_exception(std::exception& ex) noexcept {
    try {
        return encode_replica_exception_or_fail(ex);
    } catch (...) {
        // If an exception happened during construction of the result exception
        // (e.g. failed to allocate an error description string),
        // then a dummy exception will be returned.
        return unknown_exception(seastar::sstring());
    }
}

exception_variant encode_replica_exception(std::exception_ptr eptr) noexcept {
    try {
        std::rethrow_exception(std::move(eptr));
    } catch (std::exception& ex) {
        return encode_replica_exception(ex);
    } catch (...) {
        return encode_unknown_replica_exception(std::current_exception());
    }
}

exception_variant encode_unknown_replica_exception(std::exception_ptr eptr) noexcept {
    try {
        std::stringstream ss;
        ss << std::current_exception();
        return unknown_exception(std::move(ss).str());
    } catch (...) {
        // If an exception happened during construction of the result exception
        // (e.g. failed to allocate an error description string),
        // then a dummy exception will be returned.
        return unknown_exception(seastar::sstring());
    }
}

std::exception_ptr exception_variant::into_exception_ptr() noexcept {
    return std::visit([] <typename Ex> (Ex&& ex) {
        if constexpr (std::is_same_v<Ex, std::monostate>) {
            return std::make_exception_ptr(unknown_exception(seastar::sstring()));
        } else {
            return std::make_exception_ptr(std::move(ex));
        }
    }, std::move(reason));
}

}
