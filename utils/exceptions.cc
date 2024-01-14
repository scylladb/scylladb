/*
 * Copyright 2015-present ScyllaDB
 */

/* SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/print.hh>
#include <seastar/rpc/rpc.hh>
#include <seastar/util/log.hh>
#include <seastar/util/backtrace.hh>
#include <seastar/core/abort_on_ebadf.hh>

#include <exception>
#include <system_error>
#include "exceptions.hh"
#include "utils/abi/eh_ia64.hh"

bool check_exception(system_error_lambda_t f)
{
    auto e = std::current_exception();
    if (!e) {
        return false;
    }

    try {
        std::rethrow_exception(e);
    } catch (std::system_error &e) {
        return f(e);
    } catch (...) {
        return false;
    }
    return false;
}

bool is_system_error_errno(int err_no)
{
    return check_exception([err_no] (const std::system_error &e) {
        auto code = e.code();
        return code.value() == err_no &&
               code.category() == std::system_category();
    });
}

bool should_stop_on_system_error(const std::system_error& e) {
    if (e.code().category() == std::system_category()) {
        // Whitelist of errors that don't require us to stop the server:
        switch (e.code().value()) {
        case EEXIST:
        case ENOENT:
            return false;
        default:
            break;
        }
    }
    return true;
}

bool is_timeout_exception(std::exception_ptr e) {
    if (try_catch<seastar::rpc::timeout_error>(e)) {
        return true;
    } else if (try_catch<seastar::semaphore_timed_out>(e)) {
        return true;
    } else if (try_catch<seastar::timed_out_error>(e)) {
        return true;
    } else if (const auto* ex = try_catch<const std::nested_exception>(e)) {
        return is_timeout_exception(ex->nested_ptr());
    }
    return false;
}

#if defined(OPTIMIZED_EXCEPTION_HANDLING_AVAILABLE)

#include <typeinfo>
#include "utils/abi/eh_ia64.hh"

void* utils::internal::try_catch_dynamic(std::exception_ptr& eptr, const std::type_info* catch_type) noexcept {
    // In both libstdc++ and libc++, exception_ptr has just one field
    // which is a pointer to the exception data
    void* raw_ptr = reinterpret_cast<void*&>(eptr);
    const std::type_info* ex_type = utils::abi::get_cxa_exception(raw_ptr)->exceptionType;

    // __do_catch can return true and set raw_ptr to nullptr, but only in the case
    // when catch_type is a pointer and a nullptr is thrown. try_catch_dynamic
    // doesn't work with catching pointers.
    if (catch_type->__do_catch(ex_type, &raw_ptr, 1)) {
        return raw_ptr;
    }
    return nullptr;
}

#endif // __GLIBCXX__
