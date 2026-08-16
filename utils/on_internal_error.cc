/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <exception>

#include <seastar/core/on_internal_error.hh>
#include <seastar/util/log.hh>
#include <seastar/core/format.hh>

#include "on_internal_error.hh"

static seastar::logger on_internal_error_logger("on_internal_error");

namespace utils {

[[noreturn]] void on_internal_error(std::string_view reason) {
    if (std::uncaught_exceptions() > 0) {
        // We are being called during stack unwinding. Throwing here would
        // trigger std::terminate and hide the original exception. Log the
        // situation and abort instead of throwing.
        on_internal_error_logger.error("on_internal_error called during stack unwinding: {}", reason);
        if (auto ep = std::current_exception()) {
            try {
                std::rethrow_exception(ep);
            } catch (const std::exception& e) {
                on_internal_error_logger.error("original exception that is being handled: {}", e.what());
            } catch (...) {
                on_internal_error_logger.error("original exception that is being handled is not a std::exception");
            }
        }
        seastar::on_fatal_internal_error(on_internal_error_logger, reason);
    }
    seastar::on_internal_error(on_internal_error_logger, reason);
}

[[noreturn]] void on_fatal_internal_error(std::string_view reason) noexcept {
    seastar::on_fatal_internal_error(on_internal_error_logger, reason);
}

[[noreturn]] void __assert_fail_on_internal_error(const char* expr, const char* file, int line, const char* function) {
    on_internal_error(fmt::format("Assertion '{}' failed at {}:{} in {}", expr, file, line, function));
}

} // namespace utils
