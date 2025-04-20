/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/on_internal_error.hh>
#include <seastar/util/log.hh>

#include "on_internal_error.hh"

static seastar::logger on_internal_error_logger("on_internal_error");

namespace utils {

[[noreturn]] void on_internal_error(std::string_view reason) {
    seastar::on_internal_error(on_internal_error_logger, reason);
}

}