/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <unordered_map>
#include "unimplemented.hh"
#include <seastar/core/sstring.hh>
#include <seastar/core/enum.hh>
#include "utils/log.hh"
#include "seastarx.hh"

namespace unimplemented {

static thread_local std::unordered_map<cause, bool> _warnings;

static logging::logger ulogger("unimplemented");

std::string_view format_as(cause c) {
    switch (c) {
        case cause::API: return "API";
        case cause::INDEXES: return "INDEXES";
        case cause::TRIGGERS: return "TRIGGERS";
        case cause::METRICS: return "METRICS";
        case cause::VALIDATION: return "VALIDATION";
        case cause::REVERSED: return "REVERSED";
        case cause::HINT: return "HINT";
        case cause::SUPER: return "SUPER";
    }
    abort();
}

void warn(cause c) {
    if (!_warnings.contains(c)) {
        _warnings.insert({c, true});
        ulogger.debug("{}", format_as(c));
    }
}

void fail(cause c) {
    throw std::runtime_error(fmt::format("Not implemented: {}", format_as(c)));
}

}
