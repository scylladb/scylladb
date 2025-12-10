/*
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <concepts>
#include <seastar/core/format.hh>

#include "sstables/component_type.hh"
#include "seastarx.hh"

namespace sstables {
class malformed_sstable_exception : public std::exception {
    sstring _msg;
public:
    malformed_sstable_exception(sstring msg, component_name filename)
        : malformed_sstable_exception{format("{} in sstable {}", msg, filename)}
    {}
    malformed_sstable_exception(sstring s) : _msg(s) {}
    const char *what() const noexcept {
        return _msg.c_str();
    }
};

[[noreturn]] void on_parse_error(sstring message, std::optional<component_name> filename);
[[noreturn, gnu::noinline]] void on_bti_parse_error(uint64_t pos);

// Use this instead of scylla_assert() or assert() in code that is used while parsing SSTables.
// SSTables can be corrupted either by ScyllaDB itself or by a freak accident like cosmic background
// radiation hitting the disk the wrong way. Either way a corrupt SSTable should not bring down the
// whole server. This method will call on_internal_error() if the condition is false.
// The exception will include a complete backtrace, so no need to add call-site identifiers to the message.
inline void parse_assert(bool condition, std::optional<component_name> filename = {}, const char* message = nullptr) {
    if (!condition) [[unlikely]] {
        on_parse_error(message, filename);
    }
}

struct bufsize_mismatch_exception : malformed_sstable_exception {
    bufsize_mismatch_exception(size_t size, size_t expected) :
        malformed_sstable_exception(format("Buffer improperly sized to hold requested data. Got: {:d}. Expected: {:d}", size, expected))
    {}
};

}
