/*
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
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

// Use this instead of SCYLLA_ASSERT() or assert() in code that is used while parsing SSTables.
// SSTables can be corrupted either by ScyllaDB itself or by a freak accident like cosmic background
// radiation hitting the disk the wrong way. Either way a corrupt SSTable should not bring down the
// whole server. This method will call on_internal_error() if the condition is false.
// The exception will include a complete backtrace, so no need to add call-site identifiers to the message.
inline void parse_assert(bool condition, std::optional<component_name> filename = {}, const char* message = nullptr) {
    if (!condition) [[unlikely]] {
        on_parse_error(message ? message : sstring(), filename);
    }
}

struct bufsize_mismatch_exception : malformed_sstable_exception {
    bufsize_mismatch_exception(size_t size, size_t expected) :
        malformed_sstable_exception(format("Buffer improperly sized to hold requested data. Got: {:d}. Expected: {:d}", size, expected))
    {}
};

// Controls whether malformed sstable errors abort the process (generating a coredump) or throw an
// exception. Aborting is useful when the malformed sstable error is caused by memory corruption
// rather than actual sstable corruption, as it allows post-mortem analysis of the coredump.
// Controlled by the --abort-on-malformed-sstable-error command-line option.
// Returns the previous value of the flag.
bool set_abort_on_malformed_sstable_error(bool value) noexcept;
bool abort_on_malformed_sstable_error() noexcept;

// Use these helpers instead of directly throwing malformed_sstable_exception or
// bufsize_mismatch_exception. They check the abort_on_malformed_sstable_error flag and either
// abort the process (with logging) or throw the appropriate exception.
[[noreturn]] void throw_malformed_sstable_exception(sstring msg);
[[noreturn]] void throw_malformed_sstable_exception(sstring msg, component_name filename);
[[noreturn]] void throw_bufsize_mismatch_exception(size_t size, size_t expected);

// Disables aborting on malformed sstable errors for a scope.
//
// Intended for tests which intentionally corrupt sstables and expect
// malformed_sstable_exception to be thrown rather than the process aborting.
class scoped_no_abort_on_malformed_sstable_error {
    bool _prev;
public:
    scoped_no_abort_on_malformed_sstable_error() noexcept;
    ~scoped_no_abort_on_malformed_sstable_error();
};

}
