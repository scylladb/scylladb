/*
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <concepts>
#include <seastar/core/print.hh>

#include "seastarx.hh"

namespace sstables {
class malformed_sstable_exception : public std::exception {
    sstring _msg;
public:
    malformed_sstable_exception(sstring msg, sstring filename)
        : malformed_sstable_exception{format("{} in sstable {}", msg, filename)}
    {}
    malformed_sstable_exception(sstring s) : _msg(s) {}
    const char *what() const noexcept {
        return _msg.c_str();
    }
};

[[noreturn]] void on_parse_error(sstring message, std::optional<component_name> filename);

// Use this instead of SCYLLA_ASSERT() or assert() in code that is used while parsing SSTables.
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

class compaction_job_exception : public std::exception {
    sstring _msg;
public:
    compaction_job_exception(sstring msg) noexcept : _msg(std::move(msg)) {}
    const char *what() const noexcept {
        return _msg.c_str();
    }
};

// Indicates that compaction was stopped via an external event,
// E.g. shutdown or api call.
class compaction_stopped_exception : public compaction_job_exception {
public:
    compaction_stopped_exception(sstring ks, sstring cf, sstring reason)
        : compaction_job_exception(format("Compaction for {}/{} was stopped due to: {}", ks, cf, reason)) {}
};

// Indicates that compaction hit an unrecoverable error
// and should be aborted.
class compaction_aborted_exception : public compaction_job_exception {
public:
    compaction_aborted_exception(sstring ks, sstring cf, sstring reason)
        : compaction_job_exception(format("Compaction for {}/{} was aborted due to: {}", ks, cf, reason)) {}
};

}

#if FMT_VERSION < 100000
 // fmt v10 introduced formatter for std::exception
 template <std::derived_from<sstables::malformed_sstable_exception> T>
 struct fmt::formatter<T> : fmt::formatter<string_view> {
     auto format(const T& e, fmt::format_context& ctx) const {
         return fmt::format_to(ctx.out(), "{}", e.what());
     }
 };
 #endif
