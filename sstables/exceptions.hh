/*
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

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
    compaction_aborted_exception(std::string_view reason)
        : compaction_job_exception(format("Compaction was aborted due to: {}", reason)) {}
    compaction_aborted_exception(sstring ks, sstring cf, sstring reason)
        : compaction_job_exception(format("Compaction for {}/{} was aborted due to: {}", ks, cf, reason)) {}
};

}
