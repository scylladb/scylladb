/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/format.hh>
#include <exception>

#include "seastarx.hh"

namespace compaction {

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

// Thrown by the scrub compaction in abort mode, when the abort
// was caused by a validation error with the sstable.
class scrub_abort_invalid_sstable_compaction_aborted_exception : public compaction_aborted_exception {
public:
    scrub_abort_invalid_sstable_compaction_aborted_exception(sstring ks, sstring cf, sstring reason)
        : compaction_aborted_exception(std::move(ks), std::move(cf), std::move(reason)) {}
};

} // namespace compaction
