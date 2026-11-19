/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <functional>

#include <fmt/format.h>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

#include "replica/logstor/segment_types.hh"
#include "seastarx.hh"

namespace replica::logstor {

extern seastar::logger logstor_logger;

// A shared reference to a segment. The last copy that goes releases the segment, and the release
// frees the segment unless a copy kept it with keep_on_release() because records in the segment
// can still be needed.
// The state calls `on_kept` at each keep_on_release(), and `on_last_release` when the last copy
// goes, also for a kept segment.
class segment_ref {
    struct state {
        log_segment_id id;
        std::function<void()> on_last_release;
        std::function<void()> on_kept;
        ~state() {
            if (on_last_release) on_last_release();
        }
    };
    lw_shared_ptr<state> _state;
public:
    segment_ref() = default;

    // Copyable: copying increments the shared ref count
    segment_ref(const segment_ref&) = default;
    segment_ref& operator=(const segment_ref&) = default;
    segment_ref(segment_ref&&) noexcept = default;
    segment_ref& operator=(segment_ref&&) noexcept = default;

    log_segment_id id() const noexcept { return _state->id; }
    bool empty() const noexcept { return !_state; }

    void keep_on_release() noexcept {
        if (_state && _state->on_kept) {
            _state->on_kept();
        }
    }

private:
    friend class segment_manager_impl;
    explicit segment_ref(log_segment_id id, std::function<void()> on_last_release, std::function<void()> on_kept)
        : _state(make_lw_shared<state>(id, std::move(on_last_release), std::move(on_kept)))
    {}
};

// Holds a reference to a segment until a step for the records in the segment completes, for
// example their write to the separator. The destructor calls keep_on_release() unless the owner
// called cancel(), so that the store does not free a segment that the index can still point into.
class segment_keep_guard {
    segment_ref _ref;
public:
    segment_keep_guard() = default;
    explicit segment_keep_guard(segment_ref ref) noexcept : _ref(std::move(ref)) {}

    segment_keep_guard(segment_keep_guard&&) noexcept = default;
    // The target must be empty, because the assignment would drop its segment without
    // keep_on_release().
    segment_keep_guard& operator=(segment_keep_guard&& o) noexcept {
        if (!_ref.empty()) [[unlikely]] {
            on_fatal_internal_error(logstor_logger, fmt::format("Move-assigning to a segment keep guard that holds {}", _ref.id()));
        }
        _ref = std::move(o._ref);
        return *this;
    }

    ~segment_keep_guard() {
        _ref.keep_on_release();
    }

    const segment_ref& ref() const noexcept { return _ref; }

    // Drops the reference without keep_on_release().
    // The caller must call cancel() only after the step completed.
    void cancel() noexcept { _ref = segment_ref(); }
};

}
