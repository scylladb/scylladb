/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <functional>

#include <seastar/core/shared_ptr.hh>

#include "replica/logstor/segment_types.hh"
#include "seastarx.hh"

namespace replica::logstor {

class segment_ref {
    struct state {
        log_segment_id id;
        std::function<void()> on_last_release;
        std::function<void()> on_failure;
        bool flush_failure{false};
        ~state() {
            if (!flush_failure) {
                if (on_last_release) on_last_release();
            } else {
                if (on_failure) on_failure();
            }
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

    void set_flush_failure() noexcept { if (_state) _state->flush_failure = true; }

private:
    friend class segment_manager_impl;
    explicit segment_ref(log_segment_id id, std::function<void()> on_last_release, std::function<void()> on_failure)
        : _state(make_lw_shared<state>(id, std::move(on_last_release), std::move(on_failure)))
    {}
};

}
