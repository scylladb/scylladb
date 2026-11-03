/*
 * Copyright 2026-present ScyllaDB
 *
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <optional>
#include <utility>

#include "db/timeout_clock.hh"
#include "service/client_state.hh"
#include "utils/assert.hh"

namespace alternator {

/// State belonging to one HTTP request, including its optional timeout override.
class client_state : public service::client_state {
    std::optional<db::timeout_clock::time_point> _request_deadline;

    client_state(service::client_state&& state, std::optional<db::timeout_clock::time_point> deadline)
        : service::client_state(std::move(state)), _request_deadline(deadline) {}
public:
    using service::client_state::client_state;

    // A request has one deadline: set it once, before the operation starts.
    void set_request_deadline(db::timeout_clock::time_point deadline) {
        throwing_assert(!_request_deadline);
        _request_deadline = deadline;
    }

    std::optional<db::timeout_clock::time_point> request_deadline() const {
        return _request_deadline;
    }

    /// A shard-safe snapshot that preserves the original request deadline.
    struct client_state_for_another_shard {
        service::client_state::client_state_for_another_shard state;
        std::optional<db::timeout_clock::time_point> request_deadline;

        client_state get() const {
            return client_state(state.get(), request_deadline);
        }
    };

    // Hide the base method: reconstructing only service::client_state would
    // discard the HTTP deadline when an operation moves to another shard.
    client_state_for_another_shard move_to_other_shard() const {
        return {service::client_state::move_to_other_shard(), _request_deadline};
    }
};

}
