/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "gms/inet_address.hh"
#include "streaming/stream_session.hh"
#include "streaming/session_info.hh"
#include "streaming/progress_info.hh"

namespace streaming {

class stream_event {
public:
    enum class type {
        STREAM_PREPARED,
        STREAM_COMPLETE,
        FILE_PROGRESS,
    };

    type event_type;
    streaming::plan_id plan_id;

    stream_event(type event_type_, streaming::plan_id plan_id_)
        : event_type(event_type_)
        , plan_id(plan_id_) {
    }
};

struct session_complete_event : public stream_event {
    using inet_address = gms::inet_address;
    inet_address peer;
    bool success;

    session_complete_event(shared_ptr<stream_session> session)
        : stream_event(stream_event::type::STREAM_COMPLETE, session->plan_id())
        , peer(session->peer)
        , success(session->is_success()) {
    }
};

struct progress_event : public stream_event {
    progress_info progress;
    progress_event(streaming::plan_id plan_id_, progress_info progress_)
        : stream_event(stream_event::type::FILE_PROGRESS, plan_id_)
        , progress(std::move(progress_)) {
    }
};

struct session_prepared_event : public stream_event {
    session_info session;
    session_prepared_event(streaming::plan_id plan_id_, session_info session_)
        : stream_event(stream_event::type::STREAM_PREPARED, plan_id_)
        , session(std::move(session_)) {
    }
};

} // namespace streaming
