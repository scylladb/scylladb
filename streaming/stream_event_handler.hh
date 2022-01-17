/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "streaming/stream_event.hh"

namespace streaming {

class stream_event_handler /* extends FutureCallback<StreamState> */ {
public:
    /**
     * Callback for various streaming events.
     *
     * @see StreamEvent.Type
     * @param event Stream event.
     */
    virtual void handle_stream_event(session_complete_event event) {}
    virtual void handle_stream_event(progress_event event) {}
    virtual void handle_stream_event(session_prepared_event event) {}
    virtual ~stream_event_handler() {};
};

} // namespace streaming
