/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>
#include "gms/inet_address.hh"
#include "streaming/stream_coordinator.hh"
#include "streaming/stream_event_handler.hh"
#include "streaming/stream_state.hh"
#include "streaming/progress_info.hh"
#include <vector>

namespace streaming {
    using inet_address = gms::inet_address;
/**
 * A future on the result ({@link StreamState}) of a streaming plan.
 *
 * In practice, this object also groups all the {@link StreamSession} for the streaming job
 * involved. One StreamSession will be created for every peer involved and said session will
 * handle every streaming (outgoing and incoming) to that peer for this job.
 * <p>
 * The future will return a result once every session is completed (successfully or not). If
 * any session ended up with an error, the future will throw a StreamException.
 * <p>
 * You can attach {@link StreamEventHandler} to this object to listen on {@link StreamEvent}s to
 * track progress of the streaming.
 */
class stream_result_future {
public:
    streaming::plan_id plan_id;
    sstring description;
private:
    stream_manager& _mgr;
    shared_ptr<stream_coordinator> _coordinator;
    std::vector<stream_event_handler*> _event_listeners;
    promise<stream_state> _done;
    lowres_clock::time_point _start_time;
public:
    stream_result_future(stream_manager& mgr, streaming::plan_id plan_id_, sstring description_, bool is_receiving)
        : stream_result_future(mgr, plan_id_, description_, make_shared<stream_coordinator>(is_receiving)) {
        // Note: Origin sets connections_per_host = 0 on receiving side, We set 1 to
        // refelct the fact that we actually create one connection to the initiator.
    }

    /**
     * Create new StreamResult of given {@code planId} and type.
     *
     * Constructor is package private. You need to use {@link StreamPlan#execute()} to get the instance.
     *
     * @param planId Stream plan ID
     * @param description Stream description
     */
    stream_result_future(stream_manager& mgr, streaming::plan_id plan_id_, sstring description_, shared_ptr<stream_coordinator> coordinator_)
        : plan_id(std::move(plan_id_))
        , description(std::move(description_))
        , _mgr(mgr)
        , _coordinator(coordinator_)
        , _start_time(lowres_clock::now()) {
        // if there is no session to listen to, we immediately set result for returning
        if (!_coordinator->is_receiving() && !_coordinator->has_active_sessions()) {
            _done.set_value(get_current_state());
        }
    }

public:
    shared_ptr<stream_coordinator> get_coordinator() { return _coordinator; };

public:
    static future<stream_state> init_sending_side(stream_manager& mgr, streaming::plan_id plan_id_, sstring description_, std::vector<stream_event_handler*> listeners_, shared_ptr<stream_coordinator> coordinator_);
    static shared_ptr<stream_result_future> init_receiving_side(stream_manager& mgr, streaming::plan_id plan_id, sstring description, inet_address from);

public:
    void add_event_listener(stream_event_handler* listener) {
        // FIXME: Futures.addCallback(this, listener);
        _event_listeners.push_back(listener);
    }

    /**
     * @return Current snapshot of streaming progress.
     */
    stream_state get_current_state();

    void handle_session_prepared(shared_ptr<stream_session> session);


    void handle_session_complete(shared_ptr<stream_session> session);

    void handle_progress(progress_info progress);

    template <typename Event>
    void fire_stream_event(Event event);

    stream_manager& manager() noexcept { return _mgr; }
    const stream_manager& manager() const noexcept { return _mgr; }

private:
    void maybe_complete();
};

} // namespace streaming
