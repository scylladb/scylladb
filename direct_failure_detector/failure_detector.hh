/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include "utils/UUID.hh"

#include <seastar/core/sharded.hh>

using namespace seastar;

namespace seastar {
class abort_source;
}

namespace direct_failure_detector {

class pinger {
public:
    // Opaque endpoint ID.
    // A specific implementation of `pinger` maps those IDs to 'real' addresses.
    using endpoint_id = utils::UUID;

    // Send a message to `ep` and wait until it responds.
    // The wait can be aborted using `as`.
    // Abort should be signalized with `abort_requested_exception`.
    //
    // If the ping fails in an expected way (e.g. the endpoint is down and refuses to connect),
    // returns `false`. If it succeeds, returns `true`.
    virtual future<bool> ping(endpoint_id ep, abort_source& as) = 0;

protected:
    // The `pinger` object must not be destroyed through the `pinger` interface.
    // `failure_detector` does not take ownership of `pinger`, only a non-owning reference.
    ~pinger() = default;
};

// A clock that uses abstract units to measure time.
// The implementation is responsible for periodically advancing the clock.
//
// The timepoints used by the clock do not have to be related to wall-clock time. The clock is only used as a 'stopwatch' (for measuring intervals).
// For useful failure detection the clock should be steady:
// it should be monotonic and the time elapsed between two consecutive ticks should be ~constant.
class clock {
public:
    using timepoint_t = int64_t;
    using interval_t = int64_t;

    // Returns current time.
    // The returned values should be non-decreasing.
    virtual timepoint_t now() noexcept = 0;

    // Wait until `now()` >= `tp`.
    // The condition is guaranteed to be true when the future resolves unless the sleep was aborted using `as`.
    // If `now()` >= `tp` when the function is called, it returns a ready future.
    // Aborts should be signalized using `seastar::sleep_aborted`.
    virtual future<> sleep_until(timepoint_t tp, abort_source& as) = 0;

protected:
    // The `clock` object must not be destroyed through the `clock` interface.
    // `failure_detector` does not take ownership of `clock`, only a non-owning reference.
    ~clock() = default;
};

class listener {
public:
    // Called when an endpoint in the detected set (added by `failure_detector::add_endpoint`) responds to a ping
    // after being previously marked dead.
    virtual future<> mark_alive(pinger::endpoint_id) = 0;

    // Called when an endpoint in the detected set does not respond to a ping for a long enough time
    // after being previously marked 'alive'.
    // The time threshold is specified when registering the listener.
    //
    // A newly added endpoint is considered dead until it first responds.
    //
    // When an alive endpoint is removed from the detected set, a final mark_dead notification is sent.
    virtual future<> mark_dead(pinger::endpoint_id) = 0;

protected:
    // The `listener` object must not be destroyed through the `listener` interface.
    // `failure_detector` does not take ownership of `listener`s, only non-owning references.
    ~listener() = default;
};

class failure_detector;

// A RAII object returned when registering a `listener`.
// The listener is unregistered when this object is destroyed.
//
// All subscriptions must be destroyed before the failure detector service is stopped.
class subscription {
    failure_detector& _fd;
    listener* _listener;

    subscription(failure_detector&, listener&) noexcept;
    friend class failure_detector;

public:
    subscription(subscription&&) noexcept;

    // Unregisters the listener.
    ~subscription();
};

class failure_detector : public seastar::peering_sharded_service<failure_detector> {
    class impl;
    std::unique_ptr<impl> _impl;

    friend struct endpoint_worker;
    friend struct subscription;

public:
    failure_detector(
        pinger& pinger, clock& clock,

        // Every endpoint in the detected set will be periodically pinged every `ping_period`,
        // assuming that the pings return in a timely manner. A ping may take longer than `ping_period`
        // before it's aborted (up to `ping_timeout`), in which case the next ping will start immediately.
        //
        // The passed-in value must be the same on every shard.
        clock::interval_t ping_period,

        // Duration after which a ping is aborted, so that next ping can be started
        // (pings are sent sequentially).
        clock::interval_t ping_timeout
    );

    ~failure_detector();

    // Stop all tasks. Must be called before the object is destroyed.
    // All listener subscriptions must be destroyed before `stop()` is called.
    future<> stop();

    // Receive updates about endpoints in the detected set, where an endpoint is considered dead
    // if the duration between last `ping()` response and `clock.now()` passes `threshold`.
    // Note: the `mark_dead` notification may be sent earlier if we know ahead of time
    // that `threshold` will be crossed before the next `ping()` can start.
    //
    // Listeners must be added before any endpoints are added to the detected set (e.g. during Scylla startup).
    // The same listener& must not be registered twice (even if it was deregistered before the second time).
    //
    // The listener stops being called when the returned subscription is destroyed.
    // The subscription must be destroyed before service is stopped.
    //
    // `threshold` should be significantly larger than `ping_timeout`, preferably at least an order of magnitude larger.
    //
    // Different listeners may use different thresholds, depending on the use case:
    // some listeners may want to mark endpoints as dead more aggressively if fast reaction times are important
    // (e.g. it's important to elect a new Raft leader quickly when the current one is dead),
    // while others may prefer stability (e.g. if availability is not lost even though a dead endpoint is considered alive).
    //
    // Listeners can be registered on any shard. If your use case requires to have a listener on multiple shards,
    // you need to register a separate listener on each shard.
    future<subscription> register_listener(
            listener&,
            clock::interval_t threshold);

    // Add this endpoint to the detected set.
    // Has no effect if the endpoint is already there.
    // The newly added endpoing is initially considered dead for all listeners.
    // Run only on shard 0.
    void add_endpoint(pinger::endpoint_id);

    // Remove this endpoint from the detected set.
    // Has no effect if the endpoint is not there.
    // If the endpoint is considered alive when removed, a final mark_dead notification is sent to all listeners.
    // Run only on shard 0.
    void remove_endpoint(pinger::endpoint_id);
};

} // namespace direct_failure_detector
