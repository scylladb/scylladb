/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include <unordered_set>

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/defer.hh>

#include "log.hh"

#include "direct_failure_detector/failure_detector.hh"

namespace direct_failure_detector {

static logging::logger logger("direct_failure_detector");

// Each registered listener has a unique address, so we can use it to uniquely identify the listener.
using listener_id = listener*;

// Information about a listener registered on a given shard.
// Can be replicated to other shards, which treat the `id` as an opaque value (not a pointer).
struct listener_info {
    listener_id id;
    seastar::shard_id shard;
};

// Tracks the liveness of a given endpoint for a given listener threshold.
// See `endpoint_worker::ping_fiber()` and `endpoint_worker::notify_fiber()`.
struct endpoint_liveness {
    bool alive = false;
    bool marked_alive = false;
};

// Tracks the liveness of all endpoints managed by a given shard for all listeners with a given threshold.
struct listeners_liveness {
    // Vector of all listeners with the same threshold.
    std::vector<listener_info> listeners;

    // For each endpoint managed by this shard, the liveness state of this endpoint shared by all listeners in `listeners`.
    std::unordered_map<pinger::endpoint_id, direct_failure_detector::endpoint_liveness> endpoint_liveness;
};

enum class endpoint_update {
    added,
    removed
};

// Stores state used for pinging a single endpoint and notifying listeners about its liveness.
// The actual work is done in `ping_fiber()` and `notify_fiber()`.
struct endpoint_worker {
    failure_detector::impl& _fd;
    pinger::endpoint_id _id;

    // Used when this worker is destroyed, either because the endpoint is removed from detected set
    // or the failure detector service is stopped.
    abort_source _as;

    // When `ping_fiber()` changes the liveness state of an endpoint (`endpoint_liveness::alive`), it signals
    // this condition variable. `notify_fiber()` sleeps on it; on wake up sends a notification and marks
    // that it sent the update (`endpoint_liveness:marked_alive`)
    condition_variable _alive_changed;

    // Pings endpoints and updates `endpoint_liveness::alive`.
    // The only exception possibly returned from the future is `sleep_aborted` when destroying the worker.
    future<> ping_fiber() noexcept;
    future<> _ping_fiber = make_ready_future<>();

    // Waits for `endpoint_liveness::alive` to change and notifies listeners.
    // Updates `endpoint_liveness:marked_alive` to remember that a notification was sent.
    // The returned future is never exceptional.
    future<> notify_fiber() noexcept;
    future<> _notify_fiber = make_ready_future<>();

    endpoint_worker(failure_detector::impl&, pinger::endpoint_id);
    ~endpoint_worker();

    endpoint_worker(const endpoint_worker&) = delete;
    endpoint_worker(endpoint_worker&&) = delete;
};

struct failure_detector::impl {
    failure_detector& _parent;

    pinger& _pinger;
    clock& _clock;

    clock::interval_t _ping_period;
    clock::interval_t _ping_timeout;

    // Number of workers on each shard.
    // We use this to decide where to create new workers (we pick a shard with the smallest number of workers).
    // Used on shard 0 only.
    // The size of this vector is smp::count on shard 0 and it's empty on other shards.
    std::vector<size_t> _num_workers;

    // For each endpoint in the detected set, the shard of its worker.
    // Used on shard 0 only.
    std::unordered_map<pinger::endpoint_id, seastar::shard_id> _workers;

    // The {add/remove}_endpoint user API only inserts the request into `_endpoint_updates` and signals `_endpoint_changed`.
    // The actual add/remove operation (which requires cross-shard ops) is performed by update_endpoint_fiber(),
    // which waits on the condition variable and removes elements from this map.
    // Used on shard 0 only.
    std::unordered_map<pinger::endpoint_id, endpoint_update> _endpoint_updates;
    condition_variable _endpoint_changed;

    // Fetches endpoint updates from _endpoint_queue and performs the add/remove operation.
    // Runs on shard 0 only.
    future<> update_endpoint_fiber();
    future<> _update_endpoint_fiber = make_ready_future<>();

    // Workers running on this shard.
    using workers_map_t = std::unordered_map<pinger::endpoint_id, endpoint_worker>;
    workers_map_t _shard_workers;

    // For each threshold:
    // - the set of all listeners registered with this threshold (this is replicated to every shard),
    // - the liveness state of all endpoints managed by this shard for this threshold.
    //
    // Each `endpoint_worker` running on this shard is managing, for each threshold, the `endpoint_liveness` state
    // at `listeners_liveness::endpoint_liveness[ep]`, where `ep` is the endpoint of that worker.
    std::unordered_map<clock::interval_t, listeners_liveness> _listeners_liveness;

    // The listeners registered on this shard.
    std::unordered_set<listener*> _registered;

    // Listeners are unregistered by destroying their `subscription` objects.
    // The unregistering process requires cross-shard operations which we perform on this fiber.
    future<> _destroy_subscriptions = make_ready_future<>();

    impl(failure_detector& parent, pinger&, clock&, clock::interval_t ping_period, clock::interval_t ping_timeout);
    ~impl();

    // Inform update_endpoint_fiber() about an added/removed endpoint.
    void send_update_endpoint(pinger::endpoint_id, endpoint_update update);

    // Add an endpoint to the detected set.
    // Creates a worker on a shard with the smallest number of workers running.
    // Strong exception guarantee.
    future<> add_endpoint(pinger::endpoint_id);

    // Remove an endpoint from the detected set.
    // Destroys its worker.
    // Strong exception guarantee.
    future<> remove_endpoint(pinger::endpoint_id);

    // Create a worker on current shard for detecting the given endpoint.
    // Strong exception guarantee.
    void create_worker(pinger::endpoint_id);

    // Destroy a worker on current shard for the given endpoint.
    // Strong exception guarantee.
    future<> destroy_worker(pinger::endpoint_id);
    // The returned future is never exceptional:
    future<> destroy_worker(workers_map_t::iterator) noexcept;

    // Add information about a listener registered on shard `s` with threshold `t`
    // on the current shard so workers running on this shard can notify it.
    void add_listener(listener_id, clock::interval_t t, seastar::shard_id s);
    // Remove information about a registered listener from the current shard.
    void remove_listener(listener_id);

    // Send `mark_alive(ep)` (if `alive`) or `mark_dead(ep)` (otherwise) to `l`.
    future<> mark(listener* l, pinger::endpoint_id ep, bool alive);
};

failure_detector::failure_detector(
    pinger& pinger, clock& clock, clock::interval_t ping_period, clock::interval_t ping_timeout)
        : _impl(std::make_unique<impl>(*this, pinger, clock, ping_period, ping_timeout))
{}

failure_detector::impl::impl(
    failure_detector& parent, pinger& pinger, clock& clock, clock::interval_t ping_period, clock::interval_t ping_timeout)
        : _parent(parent), _pinger(pinger), _clock(clock), _ping_period(ping_period), _ping_timeout(ping_timeout) {
    if (this_shard_id() != 0) {
        return;
    }

    _num_workers.resize(smp::count, 0);
    _update_endpoint_fiber = update_endpoint_fiber();
}

void failure_detector::impl::send_update_endpoint(pinger::endpoint_id ep, endpoint_update update) {
    SCYLLA_ASSERT(this_shard_id() == 0);

    auto it = _endpoint_updates.find(ep);
    if (it == _endpoint_updates.end()) {
        _endpoint_updates.emplace(ep, update);
    } else {
        it->second = update;
    }

    _endpoint_changed.signal();
}

future<> failure_detector::impl::update_endpoint_fiber() {
    SCYLLA_ASSERT(this_shard_id() == 0);

    while (true) {
        co_await _endpoint_changed.wait([this] { return !_endpoint_updates.empty(); });

        auto it = _endpoint_updates.begin();
        auto [ep, update] = *it;
        try {
            if (update == endpoint_update::added) {
                co_await add_endpoint(ep);
            } else {
                co_await remove_endpoint(ep);
            }

            if (it->second == update) {
                // Safe to remove the entry.
                _endpoint_updates.erase(it);
            } else {
                // While we were updating the endpoint, the user requested the opposite update.
                // Need to handle this endpoint again.
            }

            continue;
        } catch (...) {
            logger.error("update_endpoint_fiber: failed to add/remove endpoint {}: {}", ep, std::current_exception());
        }

        // There was an exception.
        // Wait for a while before proceeding (although the operation should only fail due to OOM, so we're probably already doomed).
        // Note: `add_endpoint` and `remove_endpoint` provide strong exception guarantees, so we can retry the operation on the same endpoint.
        try {
            // Use a dummy abort source for the sleep.
            // We don't react to shutdowns so the sleep can theoretically prolong it a bit if it happens,
            // but this codepath should in practice never be reached in the first place, so whatever.
            abort_source as;
            co_await _clock.sleep_until(_clock.now() + 10 * _ping_period, as);
        } catch (sleep_aborted&) {}
    }
}

future<> failure_detector::impl::add_endpoint(pinger::endpoint_id ep) {
    SCYLLA_ASSERT(this_shard_id() == 0);

    if (_workers.contains(ep)) {
        co_return;
    }

    // Pick a shard with the smallest number of workers to create a new worker.
    auto shard = std::distance(_num_workers.begin(), std::min_element(_num_workers.begin(), _num_workers.end()));
    SCYLLA_ASSERT(_num_workers.size() == smp::count);

    ++_num_workers[shard];
    auto [it, _] = _workers.emplace(ep, shard);

    try {
        co_await _parent.container().invoke_on(shard, [ep] (failure_detector& fd) { fd._impl->create_worker(ep); });
    } catch (...) {
        --_num_workers[shard];
        _workers.erase(it);
        throw;
    }
}

future<> failure_detector::impl::remove_endpoint(pinger::endpoint_id ep) {
    SCYLLA_ASSERT(this_shard_id() == 0);

    auto it = _workers.find(ep);
    if (it == _workers.end()) {
        co_return;
    }

    auto shard = it->second;
    co_await _parent.container().invoke_on(shard, [ep] (failure_detector& fd) { return fd._impl->destroy_worker(ep); });

    SCYLLA_ASSERT(_num_workers.size() == smp::count);
    SCYLLA_ASSERT(shard < _num_workers.size());
    --_num_workers[shard];
    _workers.erase(it);

    // Note: removing endpoints may create imbalance of worker distribution across shards.
    // Right now we don't do anything with it, as we don't expect huge number of workers,
    // and if new workers are added eventually, balance will be restored.
    // Alternatively we could migrate running workers among shards but it's probably not worth it.
}

void failure_detector::impl::create_worker(pinger::endpoint_id ep) {
    // To provide strong exception guarantee.
    std::vector<deferred_action<noncopyable_function<void()>>> guards;

    guards.emplace_back([this, ep] { _shard_workers.erase(ep); });
    auto [worker_it, inserted] = _shard_workers.try_emplace(ep, *this, ep);
    if (!inserted) {
        // `failure_detector::impl::add_endpoint` checks `_workers` before creating a worker.
        // Since `add_endpoint` and `remove_endpoint` give strong exception guarantees, there must be no worker.
        // If there is, there's a bug.
        guards.back().cancel();
        on_internal_error(logger, format("attempted to create worker for endpoint {} when one already exists", ep));
    }

    for (auto& [_, l]: _listeners_liveness) {
        guards.emplace_back([&l = l, ep] { l.endpoint_liveness.erase(ep); });
        auto [it, inserted] = l.endpoint_liveness.emplace(ep, endpoint_liveness{});
        if (!inserted) {
            // `endpoint_liveness` entries in the `liveness` maps are created and destroyed together with `endpoint_workers`,
            // so the logic from comment above applies as well.
            guards.back().cancel();
            on_internal_error(logger, format("liveness info for endpoint {} already exists when trying to create a worker", ep));
        }
    }

    for (auto& g: guards) {
        g.cancel();
    }

    auto& worker = worker_it->second;
    worker._notify_fiber = worker.notify_fiber();
    worker._ping_fiber = worker.ping_fiber();
}

future<> failure_detector::impl::destroy_worker(pinger::endpoint_id ep) {
    auto it = _shard_workers.find(ep);
    if (it == _shard_workers.end()) {
        // If `destroy_worker` was invoked it means that `_workers` contained `ep`.
        // Since `add_endpoint` and `remove_endpoint` give strong exception guarantees, the worker must be present. If not, there's a bug.
        on_internal_error(logger, format("attempted to destroy worker for endpoint {} but no such worker exists", ep));
    }
    return destroy_worker(it);
}

future<> failure_detector::impl::destroy_worker(workers_map_t::iterator it) noexcept {
    auto& worker = it->second;

    worker._as.request_abort();

    try {
        co_await std::exchange(worker._ping_fiber, make_ready_future<>());
    } catch (sleep_aborted&) {
        // Expected, ignore.
    } catch (...) {
        // Unexpected exception, log and continue.
        logger.error("unexpected exception from ping_fiber when destroying worker for endpoint {}: {}", it->first, std::current_exception());
    }

    // Mark the endpoint dead for all listeners which still consider it alive.
    // ping_fiber() is running no more so it's safe to adjust the `alive` flags.
    for (auto& [_, l]: _listeners_liveness) {
        l.endpoint_liveness[it->first].alive = false;
    }
    worker._alive_changed.signal();

    try {
        // The fiber will stop after checking `_as.abort_requested()`.
        co_await std::exchange(worker._notify_fiber, make_ready_future<>());
    } catch (...) {
        // Unexpected exception, log and continue.
        logger.error("unexpected exception from notify_fiber when destroying worker for endpoint {}: {}", it->first, std::current_exception());
    }

    for (auto& [_, l]: _listeners_liveness) {
        l.endpoint_liveness.erase(it->first);
    }
    _shard_workers.erase(it);
}

endpoint_worker::endpoint_worker(failure_detector::impl& fd, pinger::endpoint_id id)
        : _fd(fd), _id(id) {
}

endpoint_worker::~endpoint_worker() {
    SCYLLA_ASSERT(_ping_fiber.available());
    SCYLLA_ASSERT(_notify_fiber.available());
}

future<subscription> failure_detector::register_listener(listener& l, clock::interval_t threshold) {
    // The pointer acts as a listener ID.
    if (!_impl->_registered.insert(&l).second) {
        throw std::runtime_error{format("direct_failure_detector: trying to register the same listener ({}) twice", fmt::ptr(&l))};
    }

    subscription s{*this, l};

    co_await container().invoke_on_all([l = &l, threshold, shard = this_shard_id()] (failure_detector& fd) {
        fd._impl->add_listener(l, threshold, shard);
    });

    co_return s;
}

subscription::subscription(failure_detector& fd, listener& l) noexcept
        : _fd(fd), _listener(&l) {
}

subscription::subscription(subscription&& s) noexcept
        : _fd(s._fd), _listener(s._listener) {
    // So the moved-from subscription does not deregister the listener when destroyed.
    s._listener = nullptr;
}

subscription::~subscription() {
    if (!_listener) {
        // We were moved from, nothing to do.
        return;
    }

    // Start by removing the listener from _registered set which prevents the failure detector from dereferencing the listener.
    if (!_fd._impl->_registered.erase(_listener)) {
        return;
    }

    // Cleaning up the data structures on each shard happens in the background.
    //
    // If we immediately register the same listener after deregistering it, we may have a problem.
    // Hence we require each listener to be registered at most once.
    _fd._impl->_destroy_subscriptions = _fd._impl->_destroy_subscriptions.then([l_ = _listener, &fd = _fd] () -> future<> {
        auto l = l_;
        try {
            co_await fd.container().invoke_on_all([l] (failure_detector& fd) {
                fd._impl->remove_listener(l);
            });
        } catch (...) {
            logger.error("unexpected exception when deregistering listener {}: {}", fmt::ptr(l), std::current_exception());
        }
    });
}

void failure_detector::impl::add_listener(listener_id id, clock::interval_t threshold, seastar::shard_id shard) {
    if (!_shard_workers.empty()) {
        throw std::runtime_error{"direct_failure_detector: trying to register a listener after endpoints were added"};
    }

    auto [it, _] = _listeners_liveness.try_emplace(threshold);
    it->second.listeners.push_back(listener_info{id, shard});
}

void failure_detector::impl::remove_listener(listener_id id) {
    // Linear search, but we don't expect a huge number of listeners and it's a rare operation.
    for (auto ll_it = _listeners_liveness.begin(); ll_it != _listeners_liveness.end(); ++ll_it) {
        auto& ll = ll_it->second;
        for (auto it = ll.listeners.begin(); it != ll.listeners.end(); ++it) {
            if (it->id == id) {
                ll.listeners.erase(it);
                if (ll.listeners.empty()) {
                    // No more listeners with this threshold.
                    // Remove the whole entry.
                    _listeners_liveness.erase(ll_it);
                }
                return;
            }
        }
    }
}

void failure_detector::add_endpoint(pinger::endpoint_id ep) {
    if (_impl) {
        _impl->send_update_endpoint(ep, endpoint_update::added);
    }
}

void failure_detector::remove_endpoint(pinger::endpoint_id ep) {
    if (_impl) {
        _impl->send_update_endpoint(ep, endpoint_update::removed);
    }
}

// Performs `pinger.ping(...)` but aborts it if `timeout` is reached first or externally aborted (by `as`).
static future<bool> ping_with_timeout(pinger::endpoint_id id, clock::timepoint_t timeout, abort_source& as, pinger& pinger, clock& c) {
    abort_source timeout_as;
    // External abort will also abort our operation.
    auto sub = as.subscribe([&timeout_as] () noexcept {
        if (!timeout_as.abort_requested()) {
            timeout_as.request_abort();
        }
    });

    auto f = pinger.ping(id, timeout_as);
    auto sleep_and_abort = [] (clock::timepoint_t timeout, abort_source& timeout_as, clock& c) -> future<> {
        co_await c.sleep_until(timeout, timeout_as).then_wrapped([&timeout_as] (auto&& f) {
            // Avoid throwing if sleep was aborted.
            if (f.failed() && timeout_as.abort_requested()) {
                // Expected (if ping() resolved first or we were externally aborted).
                f.ignore_ready_future();
                return make_ready_future<>();
            }
            return f;
        });
        if (!timeout_as.abort_requested()) {
            // We resolved before `f`. Abort the operation.
            timeout_as.request_abort();
        }
    }(timeout, timeout_as, c);

    bool result = false;
    std::exception_ptr ep;
    try {
        result = co_await std::move(f);
    } catch (...) {
        ep = std::current_exception();
    }

    if (!timeout_as.abort_requested()) {
        // `f` has already resolved, but abort the sleep.
        timeout_as.request_abort();
    }

    // Wait on the sleep as well (it should return shortly, being aborted) so we don't discard the future.
    try {
        co_await std::move(sleep_and_abort);
    } catch (...) {
        // There should be no other exceptions, but just in case... log it and discard,
        // we want to propagate exceptions from `f`, not from sleep.
        logger.error("unexpected exception from sleep_and_abort when pinging endpoint{}: {}", id, std::current_exception());
    }

    if (ep) {
        std::rethrow_exception(ep);
    }

    co_return result;
}

future<> endpoint_worker::ping_fiber() noexcept {
    auto& pinger = _fd._pinger;
    auto& clock = _fd._clock;

    // `last_response` is uninitialized until we get the first response to `ping()`.
    // That's fine since we don't use it until then (every use is protected with checking that at least one listener is `alive`,
    // which can only be true if there was a successful ping response).
    clock::timepoint_t last_response;

    while (!_as.abort_requested()) {
        bool success = false;
        auto start = clock.now();
        auto next_ping_start = start + _fd._ping_period;

        auto timeout = start + _fd._ping_timeout;
        // If there's a listener that's going to timeout soon (before the ping returns), we abort the ping in order to handle
        // the listener (mark it as dead).
        for (auto& [threshold, l]: _fd._listeners_liveness) {
            if (l.endpoint_liveness[_id].alive && last_response + threshold < timeout) {
                timeout = last_response + threshold;
            }
        }

        if (timeout > start) {
            try {
                success = co_await ping_with_timeout(_id, timeout, _as, pinger, clock);
            } catch (abort_requested_exception&) {
                if (_as.abort_requested()) {
                    // External abort, means we should stop.
                    co_return;
                }

                // Internal abort - the ping timed out. Continue.
                logger.debug("ping to endpoint {} timed out after {} clock ticks", _id, clock.now() - start);
            } catch (...) {
                // Unexpected exception, probably from `pinger.ping(...)`. Log and continue.
                logger.warn("unexpected exception when pinging {}: {}", _id, std::current_exception());
            }
        } else {
            // We have a listener which already timed out.
            // Abandon the ping, instead proceed to marking it dead below and do the ping in the next iteration.
            next_ping_start = start;
        }

        bool alive_changed = false;
        if (success) {
            last_response = clock.now();

            for (auto& [_, l]: _fd._listeners_liveness) {
                bool& alive = l.endpoint_liveness[_id].alive;
                if (!alive) {
                    alive = true;
                    alive_changed = true;
                }
            }
        } else {
            // Handle listeners which time-out before the next ping starts.
            // We could sleep until their threshold is actually crossed, but since we already know they will time-out
            // and there's no way to save them, it's simpler to just send the notifications immediately.
            for (auto& [threshold, l]: _fd._listeners_liveness) {
                bool& alive = l.endpoint_liveness[_id].alive;
                if (alive && last_response + threshold <= next_ping_start) {
                    alive = false;
                    alive_changed = true;
                }
            }
        }

        if (alive_changed) {
            _alive_changed.signal();
        }

        co_await clock.sleep_until(next_ping_start, _as);
    }
}

future<> endpoint_worker::notify_fiber() noexcept {
    auto all_listeners_dead = [this] {
        return std::none_of(_fd._listeners_liveness.begin(), _fd._listeners_liveness.end(),
                [id = _id] (auto& p) { return p.second.endpoint_liveness[id].alive; });
    };

    auto find_changed_liveness = [this] {
        return std::find_if(_fd._listeners_liveness.begin(), _fd._listeners_liveness.end(),
                [id = _id] (auto& p) {
                    auto& endpoint_liveness = p.second.endpoint_liveness[id];
                    return endpoint_liveness.alive != endpoint_liveness.marked_alive;
                });
    };

    while (true) {
        co_await _alive_changed.wait([&] {
            return (_as.abort_requested() && all_listeners_dead()) || find_changed_liveness() != _fd._listeners_liveness.end();
        });

        for (auto it = find_changed_liveness(); it != _fd._listeners_liveness.end(); it = find_changed_liveness()) {
            auto& listeners = it->second.listeners;
            auto& endpoint_liveness = it->second.endpoint_liveness[_id];
            bool alive = endpoint_liveness.alive;
            SCYLLA_ASSERT(alive != endpoint_liveness.marked_alive);
            endpoint_liveness.marked_alive = alive;

            try {
                co_await coroutine::parallel_for_each(listeners.begin(), listeners.end(), [this, endpoint = _id, alive] (const listener_info& listener) {
                    return _fd._parent.container().invoke_on(listener.shard, [listener = listener.id, endpoint, alive] (failure_detector& fd) {
                        return fd._impl->mark(listener, endpoint, alive);
                    });
                });
            } catch (...) {
                // Unexpected exception. If `mark` failed for some reason, there's not much we can do.
                // Log and continue.
                logger.error("unexpected exception when marking endpoint {} as {} for threshold {}: {}",
                        _id, alive ? "alive" : "dead", it->first, std::current_exception());
            }
        }

        // We check for shutdown at the end of the loop so we send final mark_dead notifications
        // before destroying the worker (see `failure_detector::impl::destroy_worker`).
        if (_as.abort_requested() && all_listeners_dead()) {
            co_return;
        }
    }
}

future<> failure_detector::impl::mark(listener* l, pinger::endpoint_id ep, bool alive) {
    // Check if the listener is still registered by the time we received the notification.
    if (!_registered.contains(l)) {
        return make_ready_future<>();
    }

    if (alive) {
        return l->mark_alive(ep);
    } else {
        return l->mark_dead(ep);
    }
}

future<> failure_detector::stop() {
    if (this_shard_id() != 0) {
        // Shard 0 coordinates the stop.
        co_return;
    }

    _impl->_endpoint_changed.broken(std::make_exception_ptr(abort_requested_exception{}));
    try {
        co_await std::exchange(_impl->_update_endpoint_fiber, make_ready_future<>());
    } catch (abort_requested_exception&) {
        // Expected.
    } catch (...) {
        // Unexpected exception, log and continue.
        logger.error("unexpected exception when stopping update_endpoint_fiber: {}", std::current_exception());
    }

    co_await container().invoke_on_all([] (failure_detector& fd) -> future<> {
        // All subscriptions must be destroyed before stopping the fd.
        SCYLLA_ASSERT(fd._impl->_registered.empty());

        // There are no concurrent `{create,destroy}_worker` calls running since we waited for `update_endpoint_fiber` to finish.
        while (!fd._impl->_shard_workers.empty()) {
            co_await fd._impl->destroy_worker(fd._impl->_shard_workers.begin());
        }

        co_await std::exchange(fd._impl->_destroy_subscriptions, make_ready_future<>());
    });

    // Destroy `impl` only after each shard finished its cleanup work (since cleanup may perform cross-shard ops).
    co_await container().invoke_on_all([] (failure_detector& fd) {
        fd._impl = nullptr;
    });
}

failure_detector::impl::~impl() {
    SCYLLA_ASSERT(_shard_workers.empty());
    SCYLLA_ASSERT(_destroy_subscriptions.available());
    SCYLLA_ASSERT(_update_endpoint_fiber.available());
}

failure_detector::~failure_detector() {
    SCYLLA_ASSERT(!_impl);
}

} // namespace direct_failure_detector
