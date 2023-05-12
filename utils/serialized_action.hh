/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <functional>
#include <seastar/core/semaphore.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/util/later.hh>
#include <seastar/core/abort_source.hh>

// An async action wrapper which ensures that at most one action
// is running at any time.
class serialized_action {
public:
    template <typename... T>
    using future = seastar::future<T...>;
private:
    std::function<future<>()> _func;
    seastar::shared_future<> _pending;
    seastar::semaphore _sem;
private:
    future<> do_trigger() {
        _pending = {};
        return futurize_invoke(_func);
    }
public:
    serialized_action(std::function<future<>()> func)
        : _func(std::move(func))
        , _sem(1)
    { }
    serialized_action(serialized_action&&) = delete;
    serialized_action(const serialized_action&) = delete;

    // Makes sure that a new action will be started after this call and
    // returns a future which resolves when that action completes.
    // At most one action can run at any given moment.
    // A single action is started on behalf of all earlier triggers.
    //
    // When action is not currently running, it is started immediately if !later or
    // at some point in time soon after current fiber defers when later is true.
    future<> trigger(bool later = false, seastar::abort_source* as = nullptr) {
        if (_pending.valid()) {
            return as ? _pending.get_future(*as) : _pending.get_future();
        }
        seastar::shared_promise<> pr;
        _pending = pr.get_shared_future();
        future<> ret = _pending;
        std::optional<future<>> abortable;

        if (as) {
            abortable = _pending.get_future(*as);
        }

        // run in background, synchronize using `ret`
        (void)_sem.wait().then([this, later] () mutable {
            if (later) {
                return seastar::yield().then([this] () mutable {
                    return do_trigger();
                });
            }
            return do_trigger();
        }).then_wrapped([pr = std::move(pr)] (auto&& f) mutable {
            if (f.failed()) {
                pr.set_exception(f.get_exception());
            } else {
                pr.set_value();
            }
        });

        ret = ret.finally([this] {
            _sem.signal();
        });

        if (abortable) {
            // exception will be delivered to each individual future as well
            (void)std::move(ret).handle_exception([] (std::exception_ptr ep) {});
            return std::move(*abortable);
        }

        return ret;
    }

    // Like trigger() but can be aborted
    future<> trigger(seastar::abort_source& as) {
        return trigger(false, &as);
    }

    // Like trigger(), but defers invocation of the action to allow for batching
    // more requests.
    future<> trigger_later() {
        return trigger(true);
    }

    // Waits for all invocations initiated in the past.
    future<> join() {
        return get_units(_sem, 1).discard_result();
    }

    // The adaptor is to be used as an argument to utils::observable.observe()
    // When the notification happens the adaptor just triggers the action
    // Note, that all arguments provided by the notification callback are lost,
    // its up to the action to get the needed values
    // Also, the future<> returned from .trigger() is ignored, the action code
    // runs in the background. The user should .join() the action if it needs
    // to wait for it to finish on stop/drain/shutdown
    class observing_adaptor {
        friend class serialized_action;
        serialized_action& _action;
        observing_adaptor(serialized_action& act) noexcept : _action(act) {}

    public:
        template <typename... Args>
        void operator()(Args&&...) { (void)_action.trigger(); };
    };

    observing_adaptor make_observer() noexcept {
        return observing_adaptor(*this);
    }
};
