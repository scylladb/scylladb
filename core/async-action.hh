/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef ASYNC_ACTION_HH_
#define ASYNC_ACTION_HH_

#include "future.hh"
#include "reactor.hh"

// The AsyncAction concept represents an action which can complete later than
// the actual function invocation. It is represented by a function which
// returns a future which resolves when the action is done.

template<typename AsyncAction, typename StopCondition>
static inline
void do_until_continued(StopCondition&& stop_cond, AsyncAction&& action, promise<> p) {
    while (!stop_cond()) {
        auto&& f = action();
        if (!f.available()) {
            f.then([action = std::forward<AsyncAction>(action),
                    stop_cond = std::forward<StopCondition>(stop_cond), p = std::move(p)]() mutable {
                do_until_continued(stop_cond, action, std::move(p));
            });
            return;
        }

        if (f.failed()) {
            f.forward_to(std::move(p));
            return;
        }
    }

    p.set_value();
}

// Invokes given action until it fails or given condition evaluates to true.
template<typename AsyncAction, typename StopCondition>
static inline
future<> do_until(StopCondition&& stop_cond, AsyncAction&& action) {
    promise<> p;
    auto f = p.get_future();
    do_until_continued(std::forward<StopCondition>(stop_cond),
        std::forward<AsyncAction>(action), std::move(p));
    return f;
}

// Invoke given action undefinitely. Next invocation starts when previous completes or fails.
template<typename AsyncAction>
static inline
void keep_doing(AsyncAction&& action) {
    while (true) {
        auto f = action();
        if (!f.available()) {
            f.then([action = std::forward<AsyncAction>(action)] () mutable {
                 keep_doing(std::forward<AsyncAction>(action));
            });
            return;
        }
    }
}

#endif
