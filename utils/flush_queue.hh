/*
 * Copyright 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <map>
#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>

#include "seastarx.hh"

namespace utils {

/*
 * Small utility to order func()->post() operation
 * so that the "post" step is guaranteed to only be run
 * when all func+post-ops for lower valued keys (T) are
 * completed.
 */
template<typename T, typename Comp = std::less<T>, typename Clock = steady_clock_type>
class flush_queue {
public:
    using timeout_clock = Clock;
    using time_point = typename timeout_clock::time_point;
    using promise_type = shared_promise<with_clock<timeout_clock>>;
private:
    // Lifting the restriction of a single "post"
    // per key; Use a "ref count" for each execution
    struct notifier {
        promise_type pr;
        size_t count = 0;
    };

    typedef std::map<T, notifier, Comp> map_type;
    typedef typename map_type::reverse_iterator reverse_iterator;
    typedef typename map_type::iterator iterator;

    map_type _map;
    // embed all ops in a seastar::gate as well
    // so we can effectively block incoming ops
    seastar::gate _gate;
    bool _chain_exceptions;

    template<typename Func>
    static auto call_helper(Func&& func, future<> f) {
        return f.then([func = std::move(func)] {
            return func();
        });
    }

    template<typename Func, typename Arg>
    static auto call_helper(Func&& func, future<Arg> f) {
        using futurator = futurize<std::result_of_t<Func(Arg&&)>>;
        try {
            return futurator::invoke(std::forward<Func>(func), f.get0());
        } catch (...) {
            return futurator::make_exception_future(std::current_exception());
        }
    }

    template<typename Func, typename... Args>
    static auto call_helper(Func&& func, future<std::tuple<Args...>> f) {
        using futurator = futurize<std::result_of_t<Func(std::tuple<Args&&...>)>>;
        try {
            return futurator::invoke(std::forward<Func>(func), f.get());
        } catch (...) {
            return futurator::make_exception_future(std::current_exception());
        }
    }

    template<typename... Types>
    static future<Types...> handle_failed_future(future<Types...> f, promise_type& pr) {
        assert(f.failed());
        auto ep = std::move(f).get_exception();
        pr.set_exception(ep);
        return make_exception_future<Types...>(ep);
    }
public:
    flush_queue(bool chain_exceptions = false)
        : _chain_exceptions(chain_exceptions)
    {}
    // we are repeatedly using lambdas with "this" captured.
    // allowing moving would not be wise.
    flush_queue(flush_queue&&) = delete;
    flush_queue(const flush_queue&) = delete;

    /*
     * Runs func() followed by post(), but guaranteeing that
     * all operations with lower <T> keys have completed before
     * post() is executed.
     *
     * Post is invoked on the return value of Func
     * Returns a future containing the result of post()
     *
     * Any exception from Func is forwarded to end result, but
     * in case of exception post is _not_ run.
     */
    template<typename Func, typename Post>
    auto run_with_ordered_post_op(T rp, Func&& func, Post&& post) {
        // Slightly eased restrictions: We allow inserting an element
        // if it is either larger than any existing one, or if it
        // already is in the map. If either condition is true we can
        // uphold the guarantee to enforce ordered "post" execution
        // and signalling of all larger elements.
        if (!_map.empty() && !_map.contains(rp) && rp < _map.rbegin()->first) {
            throw std::invalid_argument(format("Attempting to insert key out of order: {}", rp));
        }

        _gate.enter();
        _map[rp].count++;

        using futurator = futurize<std::result_of_t<Func()>>;

        return futurator::invoke(std::forward<Func>(func)).then_wrapped([this, rp, post = std::forward<Post>(post)](typename futurator::type f) mutable {
            auto i = _map.find(rp);
            assert(i != _map.end());

            using post_result = decltype(call_helper(std::forward<Post>(post), std::move(f)));

            auto run_post = [this, post = std::forward<Post>(post), f = std::move(f), i]() mutable {
                assert(i == _map.begin());
                return call_helper(std::forward<Post>(post), std::move(f)).then_wrapped([this, i](post_result f) {
                    if (--i->second.count == 0) {
                        auto pr = std::move(i->second.pr);
                        assert(i == _map.begin());
                        _map.erase(i);
                        if (f.failed() && _chain_exceptions) {
                            return handle_failed_future(std::move(f), pr);
                        } else {
                            pr.set_value();
                        }
                    }
                    return f;
                });
            };

            if (i == _map.begin()) {
                return run_post();
            }

            --i;

            return i->second.pr.get_shared_future().then(std::move(run_post));
        }).finally([this] {
            // note: would have liked to use "with_gate", but compiler fails to
            // infer return type then, since we use "auto" because of future
            // chaining
            _gate.leave();
        });
    }
private:
    // waits for the entry at "i" to complete (and thus all before it)
    future<> wait_for_pending(reverse_iterator i, time_point timeout = time_point::max()) {
        if (i == _map.rend()) {
            return make_ready_future<>();
        }
        return i->second.pr.get_shared_future(timeout);
    }
public:
    // Waits for all operations currently active to finish
    future<> wait_for_pending(time_point timeout = time_point::max()) {
        return wait_for_pending(_map.rbegin(), timeout);
    }
    // Waits for all operations whose key is less than or equal to "rp"
    // to complete
    future<> wait_for_pending(T rp, time_point timeout = time_point::max()) {
        auto i = _map.upper_bound(rp);
        return wait_for_pending(reverse_iterator(i), timeout);
    }
    bool empty() const {
        return _map.empty();
    }
    size_t size() const {
        return _map.size();
    }
    bool has_operation(T rp) const {
        return _map.contains(rp);
    }
    T highest_key() const {
        return _map.rbegin()->first;
    }
    // Closes this queue
    future<> close() {
        return _gate.close();
    }
};

}
