/*
 * Copyright 2015 ScyllaDB
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

namespace utils {

/*
 * Small utility to order func()->post() operation
 * so that the "post" step is guaranteed to only be run
 * when all func+post-ops for lower valued keys (T) are
 * completed.
 */
template<typename T, typename Comp = std::less<T>>
class flush_queue {
private:
    enum class state {
        running,
        waiting,
    };

    struct notifier {
        state s;
        // to signal when "post" action may run
        promise<> pr;
        // to carry any "func" exception
        std::exception_ptr ex;
        // to wait for before issuing next post
        std::experimental::optional<future<>> done;

        future<> signal() {
            if (ex) {
                pr.set_exception(ex);
            } else {
                pr.set_value();
            }
            return std::move(*done);
        }
    };

    typedef std::map<T, notifier, Comp> map_type;
    typedef typename map_type::reverse_iterator reverse_iterator;

    map_type _map;
    // embed all ops in a seastar::gate as well
    // so we can effectively block incoming ops
    seastar::gate _gate;

    // Adds/updates an entry to be called once "rp" maps the lowest, finished
    // operation
    template<typename Post>
    future<> add_callback(T rp, Post&& post, state s = state::running) {
        promise<> pr;
        auto fut = pr.get_future();

        notifier n;
        n.s = s;
        n.done = n.pr.get_future().then(std::forward<Post>(post)).then_wrapped([this, pr = std::move(pr)](future<> f) mutable {
            f.forward_to(std::move(pr));
        });

        // Do not use emplace, since we might want to overwrite
        _map[rp] = std::move(n);

        // We also go through gate the whole func + post chain
        _gate.enter();
        return fut.finally([this] {
            _gate.leave();
        });
    }

public:
    /*
     * Runs func() followed by post(), but guaranteeing that
     * all operations with lower <T> keys have completed before
     * post() is executed.
     *
     * Returns a future containing the result of post()
     *
     * Func & Post are both restricted to return future<>
     * Any exception from Func is forwarded to end result, but
     * in case of exception post is _not_ run.
     */
    template<typename Func, typename Post>
    future<> run_with_ordered_post_op(T rp, Func&& func, Post&& post) {
        assert(!_map.count(rp));
        assert(_map.empty() || _map.rbegin()->first < rp);

        auto fut = add_callback(rp, std::forward<Post>(post));

        using futurator = futurize<std::result_of_t<Func()>>;

        futurator::apply(std::forward<Func>(func)).then_wrapped([this, rp](future<> f) {
            auto i = _map.find(rp);
            // mark us as done (waiting for notofication)
            i->second.s = state::waiting;

            try {
                f.get();
            } catch (...) {
                i->second.ex = std::current_exception();
            }

            // if we are the first item, dequeue and signal all
            // that are currently waiting, starting with ourself
            if (i == _map.begin()) {
                return do_until([this] {return _map.empty() || _map.begin()->second.s != state::waiting;},
                        [this] {
                            auto i = _map.begin();
                            auto n = std::move(i->second);
                            _map.erase(i);
                            return n.signal();
                        }
                );
            }
            return make_ready_future<>();
        });
        return fut;
    }
private:
    // waits for the entry at "i" to complete (and thus all before it)
    future<> wait_for_pending(reverse_iterator i) {
        if (i == _map.rend()) {
            return make_ready_future<>();
        }
        auto n = std::move(i->second);
        auto s = n.s;
        return add_callback(i->first, [n = std::move(n)]() {
            // wait for original callback
            return n.signal();
        }, s);
    }
public:
    // Waits for all operations currently active to finish
    future<> wait_for_pending() {
        return wait_for_pending(_map.rbegin());
    }
    // Waits for all operations whose key is less than or equal to "rp"
    // to complete
    future<> wait_for_pending(T rp) {
        auto i = _map.upper_bound(rp);
        return wait_for_pending(reverse_iterator(i));
    }
    // Closes this queue
    future<> close() {
        return _gate.close();
    }
    // Poll-check that queue is still open
    void check_open_gate() {
        _gate.enter();
        _gate.leave();
    }
};

}
