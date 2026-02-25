/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

/**
 * scoped_item_list<T> holds a list of items of type T, which are
 * automatically removed from the list when they go out of scope.
 * This is useful, for example, to hold a list of ongoing requests in a server
 * (on one particular shard), so that they can be listed by an API that wants
 * to list such requests (e.g., the "system.clients" virtual table).
 *
 * When adding an item to the list with the emplace() method, a handle is
 * returned which can be used to continue accessing the item. When this handle
 * goes out of scope, the item is automatically removed from the list.
 * 
 * Importantly, scoped_item_list<T> also provides for_each_gently() - a
 * stall-safe way to iterate over the list. for_each_gently() run a function
 * on each item in the list, but can preempt between items when our time quota
 * is finished, to avoid stalls.
 * 
 * For example, code that tracks ongoing requests in a server might look
 * like this:
 *
 * class request_data {
 *     ...
 * }
 * scoped_item_list<request_data> ongoing_requests;
 *
 * In the handler of each incoming request:
 *     auto req_handle = ongoing_requests.emplace(client_ip, client_port, ...);
 *     // the handler continues to run and perform its work.
 *     // If it wishes, it can use and modify req_handle.
 *     ...
 *     // when the handler is done, req_handle goes out of scope and the
 *     // request's entry is automatically removed from ongoing_requests.
 *
 * In the API that lists the ongoing requests:
 *     co_await ongoing_requests.for_each_gently([] (request_data& req) {
 *         // Do something with req, e.g., format it and add it to a
 *         // a chunked_vector.
 *         ...
 *      });
 *
 * NOTE:
 * scoped_item_list<T> currently assumes that when it is destroyed, it is
 * already empty, i.e., all handles returned by it had already been destroyed.
 * If this is not the case, on_fatal_internal_error() is called.
 * This assumption holds when used in the Alternator server, where the server
 * and the scoped_item_list<T> it contains, are only destroyed after
 * waiting for the _pending_requests gate to close, and it cannot close
 * before all calls to handle_api_request() are done. And since
 * handle_api_request() is the only place that holds scoped_item_list<T>
 * handles, when all these calls finished no handles can be held.
 * 
 * This class is inspired by the generic_server::connection class,
 * which uses similar code to hold ongoing requests in a list, but
 * is too tightly integrated into generic_server, which Alternator
 * cannot use because it uses Seastar's HTTP server for managing the
 * connections.
 */

#pragma once


#include <list>

#include <boost/intrusive/list.hpp>

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/noncopyable_function.hh>

#include "seastarx.hh"
#include "utils/on_internal_error.hh"

namespace utils {

template <typename T>
class scoped_item_list {
public:
    class handle : public boost::intrusive::list_base_hook<> {
        friend scoped_item_list;
        T value;
        scoped_item_list* owner;
        template <typename... Args>
        handle(scoped_item_list* o, Args&&... args)
            : value(std::forward<Args>(args)...), owner(o) {}
        // Note: call unlink() only if owner != nullptr, and after it set
        // owner to nullptr.
        void unlink() {
            // If we're in the middle of for_each_gently() on this list,
            // and it was planning to continue on this item after its
            // interruption, we have to help it by updating its iterator
            // to skip this item that we now plan to delete.
            typename scoped_item_list::list_type::iterator iter = owner->_list.iterator_to(*this);
            for (auto&& gi : owner->_gentle_iterators) {
                if (gi.iter == iter) {
                    gi.iter++;
                }
            }
            owner->_list.erase(iter);
        }
    public:
        ~handle() {
            if (owner) {
                unlink();
            }
        }
        T& get() { return value; }
        T& operator*() { return value; }
        handle(handle&& other) noexcept :
                value(std::move(other.value)), owner(other.owner) {
            if (owner) {
                // other was linked in its owner's list, we need to unlink it
                // and add this handle to the same list instead.
                other.unlink();
                other.owner = nullptr;
                owner->_list.push_back(*this);
            }
        }
        handle& operator=(handle&& other) = delete;
        handle(const handle&) = delete;
        handle& operator=(const handle&) = delete;
    };

    using list_type = boost::intrusive::list<handle, boost::intrusive::constant_time_size<false>>;
    list_type _list;

    template <typename... Args>
    handle emplace(Args&&... args) {
        handle ret(this, std::forward<Args>(args)...);
        _list.push_back(ret);
        return ret;
    }
    scoped_item_list() = default;
    ~scoped_item_list() {
        // We usually expect that when the scoped_item_list<T> is destroyed,
        // the list is already empty. For example, in the Alternator server,
        // the list is destroyed when the server object is destroyed, and the
        // server is only destroyed after all requests are done - so all
        // handles those requests held to list items have been destroyed, so
        // the list is empty.
        // But if for some reason the list is not empty, it's not a disaster
        // either - we just remove the "owner" pointer from all the handles.
        // Each handle will continue to hold its item as before, but it is
        // detached from any list.
        for (auto& h : _list) {
            h.owner = nullptr;
        }
        // We don't expect that the scoped_item_list<T> be destroyed while any
        // for_each_gently() are still running. In general trying to do that
        // doesn't make sense - since for_each_gently() is a method of the
        // object being destroyed. We could have allowed this by making the
        // _gentle_iterators list an intrusive list as well, but it's not
        // worth the effort, since we don't expect this to ever happen.
        if (!_gentle_iterators.empty()) {
            utils::on_fatal_internal_error("scoped_item_list destroyed while "
                "for_each_gently() is still running");
        }
    }
    scoped_item_list(const scoped_item_list&) = delete;
    scoped_item_list& operator=(const scoped_item_list&) = delete;

    bool empty() const { return _list.empty(); }

    struct gentle_iterator {
        list_type::iterator iter, end;
        gentle_iterator(scoped_item_list<T>& s) : iter(s._list.begin()), end(s._list.end()) {}
        gentle_iterator(const gentle_iterator&) = delete;
        gentle_iterator(gentle_iterator&&) = delete;
    };
    std::list<gentle_iterator> _gentle_iterators;

    // for_each_gently() is a stall-safe way to iterate over the list, i.e.,
    // run a given function fn on each item in the list, possibly preempting
    // between items when our time quota is finished, to avoid stalls.
    // Note that the function fn cannot be preempted (it does not return a
    // future) - it would not be safe if it did, and the item it was working
    // on was destroyed.
    future<> for_each_gently(noncopyable_function<void(T&)> fn) {
        _gentle_iterators.emplace_front(*this);
        typename std::list<gentle_iterator>::iterator gi = _gentle_iterators.begin();
        return seastar::do_until([ gi ] { return gi->iter == gi->end; },
            [ gi, fn = std::move(fn) ] {
                fn((gi->iter++)->get());
                return make_ready_future<>();
            }
        ).finally([ this, gi ] { _gentle_iterators.erase(gi); });
    }

    // Returns the number of items in the list. Note that we use
    // boost::intrusive::constant_time_size<false> so size() has O(N)
    // complexity, so should be avoided in performance critical code.
    size_t size() const {
        return _list.size();
    }
};

} // namespace utils