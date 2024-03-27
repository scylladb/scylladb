/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <any>

#include <boost/signals2.hpp>
#include <boost/signals2/dummy_mutex.hpp>

#include <seastar/core/shared_future.hh>
#include <seastar/util/noncopyable_function.hh>

using namespace seastar;

namespace bs2 = boost::signals2;

namespace gms {

class feature_service;

/**
 * A gossip feature tracks whether all the nodes the current one is
 * aware of support the specified feature.
 *
 * A feature should only be created once the gossiper is available.
 */
class feature final {
    using signal_type = bs2::signal_type<void (), bs2::keywords::mutex_type<bs2::dummy_mutex>>::type;

    feature_service* _service = nullptr;
    sstring _name;
    bool _enabled = false;
    mutable signal_type _s;
public:
    using listener_registration = std::any;
    class listener {
        friend class feature;
        bs2::scoped_connection _conn;
        signal_type::slot_type _slot;
        const signal_type::slot_type& get_slot() const { return _slot; }
        void set_connection(bs2::scoped_connection&& conn) { _conn = std::move(conn); }
        void callback() {
            _conn.disconnect();
            on_enabled();
        }
    protected:
        bool _started = false;
    public:
        listener() : _slot(signal_type::slot_type(&listener::callback, this)) {}
        listener(const listener&) = delete;
        listener(listener&&) = delete;
        listener& operator=(const listener&) = delete;
        listener& operator=(listener&&) = delete;
        // Has to run inside seastar::async context
        virtual void on_enabled() = 0;
    };
    explicit feature(feature_service& service, std::string_view name, bool enabled = false);
    feature() = default;
    ~feature();
    feature(const feature& other) = delete;
    // Has to run inside seastar::async context
    void enable();
    feature& operator=(feature&& other);
    const sstring& name() const {
        return _name;
    }
    operator bool() const {
        return _enabled;
    }
    void when_enabled(listener& callback) const {
        callback.set_connection(_s.connect(callback.get_slot()));
        if (_enabled) {
            _s();
        }
    }
    // Will call the callback functor when this feature is enabled, unless
    // the returned listener_registration is destroyed earlier.
    [[nodiscard("the listener_registration returned by when_enabled must be kept alive "
                "in order to keep the callback registered")]]
    listener_registration when_enabled(seastar::noncopyable_function<void()> callback) const {
        struct wrapper : public listener {
            seastar::noncopyable_function<void()> _func;
            wrapper(seastar::noncopyable_function<void()> func) : _func(std::move(func)) {}
            void on_enabled() override { _func(); }
        };
        auto holder = make_lw_shared<wrapper>(std::move(callback));
        when_enabled(*holder);
        return holder;
    }
};


} // namespace gms
