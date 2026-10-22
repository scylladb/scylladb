/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <any>

#include <boost/signals2/connection.hpp>
#include <boost/signals2/signal_type.hpp>
#include <boost/signals2/dummy_mutex.hpp>

#include <seastar/util/noncopyable_function.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <string_view>

using namespace seastar;

namespace bs2 = boost::signals2;

namespace gms {

/// The definition of one cluster feature: its name, and its index - its
/// position in the list of all the cluster features.
///
/// A definition holds no state. Whether the feature is enabled, and the
/// listeners waiting for it to be enabled, are the feature's value: a
/// gms::feature, kept in a feature_service, which holds the values of all the
/// definitions in an array indexed by index().
///
/// All the cluster features are defined in gms/feature_service.cc, and that is
/// the only file which must be recompiled when a feature is added. Code that
/// uses a feature declares it, like we declare loggers:
///
///     namespace gms::features {
///     extern const feature_definition my_feature;
///     }
///
/// and gets the feature's value with feature_service[my_feature].
class feature_definition {
    std::string_view _name;
    size_t _index;
public:
    // Should only be called in gms/feature_service.cc, which defines all the
    // features and assigns their indexes in the order of their definitions.
    explicit feature_definition(std::string_view name);
    feature_definition(const feature_definition&) = delete;
    feature_definition& operator=(const feature_definition&) = delete;
    std::string_view name() const noexcept {
        return _name;
    }
    size_t index() const noexcept {
        return _index;
    }
};

/**
 * The value of one cluster feature in one feature_service: whether the
 * feature is enabled - which happens once all the nodes in the cluster
 * support it - and the listeners waiting for it to be enabled.
 *
 * A feature_service holds one such value for each feature_definition, and
 * one for each feature it creates at run time (see
 * feature_service::add_feature()).
 */
class feature final {
    using signal_type = bs2::signal_type<void (), bs2::keywords::mutex_type<bs2::dummy_mutex>>::type;

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
    explicit feature(std::string_view name, bool enabled = false);
    feature() = default;
    // Has to run inside seastar::async context
    void enable();
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
