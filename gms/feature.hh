/*
 * Copyright (C) 2016-present ScyllaDB
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
    friend class gossiper;
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
    explicit operator bool() const {
        return _enabled;
    }
    friend inline std::ostream& operator<<(std::ostream& os, const feature& f) {
        return os << "{ gossip feature = " << f._name << " }";
    }
    void when_enabled(listener& callback) const {
        callback.set_connection(_s.connect(callback.get_slot()));
        if (_enabled) {
            _s();
        }
    }
    // Will call the callback functor when this feature is enabled, unless
    // the returned listener_registration is destroyed earlier.
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

namespace features {

extern const std::string_view RANGE_TOMBSTONES;
extern const std::string_view LARGE_PARTITIONS;
extern const std::string_view MATERIALIZED_VIEWS;
extern const std::string_view COUNTERS;
extern const std::string_view INDEXES;
extern const std::string_view DIGEST_MULTIPARTITION_READ;
extern const std::string_view CORRECT_COUNTER_ORDER;
extern const std::string_view SCHEMA_TABLES_V3;
extern const std::string_view CORRECT_NON_COMPOUND_RANGE_TOMBSTONES;
extern const std::string_view WRITE_FAILURE_REPLY;
extern const std::string_view XXHASH;
extern const std::string_view UDF;
extern const std::string_view ROLES;
extern const std::string_view LA_SSTABLE;
extern const std::string_view STREAM_WITH_RPC_STREAM;
extern const std::string_view MC_SSTABLE;
extern const std::string_view MD_SSTABLE;
extern const std::string_view ROW_LEVEL_REPAIR;
extern const std::string_view TRUNCATION_TABLE;
extern const std::string_view CORRECT_STATIC_COMPACT_IN_MC;
extern const std::string_view UNBOUNDED_RANGE_TOMBSTONES;
extern const std::string_view VIEW_VIRTUAL_COLUMNS;
extern const std::string_view DIGEST_INSENSITIVE_TO_EXPIRY;
extern const std::string_view COMPUTED_COLUMNS;
extern const std::string_view CDC;
extern const std::string_view NONFROZEN_UDTS;
extern const std::string_view HINTED_HANDOFF_SEPARATE_CONNECTION;
extern const std::string_view LWT;
extern const std::string_view PER_TABLE_PARTITIONERS;
extern const std::string_view PER_TABLE_CACHING;
extern const std::string_view DIGEST_FOR_NULL_VALUES;
extern const std::string_view CORRECT_IDX_TOKEN_IN_SECONDARY_INDEX;
extern const std::string_view ALTERNATOR_STREAMS;
extern const std::string_view RANGE_SCAN_DATA_VARIANT;
extern const std::string_view CDC_GENERATIONS_V2;

}

} // namespace gms
