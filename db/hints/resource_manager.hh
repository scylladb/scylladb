/*
 * Copyright (C) 2018 ScyllaDB
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

#include <cstdint>
#include <seastar/core/abort_source.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/future.hh>
#include "seastarx.hh"
#include <unordered_set>
#include "gms/gossiper.hh"
#include "utils/small_vector.hh"
#include "lister.hh"

namespace service {
class storage_proxy;
class storage_service;
}

namespace db {
namespace hints {

future<dev_t> get_device_id(const fs::path& path);
future<bool> is_mountpoint(const fs::path& path);

using timer_clock_type = seastar::lowres_clock;

class manager;

class space_watchdog {
private:
    using ep_key_type = gms::inet_address;
    static const std::chrono::seconds _watchdog_period;

    struct manager_hash {
        size_t operator()(const manager& manager) const {
            return reinterpret_cast<uintptr_t>(&manager);
        }
    };
    struct manager_comp {
        bool operator()(const std::reference_wrapper<manager>& m1, const std::reference_wrapper<manager>& m2) const {
            return std::addressof(m1.get()) == std::addressof(m2.get());
        }
    };

public:

    struct per_device_limits {
        utils::small_vector<std::reference_wrapper<manager>, 2> managers;
        size_t max_shard_disk_space_size;
    };

    using shard_managers_set = std::unordered_set<std::reference_wrapper<manager>, manager_hash, manager_comp>;
    using per_device_limits_map = std::unordered_map<dev_t, per_device_limits>;

private:
    size_t _total_size = 0;
    shard_managers_set& _shard_managers;
    per_device_limits_map& _per_device_limits_map;

    future<> _started = make_ready_future<>();
    seastar::abort_source _as;
    int _files_count = 0;

public:
    space_watchdog(shard_managers_set& managers, per_device_limits_map& per_device_limits_map);
    void start();
    future<> stop() noexcept;

private:
    /// \brief Check that hints don't occupy too much disk space.
    ///
    /// Verifies that all \ref manager::_hints_dir dirs for all managers occupy less than \ref resource_manager::max_shard_disk_space_size.
    ///
    /// If they do, stop all end point managers that have more than one hints file - we don't want some DOWN Node to
    /// prevent hints to other Nodes from being generated (e.g. due to some temporary overload and timeout).
    ///
    /// This is a simplistic implementation of a manager for a limited shared resource with a minimum guaranteed share for all
    /// participants.
    ///
    /// This implementation guarantees at least a single hint share for all end point managers.
    void on_timer();

    /// \brief Scan files in a single end point directory.
    ///
    /// Add sizes of files in the directory to _total_size. If number of files is greater than 1 add this end point ID
    /// to _eps_with_pending_hints so that we may block it if _total_size value becomes greater than the maximum allowed
    /// value.
    ///
    /// \param path directory to scan
    /// \param ep_name end point ID (as a string)
    /// \return future that resolves when scanning is complete
    future<> scan_one_ep_dir(fs::path path, manager& shard_manager, ep_key_type ep_key);
};

class resource_manager {
    const size_t _max_send_in_flight_memory;
    const size_t _min_send_hint_budget;
    seastar::named_semaphore _send_limiter;

    space_watchdog::shard_managers_set _shard_managers;
    space_watchdog::per_device_limits_map _per_device_limits_map;
    space_watchdog _space_watchdog;

public:
    static constexpr size_t hint_segment_size_in_mb = 32;
    static constexpr size_t max_hints_per_ep_size_mb = 128; // 4 files 32MB each
    static constexpr size_t max_hints_send_queue_length = 128;

public:
    resource_manager(size_t max_send_in_flight_memory)
        : _max_send_in_flight_memory(std::max(max_send_in_flight_memory, max_hints_send_queue_length))
        , _min_send_hint_budget(_max_send_in_flight_memory / max_hints_send_queue_length)
        , _send_limiter(_max_send_in_flight_memory, named_semaphore_exception_factory{"send limiter"})
        , _space_watchdog(_shard_managers, _per_device_limits_map)
    {}

    resource_manager(resource_manager&&) = delete;
    resource_manager& operator=(resource_manager&&) = delete;

    future<semaphore_units<named_semaphore::exception_factory>> get_send_units_for(size_t buf_size);
    size_t sending_queue_length() const;

    future<> start(shared_ptr<service::storage_proxy> proxy_ptr, shared_ptr<gms::gossiper> gossiper_ptr, shared_ptr<service::storage_service> ss_ptr);
    void allow_replaying() noexcept;
    future<> stop() noexcept;
    void register_manager(manager& m);
    future<> prepare_per_device_limits();
};

}
}
