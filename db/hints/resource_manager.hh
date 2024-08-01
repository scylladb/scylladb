/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <filesystem>
#include <seastar/core/abort_source.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/future.hh>
#include "seastarx.hh"
#include <unordered_set>
#include "utils/small_vector.hh"
#include "utils/updateable_value.hh"
#include "enum_set.hh"
#include "db/hints/internal/common.hh"
#include "gms/inet_address.hh"

// Usually we don't define namespace aliases in our headers
// but this one is already entrenched.
namespace fs = std::filesystem;

namespace service {
class storage_proxy;
}

namespace gms {
    class gossiper;
} // namespace gms

namespace db {
namespace hints {

future<dev_t> get_device_id(const fs::path& path);
future<bool> is_mountpoint(const fs::path& path);

using timer_clock_type = seastar::lowres_clock;

class manager;

class space_watchdog {
private:
    using endpoint_id = internal::endpoint_id;

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
    seastar::named_semaphore _update_lock;

    future<> _started = make_ready_future<>();
    seastar::abort_source _as;
    int _files_count = 0;

public:
    space_watchdog(shard_managers_set& managers, per_device_limits_map& per_device_limits_map);
    void start();
    future<> stop() noexcept;

    seastar::named_semaphore& update_lock() noexcept {
        return _update_lock;
    }

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
    /// \param shard_manager the hint manager managing the directory specified by `path`
    /// \param maybe_ep_key endpoint ID corresponding to the scanned directory
    /// \return future that resolves when scanning is complete
    future<> scan_one_ep_dir(fs::path path, manager& shard_manager,
            std::optional<std::variant<locator::host_id, gms::inet_address>> maybe_ep_key);
};

class resource_manager {
    const size_t _max_send_in_flight_memory;
    utils::updateable_value<uint32_t> _max_hints_send_queue_length;
    seastar::named_semaphore _send_limiter;

    seastar::named_semaphore _operation_lock;
    space_watchdog::shard_managers_set _shard_managers;
    space_watchdog::per_device_limits_map _per_device_limits_map;
    space_watchdog _space_watchdog;

    service::storage_proxy& _proxy;
    shared_ptr<const gms::gossiper> _gossiper_ptr;

    enum class state {
        running,
        replay_allowed,
    };
    using state_set = enum_set<super_enum<state,
        state::running,
        state::replay_allowed>>;

    state_set _state;

    void set_running() noexcept {
        _state.set(state::running);
    }

    void unset_running() noexcept {
        _state.remove(state::running);
    }

    bool running() const noexcept {
        return _state.contains(state::running);
    }

    void set_replay_allowed() noexcept {
        _state.set(state::replay_allowed);
    }

    bool replay_allowed() const noexcept {
        return _state.contains(state::replay_allowed);
    }

    future<> prepare_per_device_limits(manager& shard_manager);

public:
    static constexpr size_t hint_segment_size_in_mb = 32;
    static constexpr size_t max_hints_per_ep_size_mb = 128; // 4 files 32MB each
    static constexpr size_t default_per_shard_concurrency_limit = 8;

public:
    resource_manager(service::storage_proxy& proxy, size_t max_send_in_flight_memory,
            utils::updateable_value<uint32_t> max_hint_sending_concurrency)
        : _max_send_in_flight_memory(max_send_in_flight_memory)
        , _max_hints_send_queue_length(std::move(max_hint_sending_concurrency))
        , _send_limiter(_max_send_in_flight_memory, named_semaphore_exception_factory{"send limiter"})
        , _operation_lock(1, named_semaphore_exception_factory{"operation lock"})
        , _space_watchdog(_shard_managers, _per_device_limits_map)
        , _proxy(proxy)
    {}

    resource_manager(resource_manager&&) = delete;
    resource_manager& operator=(resource_manager&&) = delete;

    future<semaphore_units<named_semaphore::exception_factory>> get_send_units_for(size_t buf_size);
    size_t sending_queue_length() const;

    future<> start(shared_ptr<const gms::gossiper> gossiper_ptr);
    future<> stop() noexcept;

    /// \brief Allows replaying hints for managers which are registered now or will be in the future.
    void allow_replaying() noexcept;

    /// \brief Registers the hints::manager in resource_manager, and starts it, if resource_manager is already running.
    ///
    /// The hints::managers can be added either before or after resource_manager starts.
    /// If resource_manager is already started, the hints manager will also be started.
    future<> register_manager(manager& m);

    seastar::named_semaphore& update_lock() noexcept {
        return _space_watchdog.update_lock();
    }
};

}
}
