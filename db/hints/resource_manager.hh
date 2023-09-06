/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

// Seastar features.
#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/semaphore.hh>

// Scylla includes.
#include "db/hints/internal/common.hh"
#include "utils/small_vector.hh"
#include "utils/updateable_value.hh"
#include "enum_set.hh"
#include "seastarx.hh"

// STD.
#include <cstdint>
#include <filesystem>
#include <unordered_set>

namespace service {
class storage_proxy;
} // namespace service

namespace gms {
class gossiper;
} // namespace gms

namespace db::hints {

future<::dev_t> get_device_id(const std::filesystem::path& path);

class shard_hint_manager;

class space_watchdog {
private:
    using endpoint_id = internal::endpoint_id;

    struct manager_hash {
        size_t operator()(const shard_hint_manager& manager) const {
            return reinterpret_cast<uintptr_t>(&manager);
        }
    };

    struct manager_comp {
        bool operator()(const std::reference_wrapper<shard_hint_manager>& m1,
                const std::reference_wrapper<shard_hint_manager>& m2) const {
            return std::addressof(m1.get()) == std::addressof(m2.get());
        }
    };

private:
    static constexpr std::chrono::seconds WATCHDOG_PERIOD = std::chrono::seconds(1);

public:
    struct per_device_limits {
        utils::small_vector<std::reference_wrapper<shard_hint_manager>, 2> managers;
        size_t max_shard_disk_space_size;
    };

    using shard_managers_set = std::unordered_set<std::reference_wrapper<shard_hint_manager>,
            manager_hash, manager_comp>;
    using per_device_limits_map = std::unordered_map<::dev_t, per_device_limits>;

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

public:
    void start();
    future<> stop() noexcept;

    seastar::named_semaphore& update_lock() {
        return _update_lock;
    }

private:
    /// \brief Check that hints don't occupy too much disk space.
    ///
    /// Verifies that all \ref shard_hint_manager::_hints_dir dirs for all managers occupy less
    /// than \ref resource_manager::max_shard_disk_space_size.
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
    /// to _hosts_with_pending_hints so that we may block it if _total_size value becomes greater than the maximum allowed
    /// value.
    ///
    /// \param path directory to scan
    /// \param ep end point ID
    /// \return future that resolves when scanning is complete
    future<> scan_one_ep_dir(std::filesystem::path path, shard_hint_manager& shard_manager, endpoint_id ep);
};

class resource_manager {
private:
    // TODO: Explain these values.
    enum class state {
        running,
        replay_allowed,
    };

    using state_set = enum_set<super_enum<state,
        state::running,
        state::replay_allowed>>;

public:
    // TODO: Explain these briefly.
    static constexpr size_t HINT_SEGMENT_SIZE_IN_MB = 32;
    static constexpr size_t MAX_HINTS_PER_EP_SIZE_MB = 128; // 4 files 32 MB each
    static constexpr size_t DEFAULT_PER_SHARD_CONCURRENCY_LIMIT = 8;

private:
    const size_t _max_send_in_flight_memory;
    utils::updateable_value<uint32_t> _max_hints_send_queue_length;
    seastar::named_semaphore _send_limiter;

    seastar::named_semaphore _operation_lock{1, named_semaphore_exception_factory{"operation lock"}};
    space_watchdog::shard_managers_set _shard_managers{};
    space_watchdog::per_device_limits_map _per_device_limits_map{};
    space_watchdog _space_watchdog;

    shared_ptr<service::storage_proxy> _proxy_ptr = nullptr;
    shared_ptr<gms::gossiper> _gossiper_ptr = nullptr;

    state_set _state{};

public:
    resource_manager(size_t max_send_in_flight_memory,
            utils::updateable_value<uint32_t> max_hint_sending_concurrency)
        : _max_send_in_flight_memory(max_send_in_flight_memory)
        , _max_hints_send_queue_length(std::move(max_hint_sending_concurrency))
        , _send_limiter(_max_send_in_flight_memory, named_semaphore_exception_factory{"send limiter"})
        , _space_watchdog(_shard_managers, _per_device_limits_map)
    {}

    resource_manager(resource_manager&&) = delete;
    resource_manager& operator=(resource_manager&&) = delete;

    ~resource_manager() noexcept = default;

public:
    future<> start(shared_ptr<service::storage_proxy> proxy_ptr, shared_ptr<gms::gossiper> gossiper_ptr);
    future<> stop() noexcept;

    /// \brief Registers a shard hint manager in resource_manager, and starts it if resource_manager is already running.
    ///
    /// The hints::managers can be added either before or after resource_manager starts.
    /// If resource_manager is already started, the hints manager will also be started.
    future<> register_manager(shard_hint_manager& m);

    /// \brief Allows replaying hints for managers which are registered now or will be in the future.
    void allow_replaying() noexcept;
    
    future<semaphore_units<named_semaphore::exception_factory>> get_send_units_for(size_t buf_size);
    size_t sending_queue_length() const;

private:
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

    future<> prepare_per_device_limits(shard_hint_manager& shard_manager);
};

} // namespace db::hints
