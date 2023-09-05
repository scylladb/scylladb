/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "db/hints/resource_manager.hh"

// Seastar features.
#include <seastar/core/sleep.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/file.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/coroutine/parallel_for_each.hh>

// Boost features.
#include <boost/range/algorithm/for_each.hpp>
#include <boost/range/adaptor/map.hpp>

// Scylla includes.
#include "db/hints/manager.hh"
#include "utils/disk-error-handler.hh"
#include "utils/div_ceil.hh"
#include "utils/lister.hh"
#include "log.hh"
#include "seastarx.hh"

// STD.
#include <exception>

namespace fs = std::filesystem;

namespace db::hints {

namespace {

logging::logger resource_manager_logger{"hints_resource_manager"};

future<bool> is_mountpoint(const fs::path& path) {
    // Special case for '/', which is always a mountpoint.
    if (path == path.parent_path()) {
        co_return true;
    }

    const fs::path parent_path = path.parent_path();
    
    // Don't change the order below. We must use the reference immediately to ensure it still lives.
    const auto did1 = co_await get_device_id(path);
    const auto did2 = co_await get_device_id(parent_path);

    co_return did1 != did2;
}

} // anonymous namespace

future<dev_t> get_device_id(const fs::path& path) {
    const auto sd = co_await file_stat(path.native());
    co_return sd.device_id;
}

/////////////////////////////////
/////////////////////////////////

space_watchdog::space_watchdog(shard_managers_set& managers, per_device_limits_map& per_device_limits_map)
    : _shard_managers(managers)
    , _per_device_limits_map(per_device_limits_map)
    , _update_lock(1, named_semaphore_exception_factory{"update lock"})
{}

void space_watchdog::start() {
    _started = seastar::async([this] {
        while (!_as.abort_requested()) {
            try {
                const auto units = get_units(_update_lock, 1).get();
                on_timer();
            } catch (...) {
                resource_manager_logger.trace("space_watchdog: unexpected exception - stop all hints generators");
                // Stop all hint generators if space_watchdog callback failed
                for (manager& shard_manager : _shard_managers) {
                    shard_manager.forbid_hints();
                }
            }
            seastar::sleep_abortable(WATCHDOG_PERIOD, _as).get();
        }
    }).handle_exception_type([] (const seastar::sleep_aborted& ignored) { });
}

future<> space_watchdog::stop() noexcept {
    _as.request_abort();
    return std::move(_started);
}

// Called from the context of a seastar::thread.
void space_watchdog::on_timer() {
    // The hints directories are organized as follows:
    // <hints root>
    //    |- <shard1 ID>
    //    |  |- <EP1 address>
    //    |     |- <hints file1>
    //    |     |- <hints file2>
    //    |     |- ...
    //    |  |- <EP2 address>
    //    |     |- ...
    //    |  |-...
    //    |- <shard2 ID>
    //    |  |- ...
    //    ...
    //    |- <shardN ID>
    //    |  |- ...
    //

    for (auto& per_device_limits : _per_device_limits_map | boost::adaptors::map_values) {
        _total_size = 0;

        for (manager& shard_manager : per_device_limits.managers) {
            shard_manager.clear_eps_with_pending_hints();
            lister::scan_dir(shard_manager.hints_dir(), lister::dir_entry_types::of<directory_entry_type::directory>(),
                    [this, &shard_manager] (fs::path dir, directory_entry de) {
                _files_count = 0;
                const endpoint_id ep = de.name;

                // Let's scan per-end-point directories and enumerate hints files...
                //
                // Let's check if there is a corresponding end point manager (may not exist if the corresponding DC is
                // not hintable).
                // If exists - let's take a file update lock so that files are not changed under our feet. Otherwise, simply
                // continue to enumeration - there is no one to change them.
                auto it = shard_manager.find_ep_manager(ep);
                if (it != shard_manager.ep_managers_end()) {
                    return with_file_update_mutex(it->second,
                            [this, &shard_manager, dir = std::move(dir), ep = std::move(ep), ep_name = std::move(de.name)] () mutable {
                        return scan_one_ep_dir(dir / ep_name, shard_manager, ep);
                    });
                } else {
                    return scan_one_ep_dir(dir / de.name, shard_manager, ep);
                }
            }).get();
        }

        // Adjust the quota to take into account the space we guarantee to every end point manager
        const size_t delta = boost::accumulate(per_device_limits.managers, size_t(0), [] (size_t sum, manager& shard_manager) {
            return sum + shard_manager.ep_managers_size() * resource_manager::HINT_SEGMENT_SIZE_IN_MB * 1024 * 1024;
        });
        const size_t adjusted_quota = per_device_limits.max_shard_disk_space_size > delta
                ? per_device_limits.max_shard_disk_space_size - delta
                : 0;

        resource_manager_logger.trace("space_watchdog: consuming {}/{} bytes", _total_size, adjusted_quota);
        for (manager& shard_manager : per_device_limits.managers) {
            shard_manager.update_backlog(_total_size, adjusted_quota);
        }
    }
}

// Called under the end_point_hints_manager::file_update_mutex() of the corresponding host_manager instance.
future<> space_watchdog::scan_one_ep_dir(fs::path path, manager& shard_manager, endpoint_id ep) {
    // It may happen that we get here and the directory has already been deleted
    // in the context of manager::drain_for(). In this case simply bail out.
    if (!co_await file_exists(path.native())) {
        co_return;
    }

    // Declare a lambda here and pass it as a reference to lister::scan_dir. It is equivalent
    // to passing a temporary lambda wrapped as seastar::coroutine::lambda, but I think
    // creating it here explicitly looks better. We simply want the captures to live as long
    // as it is necessary.
    auto lambda = [&] (fs::path dir, directory_entry de) -> future<> {
        // Put the current end point ID to state.eps_with_pending_hints
        // when we see the second hints file in its directory.
        if (_files_count == 1) {
            shard_manager.add_ep_with_pending_hints(ep);
        }
        ++_files_count;

        const fs::path path = dir / de.name;
        _total_size += co_await io_check(file_size, path.c_str());
    };

    co_await lister::scan_dir(path, lister::dir_entry_types::of<directory_entry_type::regular>(),
            std::ref(lambda));
}

/////////////////////////////////
/////////////////////////////////

future<> resource_manager::start(shared_ptr<service::storage_proxy> proxy_ptr, shared_ptr<gms::gossiper> gossiper_ptr) {
    _proxy_ptr = std::move(proxy_ptr);
    _gossiper_ptr = std::move(gossiper_ptr);

    const auto operation_mutex = co_await seastar::get_units(_operation_lock, 1);

    co_await seastar::coroutine::parallel_for_each(_shard_managers, [this] (manager& m) {
        return m.start(_proxy_ptr, _gossiper_ptr);
    });
    
    co_await do_for_each(_shard_managers, [this] (manager& m) {
        return prepare_per_device_limits(m);
    });
    
    _space_watchdog.start();
    set_running();
}

future<> resource_manager::stop() noexcept {
    const auto operation_mutex = co_await seastar::get_units(_operation_lock, 1);
    std::exception_ptr eptr = nullptr;

    try {
        co_await seastar::coroutine::parallel_for_each(_shard_managers, [] (manager& m) {
            return m.stop();
        });
    } catch (...) {
        eptr = std::current_exception();
    }
    
    co_await _space_watchdog.stop();

    if (eptr) {
        std::rethrow_exception(eptr);
    }
    
    unset_running();
}

future<> resource_manager::register_manager(manager& m) {
    const auto operation_mutex = co_await seastar::get_units(_operation_lock, 1);
    const auto space_watchdog_lock = co_await seastar::get_units(_space_watchdog.update_lock(), 1);

    const auto [it, inserted] = _shard_managers.insert(m);
    if (!inserted) {
        // Already registered.
        co_return;
    }
    if (!running()) {
        // The hints manager will be started later by resource_manager::start()
        co_return;
    }

    try {
        // If the resource_manager was started, start the hints manager too.
        co_await m.start(_proxy_ptr, _gossiper_ptr);

        // Calculate device limits for this manager so that it is accounted for
        // by the space_watchdog.
        co_await prepare_per_device_limits(m);

        if (replay_allowed()) {
            m.allow_replaying();
        }
    } catch (...) {
        _shard_managers.erase(m);
        throw;
    }
}

void resource_manager::allow_replaying() noexcept {
    set_replay_allowed();
    boost::for_each(_shard_managers, [] (manager& m) { m.allow_replaying(); });
}

future<semaphore_units<named_semaphore::exception_factory>> resource_manager::get_send_units_for(size_t buf_size) {
    // In order to impose a limit on the number of hints being sent concurrently,
    // require each hint to reserve at least 1/(max concurrency) of the shard budget
    const size_t per_node_concurrency_limit = _max_hints_send_queue_length();
    const size_t per_shard_concurrency_limit = (per_node_concurrency_limit > 0)
            ? div_ceil(per_node_concurrency_limit, smp::count)
            : DEFAULT_PER_SHARD_CONCURRENCY_LIMIT;
    const size_t min_send_hint_budget = _max_send_in_flight_memory / per_shard_concurrency_limit;
    // Let's approximate the memory size the mutation is going to consume by the size of its serialized form
    size_t hint_memory_budget = std::max(min_send_hint_budget, buf_size);
    // Allow a very big mutation to be sent out by consuming the whole shard budget
    hint_memory_budget = std::min(hint_memory_budget, _max_send_in_flight_memory);
    resource_manager_logger.trace("memory budget: need {} have {}", hint_memory_budget, _send_limiter.available_units());
    return get_units(_send_limiter, hint_memory_budget);
}

size_t resource_manager::sending_queue_length() const {
    return _send_limiter.waiters();
}

future<> resource_manager::prepare_per_device_limits(manager& shard_manager) {
    dev_t device_id = shard_manager.hints_dir_device_id();
    auto it = _per_device_limits_map.find(device_id);
    if (it == _per_device_limits_map.end()) {
        return is_mountpoint(shard_manager.hints_dir().parent_path()).then([this, device_id, &shard_manager](bool is_mountpoint) {
            auto [it, inserted] = _per_device_limits_map.emplace(device_id, space_watchdog::per_device_limits{});
            // Since we possibly deferred, we need to recheck the _per_device_limits_map.
            if (inserted) {
                // By default, give each group of managers 10% of the available disk space. Give each shard an equal share of the available space.
                it->second.max_shard_disk_space_size = fs::space(shard_manager.hints_dir().c_str()).capacity / (10 * smp::count);
                // If hints directory is a mountpoint, we assume it's on dedicated (i.e. not shared with data/commitlog/etc) storage.
                // Then, reserve 90% of all space instead of 10% above.
                if (is_mountpoint) {
                    it->second.max_shard_disk_space_size *= 9;
                }
            }
            it->second.managers.emplace_back(std::ref(shard_manager));
        });
    } else {
        it->second.managers.emplace_back(std::ref(shard_manager));
        return make_ready_future<>();
    }
}

} // namespace db::hints
