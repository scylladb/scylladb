/*
 * Copyright (C) 2018-present ScyllaDB
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

#include "resource_manager.hh"
#include "manager.hh"
#include "log.hh"
#include <boost/range/algorithm/for_each.hpp>
#include <boost/range/adaptor/map.hpp>
#include "utils/disk-error-handler.hh"
#include "seastarx.hh"
#include <seastar/core/sleep.hh>
#include <seastar/core/seastar.hh>

namespace db {
namespace hints {

static logging::logger resource_manager_logger("hints_resource_manager");

future<dev_t> get_device_id(const fs::path& path) {
    return file_stat(path.native()).then([] (struct stat_data sd) {
        return sd.device_id;
    });
}

future<bool> is_mountpoint(const fs::path& path) {
    // Special case for '/', which is always a mount point
    if (path == path.parent_path()) {
        return make_ready_future<bool>(true);
    }
    return when_all(get_device_id(path), get_device_id(path.parent_path())).then([](std::tuple<future<dev_t>, future<dev_t>> ids) {
        return std::get<0>(ids).get0() != std::get<1>(ids).get0();
    });
}

future<semaphore_units<named_semaphore::exception_factory>> resource_manager::get_send_units_for(size_t buf_size) {
    // Let's approximate the memory size the mutation is going to consume by the size of its serialized form
    size_t hint_memory_budget = std::max(_min_send_hint_budget, buf_size);
    // Allow a very big mutation to be sent out by consuming the whole shard budget
    hint_memory_budget = std::min(hint_memory_budget, _max_send_in_flight_memory);
    resource_manager_logger.trace("memory budget: need {} have {}", hint_memory_budget, _send_limiter.available_units());
    return get_units(_send_limiter, hint_memory_budget);
}

size_t resource_manager::sending_queue_length() const {
    return _send_limiter.waiters();
}

const std::chrono::seconds space_watchdog::_watchdog_period = std::chrono::seconds(1);

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
            seastar::sleep_abortable(_watchdog_period, _as).get();
        }
    }).handle_exception_type([] (const seastar::sleep_aborted& ignored) { });
}

future<> space_watchdog::stop() noexcept {
    _as.request_abort();
    return std::move(_started);
}

// Called under the end_point_hints_manager::file_update_mutex() of the corresponding end_point_hints_manager instance.
future<> space_watchdog::scan_one_ep_dir(fs::path path, manager& shard_manager, ep_key_type ep_key) {
    return do_with(std::move(path), [this, ep_key, &shard_manager] (fs::path& path) {
        // It may happen that we get here and the directory has already been deleted in the context of manager::drain_for().
        // In this case simply bail out.
        return file_exists(path.native()).then([this, ep_key, &shard_manager, &path] (bool exists) {
            if (!exists) {
                return make_ready_future<>();
            } else {
                return lister::scan_dir(path, { directory_entry_type::regular }, [this, ep_key, &shard_manager] (fs::path dir, directory_entry de) {
                    // Put the current end point ID to state.eps_with_pending_hints when we see the second hints file in its directory
                    if (_files_count == 1) {
                        shard_manager.add_ep_with_pending_hints(ep_key);
                    }
                    ++_files_count;

                    return io_check(file_size, (dir / de.name.c_str()).c_str()).then([this] (uint64_t fsize) {
                        _total_size += fsize;
                    });
                });
            }
        });
    });
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
            lister::scan_dir(shard_manager.hints_dir(), {directory_entry_type::directory}, [this, &shard_manager] (fs::path dir, directory_entry de) {
                _files_count = 0;
                // Let's scan per-end-point directories and enumerate hints files...
                //
                // Let's check if there is a corresponding end point manager (may not exist if the corresponding DC is
                // not hintable).
                // If exists - let's take a file update lock so that files are not changed under our feet. Otherwise, simply
                // continue to enumeration - there is no one to change them.
                auto it = shard_manager.find_ep_manager(de.name);
                if (it != shard_manager.ep_managers_end()) {
                    return with_file_update_mutex(it->second, [this, &shard_manager, dir = std::move(dir), ep_name = std::move(de.name)] () mutable {
                        return scan_one_ep_dir(dir / ep_name, shard_manager, ep_key_type(ep_name));
                    });
                } else {
                    return scan_one_ep_dir(dir / de.name, shard_manager, ep_key_type(de.name));
                }
            }).get();
        }

        // Adjust the quota to take into account the space we guarantee to every end point manager
        size_t adjusted_quota = 0;
        size_t delta = boost::accumulate(per_device_limits.managers, 0, [] (size_t sum, manager& shard_manager) {
            return sum + shard_manager.ep_managers_size() * resource_manager::hint_segment_size_in_mb * 1024 * 1024;
        });
        if (per_device_limits.max_shard_disk_space_size > delta) {
            adjusted_quota = per_device_limits.max_shard_disk_space_size - delta;
        }

        resource_manager_logger.trace("space_watchdog: consuming {}/{} bytes", _total_size, adjusted_quota);
        for (manager& shard_manager : per_device_limits.managers) {
            shard_manager.update_backlog(_total_size, adjusted_quota);
        }
    }
}

future<> resource_manager::start(shared_ptr<service::storage_proxy> proxy_ptr, shared_ptr<gms::gossiper> gossiper_ptr, shared_ptr<service::storage_service> ss_ptr) {
    _proxy_ptr = std::move(proxy_ptr);
    _gossiper_ptr = std::move(gossiper_ptr);
    _ss_ptr = std::move(ss_ptr);

    return with_semaphore(_operation_lock, 1, [this] () {
        return parallel_for_each(_shard_managers, [this](manager& m) {
            return m.start(_proxy_ptr, _gossiper_ptr, _ss_ptr);
        }).then([this]() {
            return do_for_each(_shard_managers, [this](manager& m) {
                return prepare_per_device_limits(m);
            });
        }).then([this]() {
            return _space_watchdog.start();
        }).then([this]() {
            set_running();
        });
    });
}

void resource_manager::allow_replaying() noexcept {
    set_replay_allowed();
    boost::for_each(_shard_managers, [] (manager& m) { m.allow_replaying(); });
}

future<> resource_manager::stop() noexcept {
    return with_semaphore(_operation_lock, 1, [this] () {
        return parallel_for_each(_shard_managers, [](manager& m) {
            return m.stop();
        }).finally([this]() {
            return _space_watchdog.stop();
        }).then([this]() {
            unset_running();
        });
    });
}

future<> resource_manager::register_manager(manager& m) {
    return with_semaphore(_operation_lock, 1, [this, &m] () {
        return with_semaphore(_space_watchdog.update_lock(), 1, [this, &m] {
            const auto [it, inserted] = _shard_managers.insert(m);
            if (!inserted) {
                // Already registered
                return make_ready_future<>();
            }
            if (!running()) {
                // The hints manager will be started later by resource_manager::start()
                return make_ready_future<>();
            }

            // If the resource_manager was started, start the hints manager, too.
            return m.start(_proxy_ptr, _gossiper_ptr, _ss_ptr).then([this, &m] {
                // Calculate device limits for this manager so that it is accounted for
                // by the space_watchdog
                return prepare_per_device_limits(m).then([this, &m] {
                    if (this->replay_allowed()) {
                        m.allow_replaying();
                    }
                });
            }).handle_exception([this, &m] (auto ep) {
                _shard_managers.erase(m);
                return make_exception_future<>(ep);
            });
        });
    });
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
                it->second.max_shard_disk_space_size = std::filesystem::space(shard_manager.hints_dir().c_str()).capacity / (10 * smp::count);
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

}
}
