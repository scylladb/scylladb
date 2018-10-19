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

#include "resource_manager.hh"
#include "manager.hh"
#include "log.hh"
#include <boost/range/algorithm/for_each.hpp>
#include <boost/range/adaptor/map.hpp>
#include "lister.hh"
#include "disk-error-handler.hh"
#include "seastarx.hh"
#include <seastar/core/sleep.hh>

namespace db {
namespace hints {

static logging::logger resource_manager_logger("hints_resource_manager");

future<dev_t> get_device_id(boost::filesystem::path path) {
    return open_directory(path.native()).then([](file f) {
        return f.stat().then([f = std::move(f)](struct stat st) {
            return st.st_dev;
        });
    });
}

future<bool> is_mountpoint(boost::filesystem::path path) {
    // Special case for '/', which is always a mount point
    if (path == path.parent_path()) {
        return make_ready_future<bool>(true);
    }
    return when_all(get_device_id(path.native()), get_device_id(path.parent_path().native())).then([](std::tuple<future<dev_t>, future<dev_t>> ids) {
        return std::get<0>(ids).get0() != std::get<1>(ids).get0();
    });
}

future<semaphore_units<semaphore_default_exception_factory>> resource_manager::get_send_units_for(size_t buf_size) {
    // Let's approximate the memory size the mutation is going to consume by the size of its serialized form
    size_t hint_memory_budget = std::max(_min_send_hint_budget, buf_size);
    // Allow a very big mutation to be sent out by consuming the whole shard budget
    hint_memory_budget = std::min(hint_memory_budget, _max_send_in_flight_memory);
    resource_manager_logger.trace("memory budget: need {} have {}", hint_memory_budget, _send_limiter.available_units());
    return get_units(_send_limiter, hint_memory_budget);
}

const std::chrono::seconds space_watchdog::_watchdog_period = std::chrono::seconds(1);

space_watchdog::space_watchdog(shard_managers_set& managers, per_device_limits_map& per_device_limits_map)
    : _shard_managers(managers)
    , _per_device_limits_map(per_device_limits_map)
{}

void space_watchdog::start() {
    _started = seastar::async([this] {
        while (!_as.abort_requested()) {
            try {
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

future<> space_watchdog::scan_one_ep_dir(boost::filesystem::path path, manager& shard_manager, ep_key_type ep_key) {
    return lister::scan_dir(path, { directory_entry_type::regular }, [this, ep_key, &shard_manager] (lister::path dir, directory_entry de) {
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
            lister::scan_dir(shard_manager.hints_dir(), {directory_entry_type::directory}, [this, &shard_manager] (lister::path dir, directory_entry de) {
                _files_count = 0;
                // Let's scan per-end-point directories and enumerate hints files...
                //
                // Let's check if there is a corresponding end point manager (may not exist if the corresponding DC is
                // not hintable).
                // If exists - let's take a file update lock so that files are not changed under our feet. Otherwise, simply
                // continue to enumeration - there is no one to change them.
                auto it = shard_manager.find_ep_manager(de.name);
                if (it != shard_manager.ep_managers_end()) {
                    return with_lock(it->second.file_update_mutex(), [this, &shard_manager, dir = std::move(dir), ep_name = std::move(de.name)]() mutable {
                        return scan_one_ep_dir(dir / ep_name.c_str(), shard_manager, ep_key_type(ep_name));
                    });
                } else {
                    return scan_one_ep_dir(dir / de.name.c_str(), shard_manager, ep_key_type(de.name));
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
    return parallel_for_each(_shard_managers, [proxy_ptr, gossiper_ptr, ss_ptr](manager& m) {
        return m.start(proxy_ptr, gossiper_ptr, ss_ptr);
    }).then([this]() {
        return prepare_per_device_limits();
    }).then([this]() {
        return _space_watchdog.start();
    });
}

void resource_manager::allow_replaying() noexcept {
    boost::for_each(_shard_managers, [] (manager& m) { m.allow_replaying(); });
}

future<> resource_manager::stop() noexcept {
    return parallel_for_each(_shard_managers, [](manager& m) {
        return m.stop();
    }).finally([this]() {
        return _space_watchdog.stop();
    });
}

void resource_manager::register_manager(manager& m) {
    _shard_managers.insert(m);
}

future<> resource_manager::prepare_per_device_limits() {
    return do_for_each(_shard_managers, [this] (manager& shard_manager) mutable {
        dev_t device_id = shard_manager.hints_dir_device_id();
        auto it = _per_device_limits_map.find(device_id);
        if (it == _per_device_limits_map.end()) {
            return is_mountpoint(shard_manager.hints_dir().parent_path()).then([this, device_id, &shard_manager](bool is_mountpoint) {
                auto [it, inserted] = _per_device_limits_map.emplace(device_id, space_watchdog::per_device_limits{});
                // Since we possibly deferred, we need to recheck the _per_device_limits_map.
                if (inserted) {
                    // By default, give each group of managers 10% of the available disk space. Give each shard an equal share of the available space.
                    it->second.max_shard_disk_space_size = boost::filesystem::space(shard_manager.hints_dir().c_str()).capacity / (10 * smp::count);
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
    });
}

}
}
