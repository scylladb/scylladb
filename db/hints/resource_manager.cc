/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "resource_manager.hh"
#include "gms/inet_address.hh"
#include "locator/token_metadata.hh"
#include "manager.hh"
#include "log.hh"
#include <boost/range/algorithm/for_each.hpp>
#include <boost/range/adaptor/map.hpp>
#include "utils/disk-error-handler.hh"
#include "seastarx.hh"
#include <seastar/core/sleep.hh>
#include <seastar/core/seastar.hh>
#include "utils/div_ceil.hh"
#include "utils/lister.hh"

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
        return std::get<0>(ids).get() != std::get<1>(ids).get();
    });
}

future<semaphore_units<named_semaphore::exception_factory>> resource_manager::get_send_units_for(size_t buf_size) {
    // In order to impose a limit on the number of hints being sent concurrently,
    // require each hint to reserve at least 1/(max concurrency) of the shard budget
    const size_t per_node_concurrency_limit = _max_hints_send_queue_length();
    const size_t per_shard_concurrency_limit = (per_node_concurrency_limit > 0)
            ? div_ceil(per_node_concurrency_limit, smp::count)
            : default_per_shard_concurrency_limit;
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
future<> space_watchdog::scan_one_ep_dir(fs::path path, manager& shard_manager,
        std::optional<std::variant<locator::host_id, gms::inet_address>> maybe_ep_key) {
    // It may happen that we get here and the directory has already been deleted in the context of manager::drain_for().
    // In this case simply bail out.
    if (!co_await file_exists(path.native())) {
        co_return;
    }

    co_await lister::scan_dir(path, lister::dir_entry_types::of<directory_entry_type::regular>(),
            coroutine::lambda([this, maybe_ep_key, &shard_manager] (fs::path dir, directory_entry de) -> future<> {
        // Put the current end point ID to state.eps_with_pending_hints when we see the second hints file in its directory
        if (maybe_ep_key && _files_count == 1) {
            shard_manager.add_ep_with_pending_hints(*maybe_ep_key);
        }
        ++_files_count;

        const auto filename = (std::move(dir) / std::move(de.name)).native();
        _total_size += co_await io_check(file_size, filename);
    }));
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
            lister::scan_dir(shard_manager.hints_dir(), lister::dir_entry_types::of<directory_entry_type::directory>(), [this, &shard_manager] (fs::path dir, directory_entry de) {
                _files_count = 0;
                // Let's scan per-end-point directories and enumerate hints files...
                //
                // Let's check if there is a corresponding end point manager (may not exist if the corresponding DC is
                // not hintable).
                // If exists - let's take a file update lock so that files are not changed under our feet. Otherwise, simply
                // continue to enumeration - there is no one to change them.
                auto maybe_variant = std::invoke([&] () -> std::optional<std::variant<locator::host_id, gms::inet_address>> {
                    try {
                        const auto hid_or_ep = locator::host_id_or_endpoint{de.name};

                        // If hinted handoff is host-ID-based, hint directories representing IP addresses must've
                        // been created by mistake and they're invalid. The same for pre-host-ID hinted handoff
                        // -- hint directories representing host IDs are NOT valid.
                        if (hid_or_ep.has_host_id() && shard_manager.uses_host_id()) {
                            return std::variant<locator::host_id, gms::inet_address>(hid_or_ep.id());
                        } else if (hid_or_ep.has_endpoint() && !shard_manager.uses_host_id()) {
                            return std::variant<locator::host_id, gms::inet_address>(hid_or_ep.endpoint());
                        } else {
                            return std::nullopt;
                        }
                    } catch (...) {
                        return std::nullopt;
                    }
                });

                // Case 1: The directory is managed by an endpoint manager.
                if (maybe_variant && shard_manager.have_ep_manager(*maybe_variant)) {
                    const auto variant = *maybe_variant;
                    return shard_manager.with_file_update_mutex_for(variant, [this, variant, &shard_manager, dir = std::move(dir), ep_name = std::move(de.name)] () mutable {
                        return scan_one_ep_dir(dir / ep_name, shard_manager, variant);
                    });
                }
                // Case 2: The directory isn't managed by an endpoint manager, but it represents either an IP address,
                //         or a host ID.
                else if (maybe_variant) {
                    return scan_one_ep_dir(dir / de.name, shard_manager, *maybe_variant);
                }
                // Case 3: The directory isn't managed by an endpoint manager, and it represents neither an IP address,
                //         nor a host ID.
                else {
                    // We use trace here to prevent flooding logs with unnecessary information.
                    resource_manager_logger.trace("Encountered a hint directory of invalid name while scanning: {}", de.name);
                    return scan_one_ep_dir(dir / de.name, shard_manager, {});
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

future<> resource_manager::start(shared_ptr<const gms::gossiper> gossiper_ptr) {
    _gossiper_ptr = std::move(gossiper_ptr);

    return with_semaphore(_operation_lock, 1, [this] () {
        return parallel_for_each(_shard_managers, [this](manager& m) {
            return m.start(_gossiper_ptr);
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
            return m.start(_gossiper_ptr).then([this, &m] {
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
