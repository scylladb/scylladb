/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <filesystem>

#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>

#include "utils/disk_space_monitor.hh"
#include "utils/assert.hh"
#include "utils/log.hh"

using namespace std::chrono_literals;

namespace utils {

seastar::logger dsmlog("disk_space_monitor");

disk_space_monitor::disk_space_monitor(abort_source& as, std::filesystem::path data_dir, config cfg)
    : _as_sub(as.subscribe([this] () noexcept {
        _as.request_abort();
        _poll_cv.broadcast();
    }))
    , _signal_barrier("disk_space_monitor::signal_barrier")
    , _data_dir(std::move(data_dir))
    , _cfg(std::move(cfg))
{
    _space_source = [this] {
        return engine().file_system_space(_data_dir.native());
    };
    _capacity_observer = make_lw_shared(_cfg.capacity_override.observe([this] (auto) {
        trigger_poll();
    }));
}

disk_space_monitor::~disk_space_monitor() {
    SCYLLA_ASSERT(_poller_fut.available());
}

disk_space_monitor::space_source_registration::space_source_registration(disk_space_monitor& m)
    : _monitor(m)
    , _prev_space_source(m._space_source)
{
}

disk_space_monitor::space_source_registration::~space_source_registration() {
    _monitor._space_source = _prev_space_source;
}

future<> disk_space_monitor::start() {
    _space_info = co_await get_filesystem_space();
    _poller_fut = poll();
}

future<> disk_space_monitor::stop() noexcept {
    _as.request_abort();
    _poll_cv.broadcast();
    co_await _signal_barrier.advance_and_await();
    co_await std::exchange(_poller_fut, make_ready_future());
}

disk_space_monitor::signal_connection_type disk_space_monitor::listen(signal_callback_type callback) {
    return _signal_source.connect([this, callback = std::move(callback)] () mutable -> future<> {
        auto op = _signal_barrier.start();
        co_await callback(*this);
    });
}

future<> disk_space_monitor::poll() {
    try {
        while (!_as.abort_requested()) {
            auto now = clock_type::now();
            _space_info = co_await get_filesystem_space();

            if (_as.abort_requested()) {
                co_return;
            }
            co_await _signal_barrier.advance_and_await();
            _signal_source();

            auto passed = clock_type::now() - now;
            auto interval = get_polling_interval();
            if (interval > passed) {
                try {
                    co_await _poll_cv.wait(interval - passed);
                } catch (const seastar::condition_variable_timed_out&) {
                }
            }
        }
    } catch (const sleep_aborted&) {
    } catch (const abort_requested_exception&) {
    } catch (...) {
        dsmlog.error("poll loop exited with error: {}", std::current_exception());
    }
}

void disk_space_monitor::trigger_poll() noexcept {
    _poll_cv.broadcast();
}

future<std::filesystem::space_info> disk_space_monitor::get_filesystem_space() {
    auto space = co_await _space_source();
    if (_cfg.capacity_override()) {
        auto not_free = space.capacity - space.free;
        auto not_available = space.capacity - space.available;
        auto new_capacity = _cfg.capacity_override();
        space = std::filesystem::space_info{
            .capacity = new_capacity,
            .free = new_capacity - std::min(not_free, new_capacity),
            .available = new_capacity - std::min(not_available, new_capacity)
        };
    }
    co_return space;
}

disk_space_monitor::clock_type::duration disk_space_monitor::get_polling_interval() const noexcept {
    auto du = disk_utilization();
    return std::chrono::seconds(du < _cfg.polling_interval_threshold.get() ? _cfg.normal_polling_interval.get() : _cfg.high_polling_interval.get());
}

} // namespace utils
