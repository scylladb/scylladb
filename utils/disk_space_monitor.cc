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
    : _as_sub(as.subscribe([this] () noexcept { _as.request_abort(); }))
    , _data_dir(std::move(data_dir))
    , _cfg(std::move(cfg))
{}

disk_space_monitor::~disk_space_monitor() {
    SCYLLA_ASSERT(_poller_fut.available());
}

future<> disk_space_monitor::start() {
    _space_info = co_await get_filesystem_space();
    _poller_fut = poll();
}

future<> disk_space_monitor::stop() noexcept {
    _as.request_abort();
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
                co_await sleep_abortable<clock_type>(interval - passed, _as);
            }
        }
    } catch (const sleep_aborted&) {
    } catch (const abort_requested_exception&) {
    } catch (...) {
        dsmlog.error("poll loop exited with error: {}", std::current_exception());
    }
}

future<std::filesystem::space_info> disk_space_monitor::get_filesystem_space() {
    return engine().file_system_space(_data_dir.native());
}

disk_space_monitor::clock_type::duration disk_space_monitor::get_polling_interval() const noexcept {
    auto du = disk_utilization();
    return std::chrono::seconds(du < _cfg.polling_interval_threshold.get() ? _cfg.normal_polling_interval.get() : _cfg.high_polling_interval.get());
}

} // namespace utils
