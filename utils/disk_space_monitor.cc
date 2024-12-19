/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

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
    co_await get_filesystem_stats();
    _poller_fut = poll();
}

future<> disk_space_monitor::stop() noexcept {
    _as.request_abort();
    co_await std::exchange(_poller_fut, make_ready_future());
}

disk_space_monitor::signal_connection_type disk_space_monitor::listen(signal_callback_type callback) {
    return _signal_source.connect([this, callback = std::move(callback)] () mutable {
        // the signal is fired inside thread context
        callback(*this).get();
    });
}

future<> disk_space_monitor::poll() {
    return async([this] {
        try {
            while (!_as.abort_requested()) {
                auto now = clock_type::now();
                get_filesystem_stats().get();

                _signal_source();

                auto passed = clock_type::now() - now;
                auto interval = get_polling_interval();
                if (interval > passed) {
                    sleep_abortable<clock_type>(interval - passed, _as).get();
                }
            }
        } catch (const sleep_aborted&) {
        } catch (const abort_requested_exception&) {
        } catch (...) {
            dsmlog.error("poll loop exited with error: {}", std::current_exception());
        }
    });
}

future<> disk_space_monitor::get_filesystem_stats() {
    auto st = co_await engine().statvfs(_data_dir.native());
    _space_info = std::filesystem::space_info{
        .capacity = (uintmax_t)st.f_blocks * st.f_frsize,
        .free = (uintmax_t)st.f_bfree * st.f_frsize,
        .available = (uintmax_t)st.f_bavail * st.f_frsize
    };
}

disk_space_monitor::clock_type::duration disk_space_monitor::get_polling_interval() const noexcept {
    auto du = disk_utilization();
    return std::chrono::seconds(du < _cfg.polling_interval_threshold.get() ? _cfg.normal_polling_interval.get() : _cfg.high_polling_interval.get());
}

} // namespace utils
