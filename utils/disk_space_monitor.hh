/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <chrono>
#include <filesystem>

#include <boost/signals2/connection.hpp>
#include <boost/signals2/signal_type.hpp>
#include <boost/signals2/dummy_mutex.hpp>

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/optimized_optional.hh>

#include "seastarx.hh"
#include "utils/assert.hh"
#include "utils/log.hh"
#include "utils/updateable_value.hh"

using namespace std::chrono_literals;
namespace bs2 = boost::signals2;

namespace utils {

extern seastar::logger dsmlog;

// Instantiated only on shard 0
template <typename Clock = lowres_clock>
class disk_space_monitor {
public:
    using signal_type = bs2::signal_type<void (), bs2::keywords::mutex_type<bs2::dummy_mutex>>::type;
    using signal_callback_type = std::function<future<>(const disk_space_monitor&)>;
    using signal_connection_type = bs2::scoped_connection;

    struct config {
        scheduling_group sched_group;
        updateable_value<int> normal_polling_interval;
        updateable_value<int> high_polling_interval;
        // Use high_polling_interval above this threshold
        updateable_value<float> polling_interval_threshold;
    };

private:
    abort_source _as;
    optimized_optional<abort_source::subscription> _as_sub;
    future<> _poller_fut = make_ready_future();
    signal_type _signal_source;
    uint64_t _total_space = 0;
    uint64_t _free_space = 0;

protected:
    std::filesystem::path _data_dir;
    config _cfg;

    virtual future<struct statvfs> get_filesystem_stats() const {
        return engine().statvfs(_data_dir.native());
    }
public:
    explicit disk_space_monitor(abort_source& as, std::filesystem::path data_dir, config cfg)
        : _as_sub(as.subscribe([this] () noexcept { _as.request_abort(); }))
        , _data_dir(std::move(data_dir))
        , _cfg(std::move(cfg))
    {}
    virtual ~disk_space_monitor() {
        SCYLLA_ASSERT(_poller_fut.available());
    }

    void start() {
        _poller_fut = poll();
    }

    future<> stop() noexcept {
        _as.request_abort();
        co_await std::exchange(_poller_fut, make_ready_future());
    }

    const std::filesystem::path& data_dir() const noexcept {
        return _data_dir;
    }

    uint64_t total_space() const noexcept { return _total_space; }
    uint64_t free_space() const noexcept { return _free_space; }
    float disk_utilization() const noexcept {
        return _total_space ? (float)(_total_space - _free_space) / _total_space : -1;
    }

    signal_connection_type listen(signal_callback_type callback) {
        return _signal_source.connect([this, callback = std::move(callback)] () mutable {
            // the signal is fired inside thread context
            callback(*this).get();
        });
    }

private:
    future<> poll() {
        return async([this] {
            try {
                while (!_as.abort_requested()) {
                    auto now = Clock::now();
                    auto st = get_filesystem_stats().get();
                    _total_space = (uint64_t)st.f_blocks * st.f_frsize;
                    _free_space = (uint64_t)st.f_bfree * st.f_frsize;

                    _signal_source();

                    auto passed = Clock::now() - now;
                    auto interval = get_polling_interval();
                    if (interval > passed) {
                        sleep_abortable<Clock>(interval - passed, _as).get();
                    }
                }
            } catch (const sleep_aborted&) {
            } catch (const abort_requested_exception&) {
            } catch (...) {
                dsmlog.error("poll loop exited with error: {}", std::current_exception());
            }
        });
    }

    Clock::duration get_polling_interval() const noexcept {
        auto du = disk_utilization();
        return std::chrono::seconds(du < _cfg.polling_interval_threshold.get() ? _cfg.normal_polling_interval.get() : _cfg.high_polling_interval.get());
    }
};

} // namespace utils
