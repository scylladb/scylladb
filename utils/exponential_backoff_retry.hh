
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/abort_source.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/do_with.hh>
#include "seastarx.hh"
#include <chrono>

// Implements retry policy that exponentially increases sleep time between retries.
class exponential_backoff_retry {
    std::chrono::milliseconds _base_sleep_time;
    std::chrono::milliseconds _sleep_time;
    std::chrono::milliseconds _max_sleep_time;
public:
    exponential_backoff_retry(std::chrono::milliseconds base_sleep_time, std::chrono::milliseconds max_sleep_time)
        : _base_sleep_time(std::min(base_sleep_time, max_sleep_time))
        , _sleep_time(_base_sleep_time)
        , _max_sleep_time(max_sleep_time) {}

    future<> retry(abort_source& as) {
        return sleep_abortable(update_sleep_time(), as);
    }

    future<> retry() {
        return sleep(update_sleep_time());
    }

    // Return sleep time in seconds to be used for next retry.
    std::chrono::milliseconds sleep_time() const {
        return _sleep_time;
    }

    // Reset sleep time to base sleep time.
    void reset() {
        _sleep_time = _base_sleep_time;
    }

private:
    std::chrono::milliseconds update_sleep_time() {
        // calculating sleep time seconds for the next retry.
        return std::exchange(_sleep_time, std::min(_sleep_time * 2, _max_sleep_time));
    }

    template <typename T>
    struct retry_type_helper;

    template <typename T>
    struct retry_type_helper<future<std::optional<T>>> {
        using optional_type = std::optional<T>;
        using future_type = future<optional_type>;
    };

public:
    template<typename Func>
    static auto do_until_value(std::chrono::milliseconds base_sleep_time, std::chrono::milliseconds max_sleep_time, seastar::abort_source& as, Func f) {
        using type_helper = retry_type_helper<std::invoke_result_t<Func>>;

        auto r = exponential_backoff_retry(base_sleep_time, max_sleep_time);
        return seastar::do_with(std::move(r), [&as, f = std::move(f)] (exponential_backoff_retry& r) mutable {
            return seastar::repeat_until_value([&r, &as, f = std::move(f)] () mutable {
                return f().then([&] (auto&& opt) -> typename type_helper::future_type {
                    if (opt) {
                        return make_ready_future<typename type_helper::optional_type>(std::move(opt));
                    }
                    return r.retry(as).then([] () -> typename type_helper::optional_type {
                        return { };
                    });
                });
            });
        });
    }
};
