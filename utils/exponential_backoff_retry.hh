
/*
 * Copyright (C) 2015 ScyllaDB
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

#pragma once

#include "core/abort_source.hh"
#include "core/sleep.hh"
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
    struct retry_type_helper<future<stdx::optional<T>>> {
        using optional_type = stdx::optional<T>;
        using future_type = future<optional_type>;
    };

public:
    template<typename Func>
    static auto do_until_value(std::chrono::milliseconds base_sleep_time, std::chrono::milliseconds max_sleep_time, seastar::abort_source& as, Func f) {
        using type_helper = retry_type_helper<std::result_of_t<Func()>>;

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