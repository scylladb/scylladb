/*
 * Copyright (C) 2017 ScyllaDB
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

#include <chrono>

#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/resource.hh>
#include <seastar/core/sstring.hh>

#include "delayed_tasks.hh"
#include "log.hh"
#include "seastarx.hh"
#include "utils/exponential_backoff_retry.hh"

using namespace std::chrono_literals;

namespace service {
class migration_manager;
}

namespace cql3 {
class query_processor;
}

namespace auth {

namespace meta {

extern const sstring DEFAULT_SUPERUSER_NAME;
extern const sstring AUTH_KS;
extern const sstring USERS_CF;
extern const sstring AUTH_PACKAGE_NAME;

}

extern logging::logger auth_log;

template <class Task>
future<> once_among_shards(Task&& f) {
    if (engine().cpu_id() == 0u) {
        return f();
    }

    return make_ready_future<>();
}

template <class Task>
static future<> do_execute_task(Task&& t, exponential_backoff_retry r) {
    auto f = t();
    return f.handle_exception([t = std::move(t), r = std::move(r)] (auto ep) mutable {
        auth_log.warn("Task failed with error, rescheduling: {}", ep);
        auto delay = r.retry();
        return delay.then([t = std::move(t), r = std::move(r)] () mutable {
            return do_execute_task(std::move(t), std::move(r));
        });
    });
}

// Task must support being invoked more than once.
template <class Task, class Clock>
void delay_until_system_ready(delayed_tasks<Clock>& ts, Task t) {
    ts.schedule_after(10s, [t = std::move(t)] () mutable {
        return do_execute_task(std::move(t), exponential_backoff_retry(1s, 1min));
    });
}

future<> create_metadata_table_if_missing(
        const sstring& table_name,
        cql3::query_processor&,
        const sstring& cql,
        ::service::migration_manager&);

}
