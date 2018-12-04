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
#include <experimental/string_view>

#include <seastar/core/future.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/resource.hh>
#include <seastar/core/sstring.hh>

#include "log.hh"
#include "seastarx.hh"
#include "utils/exponential_backoff_retry.hh"

using namespace std::chrono_literals;

class database;
class timeout_config;

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

template <class Task>
future<> once_among_shards(Task&& f) {
    if (engine().cpu_id() == 0u) {
        return f();
    }

    return make_ready_future<>();
}

inline future<> delay_until_system_ready(seastar::abort_source& as) {
    return sleep_abortable(15s, as);
}

// Func must support being invoked more than once.
future<> do_after_system_ready(seastar::abort_source& as, seastar::noncopyable_function<future<>()> func);

future<> create_metadata_table_if_missing(
        stdx::string_view table_name,
        cql3::query_processor&,
        stdx::string_view cql,
        ::service::migration_manager&);

future<> wait_for_schema_agreement(::service::migration_manager&, const database&, seastar::abort_source&);

///
/// Time-outs for internal, non-local CQL queries.
///
const timeout_config& internal_distributed_timeout_config() noexcept;

}
