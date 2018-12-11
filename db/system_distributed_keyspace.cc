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

#include "db/system_distributed_keyspace.hh"

#include "cql3/untyped_result_set.hh"
#include "database.hh"
#include "db/consistency_level_type.hh"
#include "db/system_keyspace.hh"
#include "schema_builder.hh"
#include "timeout_config.hh"
#include "types.hh"

#include <seastar/core/reactor.hh>
#include <seastar/core/shared_ptr.hh>

#include <boost/range/adaptor/transformed.hpp>

#include <optional>
#include <vector>
#include <experimental/optional>

namespace db {

schema_ptr view_build_status() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::VIEW_BUILD_STATUS);
        return schema_builder(system_distributed_keyspace::NAME, system_distributed_keyspace::VIEW_BUILD_STATUS, std::experimental::make_optional(id))
                .with_column("keyspace_name", utf8_type, column_kind::partition_key)
                .with_column("view_name", utf8_type, column_kind::partition_key)
                .with_column("host_id", uuid_type, column_kind::clustering_key)
                .with_column("status", utf8_type)
                .with_version(system_keyspace::generate_schema_version(id))
                .build();
    }();
    return schema;
}

static std::vector<schema_ptr> all_tables() {
    return {
        view_build_status(),
    };
}

system_distributed_keyspace::system_distributed_keyspace(cql3::query_processor& qp, service::migration_manager& mm)
        : _qp(qp)
        , _mm(mm) {
}

future<> system_distributed_keyspace::start() {
    if (engine().cpu_id() != 0) {
        return make_ready_future<>();
    }

    static auto ignore_existing = [] (seastar::noncopyable_function<future<>()> func) {
        return futurize_apply(std::move(func)).handle_exception_type([] (exceptions::already_exists_exception& ignored) { });
    };

    // We use min_timestamp so that the default keyspace metadata will lose with any manual adjustments.
    // See issue #2129.
    return ignore_existing([this] {
        auto ksm = keyspace_metadata::new_keyspace(
                NAME,
                "org.apache.cassandra.locator.SimpleStrategy",
                {{"replication_factor", "3"}},
                true);
        return _mm.announce_new_keyspace(ksm, api::min_timestamp, false);
    }).then([this] {
        return do_with(all_tables(), [this] (std::vector<schema_ptr>& tables) {
            return do_for_each(tables, [this] (schema_ptr table) {
                return ignore_existing([this, table = std::move(table)] {
                    return _mm.announce_new_column_family(std::move(table), api::min_timestamp, false);
                });
            });
        });
    });
}

future<> system_distributed_keyspace::stop() {
    return make_ready_future<>();
}

static const timeout_config internal_distributed_timeout_config = [] {
    using namespace std::chrono_literals;
    const auto t = 10s;
    return timeout_config{ t, t, t, t, t, t, t };
}();

future<std::unordered_map<utils::UUID, sstring>> system_distributed_keyspace::view_status(sstring ks_name, sstring view_name) const {
    return _qp.process(
            sprint("SELECT host_id, status FROM %s.%s WHERE keyspace_name = ? AND view_name = ?", NAME, VIEW_BUILD_STATUS),
            db::consistency_level::ONE,
            internal_distributed_timeout_config,
            { std::move(ks_name), std::move(view_name) },
            false).then([this] (::shared_ptr<cql3::untyped_result_set> cql_result) {
        return boost::copy_range<std::unordered_map<utils::UUID, sstring>>(*cql_result
                | boost::adaptors::transformed([] (const cql3::untyped_result_set::row& row) {
                    auto host_id = row.get_as<utils::UUID>("host_id");
                    auto status = row.get_as<sstring>("status");
                    return std::pair(std::move(host_id), std::move(status));
                }));
    });
}

future<> system_distributed_keyspace::start_view_build(sstring ks_name, sstring view_name) const {
    return db::system_keyspace::get_local_host_id().then([this, ks_name = std::move(ks_name), view_name = std::move(view_name)] (utils::UUID host_id) {
        return _qp.process(
                sprint("INSERT INTO %s.%s (keyspace_name, view_name, host_id, status) VALUES (?, ?, ?, ?)", NAME, VIEW_BUILD_STATUS),
                db::consistency_level::ONE,
                internal_distributed_timeout_config,
                { std::move(ks_name), std::move(view_name), std::move(host_id), "STARTED" },
                false).discard_result();
    });
}

future<> system_distributed_keyspace::finish_view_build(sstring ks_name, sstring view_name) const {
    return db::system_keyspace::get_local_host_id().then([this, ks_name = std::move(ks_name), view_name = std::move(view_name)] (utils::UUID host_id) {
        return _qp.process(
                sprint("UPDATE %s.%s SET status = ? WHERE keyspace_name = ? AND view_name = ? AND host_id = ?", NAME, VIEW_BUILD_STATUS),
                db::consistency_level::ONE,
                internal_distributed_timeout_config,
                { "SUCCESS", std::move(ks_name), std::move(view_name), std::move(host_id) },
                false).discard_result();
    });
}

future<> system_distributed_keyspace::remove_view(sstring ks_name, sstring view_name) const {
    return _qp.process(
            sprint("DELETE FROM %s.%s WHERE keyspace_name = ? AND view_name = ?", NAME, VIEW_BUILD_STATUS),
            db::consistency_level::ONE,
            internal_distributed_timeout_config,
            { std::move(ks_name), std::move(view_name) },
            false).discard_result();
}

}