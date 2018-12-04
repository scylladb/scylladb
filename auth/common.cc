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

#include "auth/common.hh"

#include <seastar/core/shared_ptr.hh>

#include "cql3/query_processor.hh"
#include "cql3/statements/create_table_statement.hh"
#include "database.hh"
#include "schema_builder.hh"
#include "service/migration_manager.hh"
#include "timeout_config.hh"

namespace auth {

namespace meta {

const sstring DEFAULT_SUPERUSER_NAME("cassandra");
const sstring AUTH_KS("system_auth");
const sstring USERS_CF("users");
const sstring AUTH_PACKAGE_NAME("org.apache.cassandra.auth.");

}

static logging::logger auth_log("auth");

// Func must support being invoked more than once.
future<> do_after_system_ready(seastar::abort_source& as, seastar::noncopyable_function<future<>()> func) {
    struct empty_state { };
    return delay_until_system_ready(as).then([&as, func = std::move(func)] () mutable {
        return exponential_backoff_retry::do_until_value(1s, 1min, as, [func = std::move(func)] {
            return func().then_wrapped([] (auto&& f) -> stdx::optional<empty_state> {
                if (f.failed()) {
                    auth_log.info("Auth task failed with error, rescheduling: {}", f.get_exception());
                    return { };
                }
                return { empty_state() };
            });
        });
    }).discard_result();
}

future<> create_metadata_table_if_missing(
        stdx::string_view table_name,
        cql3::query_processor& qp,
        stdx::string_view cql,
        ::service::migration_manager& mm) {
    auto& db = qp.db().local();

    if (db.has_schema(meta::AUTH_KS, sstring(table_name))) {
        return make_ready_future<>();
    }

    auto parsed_statement = static_pointer_cast<cql3::statements::raw::cf_statement>(
            cql3::query_processor::parse_statement(cql));

    parsed_statement->prepare_keyspace(meta::AUTH_KS);

    auto statement = static_pointer_cast<cql3::statements::create_table_statement>(
            parsed_statement->prepare(db, qp.get_cql_stats())->statement);

    const auto schema = statement->get_cf_meta_data(qp.db().local());
    const auto uuid = generate_legacy_id(schema->ks_name(), schema->cf_name());

    schema_builder b(schema);
    b.set_uuid(uuid);

    return mm.announce_new_column_family(b.build(), false);
}

future<> wait_for_schema_agreement(::service::migration_manager& mm, const database& db, seastar::abort_source& as) {
    static const auto pause = [] { return sleep(std::chrono::milliseconds(500)); };

    return do_until([&db, &as] {
        as.check();
        return db.get_version() != database::empty_version;
    }, pause).then([&mm, &as] {
        return do_until([&mm, &as] {
            as.check();
            return mm.have_schema_agreement();
        }, pause);
    });
}

const timeout_config& internal_distributed_timeout_config() noexcept {
    static const auto t = 5s;
    static const timeout_config tc{t, t, t, t, t, t, t};
    return tc;
}

}
