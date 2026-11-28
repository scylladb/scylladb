/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/commands/command.hh"

#include <array>
#include <map>

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "audit/audit.hh"
#include "cql3/column_specification.hh"
#include "cql3/query_processor.hh"
#include "cql3/result_set.hh"
#include "db/data_listeners.hh"
#include "db/system_keyspace.hh"
#include "exceptions/exceptions.hh"
#include "replica/database.hh"
#include "service/client_state.hh"
#include "service/storage_proxy.hh"

namespace cql3::commands {

namespace {

using namespace std::chrono_literals;

constexpr auto max_duration = std::chrono::milliseconds(1h);
// Bounds how long a role that is not a superuser keeps listeners on every shard.
constexpr auto max_non_superuser_duration = std::chrono::milliseconds(1min);

// EXECUTE COMMAND toppartitions: samples this node's reads/writes for `duration` ms, like
// nodetool toppartitions, and returns the hottest partitions.
class toppartitions_impl : public command {
    enum param { local, keyspace_name, table_name, duration, capacity, list_size, kind, count };

    static lw_shared_ptr<column_specification> result_column(sstring name, data_type type) {
        return make_column_spec(db::system_keyspace::NAME, "toppartitions", std::move(name), std::move(type));
    }

public:
    std::string_view name() const override {
        return "toppartitions";
    }

    std::span<const command_param> params() const override {
        static thread_local const std::array<command_param, param::count> params{{
                {"local", boolean_type, true},
                {"keyspace_name", utf8_type},
                {"table_name", utf8_type},
                {"duration", int32_type},
                {"capacity", int32_type},
                {"list_size", int32_type},
                {"kind", utf8_type},
        }};
        return params;
    }

    seastar::shared_ptr<const metadata> result_metadata() const override {
        static thread_local auto md = ::make_shared<const metadata>(std::vector<lw_shared_ptr<column_specification>>{
                result_column("keyspace_name", utf8_type),
                result_column("table_name", utf8_type),
                result_column("kind", utf8_type),
                result_column("rank", int32_type),
                result_column("partition_key", utf8_type),
                result_column("count", long_type),
                result_column("error", long_type),
        });
        return md;
    }

    // A named user table needs only SELECT; all-tables, keyspace-wide and internal tables stay superuser-only.
    future<> check_access(query_processor& qp, const service::client_state& state, command_args& args) const override {
        if (co_await state.has_superuser()) {
            co_return;
        }
        std::optional<sstring> ks, cf;
        try {
            ks = args.get<sstring>(keyspace_name);
            cf = args.get<sstring>(table_name);
        } catch (...) {
            // Malformed names must not be told apart from a missing permission.
        }
        auto& cfg = qp.proxy().get_db().local().get_config();
        if (!ks || !cf || ks->empty() || cf->empty() || replica::is_always_local_keyspace(*ks, audit::table_sink_configured(cfg))) {
            throw exceptions::unauthorized_exception("EXECUTE COMMAND toppartitions can only be run by a superuser, "
                    "unless keyspace_name and table_name name a user table you have SELECT on.");
        }
        state.ensure_not_anonymous();
        // Resolve before the permission lookup yields, so a drop and recreate meanwhile is not authorized.
        auto& db = qp.proxy().get_db().local();
        // A null id matches nothing, so a table created during the lookup is not authorized.
        args.authorized_table = db.has_schema(*ks, *cf) ? db.find_schema(*ks, *cf)->id() : table_id();
        co_await state.has_column_family_access(*ks, *cf, auth::permission::SELECT);
        if (args.get<int32_t>(param::duration).value_or(0) > max_non_superuser_duration.count()) {
            throw exceptions::unauthorized_exception(format("Only a superuser can sample for more than {} ms", max_non_superuser_duration.count()));
        }
    }

    future<std::unique_ptr<result_set>> run(query_processor& qp, const command_args& args, abort_source& as) const override {
        // Defaults match nodetool's own.
        auto ks = args.get<sstring>(keyspace_name);
        auto cf = args.get<sstring>(table_name);
        auto duration = std::chrono::milliseconds(args.get<int32_t>(param::duration).value_or(5000));
        auto capacity = args.get<int32_t>(param::capacity).value_or(256);
        auto list_size = args.get<int32_t>(param::list_size).value_or(std::min(10, capacity));
        auto kind = args.get<sstring>(param::kind);

        if (!args.get<bool>(local).value_or(false)) {
            throw exceptions::invalid_request_exception("Cluster-wide toppartitions is not supported yet, use local = true");
        }
        if (cf && !ks) {
            throw exceptions::invalid_request_exception("table_name requires keyspace_name");
        }
        if (duration <= 0ms || duration > max_duration) {
            throw exceptions::invalid_request_exception(format("duration must be in [1, {}] ms", max_duration.count()));
        }
        if (capacity <= 0 || size_t(capacity) > db::toppartitions_query::max_capacity) {
            throw exceptions::invalid_request_exception(format("capacity must be in [1, {}]", db::toppartitions_query::max_capacity));
        }
        if (list_size <= 0) {
            throw exceptions::invalid_request_exception("list_size must be positive");
        }
        if (list_size > capacity) {
            throw exceptions::invalid_request_exception(format("list_size ({}) must not exceed capacity ({})", list_size, capacity));
        }
        if (kind && *kind != "read" && *kind != "write") {
            throw exceptions::invalid_request_exception(format("kind must be 'read' or 'write', not '{}'", *kind));
        }

        auto& db = qp.proxy().get_db();
        std::unordered_set<std::tuple<sstring, sstring>, utils::tuple_hash> table_filters;
        std::unordered_set<sstring> keyspace_filters;
        // Unset for superusers, who may sample anything.
        auto& authorized_id = args.authorized_table;
        if (cf) {
            if (!db.local().has_schema(*ks, *cf)) {
                throw exceptions::invalid_request_exception(format("Unknown table {}.{}", *ks, *cf));
            }
            table_filters.emplace(*ks, *cf);
        } else if (ks) {
            if (!db.local().has_keyspace(*ks)) {
                throw exceptions::invalid_request_exception(format("Unknown keyspace {}", *ks));
            }
            keyspace_filters.emplace(*ks);
        }

        db::toppartitions_query q(db, std::move(table_filters), std::move(keyspace_filters), duration, list_size, capacity);
        co_await q.scatter();
        // scatter() installed listeners on every shard; only gather() removes them, so it runs
        // even when shutdown cuts the sampling window short.
        auto f = co_await coroutine::as_future(seastar::sleep_abortable(q.duration(), as));
        auto g = co_await coroutine::as_future(q.gather(capacity));
        if (g.failed()) {
            f.ignore_ready_future();
            std::rethrow_exception(g.get_exception());
        }
        try {
            f.get();
        } catch (const seastar::abort_requested_exception&) {
            throw exceptions::server_exception("toppartitions aborted: the node is shutting down or the connection closed");
        }
        auto results = g.get();

        auto rs = std::make_unique<result_set>(::make_shared<metadata>(*result_metadata()));
        // list_size applies per kind across all tables; ranks are numbered per (table, kind).
        std::map<std::tuple<sstring, sstring, std::string_view>, int32_t> ranks;
        for (std::string_view k : {"read", "write"}) {
            if (kind && *kind != k) {
                continue;
            }
            for (auto& r : (k == "read" ? results.read : results.write).top(list_size)) {
                auto& s = *r.item.schema;
                // The listeners filter by name; a table recreated meanwhile isn't the one access was checked for.
                if (authorized_id && s.id() != *authorized_id) {
                    continue;
                }
                auto rank = ranks[{s.ks_name(), s.cf_name(), k}]++;
                rs->add_row(std::vector<bytes_opt>{
                        utf8_type->decompose(s.ks_name()),
                        utf8_type->decompose(s.cf_name()),
                        utf8_type->decompose(sstring(k)),
                        int32_type->decompose(rank),
                        utf8_type->decompose(sstring(r.item)),
                        long_type->decompose(int64_t(r.count)),
                        long_type->decompose(int64_t(r.error)),
                });
                co_await coroutine::maybe_yield();
            }
        }
        co_return rs;
    }
};

} // namespace

const command& toppartitions_command() {
    static const toppartitions_impl cmd;
    return cmd;
}

} // namespace cql3::commands
