/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/execute_statement.hh"

#include <array>
#include <map>

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

#include "cql3/column_specification.hh"
#include "cql3/query_processor.hh"
#include "db/data_listeners.hh"
#include "db/system_keyspace.hh"
#include "exceptions/exceptions.hh"
#include "replica/database.hh"
#include "service/storage_proxy.hh"

namespace cql3 {

namespace statements {

namespace {

// EXECUTE COMMAND toppartitions: samples this node's reads/writes for `duration` ms, like
// nodetool toppartitions, and returns the hottest partitions.
class toppartitions_command : public command {
    enum param { keyspace_name, table_name, duration, capacity, list_size, kind, count };

    static lw_shared_ptr<column_specification> result_column(sstring name, data_type type) {
        return make_column_spec(db::system_keyspace::NAME, "toppartitions", std::move(name), std::move(type));
    }

public:
    std::string_view name() const override {
        return "toppartitions";
    }

    std::span<const command_param> params() const override {
        static thread_local const std::array<command_param, param::count> params{{
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

    future<std::unique_ptr<result_set>> run(query_processor& qp, const command_args& args, abort_source& as) const override {
        // Defaults match nodetool's own; the sampler tracks up to `capacity` keys per shard and
        // gather() merges shards x capacity entries on one shard, so both stay bounded.
        constexpr int32_t max_capacity = 4096;
        constexpr int32_t max_duration_ms = 60000;
        auto ks = args.get<sstring>(keyspace_name);
        auto cf = args.get<sstring>(table_name);
        auto duration = args.get<int32_t>(param::duration).value_or(5000);
        auto capacity = args.get<int32_t>(param::capacity).value_or(256);
        auto list_size = args.get<int32_t>(param::list_size).value_or(std::min(10, capacity));
        auto kind = args.get<sstring>(param::kind);

        if (cf && !ks) {
            throw exceptions::invalid_request_exception("table_name requires keyspace_name");
        }
        if (duration <= 0 || duration > max_duration_ms) {
            throw exceptions::invalid_request_exception(format("duration must be in [1, {}] ms", max_duration_ms));
        }
        if (capacity <= 0 || capacity > max_capacity) {
            throw exceptions::invalid_request_exception(format("capacity must be in [1, {}]", max_capacity));
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

        db::toppartitions_query q(db, std::move(table_filters), std::move(keyspace_filters), std::chrono::milliseconds(duration), list_size, capacity,
                !kind || *kind == "read", !kind || *kind == "write");
        co_await q.scatter();
        // scatter() installed listeners on every shard; only gather() removes them, so it runs
        // even when shutdown cuts the sampling window short.
        auto f = co_await coroutine::as_future(seastar::sleep_abortable(q.duration(), as));
        auto results = co_await q.gather(capacity);
        f.get();

        auto rs = std::make_unique<result_set>(::make_shared<metadata>(*result_metadata()));
        // Ranked per (table, kind), like nodetool's per-sampler lists.
        std::map<std::tuple<sstring, sstring, std::string_view>, int32_t> ranks;
        auto emit = [&](const auto& top, std::string_view k) {
            for (auto& r : top) {
                auto& s = *r.item.schema;
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
            }
        };
        emit(results.read.top(list_size), "read");
        emit(results.write.top(list_size), "write");
        co_return rs;
    }
};

} // namespace

const command& toppartitions_command_instance() {
    static const toppartitions_command cmd;
    return cmd;
}

} // namespace statements

} // namespace cql3
