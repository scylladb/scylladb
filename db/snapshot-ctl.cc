/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 *
 * Copyright (C) 2020-present ScyllaDB
 */

#include <boost/range/adaptors.hpp>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include "db/snapshot-ctl.hh"
#include "replica/database.hh"

logging::logger snap_log("snapshots");

namespace db {

snapshot_ctl::snapshot_ctl(sharded<replica::database>& db)
    : _db(db)
{
}

future<> snapshot_ctl::stop() {
    co_await _ops.close();
}

future<> snapshot_ctl::check_snapshot_not_exist(sstring ks_name, sstring name, std::optional<std::vector<sstring>> filter) {
    auto& ks = _db.local().find_keyspace(ks_name);
    return parallel_for_each(ks.metadata()->cf_meta_data(), [this, ks_name = std::move(ks_name), name = std::move(name), filter = std::move(filter)] (auto& pair) {
        auto& cf_name = pair.first;
        if (filter && std::find(filter->begin(), filter->end(), cf_name) == filter->end()) {
            return make_ready_future<>();
        }        
        auto& cf = _db.local().find_column_family(pair.second);
        return cf.snapshot_exists(name).then([ks_name = std::move(ks_name), name] (bool exists) {
            if (exists) {
                throw std::runtime_error(format("Keyspace {}: snapshot {} already exists.", ks_name, name));
            }
        });
    });
}

template <typename Func>
std::invoke_result_t<Func> snapshot_ctl::run_snapshot_modify_operation(Func&& f) {
    return with_gate(_ops, [f = std::move(f), this] () {
        return container().invoke_on(0, [f = std::move(f)] (snapshot_ctl& snap) mutable {
            return with_lock(snap._lock.for_write(), std::move(f));
        });
    });
}

future<> snapshot_ctl::take_snapshot(sstring tag, std::vector<sstring> keyspace_names, skip_flush sf) {
    if (tag.empty()) {
        throw std::runtime_error("You must supply a snapshot name.");
    }

    if (keyspace_names.size() == 0) {
        boost::copy(_db.local().get_keyspaces() | boost::adaptors::map_keys, std::back_inserter(keyspace_names));
    };

    return run_snapshot_modify_operation([tag = std::move(tag), keyspace_names = std::move(keyspace_names), sf, this] () mutable {
        return do_take_snapshot(std::move(tag), std::move(keyspace_names), sf);
    });
}

future<> snapshot_ctl::do_take_snapshot(sstring tag, std::vector<sstring> keyspace_names, skip_flush sf) {
    co_await coroutine::parallel_for_each(keyspace_names, [tag, this] (const auto& ks_name) {
        return check_snapshot_not_exist(ks_name, tag);
    });
    co_await coroutine::parallel_for_each(keyspace_names, [this, tag = std::move(tag), sf] (const auto& ks_name) {
        return replica::database::snapshot_keyspace_on_all_shards(_db, ks_name, tag, bool(sf));
    });
}

future<> snapshot_ctl::take_column_family_snapshot(sstring ks_name, std::vector<sstring> tables, sstring tag, snap_views sv, skip_flush sf) {
    if (ks_name.empty()) {
        throw std::runtime_error("You must supply a keyspace name");
    }
    if (tables.empty()) {
        throw std::runtime_error("You must supply a table name");
    }
    if (tag.empty()) {
        throw std::runtime_error("You must supply a snapshot name.");
    }

    return run_snapshot_modify_operation([this, ks_name = std::move(ks_name), tables = std::move(tables), tag = std::move(tag), sv, sf] () mutable {
        return do_take_column_family_snapshot(std::move(ks_name), std::move(tables), std::move(tag), sv, sf);
    });
}

future<> snapshot_ctl::do_take_column_family_snapshot(sstring ks_name, std::vector<sstring> tables, sstring tag, snap_views sv, skip_flush sf) {
    co_await check_snapshot_not_exist(ks_name, tag, tables);
    co_await replica::database::snapshot_tables_on_all_shards(_db, ks_name, std::move(tables), std::move(tag), sv, bool(sf));
}

future<> snapshot_ctl::take_column_family_snapshot(sstring ks_name, sstring cf_name, sstring tag, snap_views sv, skip_flush sf) {
    return take_column_family_snapshot(ks_name, std::vector<sstring>{cf_name}, tag, sv, sf);
}

future<> snapshot_ctl::clear_snapshot(sstring tag, std::vector<sstring> keyspace_names, sstring cf_name) {
    return run_snapshot_modify_operation([this, tag = std::move(tag), keyspace_names = std::move(keyspace_names), cf_name = std::move(cf_name)] {
        return _db.local().clear_snapshot(tag, keyspace_names, cf_name);
    });
}

future<std::unordered_map<sstring, snapshot_ctl::db_snapshot_details>>
snapshot_ctl::get_snapshot_details() {
    using snapshot_map = std::unordered_map<sstring, db_snapshot_details>;

    co_return co_await run_snapshot_list_operation(coroutine::lambda([this] () -> future<snapshot_map> {
        return _db.local().get_snapshot_details();
    }));
}

future<int64_t> snapshot_ctl::true_snapshots_size() {
    co_return co_await run_snapshot_list_operation(coroutine::lambda([this] () -> future<int64_t> {
        int64_t total = 0;
        for (auto& [name, details] : co_await _db.local().get_snapshot_details()) {
            total += std::accumulate(details.begin(), details.end(), int64_t(0), [] (int64_t sum, const auto& d) { return sum + d.details.live; });
        }
        co_return total;
    }));
}

future<int64_t> snapshot_ctl::true_snapshots_size(sstring ks, sstring cf) {
    co_return co_await run_snapshot_list_operation(coroutine::lambda([this, ks = std::move(ks), cf = std::move(cf)] () -> future<int64_t> {
        int64_t total = 0;
        for (auto& [name, details] : co_await _db.local().find_column_family(ks, cf).get_snapshot_details()) {
            total += details.total;
        }
        co_return total;
    }));
}

}
