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
#include "db/snapshot-ctl.hh"
#include "replica/database.hh"

namespace db {

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
std::result_of_t<Func()> snapshot_ctl::run_snapshot_modify_operation(Func&& f) {
    return with_gate(_ops, [f = std::move(f), this] () {
        return container().invoke_on(0, [f = std::move(f)] (snapshot_ctl& snap) mutable {
            return with_lock(snap._lock.for_write(), std::move(f));
        });
    });
}

template <typename Func>
std::result_of_t<Func()> snapshot_ctl::run_snapshot_list_operation(Func&& f) {
    return with_gate(_ops, [f = std::move(f), this] () {
        return container().invoke_on(0, [f = std::move(f)] (snapshot_ctl& snap) mutable {
            return with_lock(snap._lock.for_read(), std::move(f));
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
    co_await parallel_for_each(keyspace_names, [tag, this] (const auto& ks_name) {
        return check_snapshot_not_exist(ks_name, tag);
    });
    co_await parallel_for_each(keyspace_names, [this, tag = std::move(tag), sf] (const auto& ks_name) {
        return _db.local().snapshot_on_all(ks_name, tag, bool(sf));
    });
}

future<> snapshot_ctl::take_column_family_snapshot(sstring ks_name, std::vector<sstring> tables, sstring tag, skip_flush sf, allow_view_snapshots av) {
    if (ks_name.empty()) {
        throw std::runtime_error("You must supply a keyspace name");
    }
    if (tables.empty()) {
        throw std::runtime_error("You must supply a table name");
    }
    if (tag.empty()) {
        throw std::runtime_error("You must supply a snapshot name.");
    }

    return run_snapshot_modify_operation([this, ks_name = std::move(ks_name), tables = std::move(tables), tag = std::move(tag), sf, av] () mutable {
        return do_take_column_family_snapshot(std::move(ks_name), std::move(tables), std::move(tag), sf, av);
    });
}

future<> snapshot_ctl::do_take_column_family_snapshot(sstring ks_name, std::vector<sstring> tables, sstring tag, skip_flush sf, allow_view_snapshots av) {
    co_await check_snapshot_not_exist(ks_name, tag, tables);

    for (const auto& table_name : tables) {
        auto& cf = _db.local().find_column_family(ks_name, table_name);
        if (cf.schema()->is_view() && !av) {
            throw std::invalid_argument("Do not take a snapshot of a materialized view or a secondary index by itself. Run snapshot on the base table instead.");
        }
    }
    co_await _db.local().snapshot_on_all(ks_name, std::move(tables), std::move(tag), bool(sf));
}

future<> snapshot_ctl::take_column_family_snapshot(sstring ks_name, sstring cf_name, sstring tag, skip_flush sf, allow_view_snapshots av) {
    return take_column_family_snapshot(ks_name, std::vector<sstring>{cf_name}, tag, sf, av);
}

future<> snapshot_ctl::clear_snapshot(sstring tag, std::vector<sstring> keyspace_names, sstring cf_name) {
    return run_snapshot_modify_operation([this, tag = std::move(tag), keyspace_names = std::move(keyspace_names), cf_name = std::move(cf_name)] {
        return _db.local().clear_snapshot(tag, keyspace_names, cf_name);
    });
}

future<std::unordered_map<sstring, std::vector<snapshot_ctl::snapshot_details>>>
snapshot_ctl::get_snapshot_details() {
    using cf_snapshot_map = std::unordered_map<utils::UUID, replica::column_family::snapshot_details>;
    using snapshot_map = std::unordered_map<sstring, cf_snapshot_map>;

    class snapshot_reducer {
    private:
        snapshot_map _result;
    public:
        future<> operator()(const snapshot_map& value) {
            for (auto&& vp: value) {
                if (auto [ignored, added] = _result.try_emplace(vp.first, std::move(vp.second)); added) {
                    continue;
                }

                auto& rp = _result.at(vp.first);
                for (auto&& cf: vp.second) {
                    if (auto [ignored, added] = rp.try_emplace(cf.first, std::move(cf.second)); added) {
                        continue;
                    }
                    auto& rcf = rp.at(cf.first);
                    rcf.live = cf.second.live;
                    rcf.total = cf.second.total;
                }
            }
            return make_ready_future<>();
        }
        snapshot_map get() && {
            return std::move(_result);
        }
    };

    return run_snapshot_list_operation([this] {
        return _db.map_reduce(snapshot_reducer(), [] (replica::database& db) {
            auto local_snapshots = make_lw_shared<snapshot_map>();
            return parallel_for_each(db.get_column_families(), [local_snapshots] (auto& cf_pair) {
                return cf_pair.second->get_snapshot_details().then([uuid = cf_pair.first, local_snapshots] (auto map) {
                    for (auto&& snap_map: map) {
                        auto [it, ignored] = local_snapshots->try_emplace(snap_map.first);
                        it->second.emplace(uuid, snap_map.second);
                    }
                    return make_ready_future<>();
                });
            }).then([local_snapshots] {
                return make_ready_future<snapshot_map>(std::move(*local_snapshots));
            });
        }).then([this] (snapshot_map&& map) {
            std::unordered_map<sstring, std::vector<snapshot_ctl::snapshot_details>> result;
            for (auto&& pair: map) {
                std::vector<snapshot_ctl::snapshot_details> details;

                for (auto&& snap_map: pair.second) {
                    auto& cf = _db.local().find_column_family(snap_map.first);
                    details.push_back({ snap_map.second.live, snap_map.second.total, cf.schema()->cf_name(), cf.schema()->ks_name() });
                }
                result.emplace(pair.first, std::move(details));
            }

            return make_ready_future<std::unordered_map<sstring, std::vector<snapshot_ctl::snapshot_details>>>(std::move(result));
        });
    });
}

future<int64_t> snapshot_ctl::true_snapshots_size() {
    return run_snapshot_list_operation([this] () mutable {
        return do_with(int64_t(0), [this] (auto& total) {
            return parallel_for_each(_db.local().get_column_families(), [&total] (auto& cf_pair) {
                return cf_pair.second->get_snapshot_details().then([&total] (auto map) {
                    for (auto&& snap_map: map) {
                        total += snap_map.second.live;
                    }
                });
            }).then([&total] {
                return total;
            });
        });
    });
}

}
