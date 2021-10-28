/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by ScyllaDB
 * Copyright (C) 2020-present ScyllaDB
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
 *
 * Copyright (C) 2020-present ScyllaDB
 */

#include <tuple>
#include <boost/range/adaptors.hpp>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include "db/snapshot-ctl.hh"
#include "database.hh"

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

    return run_snapshot_modify_operation([tag = std::move(tag), keyspace_names = std::move(keyspace_names), sf, this] {
        return parallel_for_each(keyspace_names, [tag, this] (auto& ks_name) {
            return check_snapshot_not_exist(ks_name, tag);
        }).then([this, tag, keyspace_names, sf] {
            return _db.invoke_on_all([tag = std::move(tag), keyspace_names, sf] (database& db) {
                return parallel_for_each(keyspace_names, [&db, tag = std::move(tag), sf] (auto& ks_name) {
                    auto& ks = db.find_keyspace(ks_name);
                    return parallel_for_each(ks.metadata()->cf_meta_data(), [&db, tag = std::move(tag), sf] (auto& pair) {
                        auto& cf = db.find_column_family(pair.second);
                        return cf.snapshot(db, tag, bool(sf));
                    });
                });
            });
        });
    });
}

future<> snapshot_ctl::take_column_family_snapshot(sstring ks_name, std::vector<sstring> tables, sstring tag, skip_flush sf) {
    if (ks_name.empty()) {
        throw std::runtime_error("You must supply a keyspace name");
    }
    if (tables.empty()) {
        throw std::runtime_error("You must supply a table name");
    }
    if (tag.empty()) {
        throw std::runtime_error("You must supply a snapshot name.");
    }

    return run_snapshot_modify_operation([this, ks_name = std::move(ks_name), tables = std::move(tables), tag = std::move(tag), sf] {
        return check_snapshot_not_exist(ks_name, tag, tables).then([this, ks_name, tables, tag, sf] {
            return do_with(std::vector<sstring>(std::move(tables)),[this, ks_name, tag, sf](const std::vector<sstring>& tables) {
                return do_for_each(tables, [ks_name, tag, sf, this] (const sstring& table_name) {
                    if (table_name.find(".") != sstring::npos) {
                        throw std::invalid_argument("Cannot take a snapshot of a secondary index by itself. Run snapshot on the table that owns the index.");
                    }
                    return _db.invoke_on_all([ks_name, table_name, tag, sf] (database &db) {
                        auto& cf = db.find_column_family(ks_name, table_name);
                        return cf.snapshot(db, tag, bool(sf));
                    });
                });
            });
        });
    });
}

future<> snapshot_ctl::take_column_family_snapshot(sstring ks_name, sstring cf_name, sstring tag, skip_flush sf) {
    return take_column_family_snapshot(ks_name, std::vector<sstring>{cf_name}, tag, sf);
}

future<> snapshot_ctl::clear_snapshot(sstring tag, std::vector<sstring> keyspace_names, sstring cf_name) {
    return run_snapshot_modify_operation([this, tag = std::move(tag), keyspace_names = std::move(keyspace_names), cf_name = std::move(cf_name)] {
        return _db.local().clear_snapshot(tag, keyspace_names, cf_name);
    });
}

future<std::unordered_map<sstring, std::vector<snapshot_ctl::snapshot_details>>>
snapshot_ctl::get_snapshot_details() {
    static auto details_result_hash = [] (const database::snapshot_details_result& sdr) {
        return utils::tuple_hash()(std::make_tuple(sdr.snapshot_name, sdr.details.ks, sdr.details.cf));
    };

    // only one entry per {snapshot_name, ks, cf} triplet
    using snapshot_acc = std::unordered_set<database::snapshot_details_result, decltype(details_result_hash)>;
    using snapshot_map = std::unordered_map<sstring, std::vector<snapshot_ctl::snapshot_details>>;

    class snapshot_reducer {
    private:
        snapshot_acc _result = snapshot_acc(1, details_result_hash);
    public:
        future<> operator()(const std::vector<database::snapshot_details_result>& value) {
            for (auto&& vp : value) {
                _result.emplace(std::move(vp));
            }
            return make_ready_future<>();
        }

        snapshot_acc get() && {
            return std::move(_result);
        }
    };

    co_return co_await run_snapshot_list_operation([this] () -> future<snapshot_map> {
        snapshot_acc collected = co_await _db.map_reduce(snapshot_reducer(), [] (database& db) -> future<std::vector<database::snapshot_details_result>> {
            co_return co_await db.get_snapshot_details();
        });

        snapshot_map result;
        for (auto&& r : collected) {
            result[r.snapshot_name].emplace_back(std::move(r.details));
            
            co_await coroutine::maybe_yield();
        }

        co_return result;
    });
}

future<int64_t> snapshot_ctl::true_snapshots_size() {
    co_return co_await run_snapshot_list_operation([this] () -> future<int64_t> {
        co_return co_await _db.map_reduce(adder<int64_t>(), [] (database& db) -> future<int64_t> {
            auto results = co_await db.get_snapshot_details();
            int64_t local_total = 0;

            for (const auto& details : results) {
                local_total += details.details.live;

                co_await coroutine::maybe_yield();
            }

            co_return local_total;
        });
    });
}

}
