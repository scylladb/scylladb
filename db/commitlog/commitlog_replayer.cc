/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
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

#include <memory>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <boost/range/adaptor/map.hpp>

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include "commitlog.hh"
#include "commitlog_replayer.hh"
#include "database.hh"
#include "sstables/sstables.hh"
#include "db/system_keyspace.hh"
#include "log.hh"
#include "converting_mutation_partition_applier.hh"
#include "schema_registry.hh"
#include "commitlog_entry.hh"
#include "service/priority_manager.hh"
#include "db/extensions.hh"
#include "utils/fragmented_temporary_buffer.hh"
#include "validation.hh"

static logging::logger rlogger("commitlog_replayer");

class db::commitlog_replayer::impl {
    struct column_mappings {
        std::unordered_map<table_schema_version, column_mapping> map;
        future<> stop() { return make_ready_future<>(); }
    };

    // we want the processing methods to be const, since they use
    // shard-sharing of data -> read only
    // this one is special since it is thread local.
    // Should actually make sharded::local a const function (it does
    // not modify content), but...
    mutable seastar::sharded<column_mappings> _column_mappings;

    friend class db::commitlog_replayer;
public:
    impl(seastar::sharded<database>& db);

    future<> init();

    struct stats {
        uint64_t invalid_mutations = 0;
        uint64_t skipped_mutations = 0;
        uint64_t applied_mutations = 0;
        uint64_t corrupt_bytes = 0;

        stats& operator+=(const stats& s) {
            invalid_mutations += s.invalid_mutations;
            skipped_mutations += s.skipped_mutations;
            applied_mutations += s.applied_mutations;
            corrupt_bytes += s.corrupt_bytes;
            return *this;
        }
        stats operator+(const stats& s) const {
            stats tmp = *this;
            tmp += s;
            return tmp;
        }
    };

    // move start/stop of the thread local bookkeep to "top level"
    // and also make sure to assert on it actually being started.
    future<> start() {
        return _column_mappings.start();
    }
    future<> stop() {
        return _column_mappings.stop();
    }

    future<> process(stats*, commitlog::buffer_and_replay_position buf_rp) const;
    future<stats> recover(sstring file, const sstring& fname_prefix) const;

    typedef std::unordered_map<utils::UUID, replay_position> rp_map;
    typedef std::unordered_map<unsigned, rp_map> shard_rpm_map;
    typedef std::unordered_map<unsigned, replay_position> shard_rp_map;

    replay_position min_pos(unsigned shard) const {
        auto i = _min_pos.find(shard);
        return i != _min_pos.end() ? i->second : replay_position();
    }
    replay_position cf_min_pos(const utils::UUID& uuid, unsigned shard) const {
        auto i = _rpm.find(shard);
        if (i == _rpm.end()) {
            return replay_position();
        }
        auto j = i->second.find(uuid);
        return j != i->second.end() ? j->second : replay_position();
    }

    seastar::sharded<database>&
        _db;
    shard_rpm_map
        _rpm;
    shard_rp_map
        _min_pos;
};

db::commitlog_replayer::impl::impl(seastar::sharded<database>& db)
    : _db(db)
{}

future<> db::commitlog_replayer::impl::init() {
    return _db.map_reduce([this](shard_rpm_map map) {
        for (auto& p1 : map) {
            for (auto& p2 : p1.second) {
                auto& pp = _rpm[p1.first][p2.first];
                pp = std::max(pp, p2.second);

                auto i = _min_pos.find(p1.first);
                if (i == _min_pos.end() || p2.second < i->second) {
                    _min_pos[p1.first] = p2.second;
                }
            }
        }
    }, [this](database& db) {
        return do_with(shard_rpm_map{}, [this, &db](shard_rpm_map& map) {
            return parallel_for_each(db.get_column_families(), [&map, &db](auto& cfp) {
                auto uuid = cfp.first;
                // We do this on each cpu, for each CF, which technically is a little wasteful, but the values are
                // cached, this is only startup, and it makes the code easier.
                // Get all truncation records for the CF and initialize max rps if
                // present. Cannot do this on demand, as there may be no sstables to
                // mark the CF as "needed".
                return db::system_keyspace::get_truncated_position(uuid).then([&map, uuid](std::vector<db::replay_position> tpps) {
                    for (auto& p : tpps) {
                        rlogger.trace("CF {} truncated at {}", uuid, p);
                        auto& pp = map[p.shard_id()][uuid];
                        pp = std::max(pp, p);
                    }
                });
            }).then([&map] {
                return make_ready_future<shard_rpm_map>(map);
            });
        });
    }).finally([this] {
        // bugfix: the above map-reduce will not_ detect if sstables
        // are _missing_ from a CF. And because of re-sharding, we can't
        // just insert initial zeros into the maps, because we don't know
        // how many shards there was last time.
        // However, this only affects global min pos, since
        // for each CF, the worst that happens is that we have a missing
        // entry -> empty replay_pos == min value. But calculating
        // global min pos will be off, since we will only base it on
        // existing sstables-per-shard.
        // So, go through all CF:s and check, if a shard mapping does not
        // have data for it, assume we must set global pos to zero.
        for (auto&p : _db.local().get_column_families()) {
            for (auto&p1 : _rpm) { // for each shard
                if (!p1.second.contains(p.first)) {
                    _min_pos[p1.first] = replay_position();
                }
            }
        }
        for (auto&p : _min_pos) {
            rlogger.debug("minimum position for shard {}: {}", p.first, p.second);
        }
        for (auto&p1 : _rpm) {
            for (auto& p2 : p1.second) {
                rlogger.debug("replay position for shard/uuid {}/{}: {}", p1.first, p2.first, p2.second);
            }
        }
    });
}

future<db::commitlog_replayer::impl::stats>
db::commitlog_replayer::impl::recover(sstring file, const sstring& fname_prefix) const {
    assert(_column_mappings.local_is_initialized());

    replay_position rp{commitlog::descriptor(file, fname_prefix)};
    auto gp = min_pos(rp.shard_id());

    if (rp.id < gp.id) {
        rlogger.debug("skipping replay of fully-flushed {}", file);
        return make_ready_future<stats>();
    }
    position_type p = 0;
    if (rp.id == gp.id) {
        p = gp.pos;
    }

    auto s = make_lw_shared<stats>();
    auto& exts = _db.local().extensions();

    return db::commitlog::read_log_file(file, fname_prefix, service::get_local_commitlog_priority(),
            std::bind(&impl::process, this, s.get(), std::placeholders::_1),
            p, &exts).then_wrapped([s](future<> f) {
        try {
            f.get();
        } catch (commitlog::segment_data_corruption_error& e) {
            s->corrupt_bytes += e.bytes();
        } catch (...) {
            throw;
        }
        return make_ready_future<stats>(*s);
    });
}

future<> db::commitlog_replayer::impl::process(stats* s, commitlog::buffer_and_replay_position buf_rp) const {
    auto&& buf = buf_rp.buffer;
    auto&& rp = buf_rp.position;
    try {

        commitlog_entry_reader cer(buf);
        auto& fm = cer.mutation();

        auto& local_cm = _column_mappings.local().map;
        auto cm_it = local_cm.find(fm.schema_version());
        if (cm_it == local_cm.end()) {
            if (!cer.get_column_mapping()) {
                throw std::runtime_error(format("unknown schema version {{}}", fm.schema_version()));
            }
            rlogger.debug("new schema version {} in entry {}", fm.schema_version(), rp);
            cm_it = local_cm.emplace(fm.schema_version(), *cer.get_column_mapping()).first;
        }
        const column_mapping& src_cm = cm_it->second;

        auto shard_id = rp.shard_id();
        if (rp < min_pos(shard_id)) {
            rlogger.trace("entry {} is less than global min position. skipping", rp);
            s->skipped_mutations++;
            return make_ready_future<>();
        }

        auto uuid = fm.column_family_id();
        auto cf_rp = cf_min_pos(uuid, shard_id);
        if (rp <= cf_rp) {
            rlogger.trace("entry {} at {} is younger than recorded replay position {}. skipping", fm.column_family_id(), rp, cf_rp);
            s->skipped_mutations++;
            return make_ready_future<>();
        }

        auto shard = _db.local().shard_of(fm);
        return _db.invoke_on(shard, [this, cer = std::move(cer), &src_cm, rp, shard, s] (database& db) mutable -> future<> {
            auto& fm = cer.mutation();
            // TODO: might need better verification that the deserialized mutation
            // is schema compatible. My guess is that just applying the mutation
            // will not do this.
            auto& cf = db.find_column_family(fm.column_family_id());

            if (rlogger.is_enabled(logging::log_level::debug)) {
                rlogger.debug("replaying at {} v={} {}:{} at {}", fm.column_family_id(), fm.schema_version(),
                        cf.schema()->ks_name(), cf.schema()->cf_name(), rp);
            }
            if (const auto err = validation::is_cql_key_invalid(*cf.schema(), fm.key()); err) {
                throw std::runtime_error(fmt::format("found entry with invalid key {} at {} v={} {}:{} at {}: {}.", fm.key(), fm.column_family_id(),
                        fm.schema_version(), cf.schema()->ks_name(), cf.schema()->cf_name(), rp, *err));
            }
            // Removed forwarding "new" RP. Instead give none/empty.
            // This is what origin does, and it should be fine.
            // The end result should be that once sstables are flushed out
            // their "replay_position" attribute will be empty, which is
            // lower than anything the new session will produce.
            if (cf.schema()->version() != fm.schema_version()) {
                auto& local_cm = _column_mappings.local().map;
                auto cm_it = local_cm.try_emplace(fm.schema_version(), src_cm).first;
                const column_mapping& cm = cm_it->second;
                mutation m(cf.schema(), fm.decorated_key(*cf.schema()));
                converting_mutation_partition_applier v(cm, *cf.schema(), m.partition());
                fm.partition().accept(cm, v);
                return do_with(std::move(m), [&db, &cf] (const mutation& m) {
                    return db.apply_in_memory(m, cf, db::rp_handle(), db::no_timeout);
                });
            } else {
                return do_with(std::move(cer).mutation(), [&](const frozen_mutation& m) {
                    return db.apply_in_memory(m, cf.schema(), db::rp_handle(), db::no_timeout);
                });
            }
        }).then_wrapped([s] (future<> f) {
            try {
                f.get();
                s->applied_mutations++;
            } catch (...) {
                s->invalid_mutations++;
                // TODO: write mutation to file like origin.
                rlogger.warn("error replaying: {}", std::current_exception());
            }
        });
    } catch (no_such_column_family&) {
        // No such CF now? Origin just ignores this.
    } catch (...) {
        s->invalid_mutations++;
        // TODO: write mutation to file like origin.
        rlogger.warn("error replaying: {}", std::current_exception());
    }

    return make_ready_future<>();
}

db::commitlog_replayer::commitlog_replayer(seastar::sharded<database>& db)
    : _impl(std::make_unique<impl>(db))
{}

db::commitlog_replayer::commitlog_replayer(commitlog_replayer&& r) noexcept
    : _impl(std::move(r._impl))
{}

db::commitlog_replayer::~commitlog_replayer()
{}

future<db::commitlog_replayer> db::commitlog_replayer::create_replayer(seastar::sharded<database>& db) {
    return do_with(commitlog_replayer(db), [](auto&& rp) {
        auto f = rp._impl->init();
        return f.then([rp = std::move(rp)]() mutable {
            return make_ready_future<commitlog_replayer>(std::move(rp));
        });
    });
}

future<> db::commitlog_replayer::recover(std::vector<sstring> files, sstring fname_prefix) {
    typedef std::unordered_multimap<unsigned, sstring> shard_file_map;

    rlogger.info("Replaying {}", join(", ", files));

    // pre-compute work per shard already.
    auto map = ::make_lw_shared<shard_file_map>();
    for (auto& f : files) {
        commitlog::descriptor d(f, fname_prefix);
        replay_position p = d;
        map->emplace(p.shard_id() % smp::count, std::move(f));
    }

    return do_with(std::move(fname_prefix), [this, map] (sstring& fname_prefix) {
        return _impl->start().then([this, map, &fname_prefix] {
            return map_reduce(smp::all_cpus(), [this, map, &fname_prefix] (unsigned id) {
                return smp::submit_to(id, [this, id, map, &fname_prefix] () {
                    auto total = ::make_lw_shared<impl::stats>();
                    // TODO: or something. For now, we do this serialized per shard,
                    // to reduce mutation congestion. We could probably (says avi)
                    // do 2 segments in parallel or something, but lets use this first.
                    auto range = map->equal_range(id);
                    return do_for_each(range.first, range.second, [this, total, &fname_prefix] (const std::pair<unsigned, sstring>& p) {
                        auto&f = p.second;
                        rlogger.debug("Replaying {}", f);
                        return _impl->recover(f, fname_prefix).then([f, total](impl::stats stats) {
                            if (stats.corrupt_bytes != 0) {
                                rlogger.warn("Corrupted file: {}. {} bytes skipped.", f, stats.corrupt_bytes);
                            }
                            rlogger.debug("Log replay of {} complete, {} replayed mutations ({} invalid, {} skipped)"
                                            , f
                                            , stats.applied_mutations
                                            , stats.invalid_mutations
                                            , stats.skipped_mutations
                            );
                            *total += stats;
                        });
                    }).then([total] {
                        return make_ready_future<impl::stats>(*total);
                    });
                });
            }, impl::stats(), std::plus<impl::stats>()).then([](impl::stats totals) {
                rlogger.info("Log replay complete, {} replayed mutations ({} invalid, {} skipped)"
                                , totals.applied_mutations
                                , totals.invalid_mutations
                                , totals.skipped_mutations
                );
            });
        }).finally([this] {
            return _impl->stop();
        });
    });
}

future<> db::commitlog_replayer::recover(sstring f, sstring fname_prefix) {
    return recover(std::vector<sstring>{ f }, std::move(fname_prefix));
}

