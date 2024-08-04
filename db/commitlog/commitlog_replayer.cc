/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "utils/assert.hh"
#include <memory>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <boost/range/adaptor/map.hpp>

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include "commitlog.hh"
#include "commitlog_replayer.hh"
#include "replica/database.hh"
#include "db/system_keyspace.hh"
#include "log.hh"
#include "converting_mutation_partition_applier.hh"
#include "commitlog_entry.hh"
#include "validation.hh"
#include "mutation/mutation_partition_view.hh"

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
    impl(seastar::sharded<replica::database>& db, seastar::sharded<db::system_keyspace>& sys_ks);

    future<> init();

    struct stats {
        uint64_t invalid_mutations = 0;
        uint64_t skipped_mutations = 0;
        uint64_t applied_mutations = 0;
        uint64_t corrupt_bytes = 0;
        uint64_t truncated_at = 0;

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
    // and also make sure to SCYLLA_ASSERT on it actually being started.
    future<> start() {
        return _column_mappings.start();
    }
    future<> stop() {
        return _column_mappings.stop();
    }

    future<> process(stats*, commitlog::buffer_and_replay_position buf_rp) const;
    future<stats> recover(sstring file, const sstring& fname_prefix) const;

    typedef std::unordered_map<table_id, replay_position> rp_map;
    typedef std::unordered_map<unsigned, rp_map> shard_rpm_map;
    typedef std::unordered_map<unsigned, replay_position> shard_rp_map;

    replay_position min_pos(unsigned shard) const {
        auto i = _min_pos.find(shard);
        return i != _min_pos.end() ? i->second : replay_position();
    }
    replay_position cf_min_pos(const table_id& uuid, unsigned shard) const {
        auto i = _rpm.find(shard);
        if (i == _rpm.end()) {
            return replay_position();
        }
        auto j = i->second.find(uuid);
        return j != i->second.end() ? j->second : replay_position();
    }
    replay_position token_min_pos(const table_id& uuid, unsigned shard, dht::token token) const {
        replay_position rp;
        if (auto i = _cleanup_map.find({uuid, shard}); i != _cleanup_map.end()) {
            if (auto candidate_rp = i->second.get(dht::token::to_int64(token))) {
                rp = *candidate_rp;
            }
        }
        return rp;
    }

    seastar::sharded<replica::database>& _db;
    seastar::sharded<db::system_keyspace>& _sys_ks;
    shard_rpm_map _rpm;
    db::system_keyspace::commitlog_cleanup_map _cleanup_map;
    shard_rp_map _min_pos;
};

db::commitlog_replayer::impl::impl(seastar::sharded<replica::database>& db, seastar::sharded<db::system_keyspace>& sys_ks)
    : _db(db)
    , _sys_ks(sys_ks)
{}

future<> db::commitlog_replayer::impl::init() {
    co_await _db.local().get_tables_metadata().parallel_for_each_table([this] (table_id uuid, lw_shared_ptr<replica::table>) -> future<> {
        const auto rps = co_await _sys_ks.local().get_truncated_positions(uuid);
        for (const auto& p: rps) {
            rlogger.trace("CF {} truncated at {}", uuid, p);
            auto &pp = _rpm[p.shard_id()][uuid];
            pp = std::max(pp, p);
            const auto i = _min_pos.find(p.shard_id());
            if (i == _min_pos.end() || p < i->second) {
                _min_pos[p.shard_id()] = p;
            }
        }
    });
    _cleanup_map = co_await _sys_ks.local().get_commitlog_cleanup_records();

    // bugfix: the above code will not_ detect if sstables
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
    _db.local().get_tables_metadata().for_each_table([&] (table_id id, lw_shared_ptr<replica::table>) {
        for (auto&p1 : _rpm) { // for each shard
            if (!p1.second.contains(id)) {
                _min_pos[p1.first] = replay_position();
            }
        }
    });

    for (auto&p : _min_pos) {
        rlogger.debug("minimum position for shard {}: {}", p.first, p.second);
    }
    for (auto&p1 : _rpm) {
        for (auto& p2 : p1.second) {
            rlogger.debug("replay position for shard/uuid {}/{}: {}", p1.first, p2.first, p2.second);
        }
    }
}

future<db::commitlog_replayer::impl::stats>
db::commitlog_replayer::impl::recover(sstring file, const sstring& fname_prefix) const {
    SCYLLA_ASSERT(_column_mappings.local_is_initialized());

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

    return db::commitlog::read_log_file(file, fname_prefix,
            std::bind(&impl::process, this, s.get(), std::placeholders::_1),
            p, &exts).then_wrapped([s](future<> f) {
        try {
            f.get();
        } catch (commitlog::segment_data_corruption_error& e) {
            s->corrupt_bytes += e.bytes();
        } catch (commitlog::segment_truncation& e) {
            s->truncated_at = e.position();
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
                rlogger.debug("replaying at {} v={} at {}", fm.column_family_id(), fm.schema_version(), rp);
                throw std::runtime_error(format("unknown schema version {}, table=", fm.schema_version(), fm.column_family_id()));
            }
            rlogger.debug("new schema version {} in entry {}", fm.schema_version(), rp);
            cm_it = local_cm.emplace(fm.schema_version(), *cer.get_column_mapping()).first;
        }
        const column_mapping& src_cm = cm_it->second;

        auto shard_id = rp.shard_id();
        if (rp < min_pos(shard_id)) {
            rlogger.trace("entry {} is less than global min position. skipping", rp);
            s->skipped_mutations++;
            co_return;
        }

        auto uuid = fm.column_family_id();
        auto& table = _db.local().find_column_family(uuid);
        const auto& schema = *table.schema();
        auto token = fm.token(schema);

        auto cf_rp = cf_min_pos(uuid, shard_id);
        if (rp <= cf_rp) {
            rlogger.trace("entry {} at {} is younger than recorded replay position {}. skipping", fm.column_family_id(), rp, cf_rp);
            s->skipped_mutations++;
            co_return;
        }

        auto token_range_rp = token_min_pos(uuid, shard_id, token);
        if (rp <= token_range_rp) {
            rlogger.trace("entry {}, token {} in table {}, is younger than recorded replay position {} for its token range. skipping",
                          rp, token, fm.column_family_id(), token_range_rp);
            s->skipped_mutations++;
            co_return;
        }

      auto apply = [&] (seastar::shard_id shard) {
        return _db.invoke_on(shard, [this, &fm, &src_cm, rp] (replica::database& db) mutable -> future<> {
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
                return db.apply_in_memory(fm, cf.schema(), db::rp_handle(), db::no_timeout);
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
      };
        auto shards = table.get_effective_replication_map()->shard_for_writes(schema, token);
        if (shards.empty()) {
            rlogger.debug("no shard for token {} in table {}", token, uuid);
            s->skipped_mutations++;
        } else {
            co_await seastar::parallel_for_each(shards, apply);
        }
    } catch (replica::no_such_column_family&) {
        // No such CF now? Origin just ignores this.
    } catch (...) {
        s->invalid_mutations++;
        // TODO: write mutation to file like origin.
        rlogger.warn("error replaying: {}", std::current_exception());
    }
}

db::commitlog_replayer::commitlog_replayer(seastar::sharded<replica::database>& db, seastar::sharded<db::system_keyspace>& sys_ks)
    : _impl(std::make_unique<impl>(db, sys_ks))
{}

db::commitlog_replayer::commitlog_replayer(commitlog_replayer&& r) noexcept
    : _impl(std::move(r._impl))
{}

db::commitlog_replayer::~commitlog_replayer()
{}

future<db::commitlog_replayer> db::commitlog_replayer::create_replayer(seastar::sharded<replica::database>& db, seastar::sharded<db::system_keyspace>& sys_ks) {
    return do_with(commitlog_replayer(db, sys_ks), [](auto&& rp) {
        auto f = rp._impl->init();
        return f.then([rp = std::move(rp)]() mutable {
            return make_ready_future<commitlog_replayer>(std::move(rp));
        });
    });
}

future<> db::commitlog_replayer::recover(std::vector<sstring> files, sstring fname_prefix) {
    typedef std::unordered_multimap<unsigned, sstring> shard_file_map;

    rlogger.info("Replaying {}", fmt::join(files, ", "));

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
                            if (stats.truncated_at != 0) {
                                rlogger.warn("Truncated file: {} at position {}.", f, stats.truncated_at);
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

