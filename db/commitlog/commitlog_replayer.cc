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
 * Modified by Cloudius Systems
 * Copyright 2015 Cloudius Systems
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

#include <core/future.hh>
#include <core/sharded.hh>

#include "commitlog.hh"
#include "commitlog_replayer.hh"
#include "database.hh"
#include "sstables/sstables.hh"
#include "db/system_keyspace.hh"
#include "db/serializer.hh"
#include "cql3/query_processor.hh"
#include "log.hh"
#include "converting_mutation_partition_applier.hh"
#include "schema_registry.hh"
#include "commitlog_entry.hh"

static logging::logger logger("commitlog_replayer");

class db::commitlog_replayer::impl {
    std::unordered_map<table_schema_version, column_mapping> _column_mappings;
public:
    impl(seastar::sharded<cql3::query_processor>& db);

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

    future<> process(stats*, temporary_buffer<char> buf, replay_position rp);
    future<stats> recover(sstring file);

    typedef std::unordered_map<utils::UUID, replay_position> rp_map;
    typedef std::unordered_map<unsigned, rp_map> shard_rpm_map;
    typedef std::unordered_map<unsigned, replay_position> shard_rp_map;

    seastar::sharded<cql3::query_processor>&
        _qp;
    shard_rpm_map
        _rpm;
    shard_rp_map
        _min_pos;
};

db::commitlog_replayer::impl::impl(seastar::sharded<cql3::query_processor>& qp)
    : _qp(qp)
{}

future<> db::commitlog_replayer::impl::init() {
    return _qp.map_reduce([this](shard_rpm_map map) {
        for (auto& p1 : map) {
            for (auto& p2 : p1.second) {
                auto& pp = _rpm[p1.first][p2.first];
                pp = std::max(pp, p2.second);

                auto& min = _min_pos[p1.first];
                min = (min == replay_position()) ? p2.second : std::min(p2.second, min);
            }
        }
    }, [this](cql3::query_processor& qp) {
        return do_with(shard_rpm_map{}, [this, &qp](shard_rpm_map& map) {
            return parallel_for_each(qp.db().local().get_column_families(), [&map, &qp](auto& cfp) {
                auto uuid = cfp.first;
                for (auto& sst : *cfp.second->get_sstables() | boost::adaptors::map_values) {
                    try {
                        auto p = sst->get_stats_metadata().position;
                        logger.trace("sstable {} -> rp {}", sst->get_filename(), p);
                        if (p != replay_position()) {
                            auto& pp = map[p.shard_id()][uuid];
                            pp = std::max(pp, p);
                        }
                    } catch (...) {
                        logger.warn("Could not read sstable metadata {}", std::current_exception());
                    }
                }
                // We do this on each cpu, for each CF, which technically is a little wasteful, but the values are
                // cached, this is only startup, and it makes the code easier.
                // Get all truncation records for the CF and initialize max rps if
                // present. Cannot do this on demand, as there may be no sstables to
                // mark the CF as "needed".
                return db::system_keyspace::get_truncated_position(uuid).then([&map, &uuid](std::vector<db::replay_position> tpps) {
                    for (auto& p : tpps) {
                        logger.trace("CF {} truncated at {}", uuid, p);
                        auto& pp = map[p.shard_id()][uuid];
                        pp = std::max(pp, p);
                    }
                });
            }).then([&map] {
                return make_ready_future<shard_rpm_map>(map);
            });
        });
    }).finally([this] {
        for (auto&p : _min_pos) {
            logger.debug("minimum position for shard {}: {}", p.first, p.second);
        }
        for (auto&p1 : _rpm) {
            for (auto& p2 : p1.second) {
                logger.debug("replay position for shard/uuid {}/{}: {}", p1.first, p2.first, p2.second);
            }
        }
    });
}

future<db::commitlog_replayer::impl::stats>
db::commitlog_replayer::impl::recover(sstring file) {
    replay_position rp{commitlog::descriptor(file)};
    auto gp = _min_pos[rp.shard_id()];

    if (rp.id < gp.id) {
        logger.debug("skipping replay of fully-flushed {}", file);
        return make_ready_future<stats>();
    }
    position_type p = 0;
    if (rp.id == gp.id) {
        p = gp.pos;
    }

    auto s = make_lw_shared<stats>();

    return db::commitlog::read_log_file(file,
            std::bind(&impl::process, this, s.get(), std::placeholders::_1,
                    std::placeholders::_2), p).then([](auto s) {
        auto f = s->done();
        return f.finally([s = std::move(s)] {});
    }).then_wrapped([s](future<> f) {
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

future<> db::commitlog_replayer::impl::process(stats* s, temporary_buffer<char> buf, replay_position rp) {
    try {

        commitlog_entry_reader cer(buf);
        auto& fm = cer.mutation();

        auto cm_it = _column_mappings.find(fm.schema_version());
        if (cm_it == _column_mappings.end()) {
            if (!cer.get_column_mapping()) {
                throw std::runtime_error(sprint("unknown schema version {}", fm.schema_version()));
            }
            logger.debug("new schema version {} in entry {}", fm.schema_version(), rp);
            cm_it = _column_mappings.emplace(fm.schema_version(), *cer.get_column_mapping()).first;
        }

        auto shard_id = rp.shard_id();
        if (rp < _min_pos[shard_id]) {
            logger.trace("entry {} is less than global min position. skipping", rp);
            s->skipped_mutations++;
            return make_ready_future<>();
        }

        auto uuid = fm.column_family_id();
        auto& map = _rpm[shard_id];
        auto i = map.find(uuid);
        if (i != map.end() && rp <= i->second) {
            logger.trace("entry {} at {} is younger than recorded replay position {}. skipping", fm.column_family_id(), rp, i->second);
            s->skipped_mutations++;
            return make_ready_future<>();
        }

        auto shard = _qp.local().db().local().shard_of(fm);
        return _qp.local().db().invoke_on(shard, [this, cer = std::move(cer), cm_it, rp, shard, s] (database& db) -> future<> {
            auto& fm = cer.mutation();
            // TODO: might need better verification that the deserialized mutation
            // is schema compatible. My guess is that just applying the mutation
            // will not do this.
            auto& cf = db.find_column_family(fm.column_family_id());

            if (logger.is_enabled(logging::log_level::debug)) {
                logger.debug("replaying at {} v={} {}:{} at {}", fm.column_family_id(), fm.schema_version(),
                        cf.schema()->ks_name(), cf.schema()->cf_name(), rp);
            }
            // Removed forwarding "new" RP. Instead give none/empty.
            // This is what origin does, and it should be fine.
            // The end result should be that once sstables are flushed out
            // their "replay_position" attribute will be empty, which is
            // lower than anything the new session will produce.
            if (cf.schema()->version() != fm.schema_version()) {
                const column_mapping& cm = cm_it->second;
                mutation m(fm.decorated_key(*cf.schema()), cf.schema());
                converting_mutation_partition_applier v(cm, *cf.schema(), m.partition());
                fm.partition().accept(cm, v);
                cf.apply(std::move(m));
            } else {
                cf.apply(fm, cf.schema());
            }
            s->applied_mutations++;
            return make_ready_future<>();
        }).handle_exception([s](auto ep) {
            s->invalid_mutations++;
            // TODO: write mutation to file like origin.
            logger.warn("error replaying: {}", ep);
        });
    } catch (no_such_column_family&) {
        // No such CF now? Origin just ignores this.
    } catch (...) {
        s->invalid_mutations++;
        // TODO: write mutation to file like origin.
        logger.warn("error replaying: {}", std::current_exception());
    }

    return make_ready_future<>();
}

db::commitlog_replayer::commitlog_replayer(seastar::sharded<cql3::query_processor>& qp)
    : _impl(std::make_unique<impl>(qp))
{}

db::commitlog_replayer::commitlog_replayer(commitlog_replayer&& r) noexcept
    : _impl(std::move(r._impl))
{}

db::commitlog_replayer::~commitlog_replayer()
{}

future<db::commitlog_replayer> db::commitlog_replayer::create_replayer(seastar::sharded<cql3::query_processor>& qp) {
    return do_with(commitlog_replayer(qp), [](auto&& rp) {
        auto f = rp._impl->init();
        return f.then([rp = std::move(rp)]() mutable {
            return make_ready_future<commitlog_replayer>(std::move(rp));
        });
    });
}

future<> db::commitlog_replayer::recover(std::vector<sstring> files) {
    logger.info("Replaying {}", join(", ", files));
    return map_reduce(files, [this](auto f) {
        logger.debug("Replaying {}", f);
        return _impl->recover(f).then([f](impl::stats stats) {
            if (stats.corrupt_bytes != 0) {
                logger.warn("Corrupted file: {}. {} bytes skipped.", f, stats.corrupt_bytes);
            }
            logger.debug("Log replay of {} complete, {} replayed mutations ({} invalid, {} skipped)"
                            , f
                            , stats.applied_mutations
                            , stats.invalid_mutations
                            , stats.skipped_mutations
            );
            return make_ready_future<impl::stats>(stats);
        }).handle_exception([f](auto ep) -> future<impl::stats> {
            logger.error("Error recovering {}: {}", f, ep);
            try {
                std::rethrow_exception(ep);
            } catch (std::invalid_argument&) {
                logger.error("Scylla cannot process {}. Make sure to fully flush all Cassandra commit log files to sstable before migrating.", f);
                throw;
            } catch (...) {
                throw;
            }
        });
    }, impl::stats(), std::plus<impl::stats>()).then([](impl::stats totals) {
        logger.info("Log replay complete, {} replayed mutations ({} invalid, {} skipped)"
                        , totals.applied_mutations
                        , totals.invalid_mutations
                        , totals.skipped_mutations
        );
    });
}

future<> db::commitlog_replayer::recover(sstring f) {
    return recover(std::vector<sstring>{ f });
}

