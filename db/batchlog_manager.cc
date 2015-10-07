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
 */
/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
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

#include <chrono>
#include <core/future-util.hh>
#include <core/do_with.hh>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/sliced.hpp>

#include "batchlog_manager.hh"
#include "service/storage_service.hh"
#include "service/storage_proxy.hh"
#include "system_keyspace.hh"
#include "utils/rate_limiter.hh"
#include "log.hh"
#include "serializer.hh"
#include "db_clock.hh"
#include "database.hh"
#include "unimplemented.hh"
#include "db/config.hh"
#include "gms/failure_detector.hh"

static logging::logger logger("batchlog_manager");

const uint32_t db::batchlog_manager::replay_interval;
const uint32_t db::batchlog_manager::page_size;

db::batchlog_manager::batchlog_manager(cql3::query_processor& qp)
        : _qp(qp)
        , _e1(_rd())
{}

future<> db::batchlog_manager::start() {
    // Since replay is a "node global" operation, we should not attempt to
    // do it in parallel on each shard. It will just overlap/interfere.
    // Could just run this on cpu 0 or so, but since this _could_ be a
    // lengty operation, we'll round-robin it between shards just in case...
    if (smp::main_thread()) {
        auto cpu = engine().cpu_id();
        _timer.set_callback(
                [this, cpu]() mutable {
                    auto dest = (cpu++ % smp::count);
                    return smp::submit_to(dest, [] {
                                return get_local_batchlog_manager().replay_all_failed_batches();
                            }).then([this] {
                                _timer.arm(lowres_clock::now()
                                        + std::chrono::milliseconds(replay_interval)
                                );
                            });
                });
        _timer.arm(
                lowres_clock::now()
                        + std::chrono::milliseconds(
                                service::storage_service::RING_DELAY));
    }
    return make_ready_future<>();
}

future<> db::batchlog_manager::stop() {
    _stop = true;
    _timer.cancel();
    return _gate.close();
}

future<size_t> db::batchlog_manager::count_all_batches() const {
    sstring query = sprint("SELECT count(*) FROM %s.%s", system_keyspace::NAME, system_keyspace::BATCHLOG);
    return _qp.execute_internal(query).then([](::shared_ptr<cql3::untyped_result_set> rs) {
       return size_t(rs->one().get_as<int64_t>("count"));
    });
}

mutation db::batchlog_manager::get_batch_log_mutation_for(const std::vector<mutation>& mutations, const utils::UUID& id, int32_t version) {
    return get_batch_log_mutation_for(mutations, id, version, db_clock::now());
}

mutation db::batchlog_manager::get_batch_log_mutation_for(const std::vector<mutation>& mutations, const utils::UUID& id, int32_t version, db_clock::time_point now) {
    auto schema = _qp.db().local().find_schema(system_keyspace::NAME, system_keyspace::BATCHLOG);
    auto key = partition_key::from_singular(*schema, id);
    auto timestamp = db_clock::now_in_usecs();
    auto data = [this, &mutations] {
        std::vector<frozen_mutation> fm(mutations.begin(), mutations.end());
        const auto size = std::accumulate(fm.begin(), fm.end(), size_t(0), [](size_t s, auto& m) {
            return s + serializer<frozen_mutation>{m}.size();
        });
        bytes buf(bytes::initialized_later(), size);
        data_output out(buf);
        for (auto& m : fm) {
            serializer<frozen_mutation>{m}(out);
        }
        return buf;
    }();

    mutation m(key, schema);
    m.set_cell({}, to_bytes("version"), version, timestamp);
    m.set_cell({}, to_bytes("written_at"), now, timestamp);
    m.set_cell({}, to_bytes("data"), std::move(data), timestamp);

    return m;
}

db_clock::duration db::batchlog_manager::get_batch_log_timeout() const {
    // enough time for the actual write + BM removal mutation
    return db_clock::duration(_qp.db().local().get_config().write_request_timeout_in_ms()) * 2;
}

future<> db::batchlog_manager::replay_all_failed_batches() {
    typedef db_clock::rep clock_type;

    // rate limit is in bytes per second. Uses Double.MAX_VALUE if disabled (set to 0 in cassandra.yaml).
    // max rate is scaled by the number of nodes in the cluster (same as for HHOM - see CASSANDRA-5272).
    auto throttle_in_kb = _qp.db().local().get_config().batchlog_replay_throttle_in_kb() / service::get_storage_service().local().get_token_metadata().get_all_endpoints().size();
    auto limiter = make_lw_shared<utils::rate_limiter>(throttle_in_kb * 1000);

    auto batch = [this, limiter](const cql3::untyped_result_set::row& row) {
        auto written_at = row.get_as<db_clock::time_point>("written_at");
        // enough time for the actual write + batchlog entry mutation delivery (two separate requests).
        // enough time for the actual write + batchlog entry mutation delivery (two separate requests).
        auto timeout = get_batch_log_timeout();
        if (db_clock::now() < written_at + timeout) {
            return make_ready_future<>();
        }
        // not used currently. ever?
        //auto version = row.has("version") ? row.get_as<uint32_t>("version") : /*MessagingService.VERSION_12*/6u;
        auto id = row.get_as<utils::UUID>("id");
        auto data = row.get_blob("data");

        logger.debug("Replaying batch {}", id);

        auto fms = make_lw_shared<std::deque<frozen_mutation>>();
        data_input in(data);
        while (in.has_next()) {
            fms->emplace_back(serializer<frozen_mutation>::read(in));
        }

        auto mutations = make_lw_shared<std::vector<mutation>>();
        auto size = data.size();

        return repeat([this, fms = std::move(fms), written_at, mutations]() mutable {
            if (fms->empty()) {
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }
            auto& fm = fms->front();
            auto mid = fm.column_family_id();
            return system_keyspace::get_truncated_at(mid).then([this, &fm, written_at, mutations](db_clock::time_point t) {
                auto schema = _qp.db().local().find_schema(fm.column_family_id());
                if (written_at > t) {
                    auto schema = _qp.db().local().find_schema(fm.column_family_id());
                    mutations->emplace_back(fm.unfreeze(schema));
                }
            }).then([fms] {
                fms->pop_front();
                return make_ready_future<stop_iteration>(stop_iteration::no);
            });
        }).then([this, id, mutations, limiter, written_at, size] {
            if (mutations->empty()) {
                return make_ready_future<>();
            }
            const auto ttl = [this, mutations, written_at]() -> clock_type {
                /*
                 * Calculate ttl for the mutations' hints (and reduce ttl by the time the mutations spent in the batchlog).
                 * This ensures that deletes aren't "undone" by an old batch replay.
                 */
                auto unadjusted_ttl = std::numeric_limits<gc_clock::rep>::max();
                warn(unimplemented::cause::HINT);
#if 0
                for (auto& m : *mutations) {
                    unadjustedTTL = Math.min(unadjustedTTL, HintedHandOffManager.calculateHintTTL(mutation));
                }
#endif
                return unadjusted_ttl - std::chrono::duration_cast<gc_clock::duration>(db_clock::now() - written_at).count();
            }();

            if (ttl <= 0) {
                return make_ready_future<>();
            }
            // Origin does the send manually, however I can't see a super great reason to do so.
            // Our normal write path does not add much redundancy to the dispatch, and rate is handled after send
            // in both cases.
            // FIXME: verify that the above is reasonably true.
            return limiter->reserve(size).then([this, mutations, id] {
                return _qp.proxy().local().mutate(std::move(*mutations), db::consistency_level::ANY);
            });
        }).then([this, id] {
            // delete batch
            auto schema = _qp.db().local().find_schema(system_keyspace::NAME, system_keyspace::BATCHLOG);
            auto key = partition_key::from_singular(*schema, id);
            mutation m(key, schema);
            auto now = service::client_state(service::client_state::internal_tag()).get_timestamp();
            m.partition().apply_delete(*schema, {}, tombstone(now, gc_clock::now()));
            return _qp.proxy().local().mutate_locally(m);
        });
    };

    return seastar::with_gate(_gate, [this, batch = std::move(batch)] {
        logger.debug("Started replayAllFailedBatches");

        typedef ::shared_ptr<cql3::untyped_result_set> page_ptr;
        sstring query = sprint("SELECT id, data, written_at, version FROM %s.%s LIMIT %d", system_keyspace::NAME, system_keyspace::BATCHLOG, page_size);
        return _qp.execute_internal(query).then([this, batch = std::move(batch)](page_ptr page) {
            return do_with(std::move(page), [this, batch = std::move(batch)](page_ptr & page) mutable {
                return repeat([this, &page, batch = std::move(batch)]() mutable {
                    if (page->empty()) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    }
                    auto id = page->back().get_as<utils::UUID>("id");
                    return parallel_for_each(*page, batch).then([this, &page, id]() {
                        if (page->size() < page_size) {
                            return make_ready_future<stop_iteration>(stop_iteration::yes); // we've exhausted the batchlog, next query would be empty.
                        }
                        sstring query = sprint("SELECT id, data, written_at, version FROM %s.%s WHERE token(id) > token(?) LIMIT %d",
                                system_keyspace::NAME,
                                system_keyspace::BATCHLOG,
                                page_size);
                        return _qp.execute_internal(query, {id}).then([&page](auto res) {
                                    page = std::move(res);
                                    return make_ready_future<stop_iteration>(stop_iteration::no);
                                });
                    });
                });
            });
        }).then([this] {
        // TODO FIXME : cleanup()
#if 0
            ColumnFamilyStore cfs = Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.BATCHLOG);
            cfs.forceBlockingFlush();
            Collection<Descriptor> descriptors = new ArrayList<>();
            for (SSTableReader sstr : cfs.getSSTables())
            descriptors.add(sstr.descriptor);
            if (!descriptors.isEmpty()) // don't pollute the logs if there is nothing to compact.
            CompactionManager.instance.submitUserDefined(cfs, descriptors, Integer.MAX_VALUE).get();

#endif

        }).then([this] {
            logger.debug("Finished replayAllFailedBatches");
        });
    });
}

std::unordered_set<gms::inet_address> db::batchlog_manager::endpoint_filter(const sstring& local_rack, const std::unordered_map<sstring, std::unordered_set<gms::inet_address>>& endpoints) {
    // special case for single-node data centers
    if (endpoints.size() == 1 && endpoints.begin()->second.size() == 1) {
        return endpoints.begin()->second;
    }

    // strip out dead endpoints and localhost
    std::unordered_multimap<sstring, gms::inet_address> validated;

    auto is_valid = [](gms::inet_address input) {
        return input != utils::fb_utilities::get_broadcast_address()
            && gms::get_local_failure_detector().is_alive(input)
            ;
    };

    for (auto& e : endpoints) {
        for (auto& a : e.second) {
            if (is_valid(a)) {
                validated.emplace(e.first, a);
            }
        }
    }

    typedef std::unordered_set<gms::inet_address> return_type;

    if (validated.size() <= 2) {
        return boost::copy_range<return_type>(validated | boost::adaptors::map_values);
    }

    if (validated.size() - validated.count(local_rack) >= 2) {
        // we have enough endpoints in other racks
        validated.erase(local_rack);
    }

    if (validated.bucket_count() == 1) {
        // we have only 1 `other` rack
        auto res = validated | boost::adaptors::map_values;
        if (validated.size() > 2) {
            return boost::copy_range<return_type>(
                    boost::copy_range<std::vector<gms::inet_address>>(res)
                            | boost::adaptors::sliced(0, 2));
        }
        return boost::copy_range<return_type>(res);
    }

    // randomize which racks we pick from if more than 2 remaining

    std::vector<sstring> racks = boost::copy_range<std::vector<sstring>>(validated | boost::adaptors::map_keys);

    if (validated.bucket_count() > 2) {
        std::shuffle(racks.begin(), racks.end(), _e1);
        racks.resize(2);
    }

    std::unordered_set<gms::inet_address> result;

    // grab a random member of up to two racks
    for (auto& rack : racks) {
        auto rack_members = validated.bucket(rack);
        auto n = validated.bucket_size(rack_members);
        auto cpy = boost::copy_range<std::vector<gms::inet_address>>(validated.equal_range(rack) | boost::adaptors::map_values);
        std::uniform_int_distribution<size_t> rdist(0, n - 1);
        result.emplace(cpy[rdist(_e1)]);
    }

    return result;
}


distributed<db::batchlog_manager> db::_the_batchlog_manager;
