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
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
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
#include <seastar/core/future-util.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/metrics.hh>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/sliced.hpp>

#include "batchlog_manager.hh"
#include "canonical_mutation.hh"
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
#include "service/storage_service.hh"
#include "schema_registry.hh"
#include "idl/uuid.dist.hh"
#include "idl/frozen_schema.dist.hh"
#include "serializer_impl.hh"
#include "serialization_visitors.hh"
#include "idl/uuid.dist.impl.hh"
#include "idl/frozen_schema.dist.impl.hh"
#include "message/messaging_service.hh"
#include "cql3/untyped_result_set.hh"

static logging::logger blogger("batchlog_manager");

const uint32_t db::batchlog_manager::replay_interval;
const uint32_t db::batchlog_manager::page_size;

db::batchlog_manager::batchlog_manager(cql3::query_processor& qp)
        : _qp(qp) {
    namespace sm = seastar::metrics;

    _metrics.add_group("batchlog_manager", {
        sm::make_derive("total_write_replay_attempts", _stats.write_attempts,
                        sm::description("Counts write operations issued in a batchlog replay flow. "
                                        "The high value of this metric indicates that we have a long batch replay list.")),
    });
}

future<> db::batchlog_manager::do_batch_log_replay() {
    // Use with_semaphore is much simpler, but nested invoke_on can
    // cause deadlock.
    return get_batchlog_manager().invoke_on(0, [] (auto& bm) {
        return bm._sem.wait().then([&bm] {
            return bm._cpu++ % smp::count;
        });
    }).then([] (auto dest) {
        blogger.debug("Batchlog replay on shard {}: starts", dest);
        return get_batchlog_manager().invoke_on(dest, [] (auto& bm) {
            return bm.replay_all_failed_batches();
        }).then([dest] {
            blogger.debug("Batchlog replay on shard {}: done", dest);
        });
    }).finally([] {
        return get_batchlog_manager().invoke_on(0, [] (auto& bm) {
            return bm._sem.signal();
        });
    });
}

future<> db::batchlog_manager::start() {
    // Since replay is a "node global" operation, we should not attempt to do
    // it in parallel on each shard. It will just overlap/interfere.  To
    // simplify syncing between the timer and user initiated replay operations,
    // we use the _timer and _sem on shard zero only. Replaying batchlog can
    // generate a lot of work, so we distrute the real work on all cpus with
    // round-robin scheduling.
    if (engine().cpu_id() == 0) {
        _timer.set_callback([this] {
            do_batch_log_replay().handle_exception([] (auto ep) {
                blogger.error("Exception in batch replay: {}", ep);
            }).finally([this] {
                _timer.arm(lowres_clock::now() + std::chrono::milliseconds(replay_interval));
            });
        });
        auto ring_delay = service::get_local_storage_service().get_ring_delay();
        _timer.arm(lowres_clock::now() + ring_delay);
    }
    return make_ready_future<>();
}

future<> db::batchlog_manager::stop() {
    if (_stop) {
        return make_ready_future<>();
    }
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
    auto timestamp = api::new_timestamp();
    auto data = [this, &mutations] {
        std::vector<canonical_mutation> fm(mutations.begin(), mutations.end());
        bytes_ostream out;
        for (auto& m : fm) {
            ser::serialize(out, m);
        }
        return to_bytes(out.linearize());
    }();

    mutation m(schema, key);
    m.set_cell(clustering_key_prefix::make_empty(), to_bytes("version"), version, timestamp);
    m.set_cell(clustering_key_prefix::make_empty(), to_bytes("written_at"), now, timestamp);
    m.set_cell(clustering_key_prefix::make_empty(), to_bytes("data"), data_value(std::move(data)), timestamp);

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
        auto id = row.get_as<utils::UUID>("id");
        // enough time for the actual write + batchlog entry mutation delivery (two separate requests).
        auto timeout = get_batch_log_timeout();
        if (db_clock::now() < written_at + timeout) {
            blogger.debug("Skipping replay of {}, too fresh", id);
            return make_ready_future<>();
        }

        // check version of serialization format
        if (!row.has("version")) {
            blogger.warn("Skipping logged batch because of unknown version");
            return make_ready_future<>();
        }

        auto version = row.get_as<int32_t>("version");
        if (version != netw::messaging_service::current_version) {
            blogger.warn("Skipping logged batch because of incorrect version");
            return make_ready_future<>();
        }

        auto data = row.get_blob("data");

        blogger.debug("Replaying batch {}", id);

        auto fms = make_lw_shared<std::deque<canonical_mutation>>();
        auto in = ser::as_input_stream(data);
        while (in.size()) {
            fms->emplace_back(ser::deserialize(in, boost::type<canonical_mutation>()));
        }

        auto size = data.size();

        return map_reduce(*fms, [this, written_at] (canonical_mutation& fm) {
            return system_keyspace::get_truncated_at(fm.column_family_id()).then([written_at, &fm] (db_clock::time_point t) ->
                    std::experimental::optional<std::reference_wrapper<canonical_mutation>> {
                if (written_at > t) {
                    return { std::ref(fm) };
                } else {
                    return {};
                }
            });
        },
        std::vector<mutation>(),
        [this] (std::vector<mutation> mutations, std::experimental::optional<std::reference_wrapper<canonical_mutation>> fm) {
            if (fm) {
                schema_ptr s = _qp.db().local().find_schema(fm.value().get().column_family_id());
                mutations.emplace_back(fm.value().get().to_mutation(s));
            }
            return mutations;
        }).then([this, id, limiter, written_at, size, fms] (std::vector<mutation> mutations) {
            if (mutations.empty()) {
                return make_ready_future<>();
            }
            const auto ttl = [this, &mutations, written_at]() -> clock_type {
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
            return limiter->reserve(size).then([this, mutations = std::move(mutations), id] {
                _stats.write_attempts += mutations.size();
                // #1222 - change cl level to ALL, emulating origins behaviour of sending/hinting
                // to all natural end points.
                // Note however that origin uses hints here, and actually allows for this
                // send to partially or wholly fail in actually sending stuff. Since we don't
                // have hints (yet), send with CL=ALL, and hope we can re-do this soon.
                // See below, we use retry on write failure.
                return _qp.proxy().mutate(mutations, db::consistency_level::ALL, db::no_timeout, nullptr);
            });
        }).then_wrapped([this, id](future<> batch_result) {
            try {
                batch_result.get();
            } catch (no_such_keyspace& ex) {
                // should probably ignore and drop the batch
            } catch (...) {
                // timeout, overload etc.
                // Do _not_ remove the batch, assuning we got a node write error.
                // Since we don't have hints (which origin is satisfied with),
                // we have to resort to keeping this batch to next lap.
                return make_ready_future<>();
            }
            // delete batch
            auto schema = _qp.db().local().find_schema(system_keyspace::NAME, system_keyspace::BATCHLOG);
            auto key = partition_key::from_singular(*schema, id);
            mutation m(schema, key);
            auto now = service::client_state(service::client_state::internal_tag()).get_timestamp();
            m.partition().apply_delete(*schema, clustering_key_prefix::make_empty(), tombstone(now, gc_clock::now()));
            return _qp.proxy().mutate_locally(m);
        });
    };

    return seastar::with_gate(_gate, [this, batch = std::move(batch)] {
        blogger.debug("Started replayAllFailedBatches (cpu {})", engine().cpu_id());

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
            blogger.debug("Finished replayAllFailedBatches");
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
