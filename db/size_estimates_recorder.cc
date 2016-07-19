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

#include "db/size_estimates_recorder.hh"
#include <seastar/util/log.hh>
#include "database.hh"
#include "db/system_keyspace.hh"
#include "service/storage_service.hh"

static seastar::logger _logger("size_estimates_recorder");

namespace db {

distributed<size_estimates_recorder> _recorder;

constexpr std::chrono::minutes size_estimates_recorder::RECORD_INTERVAL;

size_estimates_recorder::size_estimates_recorder() {
    if (engine().cpu_id() == 0) {
        service::get_local_migration_manager().register_listener(this);
        _timer.set_callback([this] {
            record_size_estimates().handle_exception([](std::exception_ptr ep) {
                _logger.warn("Exception recording size estimates: {}", ep);
            }).finally([this] {
                _timer.arm(RECORD_INTERVAL);
            });
        });
        _timer.arm(std::chrono::seconds{30});
    }
}

static std::vector<db::system_keyspace::range_estimates> estimates_for(const column_family& cf, const std::vector<query::partition_range>& local_ranges) {
    // for each local primary range, estimate (crudely) mean partition size and partitions count.
    std::vector<db::system_keyspace::range_estimates> estimates;
    estimates.reserve(local_ranges.size());

    for (auto& range : local_ranges) {
        int64_t count{0};
        sstables::estimated_histogram hist{0};
        for (auto&& sstable : cf.select_sstables(range)) {
            count += sstable->get_estimated_key_count();
            hist.merge(sstable->get_stats_metadata().estimated_row_size);
        }
        estimates.emplace_back(&range, db::system_keyspace::partition_estimates{count, count > 0 ? hist.mean() : 0});
    }

    return estimates;
}

future<> size_estimates_recorder::record_size_estimates() {
    // We constrain ourselves to a single CPU since we're dealing with ranges, and
    // multipe CPUs might access the same sstables, skewing the estimations.
    if (engine().cpu_id() != 0) {
        return get_size_estimates_recorder().invoke_on(0, [](auto&& recorder) {
            return recorder.record_size_estimates();
        });
    }

    return with_gate(_gate, [] {
        // We run inside a seastar thread because storage_service::get_local_tokens() expects it.
        return seastar::async([] {
            auto&& ss = service::get_local_storage_service();
            auto&& token_metadata = ss.get_token_metadata();
            if (!token_metadata.is_member(utils::fb_utilities::get_broadcast_address())) {
                _logger.debug("Node is not part of the ring; not recording size estimates");
                return;
            }

            // Find primary token ranges for the local node.
            std::vector<query::partition_range> local_ranges;
            for (auto&& rt : token_metadata.get_primary_ranges_for(ss.get_local_tokens())) {
                local_ranges.emplace_back(std::move(rt).transform([](auto&& t) {
                    return dht::ring_position::starting_at(std::move(t));
                }));
            }

            _logger.debug("Recording size estimates");

            auto&& db = service::get_local_storage_proxy().get_db().local();
            parallel_for_each(db.get_non_system_column_families(), [&local_ranges](lw_shared_ptr<column_family> cf) {
                auto start = std::chrono::steady_clock::now();
                auto s = cf->schema();
                auto estimates = estimates_for(*cf, local_ranges);
                return db::system_keyspace::update_size_estimates(s->ks_name(), s->cf_name(), std::move(estimates)).then([start, s] {
                    auto passed = std::chrono::steady_clock::now() - start;
                    _logger.debug("Spent {} milliseconds on estimating {}.{} size",
                                  std::chrono::duration_cast<std::chrono::milliseconds>(passed).count(),
                                  s->ks_name(), s->cf_name());
                });
            }).get();
        });
    });
}

future<> size_estimates_recorder::stop() {
    if (get_size_estimates_recorder().local_is_initialized()) {
        service::get_local_migration_manager().unregister_listener(this);
        _timer.cancel();
        return _gate.close();
    }
    return make_ready_future();
}

void size_estimates_recorder::on_drop_column_family(const sstring& ks_name, const sstring& cf_name) {
    with_gate(_gate, [&] {
        return db::system_keyspace::clear_size_estimates(ks_name, cf_name).handle_exception([](std::exception_ptr ep) {
            _logger.warn("Error clearing size_estimates {}", ep);
        });
    });
}

}
