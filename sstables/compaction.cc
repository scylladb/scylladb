/*
 * Copyright (C) 2015 ScyllaDB
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

#include <vector>
#include <map>
#include <functional>
#include <utility>
#include <assert.h>
#include <algorithm>

#include <boost/range/algorithm.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/join.hpp>

#include "core/future-util.hh"
#include "core/pipe.hh"

#include "sstables.hh"
#include "compaction.hh"
#include "database.hh"
#include "mutation_reader.hh"
#include "schema.hh"
#include "db/system_keyspace.hh"
#include "db/query_context.hh"
#include "service/storage_service.hh"
#include "service/priority_manager.hh"
#include "db_clock.hh"
#include "mutation_compactor.hh"
#include "leveled_manifest.hh"

namespace sstables {

logging::logger logger("compaction");

class sstable_reader final : public ::mutation_reader::impl {
    shared_sstable _sst;
    mutation_reader _reader;
public:
    sstable_reader(shared_sstable sst, schema_ptr schema)
            : _sst(std::move(sst))
            , _reader(_sst->read_rows(schema, service::get_local_compaction_priority()))
            {}
    virtual future<streamed_mutation_opt> operator()() override {
        return _reader.read().handle_exception([sst = _sst] (auto ep) {
            logger.error("Compaction found an exception when reading sstable {} : {}",
                    sst->get_filename(), ep);
            return make_exception_future<streamed_mutation_opt>(ep);
        });
    }
};

static api::timestamp_type get_max_purgeable_timestamp(const column_family& cf, sstable_set::incremental_selector& selector,
        const std::unordered_set<shared_sstable>& compacting_set, const dht::decorated_key& dk) {
    auto timestamp = api::max_timestamp;
    for (auto&& sst : boost::range::join(selector.select(dk.token()), cf.compacted_undeleted_sstables())) {
        if (compacting_set.count(sst)) {
            continue;
        }
        if (sst->filter_has_key(*cf.schema(), dk.key())) {
            timestamp = std::min(timestamp, sst->get_stats_metadata().min_timestamp);
        }
    }
    return timestamp;
}

static bool belongs_to_current_node(const dht::token& t, const std::vector<nonwrapping_range<dht::token>>& sorted_owned_ranges) {
    auto low = std::lower_bound(sorted_owned_ranges.begin(), sorted_owned_ranges.end(), t,
            [] (const range<dht::token>& a, const dht::token& b) {
        // check that range a is before token b.
        return a.after(b, dht::token_comparator());
    });

    if (low != sorted_owned_ranges.end()) {
        const nonwrapping_range<dht::token>& r = *low;
        return r.contains(t, dht::token_comparator());
    }

    return false;
}

static void delete_sstables_for_interrupted_compaction(std::vector<shared_sstable>& new_sstables, sstring& ks, sstring& cf) {
    // Delete either partially or fully written sstables of a compaction that
    // was either stopped abruptly (e.g. out of disk space) or deliberately
    // (e.g. nodetool stop COMPACTION).
    for (auto& sst : new_sstables) {
        logger.debug("Deleting sstable {} of interrupted compaction for {}.{}", sst->get_filename(), ks, cf);
        sst->mark_for_deletion();
    }
}

static std::vector<shared_sstable> get_uncompacting_sstables(column_family& cf, std::vector<shared_sstable>& sstables) {
    auto all_sstables = boost::copy_range<std::vector<shared_sstable>>(*cf.get_sstables_including_compacted_undeleted());
    boost::sort(all_sstables, [] (const shared_sstable& x, const shared_sstable& y) {
        return x->generation() < y->generation();
    });
    std::sort(sstables.begin(), sstables.end(), [] (const shared_sstable& x, const shared_sstable& y) {
        return x->generation() < y->generation();
    });
    std::vector<shared_sstable> not_compacted_sstables;
    boost::set_difference(all_sstables, sstables,
        std::back_inserter(not_compacted_sstables), [] (const shared_sstable& x, const shared_sstable& y) {
            return x->generation() < y->generation();
        });
    return not_compacted_sstables;
}

class compacting_sstable_writer {
    const schema& _schema;
    std::function<shared_sstable()> _creator;
    uint64_t _partitions_per_sstable;
    uint64_t _max_sstable_size;
    uint32_t _sstable_level;
    db::replay_position _rp;
    std::vector<unsigned long> _ancestors;
    compaction_info& _info;
    shared_sstable _sst;
    stdx::optional<sstable_writer> _writer;
private:
    void finish_sstable_write() {
        _writer->consume_end_of_stream();
        _writer = stdx::nullopt;

        _sst->open_data().get0();
        _info.end_size += _sst->data_size();
    }
public:
    compacting_sstable_writer(const schema& s, std::function<shared_sstable()> creator, uint64_t partitions_per_sstable,
                              uint64_t max_sstable_size, uint32_t sstable_level, db::replay_position rp,
                              std::vector<unsigned long> ancestors, compaction_info& info)
        : _schema(s)
        , _creator(creator)
        , _partitions_per_sstable(partitions_per_sstable)
        , _max_sstable_size(max_sstable_size)
        , _sstable_level(sstable_level)
        , _rp(rp)
        , _ancestors(std::move(ancestors))
        , _info(info)
    { }

    void consume_new_partition(const dht::decorated_key& dk) {
        if (_info.is_stop_requested()) {
            // Compaction manager will catch this exception and re-schedule the compaction.
            throw compaction_stop_exception(_info.ks, _info.cf, _info.stop_requested);
        }
        if (!_writer) {
            _sst = _creator();
            _info.new_sstables.push_back(_sst);
            _sst->get_metadata_collector().set_replay_position(_rp);
            _sst->get_metadata_collector().sstable_level(_sstable_level);
            for (auto ancestor : _ancestors) {
                _sst->add_ancestor(ancestor);
            }

            auto&& priority = service::get_local_compaction_priority();
            _writer.emplace(_sst->get_writer(_schema, _partitions_per_sstable, _max_sstable_size, false, priority));
        }
        _info.total_keys_written++;
        _writer->consume_new_partition(dk);
    }

    void consume(tombstone t) { _writer->consume(t); }
    stop_iteration consume(static_row&& sr, tombstone, bool) { return _writer->consume(std::move(sr)); }
    stop_iteration consume(clustering_row&& cr, tombstone, bool) { return _writer->consume(std::move(cr)); }
    stop_iteration consume(range_tombstone&& rt) { return _writer->consume(std::move(rt)); }

    stop_iteration consume_end_of_partition() {
        auto ret = _writer->consume_end_of_partition();
        if (ret == stop_iteration::yes) {
            finish_sstable_write();
        }
        return ret;
    }

    void consume_end_of_stream() {
        if (_writer) {
            finish_sstable_write();
        }
    }
};

// compact_sstables compacts the given list of sstables creating one
// (currently) or more (in the future) new sstables. The new sstables
// are created using the "sstable_creator" object passed by the caller.
future<std::vector<shared_sstable>>
compact_sstables(std::vector<shared_sstable> sstables, column_family& cf, std::function<shared_sstable()> creator,
                 uint64_t max_sstable_size, uint32_t sstable_level, bool cleanup) {
    return seastar::async([sstables = std::move(sstables), &cf, creator = std::move(creator), max_sstable_size, sstable_level, cleanup] () mutable {
        // keep a immutable copy of sstable set because selector needs it alive
        // and also sstables created after compaction shouldn't be considered.
        const sstable_set s = cf.get_sstable_set();
        auto selector = s.make_incremental_selector();
        std::vector<::mutation_reader> readers;
        uint64_t estimated_partitions = 0;
        std::vector<unsigned long> ancestors;
        auto info = make_lw_shared<compaction_info>();
        auto& cm = cf.get_compaction_manager();
        sstring sstable_logger_msg = "[";

        info->type = (cleanup) ? compaction_type::Cleanup : compaction_type::Compaction;
        // register compaction_stats of starting compaction into compaction manager
        cm.register_compaction(info);

        assert(sstables.size() > 0);

        db::replay_position rp;

        auto schema = cf.schema();
        for (auto sst : sstables) {
            // We also capture the sstable, so we keep it alive while the read isn't done
            readers.emplace_back(make_mutation_reader<sstable_reader>(sst, schema));
            // FIXME: If the sstables have cardinality estimation bitmaps, use that
            // for a better estimate for the number of partitions in the merged
            // sstable than just adding up the lengths of individual sstables.
            estimated_partitions += sst->get_estimated_key_count();
            info->total_partitions += sst->get_estimated_key_count();
            // Compacted sstable keeps track of its ancestors.
            ancestors.push_back(sst->generation());
            sstable_logger_msg += sprint("%s:level=%d, ", sst->get_filename(), sst->get_sstable_level());
            info->start_size += sst->data_size();
            // TODO:
            // Note that this is not fully correct. Since we might be merging sstables that originated on
            // another shard (#cpu changed), we might be comparing RP:s with differing shard ids,
            // which might vary in "comparable" size quite a bit. However, since the worst that happens
            // is that we might miss a high water mark for the commit log replayer,
            // this is kind of ok, esp. since we will hopefully not be trying to recover based on
            // compacted sstables anyway (CL should be clean by then).
            rp = std::max(rp, sst->get_stats_metadata().position);
        }

        uint64_t estimated_sstables = std::max(1UL, uint64_t(ceil(double(info->start_size) / max_sstable_size)));
        uint64_t partitions_per_sstable = ceil(double(estimated_partitions) / estimated_sstables);

        sstable_logger_msg += "]";
        info->sstables = sstables.size();
        info->ks = schema->ks_name();
        info->cf = schema->cf_name();
        logger.info("{} {}", (!cleanup) ? "Compacting" : "Cleaning", sstable_logger_msg);

        std::vector<nonwrapping_range<dht::token>> owned_ranges;
        if (cleanup) {
            owned_ranges = service::get_local_storage_service().get_local_ranges(schema->ks_name());
        }

        auto reader = make_combined_reader(std::move(readers));

        auto start_time = db_clock::now();

        std::unordered_set<shared_sstable> compacting_set(sstables.begin(), sstables.end());
        auto get_max_purgeable = [&cf, &selector, &compacting_set] (const dht::decorated_key& dk) {
            return get_max_purgeable_timestamp(cf, selector, compacting_set, dk);
        };
        auto cr = compacting_sstable_writer(*schema, creator, partitions_per_sstable, max_sstable_size, sstable_level, rp, std::move(ancestors), *info);
        auto cfc = make_stable_flattened_mutations_consumer<compact_for_compaction<compacting_sstable_writer>>(
                *schema, gc_clock::now(), std::move(cr), get_max_purgeable);

        auto filter = [cleanup, sorted_owned_ranges = std::move(owned_ranges)] (const streamed_mutation& sm) {
            if (dht::shard_of(sm.decorated_key().token()) != engine().cpu_id()) {
                return false;
            }
            if (cleanup && !belongs_to_current_node(sm.decorated_key().token(), sorted_owned_ranges)) {
                return false;
            }
            return true;
        };

        try {
            consume_flattened_in_thread(reader, cfc, filter);
        } catch (...) {
            cm.deregister_compaction(info);
            delete_sstables_for_interrupted_compaction(info->new_sstables, info->ks, info->cf);
            throw;
        }

        // deregister compaction_stats of finished compaction from compaction manager.
        cm.deregister_compaction(info);

        double ratio = double(info->end_size) / double(info->start_size);
        auto end_time = db_clock::now();
        // time taken by compaction in seconds.
        auto duration = std::chrono::duration<float>(end_time - start_time);
        auto throughput = (double(info->end_size) / (1024*1024)) / duration.count();
        sstring new_sstables_msg;

        for (auto& newtab : info->new_sstables) {
            new_sstables_msg += sprint("%s:level=%d, ", newtab->get_filename(), newtab->get_sstable_level());
        }

        // FIXME: there is some missing information in the log message below.
        // look at CompactionTask::runMayThrow() in origin for reference.
        // - add support to merge summary (message: Partition merge counts were {%s}.).
        // - there is no easy way, currently, to know the exact number of total partitions.
        // By the time being, using estimated key count.
        logger.info("{} {} sstables to [{}]. {} bytes to {} (~{}% of original) in {}ms = {}MB/s. " \
            "~{} total partitions merged to {}.",
            (!cleanup) ? "Compacted" : "Cleaned",
            info->sstables, new_sstables_msg, info->start_size, info->end_size, (int) (ratio * 100),
            std::chrono::duration_cast<std::chrono::milliseconds>(duration).count(), throughput,
            info->total_partitions, info->total_keys_written);

        // If compaction is running for testing purposes, detect that there is
        // no query context and skip code that updates compaction history.
        if (!db::qctx) {
            return std::move(info->new_sstables);
        }

        // Skip code that updates compaction history if running on behalf of a cleanup job.
        if (cleanup) {
            return std::move(info->new_sstables);
        }

        auto compacted_at = std::chrono::duration_cast<std::chrono::milliseconds>(end_time.time_since_epoch()).count();

        // FIXME: add support to merged_rows. merged_rows is a histogram that
        // shows how many sstables each row is merged from. This information
        // cannot be accessed until we make combined_reader more generic,
        // for example, by adding a reducer method.
        db::system_keyspace::update_compaction_history(info->ks, info->cf, compacted_at,
            info->start_size, info->end_size, std::unordered_map<int32_t, int64_t>{}).get0();

        // Return vector with newly created sstable(s).
        return std::move(info->new_sstables);
    });
}

std::vector<sstables::shared_sstable>
get_fully_expired_sstables(column_family& cf, std::vector<sstables::shared_sstable>& compacting, int32_t gc_before) {
    logger.debug("Checking droppable sstables in {}.{}", cf.schema()->ks_name(), cf.schema()->cf_name());

    if (compacting.empty()) {
        return {};
    }

    std::list<sstables::shared_sstable> candidates;
    auto uncompacting_sstables = get_uncompacting_sstables(cf, compacting);
    // Get list of uncompacting sstables that overlap the ones being compacted.
    std::vector<sstables::shared_sstable> overlapping = leveled_manifest::overlapping(*cf.schema(), compacting, uncompacting_sstables);
    int64_t min_timestamp = std::numeric_limits<int64_t>::max();

    for (auto& sstable : overlapping) {
        if (sstable->get_stats_metadata().max_local_deletion_time >= gc_before) {
            min_timestamp = std::min(min_timestamp, sstable->get_stats_metadata().min_timestamp);
        }
    }

    // SStables that do not contain live data is added to list of possibly expired sstables.
    for (auto& candidate : compacting) {
        logger.debug("Checking if candidate of generation {} and max_deletion_time {} is expired, gc_before is {}",
                    candidate->generation(), candidate->get_stats_metadata().max_local_deletion_time, gc_before);
        if (candidate->get_stats_metadata().max_local_deletion_time < gc_before) {
            logger.debug("Adding candidate of generation {} to list of possibly expired sstables", candidate->generation());
            candidates.push_back(candidate);
        } else {
            min_timestamp = std::min(min_timestamp, candidate->get_stats_metadata().min_timestamp);
        }
    }

    auto it = candidates.begin();
    while (it != candidates.end()) {
        auto& candidate = *it;
        // Remove from list any candidate that may contain a tombstone that covers older data.
        if (candidate->get_stats_metadata().max_timestamp >= min_timestamp) {
            it = candidates.erase(it);
        } else {
            logger.debug("Dropping expired SSTable {} (maxLocalDeletionTime={}, gcBefore={})",
                    candidate->get_filename(), candidate->get_stats_metadata().max_local_deletion_time, gc_before);
            it++;
        }
    }
    return std::vector<sstables::shared_sstable>(candidates.begin(), candidates.end());
}

}
