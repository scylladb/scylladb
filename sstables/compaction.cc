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
    virtual future<mutation_opt> operator()() override {
        return _reader.read().handle_exception([sst = _sst] (auto ep) {
            logger.error("Compaction found an exception when reading sstable {} : {}",
                    sst->get_filename(), ep);
            return make_exception_future<mutation_opt>(ep);
        });
    }
};

static api::timestamp_type get_max_purgeable_timestamp(schema_ptr schema,
    const std::vector<shared_sstable>& not_compacted_sstables, const dht::decorated_key& dk)
{
    auto timestamp = api::max_timestamp;
    for (auto&& sst : not_compacted_sstables) {
        if (sst->filter_has_key(*schema, dk.key())) {
            timestamp = std::min(timestamp, sst->get_stats_metadata().min_timestamp);
        }
    }
    return timestamp;
}

static bool belongs_to_current_node(const dht::token& t, const std::vector<range<dht::token>>& sorted_owned_ranges) {
    auto low = std::lower_bound(sorted_owned_ranges.begin(), sorted_owned_ranges.end(), t,
            [] (const range<dht::token>& a, const dht::token& b) {
        // check that range a is before token b.
        return a.after(b, dht::token_comparator());
    });

    if (low != sorted_owned_ranges.end()) {
        const range<dht::token>& r = *low;
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

// compact_sstables compacts the given list of sstables creating one
// (currently) or more (in the future) new sstables. The new sstables
// are created using the "sstable_creator" object passed by the caller.
future<std::vector<shared_sstable>>
compact_sstables(std::vector<shared_sstable> sstables, column_family& cf, std::function<shared_sstable()> creator,
                 uint64_t max_sstable_size, uint32_t sstable_level, bool cleanup) {
    std::vector<::mutation_reader> readers;
    uint64_t estimated_partitions = 0;
    auto ancestors = make_lw_shared<std::vector<unsigned long>>();
    auto info = make_lw_shared<compaction_info>();
    auto& cm = cf.get_compaction_manager();
    sstring sstable_logger_msg = "[";

    info->type = (cleanup) ? compaction_type::Cleanup : compaction_type::Compaction;
    // register compaction_stats of starting compaction into compaction manager
    cm.register_compaction(info);

    assert(sstables.size() > 0);

    db::replay_position rp;

    auto all_sstables = cf.get_sstables_including_compacted_undeleted();
    std::sort(sstables.begin(), sstables.end(), [] (const shared_sstable& x, const shared_sstable& y) {
        return x->generation() < y->generation();
    });
    std::vector<shared_sstable> not_compacted_sstables;
    boost::set_difference(*all_sstables | boost::adaptors::map_values, sstables,
        std::back_inserter(not_compacted_sstables), [] (const shared_sstable& x, const shared_sstable& y) {
            return x->generation() < y->generation();
        });

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
        ancestors->push_back(sst->generation());
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

    class compacting_reader final : public ::mutation_reader::impl {
    private:
        schema_ptr _schema;
        ::mutation_reader _reader;
        std::vector<shared_sstable> _not_compacted_sstables;
        gc_clock::time_point _now;
        std::vector<range<dht::token>> _sorted_owned_ranges;
        bool _cleanup;
    public:
        compacting_reader(schema_ptr schema, std::vector<::mutation_reader> readers, std::vector<shared_sstable> not_compacted_sstables,
                std::vector<range<dht::token>> sorted_owned_ranges, bool cleanup)
            : _schema(std::move(schema))
            , _reader(make_combined_reader(std::move(readers)))
            , _not_compacted_sstables(std::move(not_compacted_sstables))
            , _now(gc_clock::now())
            , _sorted_owned_ranges(std::move(sorted_owned_ranges))
            , _cleanup(cleanup)
        { }

        virtual future<mutation_opt> operator()() override {
            return _reader().then([this] (mutation_opt m) {
                if (!bool(m)) {
                    return make_ready_future<mutation_opt>(std::move(m));
                }
                // Filter out mutation that doesn't belong to current shard.
                if (dht::shard_of(m->token()) != engine().cpu_id()) {
                    return operator()();
                }
                if (_cleanup && !belongs_to_current_node(m->token(), _sorted_owned_ranges)) {
                    return operator()();
                }
                auto max_purgeable = get_max_purgeable_timestamp(_schema, _not_compacted_sstables, m->decorated_key());
                m->partition().compact_for_compaction(*_schema, max_purgeable, _now);
                if (!m->partition().empty()) {
                    return make_ready_future<mutation_opt>(std::move(m));
                }
                return operator()();
            });
        }
    };
    std::vector<range<dht::token>> owned_ranges;
    if (cleanup) {
        owned_ranges = service::get_local_storage_service().get_local_ranges(schema->ks_name());
    }
    auto reader = make_mutation_reader<compacting_reader>(schema, std::move(readers), std::move(not_compacted_sstables),
        std::move(owned_ranges), cleanup);

    auto start_time = db_clock::now();

    // We use a fixed-sized pipe between the producer fiber (which reads the
    // individual sstables and merges them) and the consumer fiber (which
    // only writes to the sstable). Things would have worked without this
    // pipe (the writing fiber would have also performed the reads), but we
    // prefer to do less work in the writer (which is a seastar::thread),
    // and also want the extra buffer to ensure we do fewer context switches
    // to that seastar::thread.
    // TODO: better tuning for the size of the pipe. Perhaps should take into
    // account the size of the individual mutations?
    seastar::pipe<mutation> output{16};
    auto output_reader = make_lw_shared<seastar::pipe_reader<mutation>>(std::move(output.reader));
    auto output_writer = make_lw_shared<seastar::pipe_writer<mutation>>(std::move(output.writer));

    future<> read_done = repeat([output_writer, reader = std::move(reader), info] () mutable {
        if (info->is_stop_requested()) {
            // Compaction manager will catch this exception and re-schedule the compaction.
            throw compaction_stop_exception(info->ks, info->cf, info->stop_requested);
        }
        return reader().then([output_writer, info] (auto mopt) {
            if (mopt) {
                info->total_keys_written++;
                return output_writer->write(std::move(*mopt)).then([] {
                    return make_ready_future<stop_iteration>(stop_iteration::no);
                });
            } else {
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }
        });
    }).then([output_writer] {});

    struct queue_reader final : public ::mutation_reader::impl {
        lw_shared_ptr<seastar::pipe_reader<mutation>> pr;
        queue_reader(lw_shared_ptr<seastar::pipe_reader<mutation>> pr) : pr(std::move(pr)) {}
        virtual future<mutation_opt> operator()() override {
            return pr->read().then([] (std::experimental::optional<mutation> m) mutable {
                return make_ready_future<mutation_opt>(std::move(m));
            });
        }
    };

    bool backup = cf.incremental_backups_enabled();
    // If there is a maximum size for a sstable, it's possible that more than
    // one sstable will be generated for all partitions to be written.
    future<> write_done = repeat([creator, ancestors, rp, max_sstable_size, sstable_level, output_reader, info, partitions_per_sstable, schema, backup] {
        return output_reader->read().then(
                [creator, ancestors, rp, max_sstable_size, sstable_level, output_reader, info, partitions_per_sstable, schema, backup] (auto mut) {
            // Check if mutation is available from the pipe for a new sstable to be written. If not, just stop writing.
            if (!mut) {
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }
            // If a mutation is available, we must unread it for write_components to read it afterwards.
            output_reader->unread(std::move(*mut));

            auto newtab = creator();
            info->new_sstables.push_back(newtab);
            newtab->get_metadata_collector().set_replay_position(rp);
            newtab->get_metadata_collector().sstable_level(sstable_level);
            for (auto ancestor : *ancestors) {
                newtab->add_ancestor(ancestor);
            }

            ::mutation_reader mutation_queue_reader = make_mutation_reader<queue_reader>(output_reader);

            auto&& priority = service::get_local_compaction_priority();
            return newtab->write_components(std::move(mutation_queue_reader), partitions_per_sstable, schema, max_sstable_size, backup, priority).then([newtab, info] {
                return newtab->open_data().then([newtab, info] {
                    info->end_size += newtab->data_size();
                    return make_ready_future<stop_iteration>(stop_iteration::no);
                });
            }).handle_exception([sst = newtab] (auto ep) {
                logger.error("Compaction found an exception when writing sstable {} : {}",
                        sst->get_filename(), ep);
                return make_exception_future<stop_iteration>(ep);
            });
        });
    }).then([output_reader] {});

    // Wait for both read_done and write_done fibers to finish.
    return when_all(std::move(read_done), std::move(write_done)).then([&cm, info] (std::tuple<future<>, future<>> t) {
        // deregister compaction_stats of finished compaction from compaction manager.
        cm.deregister_compaction(info);

        sstring ex;
        try {
            std::get<0>(t).get();
        } catch(compaction_stop_exception& e) {

            std::get<1>(t).ignore_ready_future(); // ignore result of write fiber if compaction was asked to stop.
            delete_sstables_for_interrupted_compaction(info->new_sstables, info->ks, info->cf);
            throw;
        } catch(...) {

            ex += sprint("read exception: %s", std::current_exception());
        }

        try {
            std::get<1>(t).get();
        } catch(...) {
            ex += sprint("%swrite_exception: %s", (ex.size() ? ", " : ""), std::current_exception());
        }

        if (ex.size()) {
            delete_sstables_for_interrupted_compaction(info->new_sstables, info->ks, info->cf);

            throw std::runtime_error(ex);
        }
    }).then([start_time, info, cleanup] {
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
            return make_ready_future<>();
        }

        // Skip code that updates compaction history if running on behalf of a cleanup job.
        if (cleanup) {
            return make_ready_future<>();
        }

        auto compacted_at = std::chrono::duration_cast<std::chrono::milliseconds>(end_time.time_since_epoch()).count();

        // FIXME: add support to merged_rows. merged_rows is a histogram that
        // shows how many sstables each row is merged from. This information
        // cannot be accessed until we make combined_reader more generic,
        // for example, by adding a reducer method.
        return db::system_keyspace::update_compaction_history(info->ks, info->cf, compacted_at,
                info->start_size, info->end_size, std::unordered_map<int32_t, int64_t>{});
    }).then([info] {
        // Return vector with newly created sstable(s).
        return std::move(info->new_sstables);
    });
}

}
