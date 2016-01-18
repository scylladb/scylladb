/*
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
#include "compaction_strategy.hh"
#include "mutation_reader.hh"
#include "schema.hh"
#include "cql3/statements/property_definitions.hh"
#include "leveled_manifest.hh"
#include "db/system_keyspace.hh"
#include "db/query_context.hh"
#include "service/storage_service.hh"

namespace sstables {

logging::logger logger("compaction");

class sstable_reader final : public ::mutation_reader::impl {
    shared_sstable _sst;
    mutation_reader _reader;
public:
    sstable_reader(shared_sstable sst, schema_ptr schema)
            : _sst(std::move(sst)), _reader(_sst->read_rows(schema)) {}
    virtual future<mutation_opt> operator()() override {
        return _reader.read();
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
            [] (const range<dht::token>& a, const dht::token b) {
        // check that range a is before token b.
        return a.after(b, dht::token_comparator());
    });

    if (low != sorted_owned_ranges.end()) {
        const range<dht::token>& r = *low;
        return r.contains(t, dht::token_comparator());
    }

    return false;
}

// compact_sstables compacts the given list of sstables creating one
// (currently) or more (in the future) new sstables. The new sstables
// are created using the "sstable_creator" object passed by the caller.
future<> compact_sstables(std::vector<shared_sstable> sstables, column_family& cf, std::function<shared_sstable()> creator,
                          uint64_t max_sstable_size, uint32_t sstable_level, bool cleanup) {
    std::vector<::mutation_reader> readers;
    uint64_t estimated_partitions = 0;
    auto ancestors = make_lw_shared<std::vector<unsigned long>>();
    auto stats = make_lw_shared<compaction_stats>();
    auto& cm = cf.get_compaction_manager();
    sstring sstable_logger_msg = "[";

    // register compaction_stats of starting compaction into compaction manager
    cm.register_compaction(stats);

    assert(sstables.size() > 0);

    db::replay_position rp;

    auto all_sstables = cf.get_sstables();
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
        stats->total_partitions += sst->get_estimated_key_count();
        // Compacted sstable keeps track of its ancestors.
        ancestors->push_back(sst->generation());
        sstable_logger_msg += sprint("%s:level=%d, ", sst->get_filename(), sst->get_sstable_level());
        stats->start_size += sst->data_size();
        // TODO:
        // Note that this is not fully correct. Since we might be merging sstables that originated on
        // another shard (#cpu changed), we might be comparing RP:s with differing shard ids,
        // which might vary in "comparable" size quite a bit. However, since the worst that happens
        // is that we might miss a high water mark for the commit log replayer,
        // this is kind of ok, esp. since we will hopefully not be trying to recover based on
        // compacted sstables anyway (CL should be clean by then).
        rp = std::max(rp, sst->get_stats_metadata().position);
    }

    uint64_t estimated_sstables = std::max(1UL, uint64_t(ceil(double(stats->start_size) / max_sstable_size)));
    uint64_t partitions_per_sstable = ceil(double(estimated_partitions) / estimated_sstables);

    sstable_logger_msg += "]";
    stats->sstables = sstables.size();
    stats->ks = schema->ks_name();
    stats->cf = schema->cf_name();
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
        // sort owned ranges
        std::sort(owned_ranges.begin(), owned_ranges.end(), [](range<dht::token>& a, range<dht::token>& b) {
            if (!a.start()) {
                return true;
            }
            if (!b.start()) {
                return false;
            }
            const dht::token& a_start = a.start()->value();
            const dht::token& b_start = b.start()->value();
            return a_start < b_start;
        });
    }
    auto reader = make_mutation_reader<compacting_reader>(schema, std::move(readers), std::move(not_compacted_sstables),
        std::move(owned_ranges), cleanup);

    auto start_time = std::chrono::steady_clock::now();

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

    future<> read_done = repeat([output_writer, reader = std::move(reader), stats] () mutable {
        return reader().then([output_writer, stats] (auto mopt) {
            if (mopt) {
                stats->total_keys_written++;
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

    // If there is a maximum size for a sstable, it's possible that more than
    // one sstable will be generated for all partitions to be written.
    future<> write_done = repeat([creator, ancestors, rp, max_sstable_size, sstable_level, output_reader, stats, partitions_per_sstable, schema] {
        return output_reader->read().then(
                [creator, ancestors, rp, max_sstable_size, sstable_level, output_reader, stats, partitions_per_sstable, schema] (auto mut) {
            // Check if mutation is available from the pipe for a new sstable to be written. If not, just stop writing.
            if (!mut) {
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }
            // If a mutation is available, we must unread it for write_components to read it afterwards.
            output_reader->unread(std::move(*mut));

            auto newtab = creator();
            newtab->get_metadata_collector().set_replay_position(rp);
            newtab->get_metadata_collector().sstable_level(sstable_level);
            for (auto ancestor : *ancestors) {
                newtab->add_ancestor(ancestor);
            }

            ::mutation_reader mutation_queue_reader = make_mutation_reader<queue_reader>(output_reader);

            return newtab->write_components(std::move(mutation_queue_reader), partitions_per_sstable, schema, max_sstable_size).then([newtab, stats] {
                return newtab->open_data().then([newtab, stats] {
                    stats->new_sstables.push_back(newtab);
                    stats->end_size += newtab->data_size();
                    return make_ready_future<stop_iteration>(stop_iteration::no);
                });
            });
        });
    }).then([output_reader] {});

    // Wait for both read_done and write_done fibers to finish.
    return when_all(std::move(read_done), std::move(write_done)).then([&cm, stats] (std::tuple<future<>, future<>> t) {
        // deregister compaction_stats of finished compaction from compaction manager.
        cm.deregister_compaction(stats);

        sstring ex;
        try {
            std::get<0>(t).get();
        } catch(...) {
            ex += sprint("read exception: %s", std::current_exception());
        }

        try {
            std::get<1>(t).get();
        } catch(...) {
            ex += sprint("%swrite_exception: %s", (ex.size() ? ", " : ""), std::current_exception());
        }

        if (ex.size()) {
            throw std::runtime_error(ex);
        }
    }).then([start_time, stats, cleanup] {
        double ratio = double(stats->end_size) / double(stats->start_size);
        auto end_time = std::chrono::steady_clock::now();
        // time taken by compaction in seconds.
        auto duration = std::chrono::duration<float>(end_time - start_time);
        auto throughput = (double(stats->end_size) / (1024*1024)) / duration.count();
        sstring new_sstables_msg;

        for (auto& newtab : stats->new_sstables) {
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
            stats->sstables, new_sstables_msg, stats->start_size, stats->end_size, (int) (ratio * 100),
            std::chrono::duration_cast<std::chrono::milliseconds>(duration).count(), throughput,
            stats->total_partitions, stats->total_keys_written);

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
        return db::system_keyspace::update_compaction_history(stats->ks, stats->cf, compacted_at,
                stats->start_size, stats->end_size, std::unordered_map<int32_t, int64_t>{});
    });
}

class compaction_strategy_impl {
public:
    virtual ~compaction_strategy_impl() {}
    virtual compaction_descriptor get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) = 0;
    virtual compaction_strategy_type type() const = 0;
};

//
// Null compaction strategy is the default compaction strategy.
// As the name implies, it does nothing.
//
class null_compaction_strategy : public compaction_strategy_impl {
public:
    virtual compaction_descriptor get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) override {
        return sstables::compaction_descriptor();
    }

    virtual compaction_strategy_type type() const {
        return compaction_strategy_type::null;
    }
};

//
// Major compaction strategy is about compacting all available sstables into one.
//
class major_compaction_strategy : public compaction_strategy_impl {
public:
    virtual compaction_descriptor get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) override {
        static constexpr size_t min_compact_threshold = 2;

        // At least, two sstables must be available for compaction to take place.
        if (cfs.sstables_count() < min_compact_threshold) {
            return sstables::compaction_descriptor();
        }

        return sstables::compaction_descriptor(std::move(candidates));
    }

    virtual compaction_strategy_type type() const {
        return compaction_strategy_type::major;
    }
};

class size_tiered_compaction_strategy_options {
    static constexpr uint64_t DEFAULT_MIN_SSTABLE_SIZE = 50L * 1024L * 1024L;
    static constexpr double DEFAULT_BUCKET_LOW = 0.5;
    static constexpr double DEFAULT_BUCKET_HIGH = 1.5;
    static constexpr double DEFAULT_COLD_READS_TO_OMIT = 0.05;
    const sstring MIN_SSTABLE_SIZE_KEY = "min_sstable_size";
    const sstring BUCKET_LOW_KEY = "bucket_low";
    const sstring BUCKET_HIGH_KEY = "bucket_high";
    const sstring COLD_READS_TO_OMIT_KEY = "cold_reads_to_omit";

    uint64_t min_sstable_size = DEFAULT_MIN_SSTABLE_SIZE;
    double bucket_low = DEFAULT_BUCKET_LOW;
    double bucket_high = DEFAULT_BUCKET_HIGH;
    double cold_reads_to_omit =  DEFAULT_COLD_READS_TO_OMIT;

    static std::experimental::optional<sstring> get_value(const std::map<sstring, sstring>& options, const sstring& name) {
        auto it = options.find(name);
        if (it == options.end()) {
            return std::experimental::nullopt;
        }
        return it->second;
    }
public:
    size_tiered_compaction_strategy_options(const std::map<sstring, sstring>& options) {
        using namespace cql3::statements;

        auto tmp_value = get_value(options, MIN_SSTABLE_SIZE_KEY);
        min_sstable_size = property_definitions::to_long(MIN_SSTABLE_SIZE_KEY, tmp_value, DEFAULT_MIN_SSTABLE_SIZE);

        tmp_value = get_value(options, BUCKET_LOW_KEY);
        bucket_low = property_definitions::to_double(BUCKET_LOW_KEY, tmp_value, DEFAULT_BUCKET_LOW);

        tmp_value = get_value(options, BUCKET_HIGH_KEY);
        bucket_high = property_definitions::to_double(BUCKET_HIGH_KEY, tmp_value, DEFAULT_BUCKET_HIGH);

        tmp_value = get_value(options, COLD_READS_TO_OMIT_KEY);
        cold_reads_to_omit = property_definitions::to_double(COLD_READS_TO_OMIT_KEY, tmp_value, DEFAULT_COLD_READS_TO_OMIT);
    }

    size_tiered_compaction_strategy_options() {
        min_sstable_size = DEFAULT_MIN_SSTABLE_SIZE;
        bucket_low = DEFAULT_BUCKET_LOW;
        bucket_high = DEFAULT_BUCKET_HIGH;
        cold_reads_to_omit = DEFAULT_COLD_READS_TO_OMIT;
    }

    // FIXME: convert java code below.
#if 0
    public static Map<String, String> validateOptions(Map<String, String> options, Map<String, String> uncheckedOptions) throws ConfigurationException
    {
        String optionValue = options.get(MIN_SSTABLE_SIZE_KEY);
        try
        {
            long minSSTableSize = optionValue == null ? DEFAULT_MIN_SSTABLE_SIZE : Long.parseLong(optionValue);
            if (minSSTableSize < 0)
            {
                throw new ConfigurationException(String.format("%s must be non negative: %d", MIN_SSTABLE_SIZE_KEY, minSSTableSize));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", optionValue, MIN_SSTABLE_SIZE_KEY), e);
        }

        double bucketLow = parseDouble(options, BUCKET_LOW_KEY, DEFAULT_BUCKET_LOW);
        double bucketHigh = parseDouble(options, BUCKET_HIGH_KEY, DEFAULT_BUCKET_HIGH);
        if (bucketHigh <= bucketLow)
        {
            throw new ConfigurationException(String.format("%s value (%s) is less than or equal to the %s value (%s)",
                                                           BUCKET_HIGH_KEY, bucketHigh, BUCKET_LOW_KEY, bucketLow));
        }

        double maxColdReadsRatio = parseDouble(options, COLD_READS_TO_OMIT_KEY, DEFAULT_COLD_READS_TO_OMIT);
        if (maxColdReadsRatio < 0.0 || maxColdReadsRatio > 1.0)
        {
            throw new ConfigurationException(String.format("%s value (%s) should be between between 0.0 and 1.0",
                                                           COLD_READS_TO_OMIT_KEY, optionValue));
        }

        uncheckedOptions.remove(MIN_SSTABLE_SIZE_KEY);
        uncheckedOptions.remove(BUCKET_LOW_KEY);
        uncheckedOptions.remove(BUCKET_HIGH_KEY);
        uncheckedOptions.remove(COLD_READS_TO_OMIT_KEY);

        return uncheckedOptions;
    }
#endif
    friend class size_tiered_compaction_strategy;
};

class size_tiered_compaction_strategy : public compaction_strategy_impl {
    size_tiered_compaction_strategy_options _options;

    // Return a list of pair of shared_sstable and its respective size.
    std::vector<std::pair<sstables::shared_sstable, uint64_t>> create_sstable_and_length_pairs(const std::vector<sstables::shared_sstable>& sstables);

    // Group files of similar size into buckets.
    std::vector<std::vector<sstables::shared_sstable>> get_buckets(const std::vector<sstables::shared_sstable>& sstables, unsigned max_threshold);

    // Maybe return a bucket of sstables to compact
    std::vector<sstables::shared_sstable>
    most_interesting_bucket(std::vector<std::vector<sstables::shared_sstable>> buckets, unsigned min_threshold, unsigned max_threshold);

    // Return the average size of a given list of sstables.
    uint64_t avg_size(std::vector<sstables::shared_sstable>& sstables) {
        assert(sstables.size() > 0); // this should never fail
        uint64_t n = 0;

        for (auto& sstable : sstables) {
            // FIXME: Switch to sstable->bytes_on_disk() afterwards. That's what C* uses.
            n += sstable->data_size();
        }

        return n / sstables.size();
    }
public:
    size_tiered_compaction_strategy() = default;
    size_tiered_compaction_strategy(const std::map<sstring, sstring>& options) :
        _options(options) {}

    virtual compaction_descriptor get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) override;

    friend std::vector<sstables::shared_sstable> size_tiered_most_interesting_bucket(lw_shared_ptr<sstable_list>);

    virtual compaction_strategy_type type() const {
        return compaction_strategy_type::size_tiered;
    }
};

std::vector<std::pair<sstables::shared_sstable, uint64_t>>
size_tiered_compaction_strategy::create_sstable_and_length_pairs(const std::vector<sstables::shared_sstable>& sstables) {

    std::vector<std::pair<sstables::shared_sstable, uint64_t>> sstable_length_pairs;
    sstable_length_pairs.reserve(sstables.size());

    for(auto& sstable : sstables) {
        auto sstable_size = sstable->data_size();
        assert(sstable_size != 0);

        sstable_length_pairs.emplace_back(sstable, sstable_size);
    }

    return sstable_length_pairs;
}

std::vector<std::vector<sstables::shared_sstable>>
size_tiered_compaction_strategy::get_buckets(const std::vector<sstables::shared_sstable>& sstables, unsigned max_threshold) {
    // sstables sorted by size of its data file.
    auto sorted_sstables = create_sstable_and_length_pairs(sstables);

    std::sort(sorted_sstables.begin(), sorted_sstables.end(), [] (auto& i, auto& j) {
        return i.second < j.second;
    });

    std::map<size_t, std::vector<sstables::shared_sstable>> buckets;

    bool found;
    for (auto& pair : sorted_sstables) {
        found = false;
        size_t size = pair.second;

        // look for a bucket containing similar-sized files:
        // group in the same bucket if it's w/in 50% of the average for this bucket,
        // or this file and the bucket are all considered "small" (less than `minSSTableSize`)
        for (auto& entry : buckets) {
            std::vector<sstables::shared_sstable> bucket = entry.second;
            size_t old_average_size = entry.first;

            if (((size > (old_average_size * _options.bucket_low) && size < (old_average_size * _options.bucket_high))
                || (size < _options.min_sstable_size && old_average_size < _options.min_sstable_size))
                && (bucket.size() < max_threshold))
            {
                size_t total_size = bucket.size() * old_average_size;
                size_t new_average_size = (total_size + size) / (bucket.size() + 1);

                bucket.push_back(pair.first);
                buckets.erase(old_average_size);
                buckets.insert({ new_average_size, std::move(bucket) });

                found = true;
                break;
            }
        }

        // no similar bucket found; put it in a new one
        if (!found) {
            std::vector<sstables::shared_sstable> new_bucket;
            new_bucket.push_back(pair.first);
            buckets.insert({ size, std::move(new_bucket) });
        }
    }

    std::vector<std::vector<sstables::shared_sstable>> bucket_list;
    bucket_list.reserve(buckets.size());

    for (auto& entry : buckets) {
        bucket_list.push_back(std::move(entry.second));
    }

    return bucket_list;
}

std::vector<sstables::shared_sstable>
size_tiered_compaction_strategy::most_interesting_bucket(std::vector<std::vector<sstables::shared_sstable>> buckets,
        unsigned min_threshold, unsigned max_threshold)
{
    std::vector<std::pair<std::vector<sstables::shared_sstable>, uint64_t>> pruned_buckets_and_hotness;
    pruned_buckets_and_hotness.reserve(buckets.size());

    // FIXME: add support to get hotness for each bucket.

    for (auto& bucket : buckets) {
        // FIXME: the coldest sstables will be trimmed to meet the threshold, so we must add support to this feature
        // by converting SizeTieredCompactionStrategy::trimToThresholdWithHotness.
        // By the time being, we will only compact buckets that meet the threshold.
        if (bucket.size() >= min_threshold && bucket.size() <= max_threshold) {
            auto avg = avg_size(bucket);
            pruned_buckets_and_hotness.push_back({ std::move(bucket), avg });
        }
    }

    if (pruned_buckets_and_hotness.empty()) {
        return std::vector<sstables::shared_sstable>();
    }

    // NOTE: Compacting smallest sstables first, located at the beginning of the sorted vector.
    auto& min = *std::min_element(pruned_buckets_and_hotness.begin(), pruned_buckets_and_hotness.end(), [] (auto& i, auto& j) {
        // FIXME: ignoring hotness by the time being.

        return i.second < j.second;
    });
    auto hottest = std::move(min.first);

    return hottest;
}

compaction_descriptor size_tiered_compaction_strategy::get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) {
    // make local copies so they can't be changed out from under us mid-method
    int min_threshold = cfs.schema()->min_compaction_threshold();
    int max_threshold = cfs.schema()->max_compaction_threshold();

    // TODO: Add support to filter cold sstables (for reference: SizeTieredCompactionStrategy::filterColdSSTables).

    auto buckets = get_buckets(candidates, max_threshold);

    std::vector<sstables::shared_sstable> most_interesting = most_interesting_bucket(std::move(buckets), min_threshold, max_threshold);
#ifdef __DEBUG__
    printf("size-tiered: Compacting %ld out of %ld sstables\n", most_interesting.size(), candidates->size());
#endif
    if (most_interesting.empty()) {
        // nothing to do
        return sstables::compaction_descriptor();
    }

    return sstables::compaction_descriptor(std::move(most_interesting));
}

std::vector<sstables::shared_sstable> size_tiered_most_interesting_bucket(lw_shared_ptr<sstable_list> candidates) {
    size_tiered_compaction_strategy cs;

    std::vector<sstables::shared_sstable> sstables;
    sstables.reserve(candidates->size());
    for (auto& entry : *candidates) {
        sstables.push_back(entry.second);
    }

    auto buckets = cs.get_buckets(sstables, DEFAULT_MAX_COMPACTION_THRESHOLD);

    std::vector<sstables::shared_sstable> most_interesting = cs.most_interesting_bucket(std::move(buckets),
        DEFAULT_MIN_COMPACTION_THRESHOLD, DEFAULT_MAX_COMPACTION_THRESHOLD);

    return most_interesting;
}

class leveled_compaction_strategy : public compaction_strategy_impl {
    // FIXME: User may choose to change this value; add support.
    static constexpr uint32_t max_sstable_size_in_mb = 160;
public:
    virtual compaction_descriptor get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) override;

    virtual compaction_strategy_type type() const {
        return compaction_strategy_type::leveled;
    }
};

compaction_descriptor leveled_compaction_strategy::get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) {
    // NOTE: leveled_manifest creation may be slightly expensive, so later on,
    // we may want to store it in the strategy itself. However, the sstable
    // lists managed by the manifest may become outdated. For example, one
    // sstable in it may be marked for deletion after compacted.
    // Currently, we create a new manifest whenever it's time for compaction.
    leveled_manifest manifest = leveled_manifest::create(cfs, candidates, max_sstable_size_in_mb);
    auto candidate = manifest.get_compaction_candidates();

    if (candidate.sstables.empty()) {
        return sstables::compaction_descriptor();
    }

    logger.debug("leveled: Compacting {} out of {} sstables", candidate.sstables.size(), cfs.get_sstables()->size());

    return std::move(candidate);
}

compaction_strategy::compaction_strategy(::shared_ptr<compaction_strategy_impl> impl)
    : _compaction_strategy_impl(std::move(impl)) {}
compaction_strategy::compaction_strategy() = default;
compaction_strategy::~compaction_strategy() = default;
compaction_strategy::compaction_strategy(const compaction_strategy&) = default;
compaction_strategy::compaction_strategy(compaction_strategy&&) = default;
compaction_strategy& compaction_strategy::operator=(compaction_strategy&&) = default;

compaction_strategy_type compaction_strategy::type() const {
    return _compaction_strategy_impl->type();
}
compaction_descriptor compaction_strategy::get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) {
    return _compaction_strategy_impl->get_sstables_for_compaction(cfs, std::move(candidates));
}

compaction_strategy make_compaction_strategy(compaction_strategy_type strategy, const std::map<sstring, sstring>& options) {
    ::shared_ptr<compaction_strategy_impl> impl;

    switch(strategy) {
    case compaction_strategy_type::null:
        impl = make_shared<null_compaction_strategy>(null_compaction_strategy());
        break;
    case compaction_strategy_type::major:
        impl = make_shared<major_compaction_strategy>(major_compaction_strategy());
        break;
    case compaction_strategy_type::size_tiered:
        impl = make_shared<size_tiered_compaction_strategy>(size_tiered_compaction_strategy(options));
        break;
    case compaction_strategy_type::leveled:
        impl = make_shared<leveled_compaction_strategy>(leveled_compaction_strategy());
        break;
    default:
        throw std::runtime_error("strategy not supported");
    }

    return compaction_strategy(std::move(impl));
}

}
