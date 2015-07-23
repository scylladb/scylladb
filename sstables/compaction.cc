/*
 * Copyright 2015 Cloudius Systems
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

#include "core/future-util.hh"
#include "core/pipe.hh"

#include "sstables.hh"
#include "compaction.hh"
#include "compaction_strategy.hh"
#include "mutation_reader.hh"
#include "schema.hh"

namespace sstables {

logging::logger logger("compaction");

struct compaction_stats {
    size_t sstables = 0;
    uint64_t start_size = 0;
    uint64_t total_partitions = 0;
    uint64_t total_keys_written = 0;
};

// compact_sstables compacts the given list of sstables creating one
// (currently) or more (in the future) new sstables. The new sstables
// are created using the "sstable_creator" object passed by the caller.
future<> compact_sstables(std::vector<shared_sstable> sstables,
        schema_ptr schema, std::function<shared_sstable()> creator) {
    std::vector<::mutation_reader> readers;
    uint64_t estimated_partitions = 0;
    auto newtab = creator();
    auto stats = make_lw_shared<compaction_stats>();
    sstring sstable_logger_msg = "[";

    for (auto sst : sstables) {
        // We also capture the sstable, so we keep it alive while the read isn't done
        readers.emplace_back([sst, r = make_lw_shared(sst->read_rows(schema))] () mutable { return r->read(); });
        // FIXME: If the sstables have cardinality estimation bitmaps, use that
        // for a better estimate for the number of partitions in the merged
        // sstable than just adding up the lengths of individual sstables.
        estimated_partitions += sst->get_estimated_key_count();
        stats->total_partitions += sst->get_estimated_key_count();
        // Compacted sstable keeps track of its ancestors.
        newtab->add_ancestor(sst->generation());
        // FIXME: get sstable level
        sstable_logger_msg += sprint("%s:level=%d, ", sst->get_filename(), 0);
        stats->start_size += sst->data_size();
    }
    sstable_logger_msg += "]";
    stats->sstables = sstables.size();
    logger.info("Compacting {}", sstable_logger_msg);

    auto combined_reader = make_combined_reader(std::move(readers));

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

    auto done = make_lw_shared<bool>(false);
    future<> read_done = do_until([done] { return *done; }, [done, output_writer, combined_reader = std::move(combined_reader), stats] {
        return combined_reader().then([done = std::move(done), output_writer = std::move(output_writer), stats] (auto mopt) {
            if (mopt) {
                stats->total_keys_written++;
                return output_writer->write(std::move(*mopt));
            } else {
                *done = true;
                return make_ready_future<>();
            }
        });
    });

    ::mutation_reader mutation_queue_reader = [output_reader] () {
        return output_reader->read();
    };

    future<> write_done = newtab->write_components(
            std::move(mutation_queue_reader), estimated_partitions, schema).then([newtab, stats] {
        return newtab->load().then([newtab, stats] {
            uint64_t endsize = newtab->data_size();
            double ratio = (double) endsize / (double) stats->start_size;

            // FIXME: there is some missing information in the log message below.
            // look at CompactionTask::runMayThrow() in origin for reference.
            // 1) calculate data rate during compaction.
            // 2) add support to merge summary (message: Partition merge counts were {%s}.).
            // 3) there is no easy way, currently, to know the exact number of total partitions.
            // By the time being, using estimated key count.
            logger.info("Compacted {} sstables to [{}]. {} bytes to {} (~{}% of original) in {}ms = {}MB/s. " \
                "~{} total partitions merged to {}.",
                stats->sstables, newtab->get_filename(), stats->start_size, endsize, (int) (ratio * 100),
                0, 0, stats->total_partitions, stats->total_keys_written);
        });
    });

    // Wait for both read_done and write_done fibers to finish.
    // FIXME: if write_done throws an exception, we get a broken pipe
    // exception on read_done, and then we don't handle write_done's
    // exception, causing a warning message of "ignored exceptional future".
    return read_done.then([write_done = std::move(write_done)] () mutable { return std::move(write_done); });
}

class compaction_strategy_impl {
public:
    virtual ~compaction_strategy_impl() {}
    virtual future<> compact(column_family& cfs) = 0;
};

//
// Null compaction strategy is the default compaction strategy.
// As the name implies, it does nothing.
//
class null_compaction_strategy : public compaction_strategy_impl {
public:
    virtual future<> compact(column_family& cfs) override {
        return make_ready_future<>();
    }
};

//
// Major compaction strategy is about compacting all available sstables into one.
//
class major_compaction_strategy : public compaction_strategy_impl {
public:
    virtual future<> compact(column_family& cfs) override {
        static constexpr size_t min_compact_threshold = 2;

        // At least, two sstables must be available for compaction to take place.
        if (cfs.sstables_count() < min_compact_threshold) {
            return make_ready_future<>();
        }

        return cfs.compact_all_sstables();
    }
};

class size_tiered_compaction_strategy : public compaction_strategy_impl {
    static constexpr uint64_t DEFAULT_MIN_SSTABLE_SIZE = 50L * 1024L * 1024L;
    static constexpr double DEFAULT_BUCKET_LOW = 0.5;
    static constexpr double DEFAULT_BUCKET_HIGH = 1.5;
    static constexpr double DEFAULT_COLD_READS_TO_OMIT = 0.05;

    // FIXME: user should be able to configure these values.
    uint64_t min_sstable_size = DEFAULT_MIN_SSTABLE_SIZE;
    double bucket_low = DEFAULT_BUCKET_LOW;
    double bucket_high = DEFAULT_BUCKET_HIGH;
    double cold_reads_to_omit =  DEFAULT_COLD_READS_TO_OMIT;

    // Return a list of pair of shared_sstable and its respective size.
    std::vector<std::pair<sstables::shared_sstable, uint64_t>> create_sstable_and_length_pairs(const sstable_list& sstables);

    // Group files of similar size into buckets.
    std::vector<std::vector<sstables::shared_sstable>> get_buckets(const sstable_list& sstables);

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
    virtual future<> compact(column_family& cfs) override;

    friend std::vector<sstables::shared_sstable> size_tiered_most_interesting_bucket(lw_shared_ptr<sstable_list>);
};

std::vector<std::pair<sstables::shared_sstable, uint64_t>>
size_tiered_compaction_strategy::create_sstable_and_length_pairs(const sstable_list& sstables) {

    std::vector<std::pair<sstables::shared_sstable, uint64_t>> sstable_length_pairs;
    sstable_length_pairs.reserve(sstables.size());

    for(auto& entry : sstables) {
        auto& sstable = entry.second;
        auto sstable_size = sstable->data_size();
        assert(sstable_size != 0);

        sstable_length_pairs.emplace_back(sstable, sstable_size);
    }

    return sstable_length_pairs;
}

std::vector<std::vector<sstables::shared_sstable>>
size_tiered_compaction_strategy::get_buckets(const sstable_list& sstables) {
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

            if ((size > (old_average_size * bucket_low) && size < (old_average_size * bucket_high))
                || (size < min_sstable_size && old_average_size < min_sstable_size))
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

future<> size_tiered_compaction_strategy::compact(column_family& cfs) {
    // make local copies so they can't be changed out from under us mid-method
    // FIXME: instead, we should get these values from column family.
    int min_threshold = cfs.schema()->min_compaction_threshold();
    int max_threshold = cfs.schema()->max_compaction_threshold();

    auto candidates = cfs.get_sstables();

    // TODO: Add support to filter cold sstables (for reference: SizeTieredCompactionStrategy::filterColdSSTables).

    auto buckets = get_buckets(*candidates);

    std::vector<sstables::shared_sstable> most_interesting = most_interesting_bucket(std::move(buckets), min_threshold, max_threshold);
#ifdef __DEBUG__
    printf("size-tiered: Compacting %ld out of %ld sstables\n", most_interesting.size(), candidates->size());
#endif
    if (most_interesting.empty()) {
        // nothing to do
        return make_ready_future<>();
    }

    return cfs.compact_sstables(std::move(most_interesting));
}

std::vector<sstables::shared_sstable> size_tiered_most_interesting_bucket(lw_shared_ptr<sstable_list> candidates) {
    size_tiered_compaction_strategy cs;

    auto buckets = cs.get_buckets(*candidates);

    std::vector<sstables::shared_sstable> most_interesting = cs.most_interesting_bucket(std::move(buckets),
        DEFAULT_MIN_COMPACTION_THRESHOLD, DEFAULT_MAX_COMPACTION_THRESHOLD);

    return most_interesting;
}

compaction_strategy::compaction_strategy(::shared_ptr<compaction_strategy_impl> impl)
    : _compaction_strategy_impl(std::move(impl)) {}
compaction_strategy::compaction_strategy() = default;
compaction_strategy::~compaction_strategy() = default;
compaction_strategy::compaction_strategy(const compaction_strategy&) = default;
compaction_strategy::compaction_strategy(compaction_strategy&&) = default;
compaction_strategy& compaction_strategy::operator=(compaction_strategy&&) = default;

future<> compaction_strategy::compact(column_family& cfs) {
    return _compaction_strategy_impl->compact(cfs);
}

compaction_strategy make_compaction_strategy(compaction_strategy_type strategy) {
    ::shared_ptr<compaction_strategy_impl> impl;

    switch(strategy) {
    case compaction_strategy_type::null:
        impl = make_shared<null_compaction_strategy>(null_compaction_strategy());
        break;
    case compaction_strategy_type::major:
        impl = make_shared<major_compaction_strategy>(major_compaction_strategy());
        break;
    case compaction_strategy_type::size_tiered:
        impl = make_shared<size_tiered_compaction_strategy>(size_tiered_compaction_strategy());
        break;
    default:
        throw std::runtime_error("strategy not supported");
    }

    return compaction_strategy(std::move(impl));
}

}
