/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include "sstables/sstables.hh"
#include "size_tiered_compaction_strategy.hh"
#include "cql3/statements/property_definitions.hh"

#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>

namespace sstables {

static long validate_sstable_size(const std::map<sstring, sstring>& options) {
    auto tmp_value = compaction_strategy_impl::get_value(options, size_tiered_compaction_strategy_options::MIN_SSTABLE_SIZE_KEY);
    auto min_sstables_size = cql3::statements::property_definitions::to_long(size_tiered_compaction_strategy_options::MIN_SSTABLE_SIZE_KEY, tmp_value, size_tiered_compaction_strategy_options::DEFAULT_MIN_SSTABLE_SIZE);
    if (min_sstables_size < 0) {
        throw exceptions::configuration_exception(fmt::format("{} value ({}) must be non negative", size_tiered_compaction_strategy_options::MIN_SSTABLE_SIZE_KEY, min_sstables_size));
    }
    return min_sstables_size;
}

static long validate_sstable_size(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options) {
    auto min_sstables_size = validate_sstable_size(options);
    unchecked_options.erase(size_tiered_compaction_strategy_options::MIN_SSTABLE_SIZE_KEY);
    return min_sstables_size;
}

static double validate_bucket_low(const std::map<sstring, sstring>& options) {
    auto tmp_value = compaction_strategy_impl::get_value(options, size_tiered_compaction_strategy_options::BUCKET_LOW_KEY);
    auto bucket_low = cql3::statements::property_definitions::to_double(size_tiered_compaction_strategy_options::BUCKET_LOW_KEY, tmp_value, size_tiered_compaction_strategy_options::DEFAULT_BUCKET_LOW);
    if (bucket_low <= 0.0 || bucket_low >= 1.0) {
        throw exceptions::configuration_exception(fmt::format("{} value ({}) must be between 0.0 and 1.0", size_tiered_compaction_strategy_options::BUCKET_LOW_KEY, bucket_low));
    }
    return bucket_low;
}

static double validate_bucket_low(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options) {
    auto bucket_low = validate_bucket_low(options);
    unchecked_options.erase(size_tiered_compaction_strategy_options::BUCKET_LOW_KEY);
    return bucket_low;
}

static double validate_bucket_high(const std::map<sstring, sstring>& options) {
    auto tmp_value = compaction_strategy_impl::get_value(options, size_tiered_compaction_strategy_options::BUCKET_HIGH_KEY);
    auto bucket_high = cql3::statements::property_definitions::to_double(size_tiered_compaction_strategy_options::BUCKET_HIGH_KEY, tmp_value, size_tiered_compaction_strategy_options::DEFAULT_BUCKET_HIGH);
    if (bucket_high <= 1.0) {
        throw exceptions::configuration_exception(fmt::format("{} value ({}) must be greater than 1.0", size_tiered_compaction_strategy_options::BUCKET_HIGH_KEY, bucket_high));
    }
    return bucket_high;
}

static double validate_bucket_high(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options) {
    auto bucket_high = validate_bucket_high(options);
    unchecked_options.erase(size_tiered_compaction_strategy_options::BUCKET_HIGH_KEY);
    return bucket_high;
}

static double validate_cold_reads_to_omit(const std::map<sstring, sstring>& options) {
    auto tmp_value = compaction_strategy_impl::get_value(options, size_tiered_compaction_strategy_options::COLD_READS_TO_OMIT_KEY);
    auto cold_reads_to_omit = cql3::statements::property_definitions::to_double(size_tiered_compaction_strategy_options::COLD_READS_TO_OMIT_KEY, tmp_value, size_tiered_compaction_strategy_options::DEFAULT_COLD_READS_TO_OMIT);
    if (cold_reads_to_omit < 0.0 || cold_reads_to_omit > 1.0) {
        throw exceptions::configuration_exception(fmt::format("{} value ({}) must be between 0.0 and 1.0", size_tiered_compaction_strategy_options::COLD_READS_TO_OMIT_KEY, cold_reads_to_omit));
    }
    return cold_reads_to_omit;
}

static double validate_cold_reads_to_omit(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options) {
    auto cold_reads_to_omit = validate_cold_reads_to_omit(options);
    unchecked_options.erase(size_tiered_compaction_strategy_options::COLD_READS_TO_OMIT_KEY);
    return cold_reads_to_omit;
}

size_tiered_compaction_strategy_options::size_tiered_compaction_strategy_options(const std::map<sstring, sstring>& options) {
    using namespace cql3::statements;

    min_sstable_size = validate_sstable_size(options);
    bucket_low = validate_bucket_low(options);
    bucket_high = validate_bucket_high(options);
    cold_reads_to_omit = validate_cold_reads_to_omit(options);
}

size_tiered_compaction_strategy_options::size_tiered_compaction_strategy_options() {
    min_sstable_size = DEFAULT_MIN_SSTABLE_SIZE;
    bucket_low = DEFAULT_BUCKET_LOW;
    bucket_high = DEFAULT_BUCKET_HIGH;
    cold_reads_to_omit = DEFAULT_COLD_READS_TO_OMIT;
}

// options is a map of compaction strategy options and their values.
// unchecked_options is an analogical map from which already checked options are deleted.
// This helps making sure that only allowed options are being set.
void size_tiered_compaction_strategy_options::validate(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options) {
    validate_sstable_size(options, unchecked_options);
    auto bucket_low = validate_bucket_low(options, unchecked_options);
    auto bucket_high = validate_bucket_high(options, unchecked_options);
    if (bucket_high <= bucket_low) {
        throw exceptions::configuration_exception(fmt::format("{} value ({}) is less than or equal to the {} value ({})", BUCKET_HIGH_KEY, bucket_high, BUCKET_LOW_KEY, bucket_low));
    }
    validate_cold_reads_to_omit(options, unchecked_options);
    compaction_strategy_impl::validate_min_max_threshold(options, unchecked_options);
}

std::vector<std::pair<sstables::shared_sstable, uint64_t>>
size_tiered_compaction_strategy::create_sstable_and_length_pairs(const std::vector<sstables::shared_sstable>& sstables) {

    std::vector<std::pair<sstables::shared_sstable, uint64_t>> sstable_length_pairs;
    sstable_length_pairs.reserve(sstables.size());

    for(auto& sstable : sstables) {
        auto sstable_size = sstable->data_size();
        SCYLLA_ASSERT(sstable_size != 0);

        sstable_length_pairs.emplace_back(sstable, sstable_size);
    }

    return sstable_length_pairs;
}

std::vector<std::vector<sstables::shared_sstable>>
size_tiered_compaction_strategy::get_buckets(const std::vector<sstables::shared_sstable>& sstables, size_tiered_compaction_strategy_options options) {
    // sstables sorted by size of its data file.
    auto sorted_sstables = create_sstable_and_length_pairs(sstables);

    std::sort(sorted_sstables.begin(), sorted_sstables.end(), [] (auto& i, auto& j) {
        return i.second < j.second;
    });

    using bucket_type = std::vector<sstables::shared_sstable>;
    std::vector<bucket_type> bucket_list;
    std::vector<double> bucket_average_size_list;

    for (auto& pair : sorted_sstables) {
        size_t size = pair.second;

        // look for a bucket containing similar-sized files:
        // group in the same bucket if it's w/in (bucket_low, bucket_high) of the average for this bucket,
        // or this file and the bucket are all considered "small" (less than `minSSTableSize`)
        if (!bucket_list.empty()) {
            auto& bucket_average_size = bucket_average_size_list.back();

            if ((size > (bucket_average_size * options.bucket_low) && size < (bucket_average_size * options.bucket_high)) ||
                    (size < options.min_sstable_size && bucket_average_size < options.min_sstable_size)) {
                auto& bucket = bucket_list.back();
                auto total_size = bucket.size() * bucket_average_size;
                auto new_average_size = (total_size + size) / (bucket.size() + 1);
                auto smallest_sstable_in_bucket = bucket[0]->data_size();

                // SSTables are added in increasing size order so the bucket's
                // average might drift upwards.
                // Don't let it drift too high, to a point where the smallest
                // SSTable might fall out of range.
                if (size < options.min_sstable_size || smallest_sstable_in_bucket > new_average_size * options.bucket_low) {
                    bucket.push_back(pair.first);
                    bucket_average_size = new_average_size;
                    continue;
                }
            }
        }

        // no similar bucket found; put it in a new one
        bucket_type new_bucket = {pair.first};
        bucket_list.push_back(std::move(new_bucket));
        bucket_average_size_list.push_back(size);
    }

    return bucket_list;
}

std::vector<std::vector<sstables::shared_sstable>>
size_tiered_compaction_strategy::get_buckets(const std::vector<sstables::shared_sstable>& sstables) const {
    return get_buckets(sstables, _options);
}

std::vector<sstables::shared_sstable>
size_tiered_compaction_strategy::most_interesting_bucket(std::vector<std::vector<sstables::shared_sstable>> buckets,
        unsigned min_threshold, unsigned max_threshold)
{
    using bucket_t = std::vector<sstables::shared_sstable>;
    std::vector<bucket_t> pruned_buckets;
    pruned_buckets.reserve(buckets.size());

    // FIXME: add support to get hotness for each bucket.

    for (auto& bucket : buckets) {
        // FIXME: the coldest sstables will be trimmed to meet the threshold, so we must add support to this feature
        // by converting SizeTieredCompactionStrategy::trimToThresholdWithHotness.
        // By the time being, we will only compact buckets that meet the threshold.
        if (!is_bucket_interesting(bucket, min_threshold)) {
            continue;
        }
        bucket.resize(std::min(bucket.size(), size_t(max_threshold)));
        pruned_buckets.push_back(std::move(bucket));
    }

    if (pruned_buckets.empty()) {
        return std::vector<sstables::shared_sstable>();
    }

    // Pick the bucket with more elements, as efficiency of same-tier compactions increases with number of files.
    auto& max = *std::max_element(pruned_buckets.begin(), pruned_buckets.end(), [] (const bucket_t& i, const bucket_t& j) {
        // FIXME: ignoring hotness by the time being.
        return i.size() < j.size();
    });
    return std::move(max);
}

compaction_descriptor
size_tiered_compaction_strategy::get_sstables_for_compaction(table_state& table_s, strategy_control& control) {
    // make local copies so they can't be changed out from under us mid-method
    int min_threshold = table_s.min_compaction_threshold();
    int max_threshold = table_s.schema()->max_compaction_threshold();
    auto compaction_time = gc_clock::now();
    auto candidates = control.candidates(table_s);

    // TODO: Add support to filter cold sstables (for reference: SizeTieredCompactionStrategy::filterColdSSTables).

    auto buckets = get_buckets(candidates);

    if (is_any_bucket_interesting(buckets, min_threshold)) {
        std::vector<sstables::shared_sstable> most_interesting = most_interesting_bucket(std::move(buckets), min_threshold, max_threshold);
        return sstables::compaction_descriptor(std::move(most_interesting));
    }

    // If we are not enforcing min_threshold explicitly, try any pair of SStables in the same tier.
    if (!table_s.compaction_enforce_min_threshold() && is_any_bucket_interesting(buckets, 2)) {
        std::vector<sstables::shared_sstable> most_interesting = most_interesting_bucket(std::move(buckets), 2, max_threshold);
        return sstables::compaction_descriptor(std::move(most_interesting));
    }

    if (!table_s.tombstone_gc_enabled()) {
        return compaction_descriptor();
    }

    // if there is no sstable to compact in standard way, try compacting single sstable whose droppable tombstone
    // ratio is greater than threshold.
    // prefer oldest sstables from biggest size tiers because they will be easier to satisfy conditions for
    // tombstone purge, i.e. less likely to shadow even older data.
    for (auto&& sstables : buckets | boost::adaptors::reversed) {
        // filter out sstables which droppable tombstone ratio isn't greater than the defined threshold.
        auto e = boost::range::remove_if(sstables, [this, compaction_time, &table_s] (const sstables::shared_sstable& sst) -> bool {
            return !worth_dropping_tombstones(sst, compaction_time, table_s);
        });
        sstables.erase(e, sstables.end());
        if (sstables.empty()) {
            continue;
        }
        // find oldest sstable from current tier
        auto it = std::min_element(sstables.begin(), sstables.end(), [] (auto& i, auto& j) {
            return i->get_stats_metadata().min_timestamp < j->get_stats_metadata().min_timestamp;
        });
        return sstables::compaction_descriptor({ *it });
    }
    return sstables::compaction_descriptor();
}

int64_t size_tiered_compaction_strategy::estimated_pending_compactions(const std::vector<sstables::shared_sstable>& sstables,
        int min_threshold, int max_threshold, size_tiered_compaction_strategy_options options) {
    int64_t n = 0;
    for (auto& bucket : get_buckets(sstables, options)) {
        if (bucket.size() >= size_t(min_threshold)) {
            n += std::ceil(double(bucket.size()) / max_threshold);
        }
    }
    return n;
}

int64_t size_tiered_compaction_strategy::estimated_pending_compactions(table_state& table_s) const {
    int min_threshold = table_s.min_compaction_threshold();
    int max_threshold = table_s.schema()->max_compaction_threshold();
    std::vector<sstables::shared_sstable> sstables;

    auto all_sstables = table_s.main_sstable_set().all();
    sstables.reserve(all_sstables->size());
    for (auto& entry : *all_sstables) {
        sstables.push_back(entry);
    }

    return estimated_pending_compactions(sstables, min_threshold, max_threshold, _options);
}

std::vector<sstables::shared_sstable>
size_tiered_compaction_strategy::most_interesting_bucket(const std::vector<sstables::shared_sstable>& candidates,
        int min_threshold, int max_threshold, size_tiered_compaction_strategy_options options) {
    size_tiered_compaction_strategy cs(options);

    auto buckets = cs.get_buckets(candidates);

    std::vector<sstables::shared_sstable> most_interesting = cs.most_interesting_bucket(std::move(buckets),
        min_threshold, max_threshold);

    return most_interesting;
}

compaction_descriptor
size_tiered_compaction_strategy::get_reshaping_job(std::vector<shared_sstable> input, schema_ptr schema, reshape_config cfg) const
{
    auto mode = cfg.mode;
    size_t offstrategy_threshold = std::max(schema->min_compaction_threshold(), 4);
    size_t max_sstables = std::max(schema->max_compaction_threshold(), int(offstrategy_threshold));

    if (mode == reshape_mode::relaxed) {
        offstrategy_threshold = max_sstables;
    }

    if (input.size() >= offstrategy_threshold && mode == reshape_mode::strict) {
        std::sort(input.begin(), input.end(), [&schema] (const shared_sstable& a, const shared_sstable& b) {
            return dht::ring_position(a->get_first_decorated_key()).less_compare(*schema, dht::ring_position(b->get_first_decorated_key()));
        });
        // All sstables can be reshaped at once if the amount of overlapping will not cause memory usage to be high,
        // which is possible because partitioned set is able to incrementally open sstables during compaction
        if (sstable_set_overlapping_count(schema, input) <= max_sstables) {
            compaction_descriptor desc(std::move(input));
            desc.options = compaction_type_options::make_reshape();
            return desc;
        }
    }

    for (auto& bucket : get_buckets(input)) {
        if (bucket.size() >= offstrategy_threshold) {
            // reshape job can work on #max_sstables sstables at once, so by reshaping sstables with the smallest tokens first,
            // token contiguity is preserved iff sstables are disjoint.
            if (bucket.size() > max_sstables) {
                std::partial_sort(bucket.begin(), bucket.begin() + max_sstables, bucket.end(), [&schema](const sstables::shared_sstable& a, const sstables::shared_sstable& b) {
                    return a->get_first_decorated_key().tri_compare(*schema, b->get_first_decorated_key()) <= 0;
                });
                bucket.resize(max_sstables);
            }
            compaction_descriptor desc(std::move(bucket));
            desc.options = compaction_type_options::make_reshape();
            return desc;
        }
    }

    return compaction_descriptor();
}

std::vector<compaction_descriptor>
size_tiered_compaction_strategy::get_cleanup_compaction_jobs(table_state& table_s, std::vector<shared_sstable> candidates) const {
    std::vector<compaction_descriptor> ret;
    const auto& schema = table_s.schema();
    unsigned max_threshold = schema->max_compaction_threshold();

    for (auto& bucket : get_buckets(candidates)) {
        if (bucket.size() > max_threshold) {
            // preserve token contiguity
            std::ranges::sort(bucket, [&schema] (const shared_sstable& a, const shared_sstable& b) {
                return a->get_first_decorated_key().tri_compare(*schema, b->get_first_decorated_key()) < 0;
            });
        }
        auto it = bucket.begin();
        while (it != bucket.end()) {
            unsigned remaining = std::distance(it, bucket.end());
            unsigned needed = std::min(remaining, max_threshold);
            std::vector<shared_sstable> sstables;
            std::move(it, it + needed, std::back_inserter(sstables));
            ret.push_back(compaction_descriptor(std::move(sstables)));
            std::advance(it, needed);
        }
    }
    return ret;
}

}
