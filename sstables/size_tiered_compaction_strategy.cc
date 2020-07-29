/*
 * Copyright (C) 2020 ScyllaDB
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

#include "size_tiered_compaction_strategy.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>

namespace sstables {

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
size_tiered_compaction_strategy::get_buckets(const std::vector<sstables::shared_sstable>& sstables, size_tiered_compaction_strategy_options options) {
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
        for (auto it = buckets.begin(); it != buckets.end(); it++) {
            size_t old_average_size = it->first;

            if ((size > (old_average_size * options.bucket_low) && size < (old_average_size * options.bucket_high)) ||
                    (size < options.min_sstable_size && old_average_size < options.min_sstable_size)) {
                auto bucket = std::move(it->second);
                size_t total_size = bucket.size() * old_average_size;
                size_t new_average_size = (total_size + size) / (bucket.size() + 1);

                bucket.push_back(pair.first);
                buckets.erase(it);
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

std::vector<std::vector<sstables::shared_sstable>>
size_tiered_compaction_strategy::get_buckets(const std::vector<sstables::shared_sstable>& sstables) const {
    return get_buckets(sstables, _options);
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
        bucket.resize(std::min(bucket.size(), size_t(max_threshold)));
        if (is_bucket_interesting(bucket, min_threshold)) {
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

compaction_descriptor
size_tiered_compaction_strategy::get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) {
    // make local copies so they can't be changed out from under us mid-method
    int min_threshold = cfs.min_compaction_threshold();
    int max_threshold = cfs.schema()->max_compaction_threshold();
    auto gc_before = gc_clock::now() - cfs.schema()->gc_grace_seconds();

    // TODO: Add support to filter cold sstables (for reference: SizeTieredCompactionStrategy::filterColdSSTables).

    auto buckets = get_buckets(candidates);

    if (is_any_bucket_interesting(buckets, min_threshold)) {
        std::vector<sstables::shared_sstable> most_interesting = most_interesting_bucket(std::move(buckets), min_threshold, max_threshold);
        return sstables::compaction_descriptor(std::move(most_interesting), cfs.get_sstable_set(), service::get_local_compaction_priority());
    }

    // If we are not enforcing min_threshold explicitly, try any pair of SStables in the same tier.
    if (!cfs.compaction_enforce_min_threshold() && is_any_bucket_interesting(buckets, 2)) {
        std::vector<sstables::shared_sstable> most_interesting = most_interesting_bucket(std::move(buckets), 2, max_threshold);
        return sstables::compaction_descriptor(std::move(most_interesting), cfs.get_sstable_set(), service::get_local_compaction_priority());
    }

    // if there is no sstable to compact in standard way, try compacting single sstable whose droppable tombstone
    // ratio is greater than threshold.
    // prefer oldest sstables from biggest size tiers because they will be easier to satisfy conditions for
    // tombstone purge, i.e. less likely to shadow even older data.
    for (auto&& sstables : buckets | boost::adaptors::reversed) {
        // filter out sstables which droppable tombstone ratio isn't greater than the defined threshold.
        auto e = boost::range::remove_if(sstables, [this, &gc_before] (const sstables::shared_sstable& sst) -> bool {
            return !worth_dropping_tombstones(sst, gc_before);
        });
        sstables.erase(e, sstables.end());
        if (sstables.empty()) {
            continue;
        }
        // find oldest sstable from current tier
        auto it = std::min_element(sstables.begin(), sstables.end(), [] (auto& i, auto& j) {
            return i->get_stats_metadata().min_timestamp < j->get_stats_metadata().min_timestamp;
        });
        return sstables::compaction_descriptor({ *it }, cfs.get_sstable_set(), service::get_local_compaction_priority());
    }
    return sstables::compaction_descriptor();
}

int64_t size_tiered_compaction_strategy::estimated_pending_compactions(column_family& cf) const {
    int min_threshold = cf.min_compaction_threshold();
    int max_threshold = cf.schema()->max_compaction_threshold();
    std::vector<sstables::shared_sstable> sstables;
    int64_t n = 0;

    sstables.reserve(cf.sstables_count());
    for (auto& entry : *cf.get_sstables()) {
        sstables.push_back(entry);
    }

    for (auto& bucket : get_buckets(sstables)) {
        if (bucket.size() >= size_t(min_threshold)) {
            n += std::ceil(double(bucket.size()) / max_threshold);
        }
    }
    return n;
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
size_tiered_compaction_strategy::get_reshaping_job(std::vector<shared_sstable> input, schema_ptr schema, const ::io_priority_class& iop, reshape_mode mode)
{
    size_t offstrategy_threshold = std::max(schema->min_compaction_threshold(), 4);
    size_t max_sstables = std::max(schema->max_compaction_threshold(), int(offstrategy_threshold));

    if (mode == reshape_mode::relaxed) {
        offstrategy_threshold = max_sstables;
    }

    for (auto& bucket : get_buckets(input)) {
        if (bucket.size() >= offstrategy_threshold) {
            bucket.resize(std::min(max_sstables, bucket.size()));
            compaction_descriptor desc(std::move(bucket), std::optional<sstables::sstable_set>(), iop);
            desc.options = compaction_options::make_reshape();
        }
    }

    return compaction_descriptor();
}

}
