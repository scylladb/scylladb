/*
 * Copyright (C) 2019 ScyllaDB
 *
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "sstables/sstables.hh"
#include "sstables/sstable_set.hh"
#include "cql3/statements/property_definitions.hh"
#include "compaction.hh"
#include "compaction_manager.hh"
#include "incremental_compaction_strategy.hh"
#include "incremental_backlog_tracker.hh"
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/numeric.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/range/adaptors.hpp>
#include <ranges>

namespace sstables {

extern logging::logger clogger;

static long validate_min_sstable_size(const std::map<sstring, sstring>& options) {
    auto tmp_value = compaction_strategy_impl::get_value(options,
        incremental_compaction_strategy_options::MIN_SSTABLE_SIZE_KEY);
    auto min_sstable_size = cql3::statements::property_definitions::to_long(incremental_compaction_strategy_options::MIN_SSTABLE_SIZE_KEY,
        tmp_value, incremental_compaction_strategy_options::DEFAULT_MIN_SSTABLE_SIZE);
    if (min_sstable_size < 0) {
        throw exceptions::configuration_exception(fmt::format("{} value ({}) must be non negative",
            incremental_compaction_strategy_options::MIN_SSTABLE_SIZE_KEY, min_sstable_size));
    }
    return min_sstable_size;
}

static long validate_min_sstable_size(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options) {
    auto min_sstable_size = validate_min_sstable_size(options);
    unchecked_options.erase(incremental_compaction_strategy_options::MIN_SSTABLE_SIZE_KEY);
    return min_sstable_size;
}

static double validate_bucket_low(const std::map<sstring, sstring>& options) {
    auto tmp_value = compaction_strategy_impl::get_value(options,
        incremental_compaction_strategy_options::BUCKET_LOW_KEY);
    auto bucket_low = cql3::statements::property_definitions::to_double(incremental_compaction_strategy_options::BUCKET_LOW_KEY,
        tmp_value, incremental_compaction_strategy_options::DEFAULT_BUCKET_LOW);
    if (bucket_low <= 0.0 || bucket_low >= 1.0) {
        throw exceptions::configuration_exception(fmt::format("{} value ({}) must be between 0.0 and 1.0",
            incremental_compaction_strategy_options::BUCKET_LOW_KEY, bucket_low));
    }
    return bucket_low;
}

static double validate_bucket_low(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options) {
    auto bucket_low = validate_bucket_low(options);
    unchecked_options.erase(incremental_compaction_strategy_options::BUCKET_LOW_KEY);
    return bucket_low;
}

static double validate_bucket_high(const std::map<sstring, sstring>& options) {
    auto tmp_value = compaction_strategy_impl::get_value(options,
        incremental_compaction_strategy_options::BUCKET_HIGH_KEY);
    auto bucket_high = cql3::statements::property_definitions::to_double(incremental_compaction_strategy_options::BUCKET_HIGH_KEY,
        tmp_value, incremental_compaction_strategy_options::DEFAULT_BUCKET_HIGH);
    if (bucket_high <= 1.0) {
        throw exceptions::configuration_exception(fmt::format("{} value ({}) must be greater than 1.0",
            incremental_compaction_strategy_options::BUCKET_HIGH_KEY, bucket_high));
    }
    return bucket_high;
}

static double validate_bucket_high(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options) {
    auto bucket_high = validate_bucket_high(options);
    unchecked_options.erase(incremental_compaction_strategy_options::BUCKET_HIGH_KEY);
    return bucket_high;
}

static int validate_fragment_size(const std::map<sstring, sstring>& options) {
    auto tmp_value = compaction_strategy_impl::get_value(options,
        incremental_compaction_strategy::FRAGMENT_SIZE_OPTION);
    auto fragment_size_in_mb = cql3::statements::property_definitions::to_int(incremental_compaction_strategy::FRAGMENT_SIZE_OPTION,
        tmp_value, incremental_compaction_strategy::DEFAULT_MAX_FRAGMENT_SIZE_IN_MB);
    if (fragment_size_in_mb < 100) {
        clogger.warn("SStable size of {}MB is configured. The value may lead to high memory overhead due to sstables proliferation.", fragment_size_in_mb);
    }
    return fragment_size_in_mb;
}

static int validate_fragment_size(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options) {
    auto fragment_size_in_mb = validate_fragment_size(options);
    unchecked_options.erase(incremental_compaction_strategy::FRAGMENT_SIZE_OPTION);
    return fragment_size_in_mb;
}

static std::optional<double> validate_space_amplification_goal(const std::map<sstring, sstring>& options) {
    auto tmp_value = compaction_strategy_impl::get_value(options,
        incremental_compaction_strategy::SPACE_AMPLIFICATION_GOAL_OPTION);
    if (tmp_value) {
        auto space_amplification_goal = cql3::statements::property_definitions::to_double(incremental_compaction_strategy::SPACE_AMPLIFICATION_GOAL_OPTION,
            tmp_value, 0.0);
        if (space_amplification_goal <= 1.0 || space_amplification_goal > 2.0) {
            throw exceptions::configuration_exception(fmt::format("{} value ({}) must be greater than 1.0 and less than or equal to 2.0",
                incremental_compaction_strategy::SPACE_AMPLIFICATION_GOAL_OPTION, space_amplification_goal));
        }
        return space_amplification_goal;
    }
    return std::nullopt;
}

static std::optional<double> validate_space_amplification_goal(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options) {
    auto space_amplification_goal = validate_space_amplification_goal(options);
    unchecked_options.erase(incremental_compaction_strategy::SPACE_AMPLIFICATION_GOAL_OPTION);
    return space_amplification_goal;
}

incremental_compaction_strategy_options::incremental_compaction_strategy_options(const std::map<sstring, sstring>& options) {
    min_sstable_size = validate_min_sstable_size(options);
    bucket_low = validate_bucket_low(options);
    bucket_high = validate_bucket_high(options);
}

// options is a map of compaction strategy options and their values.
// unchecked_options is an analogical map from which already checked options are deleted.
// This helps making sure that only allowed options are being set.
void incremental_compaction_strategy_options::validate(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options) {
    validate_min_sstable_size(options, unchecked_options);
    auto bucket_low = validate_bucket_low(options, unchecked_options);
    auto bucket_high = validate_bucket_high(options, unchecked_options);
    if (bucket_high <= bucket_low) {
        throw exceptions::configuration_exception(fmt::format("{} value ({}) is less than or equal to the {} "
            "value ({})", BUCKET_HIGH_KEY, bucket_high, BUCKET_LOW_KEY, bucket_low));
    }
    validate_fragment_size(options, unchecked_options);
    validate_space_amplification_goal(options, unchecked_options);
    compaction_strategy_impl::validate_min_max_threshold(options, unchecked_options);
}

uint64_t incremental_compaction_strategy::avg_size(std::vector<sstables::frozen_sstable_run>& runs) const {
    uint64_t n = 0;

    if (runs.empty()) {
        return 0;
    }
    for (auto& r : runs) {
        n += r->data_size();
    }
    return n / runs.size();
}

bool incremental_compaction_strategy::is_bucket_interesting(const std::vector<sstables::frozen_sstable_run>& bucket, size_t min_threshold) {
    return bucket.size() >= min_threshold;
}

bool incremental_compaction_strategy::is_any_bucket_interesting(const std::vector<std::vector<sstables::frozen_sstable_run>>& buckets, size_t min_threshold) const {
    return boost::algorithm::any_of(buckets, [&] (const std::vector<sstables::frozen_sstable_run>& bucket) {
        return this->is_bucket_interesting(bucket, min_threshold);
    });
}

std::vector<sstable_run_and_length>
incremental_compaction_strategy::create_run_and_length_pairs(const std::vector<sstables::frozen_sstable_run>& runs) {

    std::vector<sstable_run_and_length> run_length_pairs;
    run_length_pairs.reserve(runs.size());

    for(auto& r_ptr : runs) {
        auto& r = *r_ptr;
        assert(r.data_size() != 0);
        run_length_pairs.emplace_back(r_ptr, r.data_size());
    }

    return run_length_pairs;
}

std::vector<std::vector<sstables::frozen_sstable_run>>
incremental_compaction_strategy::get_buckets(const std::vector<sstables::frozen_sstable_run>& runs, const incremental_compaction_strategy_options& options) {
    auto sorted_runs = create_run_and_length_pairs(runs);

    std::sort(sorted_runs.begin(), sorted_runs.end(), [] (sstable_run_and_length& i, sstable_run_and_length& j) {
        return i.second < j.second;
    });

    using bucket_type = std::vector<sstables::frozen_sstable_run>;
    std::vector<bucket_type> bucket_list;
    std::vector<double> bucket_average_size_list;

    for (auto& pair : sorted_runs) {
        size_t size = pair.second;

        // look for a bucket containing similar-sized runs:
        // group in the same bucket if it's w/in (bucket_low, bucket_high) of the average for this bucket,
        // or this file and the bucket are all considered "small" (less than `minSSTableSize`)
        if (!bucket_list.empty()) {
            auto& bucket_average_size = bucket_average_size_list.back();

            if ((size > (bucket_average_size * options.bucket_low) && size < (bucket_average_size * options.bucket_high)) ||
                    (size < options.min_sstable_size && bucket_average_size < options.min_sstable_size)) {
                auto& bucket = bucket_list.back();
                auto total_size = bucket.size() * bucket_average_size;
                auto new_average_size = (total_size + size) / (bucket.size() + 1);
                auto smallest_run_in_bucket = bucket[0]->data_size();

                // SSTables are added in increasing size order so the bucket's
                // average might drift upwards.
                // Don't let it drift too high, to a point where the smallest
                // SSTable might fall out of range.
                if (size < options.min_sstable_size || smallest_run_in_bucket > new_average_size * options.bucket_low) {
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

std::vector<sstables::frozen_sstable_run>
incremental_compaction_strategy::most_interesting_bucket(std::vector<std::vector<sstables::frozen_sstable_run>> buckets,
        size_t min_threshold, size_t max_threshold)
{
    std::vector<sstable_run_bucket_and_length> interesting_buckets;
    interesting_buckets.reserve(buckets.size());

    for (auto& bucket : buckets) {
        bucket.resize(std::min(bucket.size(), max_threshold));
        if (is_bucket_interesting(bucket, min_threshold)) {
            auto avg = avg_size(bucket);
            interesting_buckets.push_back({ std::move(bucket), avg });
        }
    }

    if (interesting_buckets.empty()) {
        return std::vector<sstables::frozen_sstable_run>();
    }
    // Pick the bucket with more elements, as efficiency of same-tier compactions increases with number of files.
    auto& max = *std::max_element(interesting_buckets.begin(), interesting_buckets.end(),
                    [] (sstable_run_bucket_and_length& i, sstable_run_bucket_and_length& j) {
        return i.first.size() < j.first.size();
    });
    return std::move(max.first);
}

compaction_descriptor
incremental_compaction_strategy::find_garbage_collection_job(const compaction::table_state& t, std::vector<size_bucket_t>& buckets) {
    auto worth_dropping_tombstones = [this, &t, now = db_clock::now()] (const sstable_run& run, gc_clock::time_point compaction_time) {
        if (run.all().empty()) {
            return false;
        }
        // for the purpose of checking if a run is stale, picking any fragment *composing the same run*
        // will be enough as the difference in write time is acceptable.
        auto run_write_time = (*run.all().begin())->data_file_write_time();
        // FIXME: hack to avoid infinite loop, get rid of it once the root cause is fixed.
        // Refs #3571.
        auto min_gc_compaction_interval = std::min(db_clock::duration(std::chrono::seconds(3600)), _tombstone_compaction_interval);
        if ((now - min_gc_compaction_interval) < run_write_time) {
            return false;
        }
        if (_unchecked_tombstone_compaction) {
            return true;
        }
        auto run_max_timestamp = std::ranges::max(run.all() | std::views::transform([] (const shared_sstable& sstable) {
            return sstable->get_stats_metadata().max_timestamp;
        }));
        bool satisfy_staleness = (now - _tombstone_compaction_interval) > run_write_time;
        // Staleness condition becomes mandatory if memtable's data is possibly shadowed by tombstones.
        if (run_max_timestamp >= t.min_memtable_timestamp() && !satisfy_staleness) {
            return false;
        }
        // If interval is not satisfied, we still consider tombstone GC if the gain outweighs the increased frequency.
        // By increasing threshold to a minimum of 0.5, we're only adding a maximum of 1 to write amp as we'll be halving
        // the SSTable, containing garbage, on every GC round.
        float actual_threshold = satisfy_staleness ? _tombstone_threshold : std::clamp(_tombstone_threshold * 2, 0.5f, 1.0f);

        return run.estimate_droppable_tombstone_ratio(compaction_time, t.get_tombstone_gc_state(), t.schema()) >= actual_threshold;
    };
    auto compaction_time = gc_clock::now();
    auto can_garbage_collect = [&] (const size_bucket_t& bucket) {
        return boost::algorithm::any_of(bucket, [&] (const frozen_sstable_run& r) {
            return worth_dropping_tombstones(*r, compaction_time);
        });
    };

    // To make sure that expired tombstones are persisted in a timely manner, ICS will cross-tier compact
    // two closest-in-size buckets such that tombstones will eventually reach the top of the LSM tree,
    // making it possible to purge them.

    // Start from the largest tier as it's more likely to satisfy conditions for tombstones to be purged.
    auto it = buckets.rbegin();
    for (; it != buckets.rend(); it++) {
        if (can_garbage_collect(*it)) {
            break;
        }
    }
    if (it == buckets.rend()) {
        clogger.debug("ICS: nothing to garbage collect in {} buckets for {}.{}", buckets.size(), t.schema()->ks_name(), t.schema()->cf_name());
        return compaction_descriptor();
    }

    size_bucket_t& first_bucket = *it;
    std::vector<sstables::frozen_sstable_run> input = std::move(first_bucket);

    if (buckets.size() >= 2) {
        // If the largest tier needs GC, then compact it with the second largest.
        // Any smaller tier needing GC will be compacted with the larger and closest-in-size one.
        // It's done this way to reduce write amplification and satisfy conditions for purging tombstones.
        it = it == buckets.rbegin() ? std::next(it) : std::prev(it);

        size_bucket_t& second_bucket = *it;

        input.reserve(input.size() + second_bucket.size());
        std::move(second_bucket.begin(), second_bucket.end(), std::back_inserter(input));
    }
    clogger.debug("ICS: starting garbage collection on {} runs for {}.{}", input.size(), t.schema()->ks_name(), t.schema()->cf_name());

    return compaction_descriptor(runs_to_sstables(std::move(input)), 0, _fragment_size);
}

compaction_descriptor
incremental_compaction_strategy::get_sstables_for_compaction(table_state& t, strategy_control& control) {
    auto candidates = control.candidates_as_runs(t);

    // make local copies so they can't be changed out from under us mid-method
    size_t min_threshold = t.min_compaction_threshold();
    size_t max_threshold = t.schema()->max_compaction_threshold();

    auto buckets = get_buckets(candidates);

    if (is_any_bucket_interesting(buckets, min_threshold)) {
        std::vector<sstables::frozen_sstable_run> most_interesting = most_interesting_bucket(std::move(buckets), min_threshold, max_threshold);
        return sstables::compaction_descriptor(runs_to_sstables(std::move(most_interesting)), 0, _fragment_size);
    }
    // If we are not enforcing min_threshold explicitly, try any pair of sstable runs in the same tier.
    if (!t.compaction_enforce_min_threshold() && is_any_bucket_interesting(buckets, 2)) {
        std::vector<sstables::frozen_sstable_run> most_interesting = most_interesting_bucket(std::move(buckets), 2, max_threshold);
        return sstables::compaction_descriptor(runs_to_sstables(std::move(most_interesting)), 0, _fragment_size);
    }

    // The cross-tier behavior is only triggered once we're done with all the pending same-tier compaction to
    // increase overall efficiency.
    if (control.has_ongoing_compaction(t)) {
        return sstables::compaction_descriptor();
    }

    auto desc = find_garbage_collection_job(t, buckets);
    if (!desc.sstables.empty()) {
        return desc;
    }

    if (_space_amplification_goal) {
        if (buckets.size() < 2) {
            return sstables::compaction_descriptor();
        }
        // Let S0 be the size of largest tier
        // Let S1 be the size of second-largest tier,
        // SA will be (S0 + S1) / S0

        // Don't try SAG if there's an ongoing compaction, because if largest tier is being compacted,
        // SA would be calculated incorrectly, which may result in an unneeded cross-tier compaction.

        auto find_two_largest_tiers = [this] (std::vector<size_bucket_t>&& buckets) -> std::tuple<size_bucket_t, size_bucket_t> {
            std::partial_sort(buckets.begin(), buckets.begin()+2, buckets.end(), [this] (size_bucket_t& i, size_bucket_t& j) {
                return avg_size(i) > avg_size(j); // descending order
            });
            return { std::move(buckets[0]), std::move(buckets[1]) };
        };

        auto total_size = [] (const size_bucket_t& bucket) -> uint64_t {
            return std::ranges::fold_left(bucket | std::views::transform(std::mem_fn(&sstable_run::data_size)), uint64_t(0), std::plus{});
        };

        auto [s0, s1] = find_two_largest_tiers(std::move(buckets));
        uint64_t s0_size = total_size(s0), s1_size = total_size(s1);
        double space_amplification = double(s0_size + s1_size) / s0_size;

        if (space_amplification > _space_amplification_goal) {
            clogger.debug("ICS: doing cross-tier compaction of two largest tiers, to reduce SA {} to below SAG {}",
                          space_amplification, *_space_amplification_goal);
            // Aims at reducing space amplification, to below SAG, by compacting together the two largest tiers
            std::vector<sstables::frozen_sstable_run> cross_tier_input = std::move(s0);
            cross_tier_input.reserve(cross_tier_input.size() + s1.size());
            std::move(s1.begin(), s1.end(), std::back_inserter(cross_tier_input));

            return sstables::compaction_descriptor(runs_to_sstables(std::move(cross_tier_input)),
                                                   0, _fragment_size);
        }
    }

    return sstables::compaction_descriptor();
}

compaction_descriptor
incremental_compaction_strategy::get_major_compaction_job(table_state& t, std::vector<sstables::shared_sstable> candidates) {
    if (candidates.empty()) {
        return compaction_descriptor();
    }
    return make_major_compaction_job(std::move(candidates), 0, _fragment_size);
}

int64_t incremental_compaction_strategy::estimated_pending_compactions(table_state& t) const {
    size_t min_threshold = t.schema()->min_compaction_threshold();
    size_t max_threshold = t.schema()->max_compaction_threshold();
    int64_t n = 0;

    for (auto& bucket : get_buckets(t.main_sstable_set().all_sstable_runs())) {
        if (bucket.size() >= min_threshold) {
            n += (bucket.size() + max_threshold - 1) / max_threshold;
        }
    }
    return n;
}

std::vector<shared_sstable>
incremental_compaction_strategy::runs_to_sstables(std::vector<frozen_sstable_run> runs) {
    return boost::accumulate(runs, std::vector<shared_sstable>(), [&] (std::vector<shared_sstable>&& v, const frozen_sstable_run& run_ptr) {
        auto& run = *run_ptr;
        v.insert(v.end(), run.all().begin(), run.all().end());
        return std::move(v);
    });
}

std::vector<frozen_sstable_run>
incremental_compaction_strategy::sstables_to_runs(std::vector<shared_sstable> sstables) {
    std::unordered_map<sstables::run_id, sstable_run> runs;
    for (auto&& sst : sstables) {
        // okay to ignore duplicates
        (void)runs[sst->run_identifier()].insert(std::move(sst));
    }
    auto freeze = [] (const sstable_run& run) { return make_lw_shared<const sstable_run>(run); };
    return runs | std::views::values | std::views::transform(freeze) | std::ranges::to<std::vector>();
}

void incremental_compaction_strategy::sort_run_bucket_by_first_key(size_bucket_t& bucket, size_t max_elements, const schema_ptr& schema) {
    std::partial_sort(bucket.begin(), bucket.begin() + max_elements, bucket.end(), [&schema](const frozen_sstable_run& a, const frozen_sstable_run& b) {
        auto sst_first_key_less = [&schema] (const shared_sstable& sst_a, const shared_sstable& sst_b) {
            return sst_a->get_first_decorated_key().tri_compare(*schema, sst_b->get_first_decorated_key()) <= 0;
        };
        auto& a_first = *boost::min_element(a->all(), sst_first_key_less);
        auto& b_first = *boost::min_element(b->all(), sst_first_key_less);
        return a_first->get_first_decorated_key().tri_compare(*schema, b_first->get_first_decorated_key()) <= 0;
    });
}

compaction_descriptor
incremental_compaction_strategy::get_reshaping_job(std::vector<shared_sstable> input, schema_ptr schema, reshape_config cfg) const {
    auto mode = cfg.mode;
    size_t offstrategy_threshold = std::max(schema->min_compaction_threshold(), 4);
    size_t max_sstables = std::max(schema->max_compaction_threshold(), int(offstrategy_threshold));

    if (mode == reshape_mode::relaxed) {
        offstrategy_threshold = max_sstables;
    }

    auto run_count = std::ranges::size(input | std::views::transform(std::mem_fn(&sstable::run_identifier)) | std::ranges::to<std::unordered_set>());
    if (run_count >= offstrategy_threshold && mode == reshape_mode::strict) {
        std::sort(input.begin(), input.end(), [&schema] (const shared_sstable& a, const shared_sstable& b) {
            return dht::ring_position(a->get_first_decorated_key()).less_compare(*schema, dht::ring_position(b->get_first_decorated_key()));
        });
        // All sstables can be reshaped at once if the amount of overlapping will not cause memory usage to be high,
        // which is possible because partitioned set is able to incrementally open sstables during compaction
        if (sstable_set_overlapping_count(schema, input) <= max_sstables) {
            compaction_descriptor desc(std::move(input), 0/* level */, _fragment_size);
            desc.options = compaction_type_options::make_reshape();
            return desc;
        }
    }

    for (auto& bucket : get_buckets(sstables_to_runs(std::move(input)))) {
        if (bucket.size() >= offstrategy_threshold) {
            // preserve token contiguity by prioritizing runs with the lowest first keys.
            if (bucket.size() > max_sstables) {
                sort_run_bucket_by_first_key(bucket, max_sstables, schema);
                bucket.resize(max_sstables);
            }
            compaction_descriptor desc(runs_to_sstables(std::move(bucket)), 0/* level */, _fragment_size);
            desc.options = compaction_type_options::make_reshape();
            return desc;
        }
    }

    return compaction_descriptor();
}

std::vector<compaction_descriptor>
incremental_compaction_strategy::get_cleanup_compaction_jobs(table_state& t, std::vector<shared_sstable> candidates) const {
    std::vector<compaction_descriptor> ret;
    const auto& schema = t.schema();
    unsigned max_threshold = schema->max_compaction_threshold();

    for (auto& bucket : get_buckets(sstables_to_runs(std::move(candidates)))) {
        if (bucket.size() > max_threshold) {
            // preserve token contiguity
            sort_run_bucket_by_first_key(bucket, bucket.size(), schema);
        }
        auto it = bucket.begin();
        while (it != bucket.end()) {
            unsigned remaining = std::distance(it, bucket.end());
            unsigned needed = std::min(remaining, max_threshold);
            std::vector<frozen_sstable_run> runs;
            std::move(it, it + needed, std::back_inserter(runs));
            ret.push_back(compaction_descriptor(runs_to_sstables(std::move(runs)), 0/* level */, _fragment_size));
            std::advance(it, needed);
        }
    }
    return ret;
}

std::unique_ptr<compaction_backlog_tracker::impl>
incremental_compaction_strategy::make_backlog_tracker() const {
    return std::make_unique<incremental_backlog_tracker>(_options);
}

incremental_compaction_strategy::incremental_compaction_strategy(const std::map<sstring, sstring>& options)
    : compaction_strategy_impl(options)
    , _options(options)
{
    auto fragment_size_in_mb = validate_fragment_size(options);
    _fragment_size = fragment_size_in_mb*1024*1024;
    _space_amplification_goal = validate_space_amplification_goal(options);
}

// options is a map of compaction strategy options and their values.
// unchecked_options is an analogical map from which already checked options are deleted.
// This helps making sure that only allowed options are being set.
void incremental_compaction_strategy::validate_options(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options) {
    incremental_compaction_strategy_options::validate(options, unchecked_options);
}

}
