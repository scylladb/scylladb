/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "time_window_compaction_strategy.hh"
#include "leveled_manifest.hh"
#include "mutation_writer/timestamp_based_splitting_writer.hh"
#include "mutation_source_metadata.hh"

#include <boost/range/algorithm/find.hpp>
#include <boost/range/algorithm/remove_if.hpp>
#include <boost/range/algorithm/min_element.hpp>
#include <boost/range/algorithm/partial_sort.hpp>
#include <boost/range/adaptor/reversed.hpp>

namespace sstables {

extern logging::logger clogger;

time_window_compaction_strategy_options::time_window_compaction_strategy_options(const std::map<sstring, sstring>& options) {
    std::chrono::seconds window_unit = DEFAULT_COMPACTION_WINDOW_UNIT;
    int window_size = DEFAULT_COMPACTION_WINDOW_SIZE;

    auto it = options.find(COMPACTION_WINDOW_UNIT_KEY);
    if (it != options.end()) {
        auto valid_window_units_it = valid_window_units.find(it->second);
        if (valid_window_units_it == valid_window_units.end()) {
            throw exceptions::syntax_exception(sstring("Invalid window unit ") + it->second + " for " + COMPACTION_WINDOW_UNIT_KEY);
        }
        window_unit = valid_window_units_it->second;
    }

    it = options.find(COMPACTION_WINDOW_SIZE_KEY);
    if (it != options.end()) {
        try {
            window_size = std::stoi(it->second);
        } catch (const std::exception& e) {
            throw exceptions::syntax_exception(sstring("Invalid integer value ") + it->second + " for " + COMPACTION_WINDOW_SIZE_KEY);
        }
    }

    if (window_size <= 0) {
        throw exceptions::configuration_exception(fmt::format("{} must be greater than 1 for compaction_window_size", window_size));
    }

    sstable_window_size = window_size * window_unit;

    it = options.find(EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY);
    if (it != options.end()) {
        try {
            expired_sstable_check_frequency = std::chrono::seconds(std::stol(it->second));
        } catch (const std::exception& e) {
            throw exceptions::syntax_exception(sstring("Invalid long value ") + it->second + "for " + EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY);
        }
    }

    it = options.find(TIMESTAMP_RESOLUTION_KEY);
    if (it != options.end()) {
        if (!valid_timestamp_resolutions.contains(it->second)) {
            throw exceptions::syntax_exception(sstring("Invalid timestamp resolution ") + it->second + "for " + TIMESTAMP_RESOLUTION_KEY);
        } else {
            timestamp_resolution = valid_timestamp_resolutions.at(it->second);
        }
    }
}

time_window_compaction_strategy_options::time_window_compaction_strategy_options(time_window_compaction_strategy_options&&) = default;

time_window_compaction_strategy_options::time_window_compaction_strategy_options(const time_window_compaction_strategy_options&) = default;

class classify_by_timestamp {
    time_window_compaction_strategy_options _options;
    std::vector<int64_t> _known_windows;

public:
    explicit classify_by_timestamp(time_window_compaction_strategy_options options) : _options(std::move(options)) { }
    int64_t operator()(api::timestamp_type ts) {
        const auto window = time_window_compaction_strategy::get_window_for(_options, ts);
        if (const auto it = boost::range::find(_known_windows, window); it != _known_windows.end()) {
            std::swap(*it, _known_windows.front());
            return window;
        }
        if (_known_windows.size() < time_window_compaction_strategy::max_data_segregation_window_count) {
            _known_windows.push_back(window);
            return window;
        }
        int64_t closest_window;
        int64_t min_diff = std::numeric_limits<int64_t>::max();
        for (const auto known_window : _known_windows) {
            if (const auto diff = std::abs(known_window - window); diff < min_diff) {
                min_diff = diff;
                closest_window = known_window;
            }
        }
        return closest_window;
    };
};

uint64_t time_window_compaction_strategy::adjust_partition_estimate(const mutation_source_metadata& ms_meta, uint64_t partition_estimate, schema_ptr s) {
    // If not enough information, we assume the worst
    auto estimated_window_count = max_data_segregation_window_count;
    auto default_ttl = std::chrono::duration_cast<std::chrono::microseconds>(s->default_time_to_live());
    bool min_and_max_ts_available = ms_meta.min_timestamp && ms_meta.max_timestamp;
    auto estimate_window_count = [this] (timestamp_type min_window, timestamp_type max_window) {
        const auto window_size = get_window_size(_options);
        return (max_window + (window_size - 1) - min_window) / window_size;
    };

    if (!min_and_max_ts_available && default_ttl.count()) {
        auto min_window = get_window_for(_options, timestamp_type(0));
        auto max_window = get_window_for(_options, timestamp_type(default_ttl.count()));

        estimated_window_count = estimate_window_count(min_window, max_window);
    } else if (min_and_max_ts_available) {
        auto min_window = get_window_for(_options, *ms_meta.min_timestamp);
        auto max_window = get_window_for(_options, *ms_meta.max_timestamp);

        estimated_window_count = estimate_window_count(min_window, max_window);
    }

    return partition_estimate / std::max(1UL, uint64_t(estimated_window_count));
}

reader_consumer_v2 time_window_compaction_strategy::make_interposer_consumer(const mutation_source_metadata& ms_meta, reader_consumer_v2 end_consumer) {
    if (ms_meta.min_timestamp && ms_meta.max_timestamp
            && get_window_for(_options, *ms_meta.min_timestamp) == get_window_for(_options, *ms_meta.max_timestamp)) {
        return end_consumer;
    }
    return [options = _options, end_consumer = std::move(end_consumer)] (flat_mutation_reader_v2 rd) mutable -> future<> {
        return mutation_writer::segregate_by_timestamp(
                std::move(rd),
                classify_by_timestamp(std::move(options)),
                std::move(end_consumer));
    };
}

compaction_descriptor
time_window_compaction_strategy::get_reshaping_job(std::vector<shared_sstable> input, schema_ptr schema, const ::io_priority_class& iop, reshape_mode mode) {
    std::vector<shared_sstable> single_window;
    std::vector<shared_sstable> multi_window;

    size_t offstrategy_threshold = std::max(schema->min_compaction_threshold(), 4);
    size_t max_sstables = std::max(schema->max_compaction_threshold(), int(offstrategy_threshold));

    if (mode == reshape_mode::relaxed) {
        offstrategy_threshold = max_sstables;
    }

    // Sort input sstables by first_key order
    // to allow efficient reshaping of disjoint sstables.
    std::sort(input.begin(), input.end(), [&schema] (const shared_sstable& a, const shared_sstable& b) {
        return dht::ring_position(a->get_first_decorated_key()).less_compare(*schema, dht::ring_position(b->get_first_decorated_key()));
    });

    for (auto& sst : input) {
        auto min = sst->get_stats_metadata().min_timestamp;
        auto max = sst->get_stats_metadata().max_timestamp;
        if (get_window_for(_options, min) != get_window_for(_options, max)) {
            multi_window.push_back(sst);
        } else {
            single_window.push_back(sst);
        }
    }

    auto is_disjoint = [&schema, mode, max_sstables] (const std::vector<shared_sstable>& ssts) {
        size_t tolerance = (mode == reshape_mode::relaxed) ? max_sstables : 0;
        return sstable_set_overlapping_count(schema, ssts) <= tolerance;
    };

    clogger.debug("time_window_compaction_strategy::get_reshaping_job: offstrategy_threshold={} max_sstables={} multi_window={} disjoint={} single_window={} disjoint={}",
            offstrategy_threshold, max_sstables,
            multi_window.size(), !multi_window.empty() && sstable_set_overlapping_count(schema, multi_window) == 0,
            single_window.size(), !single_window.empty() && sstable_set_overlapping_count(schema, single_window) == 0);

    auto need_trimming = [max_sstables, schema, &is_disjoint] (const std::vector<shared_sstable>& ssts) {
        // All sstables can be compacted at once if they're disjoint, given that partitioned set
        // will incrementally open sstables which translates into bounded memory usage.
        return ssts.size() > max_sstables && !is_disjoint(ssts);
    };

    if (!multi_window.empty()) {
        // Everything that spans multiple windows will need reshaping
        if (need_trimming(multi_window)) {
            // When trimming, let's keep sstables with overlapping time window, so as to reduce write amplification.
            // For example, if there are N sstables spanning window W, where N <= 32, then we can produce all data for W
            // in a single compaction round, removing the need to later compact W to reduce its number of files.
            boost::partial_sort(multi_window, multi_window.begin() + max_sstables, [](const shared_sstable &a, const shared_sstable &b) {
                return a->get_stats_metadata().max_timestamp < b->get_stats_metadata().max_timestamp;
            });
            multi_window.resize(max_sstables);
        }
        compaction_descriptor desc(std::move(multi_window), iop);
        desc.options = compaction_type_options::make_reshape();
        return desc;
    }

    // For things that don't span multiple windows, we compact windows that are individually too big
    auto all_disjoint = !single_window.empty() && is_disjoint(single_window);
    auto all_buckets = get_buckets(single_window, _options);
    single_window.clear();
    for (auto& pair : all_buckets.first) {
        auto ssts = std::move(pair.second);
        if (ssts.size() >= offstrategy_threshold) {
            clogger.debug("time_window_compaction_strategy::get_reshaping_job: bucket={} bucket_size={}", pair.first, ssts.size());
            if (all_disjoint) {
                std::copy(ssts.begin(), ssts.end(), std::back_inserter(single_window));
                continue;
            }
            // reuse STCS reshape logic which will only compact similar-sized files, to increase overall efficiency
            // when reshaping time buckets containing a huge amount of files
            auto desc = size_tiered_compaction_strategy(_stcs_options).get_reshaping_job(std::move(ssts), schema, iop, mode);
            if (!desc.sstables.empty()) {
                return desc;
            }
        }
    }
    if (!single_window.empty()) {
        compaction_descriptor desc(std::move(single_window), iop);
        desc.options = compaction_type_options::make_reshape();
        return desc;
    }

    return compaction_descriptor();
}

compaction_descriptor
time_window_compaction_strategy::get_sstables_for_compaction(table_state& table_s, strategy_control& control, std::vector<shared_sstable> candidates) {
    auto compaction_time = gc_clock::now();

    if (candidates.empty()) {
        _estimated_remaining_tasks = 0;
        return compaction_descriptor();
    }

    auto now = db_clock::now();
    if (now - _last_expired_check > _options.expired_sstable_check_frequency) {
        clogger.debug("[{}] TWCS expired check sufficiently far in the past, checking for fully expired SSTables", fmt::ptr(this));

        // Find fully expired SSTables. Those will be included no matter what.
        auto expired = table_s.fully_expired_sstables(candidates, compaction_time);
        if (!expired.empty()) {
            clogger.debug("[{}] Going to compact {} expired sstables", fmt::ptr(this), expired.size());
            return compaction_descriptor(has_only_fully_expired::yes, std::vector<shared_sstable>(expired.begin(), expired.end()), service::get_local_compaction_priority());
        }
        // Keep checking for fully_expired_sstables until we don't find
        // any among the candidates, meaning they are either already compacted
        // or registered for compaction.
        _last_expired_check = now;
    } else {
        clogger.debug("[{}] TWCS skipping check for fully expired SSTables", fmt::ptr(this));
    }

    auto compaction_candidates = get_next_non_expired_sstables(table_s, control, std::move(candidates), compaction_time);
    clogger.debug("[{}] Going to compact {} non-expired sstables", fmt::ptr(this), compaction_candidates.size());
    return compaction_descriptor(std::move(compaction_candidates), service::get_local_compaction_priority());
}

time_window_compaction_strategy::bucket_compaction_mode
time_window_compaction_strategy::compaction_mode(const bucket_t& bucket, timestamp_type bucket_key,
        timestamp_type now, size_t min_threshold) const {
    // STCS will also be performed on older window buckets, to avoid a bad write and
    // space amplification when something like read repair cause small updates to
    // those past windows.

    if (bucket.size() >= 2 && !is_last_active_bucket(bucket_key, now) && _recent_active_windows.contains(bucket_key)) {
        return bucket_compaction_mode::major;
    } else if (bucket.size() >= size_t(min_threshold)) {
        return bucket_compaction_mode::size_tiered;
    }
    return bucket_compaction_mode::none;
}

std::vector<shared_sstable>
time_window_compaction_strategy::get_next_non_expired_sstables(table_state& table_s, strategy_control& control,
        std::vector<shared_sstable> non_expiring_sstables, gc_clock::time_point compaction_time) {
    auto most_interesting = get_compaction_candidates(table_s, control, non_expiring_sstables);

    if (!most_interesting.empty()) {
        return most_interesting;
    }

    // if there is no sstable to compact in standard way, try compacting single sstable whose droppable tombstone
    // ratio is greater than threshold.
    auto e = boost::range::remove_if(non_expiring_sstables, [this, compaction_time, &table_s] (const shared_sstable& sst) -> bool {
        return !worth_dropping_tombstones(sst, compaction_time, table_s.get_tombstone_gc_state());
    });
    non_expiring_sstables.erase(e, non_expiring_sstables.end());
    if (non_expiring_sstables.empty()) {
        return {};
    }
    auto it = boost::min_element(non_expiring_sstables, [] (auto& i, auto& j) {
        return i->get_stats_metadata().min_timestamp < j->get_stats_metadata().min_timestamp;
    });
    return { *it };
}

std::vector<shared_sstable>
time_window_compaction_strategy::get_compaction_candidates(table_state& table_s, strategy_control& control, std::vector<shared_sstable> candidate_sstables) {
    auto p = get_buckets(std::move(candidate_sstables), _options);
    // Update the highest window seen, if necessary
    _highest_window_seen = std::max(_highest_window_seen, p.second);

    update_estimated_compaction_by_tasks(p.first, table_s.min_compaction_threshold(), table_s.schema()->max_compaction_threshold());

    return newest_bucket(table_s, control, std::move(p.first), table_s.min_compaction_threshold(), table_s.schema()->max_compaction_threshold(),
        _highest_window_seen);
}

timestamp_type
time_window_compaction_strategy::get_window_lower_bound(std::chrono::seconds sstable_window_size, timestamp_type timestamp) {
    using namespace std::chrono;
    auto timestamp_in_sec = duration_cast<seconds>(microseconds(timestamp)).count();

    // mask out window size from timestamp to get lower bound of its window
    auto window_lower_bound_in_sec = seconds(timestamp_in_sec - (timestamp_in_sec % sstable_window_size.count()));

    return timestamp_type(duration_cast<microseconds>(window_lower_bound_in_sec).count());
}

std::pair<std::map<timestamp_type, std::vector<shared_sstable>>, timestamp_type>
time_window_compaction_strategy::get_buckets(std::vector<shared_sstable> files, const time_window_compaction_strategy_options& options) {
    std::map<timestamp_type, std::vector<shared_sstable>> buckets;

    timestamp_type max_timestamp = 0;
    // Create map to represent buckets
    // For each sstable, add sstable to the time bucket
    // Where the bucket is the file's max timestamp rounded to the nearest window bucket
    for (auto&& f : files) {
        timestamp_type ts = to_timestamp_type(options.timestamp_resolution, f->get_stats_metadata().max_timestamp);
        timestamp_type lower_bound = get_window_lower_bound(options.sstable_window_size, ts);
        buckets[lower_bound].push_back(std::move(f));
        max_timestamp = std::max(max_timestamp, lower_bound);
    }

    return std::make_pair(std::move(buckets), max_timestamp);
}

static std::ostream& operator<<(std::ostream& os, const std::map<timestamp_type, std::vector<shared_sstable>>& buckets) {
    os << "  buckets = {\n";
    for (auto& bucket : buckets | boost::adaptors::reversed) {
        os << format("    key={}, size={}\n", bucket.first, bucket.second.size());
    }
    os << "  }\n";
    return os;
}

std::vector<shared_sstable>
time_window_compaction_strategy::newest_bucket(table_state& table_s, strategy_control& control, std::map<timestamp_type, std::vector<shared_sstable>> buckets,
        int min_threshold, int max_threshold, timestamp_type now) {
    clogger.debug("time_window_compaction_strategy::newest_bucket:\n  now {}\n{}", now, buckets);

    for (auto&& key_bucket : buckets | boost::adaptors::reversed) {
        auto key = key_bucket.first;
        auto& bucket = key_bucket.second;

        bool last_active_bucket = is_last_active_bucket(key, now);
        if (last_active_bucket) {
            _recent_active_windows.insert(key);
        }
        switch (compaction_mode(bucket, key, now, min_threshold)) {
        case bucket_compaction_mode::size_tiered: {
            // If we're in the newest bucket, we'll use STCS to prioritize sstables.
            auto stcs_interesting_bucket = size_tiered_compaction_strategy::most_interesting_bucket(bucket, min_threshold, max_threshold, _stcs_options);

            // If the tables in the current bucket aren't eligible in the STCS strategy, we'll skip it and look for other buckets
            if (!stcs_interesting_bucket.empty()) {
                clogger.debug("bucket size {} >= 2, key {}, performing STCS on what's here", bucket.size(), key);
                return stcs_interesting_bucket;
            }
            break;
        }
        case bucket_compaction_mode::major:
            // serializes per-window major on a past window, to avoid missing its files being currently compacted.
            if (control.has_ongoing_compaction(table_s)) {
                break;
            }
            clogger.debug("bucket size {} >= 2 and not in current bucket, key {}, compacting what's here", bucket.size(), key);
            return trim_to_threshold(std::move(bucket), max_threshold);
        default:
            // windows needing major will remain with major state until they're compacted into one file.
            // after that, they will fall into default mode where we'll stop considering them as a recent window
            // which needs major. That's to avoid terrible writeamp as streaming may push data into older windows.
            if (!last_active_bucket) {
                _recent_active_windows.erase(key);
            }
            clogger.debug("No compaction necessary for bucket size {} , key {}, now {}", bucket.size(), key, now);
            break;
        }
    }
    return {};
}

std::vector<shared_sstable>
time_window_compaction_strategy::trim_to_threshold(std::vector<shared_sstable> bucket, int max_threshold) {
    auto n = std::min(bucket.size(), size_t(max_threshold));
    // Trim the largest sstables off the end to meet the maxThreshold
    boost::partial_sort(bucket, bucket.begin() + n, [] (auto& i, auto& j) {
        return i->ondisk_data_size() < j->ondisk_data_size();
    });
    bucket.resize(n);
    return bucket;
}

void time_window_compaction_strategy::update_estimated_compaction_by_tasks(std::map<timestamp_type, std::vector<shared_sstable>>& tasks,
                                                                           int min_threshold, int max_threshold) {
    int64_t n = 0;
    timestamp_type now = _highest_window_seen;

    for (auto& task : tasks) {
        const bucket_t& bucket = task.second;
        timestamp_type bucket_key = task.first;

        switch (compaction_mode(bucket, bucket_key, now, min_threshold)) {
        case bucket_compaction_mode::size_tiered:
            n += size_tiered_compaction_strategy::estimated_pending_compactions(bucket, min_threshold, max_threshold, _stcs_options);
            break;
        case bucket_compaction_mode::major:
            n++;
        default:
            break;
        }
    }
    _estimated_remaining_tasks = n;
}

std::vector<compaction_descriptor>
time_window_compaction_strategy::get_cleanup_compaction_jobs(table_state& table_s, std::vector<shared_sstable> candidates) const {
    std::vector<compaction_descriptor> ret;
    for (auto&& [_, sstables] : get_buckets(std::move(candidates), _options).first) {
        auto per_window_jobs = size_tiered_compaction_strategy(_stcs_options).get_cleanup_compaction_jobs(table_s, std::move(sstables));
        std::move(per_window_jobs.begin(), per_window_jobs.end(), std::back_inserter(ret));
    }
    return ret;
}

}
