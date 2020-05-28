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

#include "sstables/time_window_compaction_strategy.hh"
#include "mutation_writer/timestamp_based_splitting_writer.hh"
#include "mutation_source_metadata.hh"

namespace sstables {

class classify_by_timestamp {
    time_window_compaction_strategy_options _options;
    std::vector<int64_t> _known_windows;

public:
    explicit classify_by_timestamp(time_window_compaction_strategy_options options) : _options(std::move(options)) { }
    int64_t operator()(api::timestamp_type ts) {
        const auto window = time_window_compaction_strategy::get_window_for(_options, ts);
        if (const auto it = boost::find(_known_windows, window); it != _known_windows.end()) {
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

uint64_t time_window_compaction_strategy::adjust_partition_estimate(const mutation_source_metadata& ms_meta, uint64_t partition_estimate) {
    if (!ms_meta.min_timestamp || !ms_meta.max_timestamp) {
        // Not enough information, we assume the worst
        return partition_estimate / max_data_segregation_window_count;
    }
    const auto min_window = get_window_for(_options, *ms_meta.min_timestamp);
    const auto max_window = get_window_for(_options, *ms_meta.max_timestamp);
    const auto window_size = get_window_size(_options);

    auto estimated_window_count = (max_window + (window_size - 1) - min_window) / window_size;

    return partition_estimate / std::max(1UL, uint64_t(estimated_window_count));
}

reader_consumer time_window_compaction_strategy::make_interposer_consumer(const mutation_source_metadata& ms_meta, reader_consumer end_consumer) {
    if (ms_meta.min_timestamp && ms_meta.max_timestamp
            && get_window_for(_options, *ms_meta.min_timestamp) == get_window_for(_options, *ms_meta.max_timestamp)) {
        return end_consumer;
    }
    return [options = _options, end_consumer = std::move(end_consumer)] (flat_mutation_reader rd) mutable -> future<> {
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

    for (auto& sst : input) {
        auto min = sst->get_stats_metadata().min_timestamp;
        auto max = sst->get_stats_metadata().max_timestamp;
        if (get_window_for(_options, min) != get_window_for(_options, max)) {
            multi_window.push_back(sst);
        } else {
            single_window.push_back(sst);
        }
    }

    if (!multi_window.empty()) {
        // Everything that spans multiple windows will need reshaping
        multi_window.resize(std::min(multi_window.size(), max_sstables));
        compaction_descriptor desc(std::move(multi_window), std::optional<sstables::sstable_set>(), iop);
        desc.options = compaction_options::make_reshape();
        return desc;
    }

    // For things that don't span multiple windows, we compact windows that are individually too big
    auto all_buckets = get_buckets(single_window, _options);
    for (auto& pair : all_buckets.first) {
        auto ssts = std::move(pair.second);
        if (ssts.size() > offstrategy_threshold) {
            ssts.resize(std::min(multi_window.size(), max_sstables));
            compaction_descriptor desc(std::move(ssts), std::optional<sstables::sstable_set>(), iop);
            desc.options = compaction_options::make_reshape();
            return desc;
        }
    }

    return compaction_descriptor();
}

}
