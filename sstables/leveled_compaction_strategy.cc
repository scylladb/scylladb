/*
 * Copyright (C) 2019-present ScyllaDB
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

#include "leveled_compaction_strategy.hh"
#include "sstables/leveled_manifest.hh"
#include <algorithm>
#include <ranges>

#include <boost/range/algorithm/remove_if.hpp>

namespace sstables {

compaction_descriptor leveled_compaction_strategy::get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) {
    // NOTE: leveled_manifest creation may be slightly expensive, so later on,
    // we may want to store it in the strategy itself. However, the sstable
    // lists managed by the manifest may become outdated. For example, one
    // sstable in it may be marked for deletion after compacted.
    // Currently, we create a new manifest whenever it's time for compaction.
    leveled_manifest manifest = leveled_manifest::create(cfs, candidates, _max_sstable_size_in_mb, _stcs_options);
    if (!_last_compacted_keys) {
        generate_last_compacted_keys(manifest);
    }
    auto candidate = manifest.get_compaction_candidates(*_last_compacted_keys, _compaction_counter);

    if (!candidate.sstables.empty()) {
        leveled_manifest::logger.debug("leveled: Compacting {} out of {} sstables", candidate.sstables.size(), cfs.get_sstables()->size());
        return candidate;
    }

    // if there is no sstable to compact in standard way, try compacting based on droppable tombstone ratio
    // unlike stcs, lcs can look for sstable with highest droppable tombstone ratio, so as not to choose
    // a sstable which droppable data shadow data in older sstable, by starting from highest levels which
    // theoretically contain oldest non-overlapping data.
    auto gc_before = gc_clock::now() - cfs.schema()->gc_grace_seconds();
    for (auto level = int(manifest.get_level_count()); level >= 0; level--) {
        auto& sstables = manifest.get_level(level);
        // filter out sstables which droppable tombstone ratio isn't greater than the defined threshold.
        auto e = boost::range::remove_if(sstables, [this, &gc_before] (const sstables::shared_sstable& sst) -> bool {
            return !worth_dropping_tombstones(sst, gc_before);
        });
        sstables.erase(e, sstables.end());
        if (sstables.empty()) {
            continue;
        }
        auto& sst = *std::max_element(sstables.begin(), sstables.end(), [&] (auto& i, auto& j) {
            return i->estimate_droppable_tombstone_ratio(gc_before) < j->estimate_droppable_tombstone_ratio(gc_before);
        });
        return sstables::compaction_descriptor({ sst }, cfs.get_sstable_set(), service::get_local_compaction_priority(), sst->get_sstable_level());
    }
    return {};
}

compaction_descriptor leveled_compaction_strategy::get_major_compaction_job(column_family& cf, std::vector<sstables::shared_sstable> candidates) {
    if (candidates.empty()) {
        return compaction_descriptor();
    }

    auto& sst = *std::max_element(candidates.begin(), candidates.end(), [&] (sstables::shared_sstable& sst1, sstables::shared_sstable& sst2) {
        return sst1->get_sstable_level() < sst2->get_sstable_level();
    });
    return compaction_descriptor(std::move(candidates), cf.get_sstable_set(), service::get_local_compaction_priority(),
                                 sst->get_sstable_level(), _max_sstable_size_in_mb*1024*1024);
}

void leveled_compaction_strategy::notify_completion(const std::vector<shared_sstable>& removed, const std::vector<shared_sstable>& added) {
    if (removed.empty() || added.empty()) {
        return;
    }
    auto min_level = std::numeric_limits<uint32_t>::max();
    for (auto& sstable : removed) {
        min_level = std::min(min_level, sstable->get_sstable_level());
    }

    const sstables::sstable *last = nullptr;
    int target_level = 0;
    for (auto& candidate : added) {
        if (!last || last->compare_by_first_key(*candidate) < 0) {
            last = &*candidate;
        }
        target_level = std::max(target_level, int(candidate->get_sstable_level()));
    }
    _last_compacted_keys.value().at(min_level) = last->get_last_decorated_key();

    for (int i = leveled_manifest::MAX_LEVELS - 1; i > 0; i--) {
        _compaction_counter[i]++;
    }
    _compaction_counter[target_level] = 0;

    if (leveled_manifest::logger.level() == logging::log_level::debug) {
        for (auto j = 0U; j < _compaction_counter.size(); j++) {
            leveled_manifest::logger.debug("CompactionCounter: {}: {}", j, _compaction_counter[j]);
        }
    }
}

void leveled_compaction_strategy::generate_last_compacted_keys(leveled_manifest& manifest) {
    std::vector<std::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
    for (auto i = 0; i < leveled_manifest::MAX_LEVELS - 1; i++) {
        if (manifest.get_level(i + 1).empty()) {
            continue;
        }

        const sstables::sstable* sstable_with_last_compacted_key = nullptr;
        std::optional<db_clock::time_point> max_creation_time;
        for (auto& sst : manifest.get_level(i + 1)) {
            auto wtime = sst->data_file_write_time();
            if (!max_creation_time || wtime >= *max_creation_time) {
                sstable_with_last_compacted_key = &*sst;
                max_creation_time = wtime;
            }
        }
        last_compacted_keys[i] = sstable_with_last_compacted_key->get_last_decorated_key();
    }
    _last_compacted_keys = std::move(last_compacted_keys);
}

int64_t leveled_compaction_strategy::estimated_pending_compactions(column_family& cf) const {
    std::vector<sstables::shared_sstable> sstables;
    sstables.reserve(cf.sstables_count());
    for (auto all_sstables = cf.get_sstables(); auto& entry : *all_sstables) {
        sstables.push_back(entry);
    }
    leveled_manifest manifest = leveled_manifest::create(cf, sstables, _max_sstable_size_in_mb, _stcs_options);
    return manifest.get_estimated_tasks();
}

compaction_descriptor
leveled_compaction_strategy::get_reshaping_job(std::vector<shared_sstable> input, schema_ptr schema, const ::io_priority_class& iop, reshape_mode mode) {
    std::array<std::vector<shared_sstable>, leveled_manifest::MAX_LEVELS> level_info;

    auto is_disjoint = [this, schema] (const std::vector<shared_sstable>& sstables, unsigned tolerance) -> std::tuple<bool, unsigned> {
        auto overlapping_sstables = sstable_set_overlapping_count(schema, sstables);
        return { overlapping_sstables <= tolerance, overlapping_sstables };
    };

    auto max_sstable_size_in_bytes = _max_sstable_size_in_mb * 1024 * 1024;

    for (auto& sst : input) {
        auto sst_level = sst->get_sstable_level();
        if (sst_level > leveled_manifest::MAX_LEVELS - 1) {
            leveled_manifest::logger.warn("Found SSTable with level {}, higher than the maximum {}. This is unexpected, but will fix", sst_level, leveled_manifest::MAX_LEVELS - 1);

            // This is really unexpected, so we'll just compact it all to fix it
            compaction_descriptor desc(std::move(input), std::optional<sstables::sstable_set>(), iop, leveled_manifest::MAX_LEVELS - 1, max_sstable_size_in_bytes);
            desc.options = compaction_options::make_reshape();
            return desc;
        }
        level_info[sst_level].push_back(sst);
    }

    // Can't use std::ranges::views::drop due to https://bugs.llvm.org/show_bug.cgi?id=47509
    for (auto i = level_info.begin(); i != level_info.end(); ++i) {
        auto& level = *i;
        std::sort(level.begin(), level.end(), [&schema] (const shared_sstable& a, const shared_sstable& b) {
            return dht::ring_position(a->get_first_decorated_key()).less_compare(*schema, dht::ring_position(b->get_first_decorated_key()));
        });
    }

    unsigned max_filled_level = 0;

    size_t offstrategy_threshold = (mode == reshape_mode::strict) ? std::max(schema->min_compaction_threshold(), 4) : std::max(schema->max_compaction_threshold(), 32);
    size_t max_sstables = std::max(schema->max_compaction_threshold(), int(offstrategy_threshold));
    auto tolerance = [mode] (unsigned level) -> unsigned {
        if (mode == reshape_mode::strict) {
            return 0;
        }
        constexpr unsigned fan_out = leveled_manifest::leveled_fan_out;
        return std::max(double(fan_out), std::ceil(std::pow(fan_out, level) * 0.1));
    };

    // If there's only disjoint L0 sstables like on bootstrap, let's compact them all into a level L which has capacity to store the output.
    // The best possible level can be calculated with the formula: log (base fan_out) of (L0_total_bytes / max_sstable_size)
    auto [l0_disjoint, _] = is_disjoint(level_info[0], 0);
    if (mode == reshape_mode::strict && level_info[0].size() >= offstrategy_threshold && level_info[0].size() == input.size() && l0_disjoint) {
        auto log_fanout = [fanout = leveled_manifest::leveled_fan_out] (double x) {
            double inv_log_fanout = 1.0f / std::log(fanout);
            return log(x) * inv_log_fanout;
        };

        auto total_bytes = std::max(leveled_manifest::get_total_bytes(level_info[0]), uint64_t(max_sstable_size_in_bytes));
        unsigned ideal_level = std::ceil(log_fanout(total_bytes / max_sstable_size_in_bytes));

        leveled_manifest::logger.info("Reshaping {} disjoint sstables in level 0 into level {}", level_info[0].size(), ideal_level);
        compaction_descriptor desc(std::move(input), std::optional<sstables::sstable_set>(), iop, ideal_level, max_sstable_size_in_bytes);
        desc.options = compaction_options::make_reshape();
        return desc;
    }

    if (level_info[0].size() > offstrategy_threshold) {
        size_tiered_compaction_strategy stcs(_stcs_options);
        return stcs.get_reshaping_job(std::move(level_info[0]), schema, iop, mode);
    }

    for (unsigned level = leveled_manifest::MAX_LEVELS - 1; level > 0; --level) {
        if (level_info[level].empty()) {
            continue;
        }
        max_filled_level = std::max(max_filled_level, level);

        auto [disjoint, overlapping_sstables] = is_disjoint(level_info[level], tolerance(level));
        if (!disjoint) {
            leveled_manifest::logger.warn("Turns out that level {} is not disjoint, found {} overlapping SSTables, so compacting everything on behalf of {}.{}", level, overlapping_sstables, schema->ks_name(), schema->cf_name());
            // Unfortunately no good limit to limit input size to max_sstables for LCS major
            compaction_descriptor desc(std::move(input), std::optional<sstables::sstable_set>(), iop, max_filled_level, max_sstable_size_in_bytes);
            desc.options = compaction_options::make_reshape();
            return desc;
        }
    }

   return compaction_descriptor();
}

}
