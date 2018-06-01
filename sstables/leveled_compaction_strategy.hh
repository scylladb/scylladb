/*
 * Copyright (C) 2017 ScyllaDB
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

#pragma once

#include "leveled_manifest.hh"

namespace sstables {

class leveled_compaction_strategy : public compaction_strategy_impl {
    static constexpr int32_t DEFAULT_MAX_SSTABLE_SIZE_IN_MB = 160;
    const sstring SSTABLE_SIZE_OPTION = "sstable_size_in_mb";

    int32_t _max_sstable_size_in_mb = DEFAULT_MAX_SSTABLE_SIZE_IN_MB;
    stdx::optional<std::vector<stdx::optional<dht::decorated_key>>> _last_compacted_keys;
    std::vector<int> _compaction_counter;
    size_tiered_compaction_strategy_options _stcs_options;
    compaction_backlog_tracker _backlog_tracker;
    int32_t calculate_max_sstable_size_in_mb(stdx::optional<sstring> option_value) const;
public:
    leveled_compaction_strategy(const std::map<sstring, sstring>& options);
    virtual compaction_descriptor get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) override;

    virtual std::vector<resharding_descriptor> get_resharding_jobs(column_family& cf, std::vector<shared_sstable> candidates) override;

    virtual void notify_completion(const std::vector<shared_sstable>& removed, const std::vector<shared_sstable>& added) override;

    // for each level > 0, get newest sstable and use its last key as last
    // compacted key for the previous level.
    void generate_last_compacted_keys(leveled_manifest& manifest);

    virtual int64_t estimated_pending_compactions(column_family& cf) const override;

    virtual bool parallel_compaction() const override {
        return false;
    }

    virtual compaction_strategy_type type() const {
        return compaction_strategy_type::leveled;
    }
    virtual std::unique_ptr<sstable_set_impl> make_sstable_set(schema_ptr schema) const override;

    virtual compaction_backlog_tracker& get_backlog_tracker() override {
        return _backlog_tracker;
    }
};

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
        return std::move(candidate);
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
        return sstables::compaction_descriptor({ sst }, sst->get_sstable_level());
    }
    return {};
}

std::vector<resharding_descriptor> leveled_compaction_strategy::get_resharding_jobs(column_family& cf, std::vector<shared_sstable> candidates) {
    leveled_manifest manifest = leveled_manifest::create(cf, candidates, _max_sstable_size_in_mb, _stcs_options);

    std::vector<resharding_descriptor> descriptors;
    shard_id target_shard = 0;
    auto get_shard = [&target_shard] { return target_shard++ % smp::count; };

    // Basically, we'll iterate through all levels, and for each, we'll sort the
    // sstables by first key because there's a need to reshard together adjacent
    // sstables.
    // The shard at which the job will run is chosen in a round-robin fashion.
    for (auto level = 0U; level <= manifest.get_level_count(); level++) {
        uint64_t max_sstable_size = !level ? std::numeric_limits<uint64_t>::max() : (_max_sstable_size_in_mb*1024*1024);
        auto& sstables = manifest.get_level(level);
        boost::sort(sstables, [] (auto& i, auto& j) {
            return i->compare_by_first_key(*j) < 0;
        });

        resharding_descriptor current_descriptor = resharding_descriptor{{}, max_sstable_size, get_shard(), level};

        for (auto it = sstables.begin(); it != sstables.end(); it++) {
            current_descriptor.sstables.push_back(*it);

            auto next = std::next(it);
            if (current_descriptor.sstables.size() == smp::count || next == sstables.end()) {
                descriptors.push_back(std::move(current_descriptor));
                current_descriptor = resharding_descriptor{{}, max_sstable_size, get_shard(), level};
            }
        }
    }
    return descriptors;
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
    for (auto& candidate : added) {
        if (!last || last->compare_by_first_key(*candidate) < 0) {
            last = &*candidate;
        }
    }
    _last_compacted_keys.value().at(min_level) = last->get_last_decorated_key();
}

void leveled_compaction_strategy::generate_last_compacted_keys(leveled_manifest& manifest) {
    std::vector<stdx::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
    for (auto i = 0; i < leveled_manifest::MAX_LEVELS - 1; i++) {
        if (manifest.get_level(i + 1).empty()) {
            continue;
        }

        const sstables::sstable* sstable_with_last_compacted_key = nullptr;
        stdx::optional<db_clock::time_point> max_creation_time;
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
    for (auto& entry : *cf.get_sstables()) {
        sstables.push_back(entry);
    }
    leveled_manifest manifest = leveled_manifest::create(cf, sstables, _max_sstable_size_in_mb, _stcs_options);
    return manifest.get_estimated_tasks();
}

}
