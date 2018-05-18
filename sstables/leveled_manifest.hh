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

/*
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "sstables.hh"
#include "compaction.hh"
#include "size_tiered_compaction_strategy.hh"
#include "range.hh"
#include "log.hh"
#include <boost/range/algorithm/partial_sort.hpp>

class leveled_manifest {
    schema_ptr _schema;
    std::vector<std::vector<sstables::shared_sstable>> _generations;
    uint64_t _max_sstable_size_in_bytes;
    const sstables::size_tiered_compaction_strategy_options& _stcs_options;

    struct candidates_info {
        std::vector<sstables::shared_sstable> candidates;
        bool can_promote = true;
    };
public:
    static logging::logger logger;

    /**
     * limit the number of L0 sstables we do at once, because compaction bloom filter creation
     * uses a pessimistic estimate of how many keys overlap (none), so we risk wasting memory
     * or even OOMing when compacting highly overlapping sstables
     */
    static constexpr int MAX_COMPACTING_L0 = 32;
    /**
     * If we go this many rounds without compacting
     * in the highest level, we start bringing in sstables from
     * that level into lower level compactions
     */
    static constexpr int NO_COMPACTION_LIMIT = 25;

    static constexpr int MAX_LEVELS = 9; // log10(1000^3);

    static constexpr unsigned leveled_fan_out = 10;
    // Lowest score (score is about how much data a level contains vs its ideal amount) for a
    // level to be considered worth compacting.
    static constexpr float TARGET_SCORE = 1.001f;
private:
    leveled_manifest(column_family& cfs, int max_sstable_size_in_MB, const sstables::size_tiered_compaction_strategy_options& stcs_options)
        : _schema(cfs.schema())
        , _max_sstable_size_in_bytes(max_sstable_size_in_MB * 1024 * 1024)
        , _stcs_options(stcs_options)
    {
        // allocate enough generations for a PB of data, with a 1-MB sstable size.  (Note that if maxSSTableSize is
        // updated, we will still have sstables of the older, potentially smaller size.  So don't make this
        // dependent on maxSSTableSize.)
        _generations.resize(MAX_LEVELS);
    }
public:
    static leveled_manifest create(column_family& cf, std::vector<sstables::shared_sstable>& sstables, int max_sstable_size_in_mb,
            const sstables::size_tiered_compaction_strategy_options& stcs_options) {
        leveled_manifest manifest = leveled_manifest(cf, max_sstable_size_in_mb, stcs_options);

        // ensure all SSTables are in the manifest
        // FIXME: there can be tens of thousands of sstables. we can avoid this potentially expensive procedure if
        // partitioned_sstable_set keeps track of a list for each level.
        for (auto& sstable : sstables) {
            uint32_t level = sstable->get_sstable_level();
            if (level >= manifest._generations.size()) {
                throw std::runtime_error(sprint("Invalid level %u out of %ld", level, (manifest._generations.size() - 1)));
            }
            logger.debug("Adding {} to L{}", sstable->get_filename(), level);
            manifest._generations[level].push_back(sstable);
        }

        return manifest;
    }

    // Return first set of overlapping sstables for a given level.
    // Assumes _generations[level] is already sorted by first key.
    std::vector<sstables::shared_sstable> overlapping_sstables(int level) const {
        const schema& s = *_schema;
        std::unordered_set<sstables::shared_sstable> result;
        stdx::optional<sstables::shared_sstable> previous;
        stdx::optional<dht::decorated_key> last; // keeps track of highest last key in result.

        for (auto& current : _generations[level]) {
            auto current_first = current->get_first_decorated_key();
            auto current_last = current->get_last_decorated_key();

            if (previous && current_first.tri_compare(s, (*previous)->get_last_decorated_key()) <= 0) {
                result.insert(*previous);
                result.insert(current);
            } else if (last && current_first.tri_compare(s, *last) <= 0) {
                // current may also overlap on some sstable other than the previous one, if there's
                // a large token span sstable that comes previously.
                result.insert(current);
            } else if (!result.empty()) {
                // first overlapping set is returned when current doesn't overlap with it
                break;
            }

            if (!last || current_last.tri_compare(s, *last) > 0) {
                last = std::move(current_last);
            }
            previous = current;
        }
        return std::vector<sstables::shared_sstable>(result.begin(), result.end());
    }

    static uint64_t max_bytes_for_level(int level, uint64_t max_sstable_size_in_bytes) {
        if (level == 0) {
            return 4L * max_sstable_size_in_bytes;
        }
        double bytes = pow(leveled_fan_out, level) * max_sstable_size_in_bytes;
        if (bytes > std::numeric_limits<int64_t>::max()) {
            throw std::runtime_error(sprint("At most %ld bytes may be in a compaction level; your maxSSTableSize must be absurdly high to compute %f", 
                std::numeric_limits<int64_t>::max(), bytes));
        }
        uint64_t bytes_u64 = bytes;
        return bytes_u64;
    }

    uint64_t max_bytes_for_level(int level) const {
        return max_bytes_for_level(level, _max_sstable_size_in_bytes);
    }


    sstables::compaction_descriptor get_descriptor_for_level(int level, const std::vector<stdx::optional<dht::decorated_key>>& last_compacted_keys,
                                                             std::vector<int>& compaction_counter) {
        auto info = get_candidates_for(level, last_compacted_keys);
        if (!info.candidates.empty()) {
            int next_level = get_next_level(info.candidates, info.can_promote);

            if (info.can_promote) {
                info.candidates = get_overlapping_starved_sstables(next_level, std::move(info.candidates), compaction_counter);
            }
            return sstables::compaction_descriptor(std::move(info.candidates), next_level, _max_sstable_size_in_bytes);
        } else {
            logger.debug("No compaction candidates for L{}", level);
            return sstables::compaction_descriptor();
        }
    }

    /**
     * @return highest-priority sstables to compact, and level to compact them to
     * If no compactions are necessary, will return null
     */
    sstables::compaction_descriptor get_compaction_candidates(const std::vector<stdx::optional<dht::decorated_key>>& last_compacted_keys,
        std::vector<int>& compaction_counter) {
#if 0
        // during bootstrap we only do size tiering in L0 to make sure
        // the streamed files can be placed in their original levels
        if (StorageService.instance.isBootstrapMode())
        {
            List<SSTableReader> mostInteresting = getSSTablesForSTCS(getLevel(0));
            if (!mostInteresting.isEmpty())
            {
                logger.info("Bootstrapping - doing STCS in L0");
                return new CompactionCandidate(mostInteresting, 0, Long.MAX_VALUE);
            }
            return null;
        }
#endif
        // LevelDB gives each level a score of how much data it contains vs its ideal amount, and
        // compacts the level with the highest score. But this falls apart spectacularly once you
        // get behind.  Consider this set of levels:
        // L0: 988 [ideal: 4]
        // L1: 117 [ideal: 10]
        // L2: 12  [ideal: 100]
        //
        // The problem is that L0 has a much higher score (almost 250) than L1 (11), so what we'll
        // do is compact a batch of MAX_COMPACTING_L0 sstables with all 117 L1 sstables, and put the
        // result (say, 120 sstables) in L1. Then we'll compact the next batch of MAX_COMPACTING_L0,
        // and so forth.  So we spend most of our i/o rewriting the L1 data with each batch.
        //
        // If we could just do *all* L0 a single time with L1, that would be ideal.  But we can't
        // -- see the javadoc for MAX_COMPACTING_L0.
        //
        // LevelDB's way around this is to simply block writes if L0 compaction falls behind.
        // We don't have that luxury.
        //
        // So instead, we
        // 1) force compacting higher levels first, which minimizes the i/o needed to compact
        //    optimially which gives us a long term win, and
        // 2) if L0 falls behind, we will size-tiered compact it to reduce read overhead until
        //    we can catch up on the higher levels.
        //
        // This isn't a magic wand -- if you are consistently writing too fast for LCS to keep
        // up, you're still screwed.  But if instead you have intermittent bursts of activity,
        // it can help a lot.
        for (auto i = _generations.size() - 1; i > 0; i--) {
            auto& sstables = get_level(i);
            if (sstables.empty()) {
                continue; // mostly this just avoids polluting the debug log with zero scores
            }
#if 0
            // we want to calculate score excluding compacting ones
            Set<SSTableReader> sstablesInLevel = Sets.newHashSet(sstables);
            Set<SSTableReader> remaining = Sets.difference(sstablesInLevel, cfs.getDataTracker().getCompacting());
#endif
            double score = (double) get_total_bytes(sstables) / (double) max_bytes_for_level(i);

            logger.debug("Compaction score for level {} is {}", i, score);

            if (score <= TARGET_SCORE) {
                continue;
            }
            // before proceeding with a higher level, let's see if L0 is far enough behind to warrant STCS
            // TODO: we shouldn't proceed with size tiered strategy if cassandra.disable_stcs_in_l0 is true.
            if (get_level_size(0) > MAX_COMPACTING_L0) {
                auto most_interesting = sstables::size_tiered_compaction_strategy::most_interesting_bucket(get_level(0),
                    _schema->min_compaction_threshold(), _schema->max_compaction_threshold(), _stcs_options);
                if (!most_interesting.empty()) {
                    logger.debug("L0 is too far behind, performing size-tiering there first");
                    return sstables::compaction_descriptor(std::move(most_interesting));
                }
            }
            auto descriptor = get_descriptor_for_level(i, last_compacted_keys, compaction_counter);
            if (descriptor.sstables.size() > 0) {
                return descriptor;
            }
        }

        // Higher levels are happy, time for a standard, non-STCS L0 compaction
        if (!get_level(0).empty()) {
            auto info = get_candidates_for(0, last_compacted_keys);
            if (!info.candidates.empty()) {
                auto next_level = get_next_level(info.candidates, info.can_promote);
                return sstables::compaction_descriptor(std::move(info.candidates), next_level, _max_sstable_size_in_bytes);
            }
        }

        for (size_t i = _generations.size() - 1; i > 0; --i) {
            auto& sstables = get_level(i);
            if (sstables.empty()) {
                continue;
            }
            auto& sstables_prev_level = get_level(i-1);
            if (sstables_prev_level.empty()) {
                continue;
            }
            auto descriptor = get_descriptor_for_level(i-1, last_compacted_keys, compaction_counter);
            if (descriptor.sstables.size() > 0) {
                return descriptor;
            }
        }
        return sstables::compaction_descriptor();
    }
private:
    /**
     * If we do something that makes many levels contain too little data (cleanup, change sstable size) we will "never"
     * compact the high levels.
     *
     * This method finds if we have gone many compaction rounds without doing any high-level compaction, if so
     * we start bringing in one sstable from the highest level until that level is either empty or is doing compaction.
     *
     * @param targetLevel the level the candidates will be compacted into
     * @param candidates the original sstables to compact
     * @return
     */
    std::vector<sstables::shared_sstable>
    get_overlapping_starved_sstables(int target_level, std::vector<sstables::shared_sstable>&& candidates, std::vector<int>& compaction_counter) {
        for (int i = _generations.size() - 1; i > 0; i--) {
            compaction_counter[i]++;
        }
        compaction_counter[target_level] = 0;

        if (logger.level() == logging::log_level::debug) {
            for (auto j = 0U; j < compaction_counter.size(); j++) {
                logger.debug("CompactionCounter: {}: {}", j, compaction_counter[j]);
            }
        }

        for (int i = _generations.size() - 1; i > target_level; i--) {
            if (!get_level_size(i) || compaction_counter[i] <= NO_COMPACTION_LIMIT) {
                continue;
            }
            // we try to find an sstable that is fully contained within  the boundaries we are compacting;
            // say we are compacting 3 sstables: 0->30 in L1 and 0->12, 12->33 in L2
            // this means that we will not create overlap in L2 if we add an sstable
            // contained within 0 -> 33 to the compaction
            stdx::optional<dht::decorated_key> max;
            stdx::optional<dht::decorated_key> min;
            for (auto& candidate : candidates) {
                auto& candidate_first = candidate->get_first_decorated_key();
                if (!min || candidate_first.tri_compare(*_schema, *min) < 0) {
                    min = candidate_first;
                }
                auto& candidate_last = candidate->get_last_decorated_key();
                if (!max || candidate_last.tri_compare(*_schema, *max) > 0) {
                    max = candidate_last;
                }
            }
#if 0
            // NOTE: We don't need to filter out compacting sstables by now because strategy only deals with
            // uncompacting sstables and parallel compaction is also disabled for lcs.
            Set<SSTableReader> compacting = cfs.getDataTracker().getCompacting();
#endif
            auto boundaries = ::range<dht::decorated_key>::make(*min, *max);
            for (auto& sstable : get_level(i)) {
                auto r = ::range<dht::decorated_key>::make(sstable->get_first_decorated_key(), sstable->get_last_decorated_key());
                if (boundaries.contains(r, dht::ring_position_comparator(*_schema))) {
                    logger.info("Adding high-level (L{}) {} to candidates", sstable->get_sstable_level(), sstable->get_filename());
                    candidates.push_back(sstable);
                    break;
                }
            }
            return candidates;
        }

        return candidates;
    }
public:
    size_t get_level_size(uint32_t level) {
        return get_level(level).size();
    }

    template <typename T>
    static std::vector<sstables::shared_sstable> overlapping(const schema& s, const std::vector<sstables::shared_sstable>& candidates, const T& others) {
        assert(!candidates.empty());
        /*
         * Picking each sstable from others that overlap one of the sstable of candidates is not enough
         * because you could have the following situation:
         *   candidates = [ s1(a, c), s2(m, z) ]
         *   others = [ s3(e, g) ]
         * In that case, s2 overlaps none of s1 or s2, but if we compact s1 with s2, the resulting sstable will
         * overlap s3, so we must return s3.
         *
         * Thus, the correct approach is to pick sstables overlapping anything between the first key in all
         * the candidate sstables, and the last.
         */
        auto it = candidates.begin();
        auto& first_sstable = *it;
        it++;
        dht::token first = first_sstable->get_first_decorated_key()._token;
        dht::token last = first_sstable->get_last_decorated_key()._token;
        while (it != candidates.end()) {
            auto& candidate_sstable = *it;
            it++;
            dht::token first_candidate = candidate_sstable->get_first_decorated_key()._token;
            dht::token last_candidate = candidate_sstable->get_last_decorated_key()._token;

            first = first <= first_candidate? first : first_candidate;
            last = last >= last_candidate ? last : last_candidate;
        }
        return overlapping(s, first, last, others);
    }

    template <typename T>
    static std::vector<sstables::shared_sstable> overlapping(const schema& s, const sstables::shared_sstable& sstable, const T& others) {
        return overlapping(s, sstable->get_first_decorated_key()._token, sstable->get_last_decorated_key()._token, others);
    }

    /**
     * @return sstables from @param sstables that contain keys between @param start and @param end, inclusive.
     */
    template <typename T>
    static std::vector<sstables::shared_sstable> overlapping(const schema& s, dht::token start, dht::token end, const T& sstables) {
        assert(start <= end);

        std::vector<sstables::shared_sstable> overlapped;
        auto range = ::range<dht::token>::make(start, end);

        for (auto& candidate : sstables) {
            auto candidate_range = ::range<dht::token>::make(candidate->get_first_decorated_key()._token, candidate->get_last_decorated_key()._token);

            if (range.overlaps(candidate_range, dht::token_comparator())) {
                overlapped.push_back(candidate);
            }
        }
        return overlapped;
    }

    bool worth_promoting_L0_candidates(const std::vector<sstables::shared_sstable>& candidates) const {
        return get_total_bytes(candidates) >= _max_sstable_size_in_bytes;
    }
private:
    candidates_info candidates_for_level_0_compaction() {
        // L0 is the dumping ground for new sstables which thus may overlap each other.
        //
        // We treat L0 compactions specially:
        // 1a. add L0 sstables as candidates
        // 1b. prefer choosing older sstables as candidates, to newer ones
        // 2. At most MAX_COMPACTING_L0 sstables from L0 will be compacted at once
        // 3. If total candidate size is less than maxSSTableSizeInMB, we won't bother compacting with L1,
        //    and the result of the compaction will stay in L0 instead of being promoted
        std::vector<sstables::shared_sstable> candidates;

        // leave everything in L0 if we didn't end up with a full sstable's worth of data
        bool can_promote = false;
        if (worth_promoting_L0_candidates(get_level(0))) {
            candidates = get_level(0);
            if (candidates.size() > MAX_COMPACTING_L0) {
                // limit to only the MAX_COMPACTING_L0 oldest candidates
                boost::partial_sort(candidates, candidates.begin() + MAX_COMPACTING_L0, [] (auto& i, auto& j) {
                    return i->compare_by_max_timestamp(*j) < 0;
                });
                candidates.resize(MAX_COMPACTING_L0);
            }
            // add sstables from L1 that overlap candidates
            auto l1overlapping = overlapping(*_schema, candidates, get_level(1));
            candidates.insert(candidates.end(), l1overlapping.begin(), l1overlapping.end());
            can_promote = true;
        } else {
            // do STCS in L0 when max_sstable_size is high compared to size of new sstables, so we'll
            // avoid quadratic behavior until L0 is worth promoting.
            candidates = sstables::size_tiered_compaction_strategy::most_interesting_bucket(get_level(0),
                _schema->min_compaction_threshold(), _schema->max_compaction_threshold(), _stcs_options);
        }
        return { std::move(candidates), can_promote };
    }

    // LCS uses a round-robin heuristic for even distribution of keys in each level.
    // FIXME: come up with a general fix instead of this heuristic which potentially has weak points. For example,
    //  it may be vulnerable to clients that perform operations by scanning the token range.
    static int sstable_index_based_on_last_compacted_key(const std::vector<sstables::shared_sstable>& sstables, int level,
            const schema& s, const std::vector<stdx::optional<dht::decorated_key>>& last_compacted_keys) {
        int start = 0; // handles case where the prior compaction touched the very last range
        int idx = 0;
        for (auto& sstable : sstables) {
            if (uint32_t(level) >= last_compacted_keys.size()) {
                throw std::runtime_error(sprint("Invalid level %u out of %ld", level, (last_compacted_keys.size() - 1)));
            }
            auto& sstable_first = sstable->get_first_decorated_key();
            if (!last_compacted_keys[level] || sstable_first.tri_compare(s, *last_compacted_keys[level]) > 0) {
                start = idx;
                break;
            }
            idx++;
        }
        return start;
    }

    candidates_info candidates_for_higher_levels_compaction(int level, const std::vector<stdx::optional<dht::decorated_key>>& last_compacted_keys) {
        const schema& s = *_schema;
        // for non-L0 compactions, pick up where we left off last time
        auto& sstables = get_level(level);
        boost::sort(sstables, [&s] (auto& i, auto& j) {
            return i->compare_by_first_key(*j) < 0;
        });

        // Restore invariant for current level, when a large token spanning sstable finds its
        // way into a level higher than 0, due to resharding or refresh, by compacting first
        // set of overlapping sstables. It means more than one compaction may be required for
        // invariant to be restored.
        auto overlapping_current_level = overlapping_sstables(level);
        if (!overlapping_current_level.empty()) {
            logger.info("Leveled compaction strategy is restoring invariant of level {} by compacting {} sstables on behalf of {}.{}",
                level, overlapping_current_level.size(), s.ks_name(), s.cf_name());
            return { overlapping_current_level, false };
        }

        int start = sstable_index_based_on_last_compacted_key(sstables, level, s, last_compacted_keys);

        auto pos = start % sstables.size();
        auto candidates = overlapping(*_schema, sstables.at(pos), get_level(level + 1));
        candidates.push_back(sstables.at(pos));

        return { candidates, true };
    }

    /**
     * @return highest-priority sstables to compact for the given level.
     * If no compactions are possible (because of concurrent compactions or because some sstables are blacklisted
     * for prior failure), will return an empty list.  Never returns null.
     */
    candidates_info get_candidates_for(int level, const std::vector<stdx::optional<dht::decorated_key>>& last_compacted_keys) {
        assert(!get_level(level).empty());

        logger.debug("Choosing candidates for L{}", level);

        if (level == 0) {
            return candidates_for_level_0_compaction();
        }
        return candidates_for_higher_levels_compaction(level, last_compacted_keys);
    }
public:
    uint32_t get_level_count() const {
        for (int i = _generations.size() - 1; i >= 0; i--) {
            if (_generations[i].size() > 0) {
                return i;
            }
        }
        return 0;
    }

    std::vector<sstables::shared_sstable>& get_level(uint32_t level) {
        if (level >= _generations.size()) {
            throw std::runtime_error("Invalid level");
        }
        return _generations[level];
    }

    int64_t get_estimated_tasks() const {
        int64_t tasks = 0;

        for (int i = static_cast<int>(_generations.size()) - 1; i >= 0; i--) {
            const auto& sstables = _generations[i];
            uint64_t total_bytes_for_this_level = get_total_bytes(sstables);
            uint64_t max_bytes_for_this_level = max_bytes_for_level(i);

            if (total_bytes_for_this_level < max_bytes_for_this_level) {
                continue;
            }
            // If there is 1 byte over TBL - (MBL * 1.001), there is still a task left, so we need to round up.
            tasks += std::ceil(float(total_bytes_for_this_level - max_bytes_for_this_level*TARGET_SCORE) / _max_sstable_size_in_bytes);
        }
        return tasks;
    }

    static int get_next_level(const std::vector<sstables::shared_sstable>& sstables, bool can_promote = true) {
        int maximum_level = std::numeric_limits<int>::min();
        int minimum_level = std::numeric_limits<int>::max();

        for (auto& sstable : sstables) {
            int sstable_level = sstable->get_sstable_level();
            maximum_level = std::max(sstable_level, maximum_level);
            minimum_level = std::min(sstable_level, minimum_level);
        }

        int new_level;
        if (minimum_level == 0 && minimum_level == maximum_level && !can_promote) {
            new_level = 0;
        } else {
            new_level = (minimum_level == maximum_level && can_promote) ? maximum_level + 1 : maximum_level;
            assert(new_level > 0);
        }
        return new_level;
    }

    template <typename T>
    static uint64_t get_total_bytes(const T& sstables) {
        uint64_t sum = 0;
        for (auto& sstable : sstables) {
            sum += sstable->ondisk_data_size();
        }
        return sum;
    }
};
