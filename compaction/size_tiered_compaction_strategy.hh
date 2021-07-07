/*
 * Copyright (C) 2017-present ScyllaDB
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

#include "compaction_strategy_impl.hh"
#include "compaction.hh"
#include "sstables/sstables.hh"
#include "database_fwd.hh"
#include <boost/algorithm/cxx11/any_of.hpp>

namespace sstables {

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
public:
    size_tiered_compaction_strategy_options(const std::map<sstring, sstring>& options);

    size_tiered_compaction_strategy_options();

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
    compaction_backlog_tracker _backlog_tracker;

    // Return a list of pair of shared_sstable and its respective size.
    static std::vector<std::pair<sstables::shared_sstable, uint64_t>> create_sstable_and_length_pairs(const std::vector<sstables::shared_sstable>& sstables);

    // Group files of similar size into buckets.
    static std::vector<std::vector<sstables::shared_sstable>> get_buckets(const std::vector<sstables::shared_sstable>& sstables, size_tiered_compaction_strategy_options options);

    std::vector<std::vector<sstables::shared_sstable>> get_buckets(const std::vector<sstables::shared_sstable>& sstables) const;

    // Maybe return a bucket of sstables to compact
    std::vector<sstables::shared_sstable>
    most_interesting_bucket(std::vector<std::vector<sstables::shared_sstable>> buckets, unsigned min_threshold, unsigned max_threshold);

    // Return the average size of a given list of sstables.
    uint64_t avg_size(std::vector<sstables::shared_sstable> const& sstables) const {
        assert(sstables.size() > 0); // this should never fail
        uint64_t n = 0;

        for (auto const& sstable : sstables) {
            n += sstable->data_size();
        }

        return n / sstables.size();
    }

    bool is_bucket_interesting(const std::vector<sstables::shared_sstable>& bucket, int min_threshold) const {
        return bucket.size() >= size_t(min_threshold);
    }

    bool is_any_bucket_interesting(const std::vector<std::vector<sstables::shared_sstable>>& buckets, int min_threshold) const {
        return boost::algorithm::any_of(buckets, [&] (const auto& bucket) {
            return this->is_bucket_interesting(bucket, min_threshold);
        });
    }
public:
    size_tiered_compaction_strategy() = default;

    size_tiered_compaction_strategy(const std::map<sstring, sstring>& options);
    explicit size_tiered_compaction_strategy(const size_tiered_compaction_strategy_options& options);

    virtual compaction_descriptor get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) override;

    static int64_t estimated_pending_compactions(const std::vector<sstables::shared_sstable>& sstables,
        int min_threshold, int max_threshold, size_tiered_compaction_strategy_options options);
    virtual int64_t estimated_pending_compactions(column_family& cf) const override;

    virtual compaction_strategy_type type() const {
        return compaction_strategy_type::size_tiered;
    }

    // Return the most interesting bucket for a set of sstables
    static std::vector<sstables::shared_sstable>
    most_interesting_bucket(const std::vector<sstables::shared_sstable>& candidates, int min_threshold, int max_threshold,
        size_tiered_compaction_strategy_options options = {});

    virtual compaction_backlog_tracker& get_backlog_tracker() override {
        return _backlog_tracker;
    }

    virtual compaction_descriptor get_reshaping_job(std::vector<shared_sstable> input, schema_ptr schema, const ::io_priority_class& iop, reshape_mode mode) override;

};

}
