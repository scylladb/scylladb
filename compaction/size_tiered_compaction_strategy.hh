/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "compaction_strategy_impl.hh"
#include "compaction.hh"
#include "sstables/shared_sstable.hh"
#include <boost/algorithm/cxx11/any_of.hpp>

class size_tiered_backlog_tracker;

namespace sstables {

class size_tiered_compaction_strategy_options {
public:
    static constexpr uint64_t DEFAULT_MIN_SSTABLE_SIZE = 50L * 1024L * 1024L;
    static constexpr double DEFAULT_BUCKET_LOW = 0.5;
    static constexpr double DEFAULT_BUCKET_HIGH = 1.5;
    static constexpr double DEFAULT_COLD_READS_TO_OMIT = 0.05;
    static constexpr auto MIN_SSTABLE_SIZE_KEY = "min_sstable_size";
    static constexpr auto BUCKET_LOW_KEY = "bucket_low";
    static constexpr auto BUCKET_HIGH_KEY = "bucket_high";
    static constexpr auto COLD_READS_TO_OMIT_KEY = "cold_reads_to_omit";
private:
    uint64_t min_sstable_size = DEFAULT_MIN_SSTABLE_SIZE;
    double bucket_low = DEFAULT_BUCKET_LOW;
    double bucket_high = DEFAULT_BUCKET_HIGH;
    double cold_reads_to_omit =  DEFAULT_COLD_READS_TO_OMIT;
public:
    size_tiered_compaction_strategy_options(const std::map<sstring, sstring>& options);

    size_tiered_compaction_strategy_options();
    size_tiered_compaction_strategy_options(const size_tiered_compaction_strategy_options&) = default;
    size_tiered_compaction_strategy_options(size_tiered_compaction_strategy_options&&) = default;
    size_tiered_compaction_strategy_options& operator=(const size_tiered_compaction_strategy_options&) = default;
    size_tiered_compaction_strategy_options& operator=(size_tiered_compaction_strategy_options&&) = default;

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
    static void validate(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options);

    friend class size_tiered_compaction_strategy;
};

class size_tiered_compaction_strategy : public compaction_strategy_impl {
    size_tiered_compaction_strategy_options _options;

    // Return a list of pair of shared_sstable and its respective size.
    static std::vector<std::pair<sstables::shared_sstable, uint64_t>> create_sstable_and_length_pairs(const std::vector<sstables::shared_sstable>& sstables);

    // Group files of similar size into buckets.
    static std::vector<std::vector<sstables::shared_sstable>> get_buckets(const std::vector<sstables::shared_sstable>& sstables, size_tiered_compaction_strategy_options options);

    std::vector<std::vector<sstables::shared_sstable>> get_buckets(const std::vector<sstables::shared_sstable>& sstables) const;

    // Maybe return a bucket of sstables to compact
    std::vector<sstables::shared_sstable>
    most_interesting_bucket(std::vector<std::vector<sstables::shared_sstable>> buckets, unsigned min_threshold, unsigned max_threshold);

    static bool is_bucket_interesting(const std::vector<sstables::shared_sstable>& bucket, int min_threshold) {
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

    virtual compaction_descriptor get_sstables_for_compaction(table_state& table_s, strategy_control& control, std::vector<sstables::shared_sstable> candidates) override;

    virtual std::vector<compaction_descriptor> get_cleanup_compaction_jobs(table_state& table_s, std::vector<shared_sstable> candidates) const override;

    static int64_t estimated_pending_compactions(const std::vector<sstables::shared_sstable>& sstables,
        int min_threshold, int max_threshold, size_tiered_compaction_strategy_options options);
    virtual int64_t estimated_pending_compactions(table_state& table_s) const override;

    virtual compaction_strategy_type type() const override {
        return compaction_strategy_type::size_tiered;
    }

    // Return the most interesting bucket for a set of sstables
    static std::vector<sstables::shared_sstable>
    most_interesting_bucket(const std::vector<sstables::shared_sstable>& candidates, int min_threshold, int max_threshold,
        size_tiered_compaction_strategy_options options = {});

    virtual std::unique_ptr<compaction_backlog_tracker::impl> make_backlog_tracker() const override;

    virtual compaction_descriptor get_reshaping_job(std::vector<shared_sstable> input, schema_ptr schema, reshape_mode mode) const override;

    friend class ::size_tiered_backlog_tracker;
};

}
