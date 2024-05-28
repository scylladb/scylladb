/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "compaction_backlog_manager.hh"
#include "compaction_strategy.hh"
#include "db_clock.hh"
#include "compaction_descriptor.hh"

namespace sstables {

class sstable_set_impl;

class compaction_strategy_impl {
public:
    static constexpr float DEFAULT_TOMBSTONE_THRESHOLD = 0.2f;
    // minimum interval needed to perform tombstone removal compaction in seconds, default 86400 or 1 day.
    static constexpr std::chrono::seconds DEFAULT_TOMBSTONE_COMPACTION_INTERVAL() { return std::chrono::seconds(86400); }
    static constexpr auto DEFAULT_UNCHECKED_TOMBSTONE_COMPACTION = false;
    static constexpr auto TOMBSTONE_THRESHOLD_OPTION = "tombstone_threshold";
    static constexpr auto TOMBSTONE_COMPACTION_INTERVAL_OPTION = "tombstone_compaction_interval";
    static constexpr auto UNCHECKED_TOMBSTONE_COMPACTION_OPTION = "unchecked_tombstone_compaction";
protected:
    bool _use_clustering_key_filter = false;
    bool _disable_tombstone_compaction = false;
    float _tombstone_threshold = DEFAULT_TOMBSTONE_THRESHOLD;
    db_clock::duration _tombstone_compaction_interval = DEFAULT_TOMBSTONE_COMPACTION_INTERVAL();
    bool _unchecked_tombstone_compaction = DEFAULT_UNCHECKED_TOMBSTONE_COMPACTION;
public:
    static std::optional<sstring> get_value(const std::map<sstring, sstring>& options, const sstring& name);
    static void validate_min_max_threshold(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options);
    static void validate_options_for_strategy_type(const std::map<sstring, sstring>& options, sstables::compaction_strategy_type type);
protected:
    static void validate_options(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options);
    compaction_strategy_impl() = default;
    explicit compaction_strategy_impl(const std::map<sstring, sstring>& options);
    static compaction_descriptor make_major_compaction_job(std::vector<sstables::shared_sstable> candidates,
            int level = compaction_descriptor::default_level,
            uint64_t max_sstable_bytes = compaction_descriptor::default_max_sstable_bytes);
public:
    virtual ~compaction_strategy_impl() {}
    virtual compaction_descriptor get_sstables_for_compaction(table_state& table_s, strategy_control& control) = 0;
    virtual compaction_descriptor get_major_compaction_job(table_state& table_s, std::vector<sstables::shared_sstable> candidates) {
        return make_major_compaction_job(std::move(candidates));
    }
    virtual std::vector<compaction_descriptor> get_cleanup_compaction_jobs(table_state& table_s, std::vector<shared_sstable> candidates) const;
    virtual void notify_completion(table_state& table_s, const std::vector<shared_sstable>& removed, const std::vector<shared_sstable>& added) { }
    virtual compaction_strategy_type type() const = 0;
    virtual bool parallel_compaction() const {
        return true;
    }
    virtual int64_t estimated_pending_compactions(table_state& table_s) const = 0;
    virtual std::unique_ptr<sstable_set_impl> make_sstable_set(schema_ptr schema) const;

    bool use_clustering_key_filter() const {
        return _use_clustering_key_filter;
    }

    // Check if a given sstable is entitled for tombstone compaction based on its
    // droppable tombstone histogram and gc_before.
    bool worth_dropping_tombstones(const shared_sstable& sst, gc_clock::time_point compaction_time, const table_state& t);

    virtual std::unique_ptr<compaction_backlog_tracker::impl> make_backlog_tracker() const = 0;

    virtual uint64_t adjust_partition_estimate(const mutation_source_metadata& ms_meta, uint64_t partition_estimate, schema_ptr schema) const;

    virtual reader_consumer_v2 make_interposer_consumer(const mutation_source_metadata& ms_meta, reader_consumer_v2 end_consumer) const;

    virtual bool use_interposer_consumer() const {
        return false;
    }

    virtual compaction_descriptor get_reshaping_job(std::vector<shared_sstable> input, schema_ptr schema, reshape_config cfg) const;
};
}
