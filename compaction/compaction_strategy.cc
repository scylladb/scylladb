/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

/*
 */

#include <vector>
#include <chrono>
#include <fmt/ranges.h>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/on_internal_error.hh>
#include "sstables/shared_sstable.hh"
#include "sstables/sstables.hh"
#include "compaction_strategy.hh"
#include "compaction_strategy_impl.hh"
#include "compaction_strategy_state.hh"
#include "cql3/statements/property_definitions.hh"
#include "schema/schema.hh"
#include "leveled_compaction_strategy.hh"
#include "time_window_compaction_strategy.hh"
#include "backlog_controller.hh"
#include "compaction_backlog_manager.hh"
#include "leveled_manifest.hh"
#include "utils/to_string.hh"
#include "incremental_compaction_strategy.hh"
#include "incremental_backlog_tracker.hh"
#include "sstables/sstable_set_impl.hh"

namespace compaction {

logging::logger leveled_manifest::logger("LeveledManifest");
logging::logger compaction_strategy_logger("CompactionStrategy");

using timestamp_type = api::timestamp_type;

compaction_descriptor compaction_strategy_impl::make_major_compaction_job(std::vector<sstables::shared_sstable> candidates, int level, uint64_t max_sstable_bytes) {
    // run major compaction in maintenance priority
    return compaction_descriptor(std::move(candidates), level, max_sstable_bytes, sstables::run_id::create_random_id(), compaction_type_options::make_major());
}

std::vector<compaction_descriptor> compaction_strategy_impl::get_cleanup_compaction_jobs(compaction_group_view& table_s, std::vector<sstables::shared_sstable> candidates) const {
    // The default implementation is suboptimal and causes the writeamp problem described issue in #10097.
    // The compaction strategy relying on it should strive to implement its own method, to make cleanup bucket aware.
    return candidates | std::views::transform([] (const sstables::shared_sstable& sst) {
        return compaction_descriptor({ sst },
            sst->get_sstable_level(), compaction_descriptor::default_max_sstable_bytes, sst->run_identifier());
    }) | std::ranges::to<std::vector>();
}

std::unique_ptr<sstables::sstable_set_impl>
compaction_strategy_impl::make_sstable_set(const compaction_group_view& ts) const {
    return std::make_unique<sstables::partitioned_sstable_set>(ts.schema());
}

bool compaction_strategy_impl::worth_dropping_tombstones(const sstables::shared_sstable& sst, gc_clock::time_point compaction_time, const compaction_group_view& t) {
    if (_disable_tombstone_compaction) {
        return false;
    }
    // ignore sstables that were created just recently because there's a chance
    // that expired tombstones still cover old data and thus cannot be removed.
    // We want to avoid a compaction loop here on the same data by considering
    // only old enough sstables.
    if (db_clock::now()-_tombstone_compaction_interval < sst->data_file_write_time()) {
        return false;
    }
    if (_unchecked_tombstone_compaction) {
        return true;
    }
    auto droppable_ratio = sst->estimate_droppable_tombstone_ratio(compaction_time, t.get_tombstone_gc_state(), t.schema());
    return droppable_ratio >= _tombstone_threshold;
}

uint64_t compaction_strategy_impl::adjust_partition_estimate(const mutation_source_metadata& ms_meta, uint64_t partition_estimate, schema_ptr schema) const {
    return partition_estimate;
}

mutation_reader_consumer compaction_strategy_impl::make_interposer_consumer(const mutation_source_metadata& ms_meta, mutation_reader_consumer end_consumer) const {
    return end_consumer;
}

compaction_descriptor
compaction_strategy_impl::get_reshaping_job(std::vector<sstables::shared_sstable> input, schema_ptr schema, reshape_config cfg) const {
    return compaction_descriptor();
}

std::optional<sstring> compaction_strategy_impl::get_value(const std::map<sstring, sstring>& options, const sstring& name) {
    auto it = options.find(name);
    if (it == options.end()) {
        return std::nullopt;
    }
    return it->second;
}

void compaction_strategy_impl::validate_min_max_threshold(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options) {
    auto min_threshold_key = "min_threshold", max_threshold_key = "max_threshold";

    auto tmp_value = compaction_strategy_impl::get_value(options, min_threshold_key);
    auto min_threshold = cql3::statements::property_definitions::to_long(min_threshold_key, tmp_value, DEFAULT_MIN_COMPACTION_THRESHOLD);
    if (min_threshold < 2) {
        throw exceptions::configuration_exception(fmt::format("{} value ({}) must be bigger or equal to 2", min_threshold_key, min_threshold));
    }

    tmp_value = compaction_strategy_impl::get_value(options, max_threshold_key);
    auto max_threshold = cql3::statements::property_definitions::to_long(max_threshold_key, tmp_value, DEFAULT_MAX_COMPACTION_THRESHOLD);
    if (max_threshold < 2) {
        throw exceptions::configuration_exception(fmt::format("{} value ({}) must be bigger or equal to 2", max_threshold_key, max_threshold));
    }

    unchecked_options.erase(min_threshold_key);
    unchecked_options.erase(max_threshold_key);
}

static double validate_tombstone_threshold(const std::map<sstring, sstring>& options) {
    auto tmp_value = compaction_strategy_impl::get_value(options, compaction_strategy_impl::TOMBSTONE_THRESHOLD_OPTION);
    auto tombstone_threshold = cql3::statements::property_definitions::to_double(compaction_strategy_impl::TOMBSTONE_THRESHOLD_OPTION, tmp_value, compaction_strategy_impl::DEFAULT_TOMBSTONE_THRESHOLD);
    if (tombstone_threshold < 0.0 || tombstone_threshold > 1.0) {
        throw exceptions::configuration_exception(fmt::format("{} value ({}) must be between 0.0 and 1.0", compaction_strategy_impl::TOMBSTONE_THRESHOLD_OPTION, tombstone_threshold));
    }
    return tombstone_threshold;
}

static double validate_tombstone_threshold(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options) {
    auto tombstone_threshold = validate_tombstone_threshold(options);
    unchecked_options.erase(compaction_strategy_impl::TOMBSTONE_THRESHOLD_OPTION);
    return tombstone_threshold;
}

static db_clock::duration validate_tombstone_compaction_interval(const std::map<sstring, sstring>& options) {
    auto tmp_value = compaction_strategy_impl::get_value(options, compaction_strategy_impl::TOMBSTONE_COMPACTION_INTERVAL_OPTION);
    auto interval = cql3::statements::property_definitions::to_long(compaction_strategy_impl::TOMBSTONE_COMPACTION_INTERVAL_OPTION, tmp_value, compaction_strategy_impl::DEFAULT_TOMBSTONE_COMPACTION_INTERVAL().count());
    auto tombstone_compaction_interval = db_clock::duration(std::chrono::seconds(interval));
    if (interval <= 0) {
        throw exceptions::configuration_exception(fmt::format("{} value ({}) must be positive", compaction_strategy_impl::TOMBSTONE_COMPACTION_INTERVAL_OPTION, tombstone_compaction_interval));
    }
    return tombstone_compaction_interval;
}

static db_clock::duration validate_tombstone_compaction_interval(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options) {
    auto tombstone_compaction_interval = validate_tombstone_compaction_interval(options);
    unchecked_options.erase(compaction_strategy_impl::TOMBSTONE_COMPACTION_INTERVAL_OPTION);
    return tombstone_compaction_interval;
}

static bool validate_unchecked_tombstone_compaction(const std::map<sstring, sstring>& options) {
    auto unchecked_tombstone_compaction = compaction_strategy_impl::DEFAULT_UNCHECKED_TOMBSTONE_COMPACTION;
    auto tmp_value = compaction_strategy_impl::get_value(options, compaction_strategy_impl::UNCHECKED_TOMBSTONE_COMPACTION_OPTION);
    if (tmp_value.has_value()) {
        if (tmp_value != "true" && tmp_value != "false") {
            throw exceptions::configuration_exception(fmt::format("{} value ({}) must be \"true\" or \"false\"", compaction_strategy_impl::UNCHECKED_TOMBSTONE_COMPACTION_OPTION, *tmp_value));
        }
        unchecked_tombstone_compaction = tmp_value == "true";
    }
    return unchecked_tombstone_compaction;
}

static bool validate_unchecked_tombstone_compaction(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options) {
    auto unchecked_tombstone_compaction = validate_unchecked_tombstone_compaction(options);
    unchecked_options.erase(compaction_strategy_impl::UNCHECKED_TOMBSTONE_COMPACTION_OPTION);
    return unchecked_tombstone_compaction;
}

void compaction_strategy_impl::validate_options_for_strategy_type(const std::map<sstring, sstring>& options, compaction_strategy_type type) {
    auto unchecked_options = options;
    compaction_strategy_impl::validate_options(options, unchecked_options);
    switch (type) {
        case compaction_strategy_type::size_tiered:
            // STCS is deprecated and is an alias of ICS, so it takes the ICS options.
            incremental_compaction_strategy::validate_options(options, unchecked_options);
            // Accept, and ignore, the one STCS option ICS doesn't have, so that a
            // schema dumped from an older version can still be replayed as-is.
            // Its value is still validated: nothing reads it any more, but a bad
            // one is a typo worth reporting, as it always was.
            compaction_strategy_impl::validate_deprecated_cold_reads_to_omit(options, unchecked_options);
            break;
        case compaction_strategy_type::incremental:
            incremental_compaction_strategy::validate_options(options, unchecked_options);
            break;
        case compaction_strategy_type::leveled:
            leveled_compaction_strategy::validate_options(options, unchecked_options);
            break;
        case compaction_strategy_type::time_window:
            time_window_compaction_strategy::validate_options(options, unchecked_options);
            break;
        default:
            break;
        case compaction_strategy_type::null:
        case compaction_strategy_type::in_memory:
            return;
    }

    unchecked_options.erase("class");
    if (!unchecked_options.empty()) {
        throw exceptions::configuration_exception(fmt::format("Invalid compaction strategy options {} for chosen strategy type", unchecked_options));
    }
}

// options is a map of compaction strategy options and their values.
// unchecked_options is an analogical map from which already checked options are deleted.
// This helps making sure that only allowed options are being set.
void compaction_strategy_impl::validate_options(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options) {
    validate_tombstone_threshold(options, unchecked_options);
    validate_tombstone_compaction_interval(options, unchecked_options);
    validate_unchecked_tombstone_compaction(options, unchecked_options);

    auto it = options.find("enabled");
    if (it != options.end() && it->second != "true" && it->second != "false") {
        throw exceptions::configuration_exception(fmt::format("enabled value ({}) must be \"true\" or \"false\"", it->second));
    }
    unchecked_options.erase("enabled");
}

void compaction_strategy_impl::validate_deprecated_cold_reads_to_omit(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options) {
    auto tmp_value = get_value(options, DEPRECATED_COLD_READS_TO_OMIT_OPTION);
    auto cold_reads_to_omit = cql3::statements::property_definitions::to_double(DEPRECATED_COLD_READS_TO_OMIT_OPTION, tmp_value, 0.05);
    if (cold_reads_to_omit < 0.0 || cold_reads_to_omit > 1.0) {
        throw exceptions::configuration_exception(fmt::format("{} value ({}) must be between 0.0 and 1.0", DEPRECATED_COLD_READS_TO_OMIT_OPTION, cold_reads_to_omit));
    }
    unchecked_options.erase(DEPRECATED_COLD_READS_TO_OMIT_OPTION);
}

compaction_strategy_impl::compaction_strategy_impl(const std::map<sstring, sstring>& options) {
    _tombstone_threshold = validate_tombstone_threshold(options);
    _tombstone_compaction_interval = validate_tombstone_compaction_interval(options);
    _unchecked_tombstone_compaction = validate_unchecked_tombstone_compaction(options);
}

extern logging::logger clogger;

// The backlog for TWCS is just the sum of the individual backlogs in each time window. Each window
// is compacted with ICS, so its backlog is the ICS backlog of the runs in it.
//
// The compactions in progress are matched to the windows of their input, and their compacted
// parts are taken off the backlog of their window. As with ICS, the writes in progress aren't
// accounted for.
class time_window_backlog_tracker final : public compaction_backlog_tracker::impl {
    time_window_compaction_strategy_options _twcs_options;
    incremental_compaction_strategy_options _ics_options;

    std::unordered_map<api::timestamp_type, incremental_subset_backlog> _windows;

    api::timestamp_type lower_bound_of(api::timestamp_type timestamp) const {
        timestamp_type ts = time_window_compaction_strategy::to_timestamp_type(_twcs_options.timestamp_resolution, timestamp);
        return time_window_compaction_strategy::get_window_lower_bound(_twcs_options.sstable_window_size, ts);
    }
public:
    time_window_backlog_tracker(time_window_compaction_strategy_options twcs_options, incremental_compaction_strategy_options ics_options)
        : _twcs_options(twcs_options)
        , _ics_options(std::move(ics_options))
    {}

    virtual double backlog(const compaction_backlog_source& src, const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const override {
        auto min_threshold = src.schema()->min_compaction_threshold();
        auto no_oc = compaction_backlog_tracker::ongoing_compactions();

        std::unordered_map<api::timestamp_type, compaction_backlog_tracker::ongoing_compactions> compactions_per_window;
        for (auto& cp : oc) {
            compactions_per_window[lower_bound_of(cp.first->get_stats_metadata().max_timestamp)].insert(cp);
        }

        double b = 0;
        for (auto& [bound, w] : _windows) {
            auto it = compactions_per_window.find(bound);
            b += w.backlog(min_threshold, _ics_options, it != compactions_per_window.end() ? it->second : no_oc);
        }
        return b;
    }

    // Provides strong exception safety guarantees
    virtual void replace_sstables(const std::vector<sstables::shared_sstable>& old_ssts, const std::vector<sstables::shared_sstable>& new_ssts) override {
        auto tmp_windows = _windows;

        for (auto& sst : new_ssts) {
            tmp_windows[lower_bound_of(sst->get_stats_metadata().max_timestamp)].add(sst);
        }
        for (auto& sst : old_ssts) {
            auto it = tmp_windows.find(lower_bound_of(sst->get_stats_metadata().max_timestamp));
            if (it == tmp_windows.end()) {
                continue;
            }
            it->second.remove(sst);
            // A window lives as long as it holds a single sstable.
            if (it->second.empty()) {
                tmp_windows.erase(it);
            }
        }

        std::invoke([&] () noexcept {
            _windows = std::move(tmp_windows);
        });
    }
};

class leveled_compaction_backlog_tracker final : public compaction_backlog_tracker::impl {
    // Because we size-tier L0 with ICS, we will account for that in the backlog.
    // Whatever backlog we accumulate here will be added to the main backlog.
    incremental_compaction_strategy_options _ics_options;
    incremental_subset_backlog _l0;
    std::vector<uint64_t> _size_per_level;
    uint64_t _max_sstable_size;
public:
    leveled_compaction_backlog_tracker(int32_t max_sstable_size_in_mb, incremental_compaction_strategy_options ics_options)
        : _ics_options(std::move(ics_options))
        , _size_per_level(leveled_manifest::MAX_LEVELS, uint64_t(0))
        , _max_sstable_size(max_sstable_size_in_mb * 1024 * 1024)
    {}

    virtual double backlog(const compaction_backlog_source& src, const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const override {
        std::vector<uint64_t> effective_size_per_level = _size_per_level;
        compaction_backlog_tracker::ongoing_compactions l0_compacted;

        for (auto& op : ow) {
            effective_size_per_level[op.second->level()] += op.second->written();
        }

        for (auto& cp : oc) {
            auto level = cp.first->get_sstable_level();
            if (level == 0) {
                l0_compacted.insert(cp);
            }
            effective_size_per_level[level] -= cp.second->compacted();
        }

        double b = _l0.backlog(src.schema()->min_compaction_threshold(), _ics_options, l0_compacted);

        size_t max_populated_level = [&effective_size_per_level] () -> size_t {
            auto it = std::find_if(effective_size_per_level.rbegin(), effective_size_per_level.rend(), [] (uint64_t s) {
                return s != 0;
            });
            if (it == effective_size_per_level.rend()) {
                return 0;
            }
            return std::distance(it, effective_size_per_level.rend()) - 1;
        }();

        // The LCS goal is to achieve a layout where for every level L, sizeof(L+1) >= (sizeof(L) * fan_out)
        // If table size is S, which is the sum of size of all levels, the target size of the highest level
        // is S % 1.111, where 1.111 refers to strategy's space amplification goal.
        // As level L is fan_out times smaller than L+1, level L-1 is fan_out^2 times smaller than L+1,
        // and so on, the target size of any level can be easily calculated.

        static constexpr auto fan_out = leveled_manifest::leveled_fan_out;
        static constexpr double space_amplification_goal = 1.111;
        uint64_t total_size = std::accumulate(effective_size_per_level.begin(), effective_size_per_level.end(), uint64_t(0));
        uint64_t target_max_level_size = std::ceil(total_size / space_amplification_goal);

        auto target_level_size = [&] (size_t level) {
            auto r = std::ceil(target_max_level_size / std::pow(fan_out, max_populated_level - level));
            return std::max(uint64_t(r), _max_sstable_size);
        };

        // The backlog for a level L is the amount of bytes to be compacted, such that:
        // sizeof(L) <= sizeof(L+1) * fan_out
        // If we start from L0, then L0 backlog is (sizeof(L0) - target_sizeof(L0)) * fan_out, where
        // (sizeof(L0) - target_sizeof(L0)) is the amount of data to be promoted into next level
        // By summing the backlog for each level, we get the total amount of work for all levels to
        // reach their target size.
        for (size_t level = 0; level < max_populated_level; ++level) {
            auto lsize = effective_size_per_level[level];
            auto target_lsize = target_level_size(level);

            // Current level satisfies the goal, skip to the next one.
            if (lsize <= target_lsize) {
                continue;
            }
            auto next_level = level + 1;
            auto bytes_for_next_level =  lsize - target_lsize;

            // The fan_out is usually 10. But if the level above us is not fully populated -- which
            // can happen when a level is still being born, we don't want that to jump abruptly.
            // So what we will do instead is to define the fan out as the minimum between 10
            // and the number of sstables that are estimated to be there.
            unsigned estimated_next_level_ssts = (effective_size_per_level[next_level] + _max_sstable_size - 1) / _max_sstable_size;
            auto estimated_fan_out = std::min(fan_out, estimated_next_level_ssts);

            b += bytes_for_next_level * estimated_fan_out;

            // Update size of next level, as data from current level can be promoted as many times
            // as needed, and therefore needs to be included in backlog calculation for the next
            // level, if needed.
            effective_size_per_level[next_level] += bytes_for_next_level;
        }
        return b;
    }

    // Provides strong exception safety guarantees
    virtual void replace_sstables(const std::vector<sstables::shared_sstable>& old_ssts, const std::vector<sstables::shared_sstable>& new_ssts) override {
        auto tmp_size_per_level = _size_per_level;
        auto tmp_l0 = _l0;
        for (auto& sst : new_ssts) {
            auto level = sst->get_sstable_level();
            tmp_size_per_level[level] += sst->data_size();
            if (level == 0) {
                tmp_l0.add(sst);
            }
        }
        for (auto& sst : old_ssts) {
            auto level = sst->get_sstable_level();
            tmp_size_per_level[level] -= sst->data_size();
            if (level == 0) {
                tmp_l0.remove(sst);
            }
        }
        std::invoke([&] () noexcept {
            _size_per_level = std::move(tmp_size_per_level);
            _l0 = std::move(tmp_l0);
        });
    }
};

struct unimplemented_backlog_tracker final : public compaction_backlog_tracker::impl {
    virtual double backlog(const compaction_backlog_source& src, const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const override {
        return compaction_controller::disable_backlog;
    }
    virtual void replace_sstables(const std::vector<sstables::shared_sstable>& old_ssts, const std::vector<sstables::shared_sstable>& new_ssts) override {}
};

struct null_backlog_tracker final : public compaction_backlog_tracker::impl {
    virtual double backlog(const compaction_backlog_source& src, const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const override {
        return 0;
    }
    virtual void replace_sstables(const std::vector<sstables::shared_sstable>& old_ssts, const std::vector<sstables::shared_sstable>& new_ssts) override {}
};

//
// Null compaction strategy is the default compaction strategy.
// As the name implies, it does nothing.
//
class null_compaction_strategy : public compaction_strategy_impl {
public:
    virtual future<compaction_descriptor> get_sstables_for_compaction(compaction_group_view& table_s, strategy_control& control) override {
        return make_ready_future<compaction_descriptor>();
    }

    virtual future<int64_t> estimated_pending_compactions(compaction_group_view& table_s) const override {
        return make_ready_future<int64_t>(0);
    }

    virtual compaction_strategy_type type() const override {
        return compaction_strategy_type::null;
    }

    virtual std::unique_ptr<compaction_backlog_tracker::impl> make_backlog_tracker() const override {
        return std::make_unique<null_backlog_tracker>();
    }
};

leveled_compaction_strategy::leveled_compaction_strategy(const std::map<sstring, sstring>& options)
        : compaction_strategy_impl(options)
        , _max_sstable_size_in_mb(calculate_max_sstable_size_in_mb(compaction_strategy_impl::get_value(options, SSTABLE_SIZE_OPTION)))
        , _ics_options(options)
{
}

// options is a map of compaction strategy options and their values.
// unchecked_options is an analogical map from which already checked options are deleted.
// This helps making sure that only allowed options are being set.
void leveled_compaction_strategy::validate_options(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options) {
    // Level 0 is size-tiered with ICS, so LCS takes its bucketing options. sstable_size_in_mb is
    // LCS's own, and is also the size of the fragments written by those compactions.
    incremental_compaction_strategy_options::validate(options, unchecked_options);
    // Accept, and ignore, the one STCS option ICS doesn't have, which LCS used to take, so
    // that a schema dumped from an older version can still be replayed as-is.
    compaction_strategy_impl::validate_deprecated_cold_reads_to_omit(options, unchecked_options);

    auto tmp_value = compaction_strategy_impl::get_value(options, SSTABLE_SIZE_OPTION);
    auto min_sstables_size = cql3::statements::property_definitions::to_int(SSTABLE_SIZE_OPTION, tmp_value, DEFAULT_MAX_SSTABLE_SIZE_IN_MB);
    if (min_sstables_size <= 0) {
        throw exceptions::configuration_exception(fmt::format("{} value ({}) must be positive", SSTABLE_SIZE_OPTION, min_sstables_size));
    }
    unchecked_options.erase(SSTABLE_SIZE_OPTION);
}

std::unique_ptr<compaction_backlog_tracker::impl> leveled_compaction_strategy::make_backlog_tracker() const {
    return std::make_unique<leveled_compaction_backlog_tracker>(_max_sstable_size_in_mb, _ics_options);
}

int32_t
leveled_compaction_strategy::calculate_max_sstable_size_in_mb(std::optional<sstring> option_value) const {
    using namespace cql3::statements;
    auto max_size = property_definitions::to_int(SSTABLE_SIZE_OPTION, option_value, DEFAULT_MAX_SSTABLE_SIZE_IN_MB);

    if (max_size >= 1000) {
        leveled_manifest::logger.warn("Max sstable size of {}MB is configured; having a unit of compaction this large is probably a bad idea",
            max_size);
    } else if (max_size < 50) {
        leveled_manifest::logger.warn("Max sstable size of {}MB is configured. Testing done for CASSANDRA-5727 indicates that performance" \
            " improves up to 160MB", max_size);
    }
    return max_size;
}

time_window_compaction_strategy::time_window_compaction_strategy(const std::map<sstring, sstring>& options)
    : compaction_strategy_impl(options)
    , _options(options)
    , _ics_options(options)
    , _fragment_size(incremental_compaction_strategy::parse_fragment_size(options))
{
    if (!options.contains(TOMBSTONE_COMPACTION_INTERVAL_OPTION) && !options.contains(TOMBSTONE_THRESHOLD_OPTION)) {
        _disable_tombstone_compaction = true;
        clogger.debug("Disabling tombstone compactions for TWCS");
    } else {
        clogger.debug("Enabling tombstone compactions for TWCS");
    }
    _use_clustering_key_filter = true;
}

// options is a map of compaction strategy options and their values.
// unchecked_options is an analogical map from which already checked options are deleted.
// This helps making sure that only allowed options are being set.
void time_window_compaction_strategy::validate_options(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options) {
    time_window_compaction_strategy_options::validate(options, unchecked_options);
    // Windows are compacted with ICS, so TWCS takes its bucketing options and fragment size,
    // but not the space amplification goal, which applies across tiers rather than within a window.
    incremental_compaction_strategy_options::validate(options, unchecked_options);
    incremental_compaction_strategy::validate_fragment_size_option(options, unchecked_options);
    // Accept, and ignore, the one STCS option ICS doesn't have, which TWCS used to take, so
    // that a schema dumped from an older version can still be replayed as-is.
    compaction_strategy_impl::validate_deprecated_cold_reads_to_omit(options, unchecked_options);
}

std::unique_ptr<compaction_backlog_tracker::impl> time_window_compaction_strategy::make_backlog_tracker() const {
    return std::make_unique<time_window_backlog_tracker>(_options, _ics_options);
}

compaction_strategy::compaction_strategy(::shared_ptr<compaction_strategy_impl> impl)
    : _compaction_strategy_impl(std::move(impl)) {}
compaction_strategy::compaction_strategy() = default;
compaction_strategy::~compaction_strategy() = default;
compaction_strategy::compaction_strategy(const compaction_strategy&) = default;
compaction_strategy::compaction_strategy(compaction_strategy&&) = default;
compaction_strategy& compaction_strategy::operator=(compaction_strategy&&) = default;

compaction_strategy_type compaction_strategy::type() const {
    return _compaction_strategy_impl->type();
}

future<compaction_descriptor> compaction_strategy::get_sstables_for_compaction(compaction_group_view& table_s, strategy_control& control) {
    return _compaction_strategy_impl->get_sstables_for_compaction(table_s, control);
}

compaction_descriptor compaction_strategy::get_major_compaction_job(compaction_group_view& table_s, std::vector<sstables::shared_sstable> candidates) {
    return _compaction_strategy_impl->get_major_compaction_job(table_s, std::move(candidates));
}

std::vector<compaction_descriptor> compaction_strategy::get_cleanup_compaction_jobs(compaction_group_view& table_s, std::vector<sstables::shared_sstable> candidates) const {
    return _compaction_strategy_impl->get_cleanup_compaction_jobs(table_s, std::move(candidates));
}

void compaction_strategy::notify_completion(compaction_group_view& table_s, const std::vector<sstables::shared_sstable>& removed, const std::vector<sstables::shared_sstable>& added) {
    _compaction_strategy_impl->notify_completion(table_s, removed, added);
}

bool compaction_strategy::parallel_compaction() const {
    return _compaction_strategy_impl->parallel_compaction();
}

future<int64_t> compaction_strategy::estimated_pending_compactions(compaction_group_view& table_s) const {
    return _compaction_strategy_impl->estimated_pending_compactions(table_s);
}

bool compaction_strategy::use_clustering_key_filter() const {
    return _compaction_strategy_impl->use_clustering_key_filter();
}

compaction_backlog_tracker compaction_strategy::make_backlog_tracker() const {
    return compaction_backlog_tracker(_compaction_strategy_impl->make_backlog_tracker());
}

compaction_descriptor
compaction_strategy::get_reshaping_job(std::vector<sstables::shared_sstable> input, schema_ptr schema, reshape_config cfg) const {
    return _compaction_strategy_impl->get_reshaping_job(std::move(input), schema, cfg);
}

uint64_t compaction_strategy::adjust_partition_estimate(const mutation_source_metadata& ms_meta, uint64_t partition_estimate, schema_ptr schema) const {
    return _compaction_strategy_impl->adjust_partition_estimate(ms_meta, partition_estimate, std::move(schema));
}

mutation_reader_consumer compaction_strategy::make_interposer_consumer(const mutation_source_metadata& ms_meta, mutation_reader_consumer end_consumer) const {
    return _compaction_strategy_impl->make_interposer_consumer(ms_meta, std::move(end_consumer));
}

bool compaction_strategy::use_interposer_consumer() const {
    return _compaction_strategy_impl->use_interposer_consumer();
}

sstables::sstable_set
compaction_strategy::make_sstable_set(const compaction::compaction_group_view& ts) const {
    return sstables::sstable_set(
            _compaction_strategy_impl->make_sstable_set(ts));
}

compaction_strategy make_compaction_strategy(compaction_strategy_type strategy, const std::map<sstring, sstring>& options) {
    ::shared_ptr<compaction_strategy_impl> impl;

    switch (strategy) {
    case compaction_strategy_type::null:
        impl = ::make_shared<null_compaction_strategy>();
        break;
    case compaction_strategy_type::leveled:
        impl = ::make_shared<leveled_compaction_strategy>(options);
        break;
    case compaction_strategy_type::time_window:
        impl = ::make_shared<time_window_compaction_strategy>(options);
        break;
    case compaction_strategy_type::in_memory:
        compaction_strategy_logger.warn(
                "{} is no longer supported. Defaulting to {}.",
                compaction_strategy::name(compaction_strategy_type::in_memory),
                compaction_strategy::name(compaction_strategy_type::null));
        impl = ::make_shared<null_compaction_strategy>();
        break;
    case compaction_strategy_type::size_tiered:
        // STCS is deprecated. It is kept only as an alias of ICS, which
        // provides the same read and write amplification with a much lower
        // space amplification. See scylladb/scylladb#22306.
        // Logged on one shard only: every shard builds a strategy per table.
        if (this_shard_id() == 0 && options.contains(compaction_strategy_impl::DEPRECATED_COLD_READS_TO_OMIT_OPTION)) {
            compaction_strategy_logger.warn("Ignoring the {} option: it is not supported by {}, which {} is now an alias of.",
                    compaction_strategy_impl::DEPRECATED_COLD_READS_TO_OMIT_OPTION,
                    compaction_strategy::name(compaction_strategy_type::incremental),
                    compaction_strategy::name(compaction_strategy_type::size_tiered));
        }
        [[fallthrough]];
    case compaction_strategy_type::incremental:
        impl = ::make_shared<incremental_compaction_strategy>(options);
        break;
    default:
        throw std::runtime_error("strategy not supported");
    }

    return compaction_strategy(std::move(impl));
}

future<reshape_config> make_reshape_config(const sstables::storage& storage, reshape_mode mode) {
    co_return reshape_config{
        .mode = mode,
        .free_storage_space = co_await storage.free_space() / this_smp_shard_count(),
    };
}

std::unique_ptr<sstables::sstable_set_impl> incremental_compaction_strategy::make_sstable_set(const compaction_group_view& ts) const {
    return std::make_unique<sstables::partitioned_sstable_set>(ts.schema());
}

}

namespace compaction {

compaction_strategy_state compaction_strategy_state::make(const compaction_strategy& cs) {
    switch (cs.type()) {
        case compaction_strategy_type::null:
        case compaction_strategy_type::incremental:
            return compaction_strategy_state(default_empty_state{});
        case compaction_strategy_type::leveled:
            return compaction_strategy_state(seastar::make_shared<leveled_compaction_strategy_state>());
        case compaction_strategy_type::time_window:
            return compaction_strategy_state(seastar::make_shared<time_window_compaction_strategy_state>());
        default:
            throw std::runtime_error("strategy not supported");
    }
}

}
