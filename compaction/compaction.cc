/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

/*
 */

#include <vector>
#include <map>
#include <functional>
#include <utility>
#include <assert.h>
#include <algorithm>

#include <boost/range/algorithm.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/join.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/algorithm/string/join.hpp>

#include <seastar/core/future-util.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/closeable.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/shard_id.hh>

#include "dht/i_partitioner.hh"
#include "sstables/exceptions.hh"
#include "sstables/sstables.hh"
#include "sstables/sstable_writer.hh"
#include "sstables/progress_monitor.hh"
#include "sstables/sstables_manager.hh"
#include "compaction.hh"
#include "schema/schema.hh"
#include "db/system_keyspace.hh"
#include "db_clock.hh"
#include "mutation/mutation_compactor.hh"
#include "leveled_manifest.hh"
#include "dht/partition_filter.hh"
#include "mutation_writer/shard_based_splitting_writer.hh"
#include "mutation_writer/partition_based_splitting_writer.hh"
#include "mutation/mutation_source_metadata.hh"
#include "mutation/mutation_fragment_stream_validator.hh"
#include "utils/assert.hh"
#include "utils/error_injection.hh"
#include "utils/pretty_printers.hh"
#include "readers/multi_range.hh"
#include "readers/compacting.hh"
#include "tombstone_gc.hh"
#include "replica/database.hh"

namespace sstables {

bool is_eligible_for_compaction(const shared_sstable& sst) noexcept {
    return !sst->requires_view_building() && !sst->is_quarantined();
}

logging::logger clogger("compaction");

static const std::unordered_map<compaction_type, sstring> compaction_types = {
    { compaction_type::Compaction, "COMPACTION" },
    { compaction_type::Cleanup, "CLEANUP" },
    { compaction_type::Validation, "VALIDATION" },
    { compaction_type::Scrub, "SCRUB" },
    { compaction_type::Index_build, "INDEX_BUILD" },
    { compaction_type::Reshard, "RESHARD" },
    { compaction_type::Upgrade, "UPGRADE" },
    { compaction_type::Reshape, "RESHAPE" },
    { compaction_type::Split, "SPLIT" },
};

sstring compaction_name(compaction_type type) {
    auto ret = compaction_types.find(type);
    if (ret != compaction_types.end()) {
        return ret->second;
    }
    throw std::runtime_error("Invalid Compaction Type");
}

compaction_type to_compaction_type(sstring type_name) {
    for (auto& it : compaction_types) {
        if (it.second == type_name) {
            return it.first;
        }
    }
    throw std::runtime_error("Invalid Compaction Type Name");
}

std::string_view to_string(compaction_type type) {
    switch (type) {
    case compaction_type::Compaction: return "Compact";
    case compaction_type::Cleanup: return "Cleanup";
    case compaction_type::Validation: return "Validate";
    case compaction_type::Scrub: return "Scrub";
    case compaction_type::Index_build: return "Index_build";
    case compaction_type::Reshard: return "Reshard";
    case compaction_type::Upgrade: return "Upgrade";
    case compaction_type::Reshape: return "Reshape";
    case compaction_type::Split: return "Split";
    }
    on_internal_error_noexcept(clogger, format("Invalid compaction type {}", int(type)));
    return "(invalid)";
}

std::string_view to_string(compaction_type_options::scrub::mode scrub_mode) {
    switch (scrub_mode) {
        case compaction_type_options::scrub::mode::abort:
            return "abort";
        case compaction_type_options::scrub::mode::skip:
            return "skip";
        case compaction_type_options::scrub::mode::segregate:
            return "segregate";
        case compaction_type_options::scrub::mode::validate:
            return "validate";
    }
    on_internal_error_noexcept(clogger, format("Invalid scrub mode {}", int(scrub_mode)));
    return "(invalid)";
}

std::string_view to_string(compaction_type_options::scrub::quarantine_mode quarantine_mode) {
    switch (quarantine_mode) {
        case compaction_type_options::scrub::quarantine_mode::include:
            return "include";
        case compaction_type_options::scrub::quarantine_mode::exclude:
            return "exclude";
        case compaction_type_options::scrub::quarantine_mode::only:
            return "only";
    }
    on_internal_error_noexcept(clogger, format("Invalid scrub quarantine mode {}", int(quarantine_mode)));
    return "(invalid)";
}

static api::timestamp_type get_max_purgeable_timestamp(const table_state& table_s, sstable_set::incremental_selector& selector,
        const std::unordered_set<shared_sstable>& compacting_set, const dht::decorated_key& dk, uint64_t& bloom_filter_checks,
        const api::timestamp_type compacting_max_timestamp) {
    if (!table_s.tombstone_gc_enabled()) [[unlikely]] {
        return api::min_timestamp;
    }

    auto timestamp = api::max_timestamp;
    auto memtable_min_timestamp = table_s.min_memtable_timestamp();
    // Use memtable timestamp if it contains data older than the sstables being compacted,
    // and if the memtable also contains the key we're calculating max purgeable timestamp for.
    // First condition helps to not penalize the common scenario where memtable only contains
    // newer data.
    if (memtable_min_timestamp <= compacting_max_timestamp && table_s.memtable_has_key(dk)) {
        timestamp = memtable_min_timestamp;
    }
    std::optional<utils::hashed_key> hk;
    for (auto&& sst : boost::range::join(selector.select(dk).sstables, table_s.compacted_undeleted_sstables())) {
        if (compacting_set.contains(sst)) {
            continue;
        }
        // There's no point in looking up the key in the sstable filter if
        // it does not contain data older than the minimum timestamp.
        if (sst->get_stats_metadata().min_timestamp >= timestamp) {
            continue;
        }
        if (!hk) {
            hk = sstables::sstable::make_hashed_key(*table_s.schema(), dk.key());
        }
        if (sst->filter_has_key(*hk)) {
            bloom_filter_checks++;
            timestamp = sst->get_stats_metadata().min_timestamp;
        }
    }
    return timestamp;
}

static std::vector<shared_sstable> get_uncompacting_sstables(const table_state& table_s, std::vector<shared_sstable> sstables) {
    auto all_sstables = boost::copy_range<std::vector<shared_sstable>>(*table_s.main_sstable_set().all());
    auto& compacted_undeleted = table_s.compacted_undeleted_sstables();
    all_sstables.insert(all_sstables.end(), compacted_undeleted.begin(), compacted_undeleted.end());
    boost::sort(all_sstables, [] (const shared_sstable& x, const shared_sstable& y) {
        return x->generation() < y->generation();
    });
    std::sort(sstables.begin(), sstables.end(), [] (const shared_sstable& x, const shared_sstable& y) {
        return x->generation() < y->generation();
    });
    std::vector<shared_sstable> not_compacted_sstables;
    boost::set_difference(all_sstables, sstables,
        std::back_inserter(not_compacted_sstables), [] (const shared_sstable& x, const shared_sstable& y) {
            return x->generation() < y->generation();
        });
    return not_compacted_sstables;
}

class compaction;

class compaction_write_monitor final : public sstables::write_monitor, public backlog_write_progress_manager {
    sstables::shared_sstable _sst;
    table_state& _table_s;
    const sstables::writer_offset_tracker* _tracker = nullptr;
    uint64_t _progress_seen = 0;
    api::timestamp_type _maximum_timestamp;
    unsigned _sstable_level;
public:
    compaction_write_monitor(sstables::shared_sstable sst, table_state& table_s, api::timestamp_type max_timestamp, unsigned sstable_level)
        : _sst(sst)
        , _table_s(table_s)
        , _maximum_timestamp(max_timestamp)
        , _sstable_level(sstable_level)
    {}

    ~compaction_write_monitor() {
        if (_sst) {
            _table_s.get_backlog_tracker().revert_charges(_sst);
        }
    }

    virtual void on_write_started(const sstables::writer_offset_tracker& tracker) override {
        _tracker = &tracker;
        _table_s.get_backlog_tracker().register_partially_written_sstable(_sst, *this);
    }

    virtual void on_data_write_completed() override {
        if (_tracker) {
            _progress_seen = _tracker->offset;
            _tracker = nullptr;
        }
    }

    virtual uint64_t written() const override {
        if (_tracker) {
            return _tracker->offset;
        }
        return _progress_seen;
    }

    api::timestamp_type maximum_timestamp() const override {
        return _maximum_timestamp;
    }

    unsigned level() const override {
        return _sstable_level;
    }
};

struct compaction_writer {
    shared_sstable sst;
    // We use a ptr for pointer stability and so that it can be null
    // when using a noop monitor.
    sstable_writer writer;
    // The order in here is important. A monitor must be destroyed before the writer it is monitoring since it has a
    // periodic timer that checks the writer.
    // The writer must be destroyed before the shared_sstable since the it may depend on the sstable
    // (as in the mx::writer over compressed_file_data_sink_impl case that depends on sstables::compression).
    std::unique_ptr<compaction_write_monitor> monitor;

    compaction_writer(std::unique_ptr<compaction_write_monitor> monitor, sstable_writer writer, shared_sstable sst)
        : sst(std::move(sst)), writer(std::move(writer)), monitor(std::move(monitor)) {}
    compaction_writer(sstable_writer writer, shared_sstable sst)
        : compaction_writer(nullptr, std::move(writer), std::move(sst)) {}
};

class compacted_fragments_writer {
    compaction& _c;
    std::optional<compaction_writer> _compaction_writer = {};
    using creator_func_t = std::function<compaction_writer(const dht::decorated_key&)>;
    using stop_func_t = std::function<void(compaction_writer*)>;
    creator_func_t _create_compaction_writer;
    stop_func_t _stop_compaction_writer;
    std::optional<utils::observer<>> _stop_request_observer;
    bool _unclosed_partition = false;
    struct partition_state {
        dht::decorated_key_opt dk;
        // Partition tombstone is saved for the purpose of replicating it to every fragment storing a partition pL.
        // Then when reading from the SSTable run, we won't unnecessarily have to open >= 2 fragments, the one which
        // contains the tombstone and another one(s) that has the partition slice being queried.
        ::tombstone tombstone;
        // Used to determine whether any active tombstones need closing at EOS.
        ::tombstone current_emitted_tombstone;
        // Track last emitted clustering row, which will be used to close active tombstone if splitting partition
        position_in_partition last_pos = position_in_partition::before_all_clustered_rows();
        bool is_splitting_partition = false;
    } _current_partition;
private:
    inline void maybe_abort_compaction();

    utils::observer<> make_stop_request_observer(utils::observable<>& sro) {
        return sro.observe([this] () mutable {
            SCYLLA_ASSERT(!_unclosed_partition);
            consume_end_of_stream();
        });
    }

    void stop_current_writer();
    bool can_split_large_partition() const;
    void track_last_position(position_in_partition_view pos);
    void split_large_partition();
    void do_consume_new_partition(const dht::decorated_key& dk);
    stop_iteration do_consume_end_of_partition();
public:
    explicit compacted_fragments_writer(compaction& c, creator_func_t cpw, stop_func_t scw)
            : _c(c)
            , _create_compaction_writer(std::move(cpw))
            , _stop_compaction_writer(std::move(scw)) {
    }
    explicit compacted_fragments_writer(compaction& c, creator_func_t cpw, stop_func_t scw, utils::observable<>& sro)
            : _c(c)
            , _create_compaction_writer(std::move(cpw))
            , _stop_compaction_writer(std::move(scw))
            , _stop_request_observer(make_stop_request_observer(sro)) {
    }
    compacted_fragments_writer(compacted_fragments_writer&& other);
    compacted_fragments_writer& operator=(const compacted_fragments_writer&) = delete;
    compacted_fragments_writer(const compacted_fragments_writer&) = delete;

    void consume_new_partition(const dht::decorated_key& dk);

    void consume(tombstone t);
    stop_iteration consume(static_row&& sr, tombstone, bool) {
        maybe_abort_compaction();
        return _compaction_writer->writer.consume(std::move(sr));
    }
    stop_iteration consume(static_row&& sr) {
        return consume(std::move(sr), tombstone{}, bool{});
    }
    stop_iteration consume(clustering_row&& cr, row_tombstone, bool);
    stop_iteration consume(clustering_row&& cr) {
        return consume(std::move(cr), row_tombstone{}, bool{});
    }
    stop_iteration consume(range_tombstone_change&& rtc);

    stop_iteration consume_end_of_partition();
    void consume_end_of_stream();
};

using use_backlog_tracker = bool_class<class use_backlog_tracker_tag>;

struct compaction_read_monitor_generator final : public read_monitor_generator {
    class compaction_read_monitor final : public  sstables::read_monitor, public backlog_read_progress_manager {
        sstables::shared_sstable _sst;
        table_state& _table_s;
        const sstables::reader_position_tracker* _tracker = nullptr;
        uint64_t _last_position_seen = 0;
        use_backlog_tracker _use_backlog_tracker;
    public:
        virtual void on_read_started(const sstables::reader_position_tracker& tracker) override {
            _tracker = &tracker;
            if (_use_backlog_tracker) {
                _table_s.get_backlog_tracker().register_compacting_sstable(_sst, *this);
            }
        }

        virtual void on_read_completed() override {
            if (_tracker) {
                _last_position_seen = _tracker->position;
                _tracker = nullptr;
            }
        }

        virtual uint64_t compacted() const override {
            if (_tracker) {
                return _tracker->position;
            }
            return _last_position_seen;
        }

        void remove_sstable() {
            if (_sst && _use_backlog_tracker) {
                _table_s.get_backlog_tracker().revert_charges(_sst);
            }
            _sst = {};
        }

        compaction_read_monitor(sstables::shared_sstable sst, table_state& table_s, use_backlog_tracker use_backlog_tracker)
            : _sst(std::move(sst)), _table_s(table_s), _use_backlog_tracker(use_backlog_tracker) { }

        ~compaction_read_monitor() {
            // We failed to finish handling this SSTable, so we have to update the backlog_tracker
            // about it.
            if (_sst && _use_backlog_tracker) {
                _table_s.get_backlog_tracker().revert_charges(_sst);
            }
        }

        friend class compaction_read_monitor_generator;
    };

    virtual sstables::read_monitor& operator()(sstables::shared_sstable sst) override {
        auto p = _generated_monitors.emplace(sst->generation(), compaction_read_monitor(sst, _table_s, _use_backlog_tracker));
        return p.first->second;
    }

    explicit compaction_read_monitor_generator(table_state& table_s, use_backlog_tracker use_backlog_tracker = use_backlog_tracker::yes)
        : _table_s(table_s), _use_backlog_tracker(use_backlog_tracker) {}

    uint64_t compacted() const {
        return boost::accumulate(_generated_monitors | boost::adaptors::map_values | boost::adaptors::transformed([](auto& monitor) { return monitor.compacted(); }), uint64_t(0));
    }

    void remove_exhausted_sstables(const std::vector<sstables::shared_sstable>& exhausted_sstables) {
        for (auto& sst : exhausted_sstables) {
            auto it = _generated_monitors.find(sst->generation());
            if (it != _generated_monitors.end()) {
                it->second.remove_sstable();
            }
        }
    }
private:
    table_state& _table_s;
    std::unordered_map<generation_type, compaction_read_monitor> _generated_monitors;
    use_backlog_tracker _use_backlog_tracker;

    friend class compaction_progress_monitor;
};

void compaction_progress_monitor::set_generator(std::unique_ptr<read_monitor_generator> generator) {
    _generator = std::move(generator);
}

void compaction_progress_monitor::reset_generator() {
    if (_generator) {
        _progress = dynamic_cast<compaction_read_monitor_generator&>(*_generator).compacted();
    }
    _generator = nullptr;
}

uint64_t compaction_progress_monitor::get_progress() const {
    if (_generator) {
        return dynamic_cast<compaction_read_monitor_generator&>(*_generator).compacted();
    }
    return _progress;
}

class compaction {
protected:
    compaction_data& _cdata;
    table_state& _table_s;
    const compaction_sstable_creator_fn _sstable_creator;
    const schema_ptr _schema;
    const reader_permit _permit;
    std::vector<shared_sstable> _sstables;
    std::vector<generation_type> _input_sstable_generations;
    // Unused sstables are tracked because if compaction is interrupted we can only delete them.
    // Deleting used sstables could potentially result in data loss.
    std::unordered_set<shared_sstable> _new_partial_sstables;
    std::vector<shared_sstable> _new_unused_sstables;
    std::vector<shared_sstable> _all_new_sstables;
    lw_shared_ptr<sstable_set> _compacting;
    const sstables::compaction_type _type;
    const uint64_t _max_sstable_size;
    const uint32_t _sstable_level;
    uint64_t _start_size = 0;
    uint64_t _end_size = 0;
    // fully expired files, which are skipped, aren't taken into account.
    uint64_t _compacting_data_file_size = 0;
    api::timestamp_type _compacting_max_timestamp = api::min_timestamp;
    uint64_t _estimated_partitions = 0;
    double _estimated_droppable_tombstone_ratio = 0;
    uint64_t _bloom_filter_checks = 0;
    db::replay_position _rp;
    encoding_stats_collector _stats_collector;
    const bool _can_split_large_partition = false;
    bool _contains_multi_fragment_runs = false;
    mutation_source_metadata _ms_metadata = {};
    const compaction_sstable_replacer_fn _replacer;
    const run_id _run_identifier;
    // optional clone of sstable set to be used for expiration purposes, so it will be set if expiration is enabled.
    std::optional<sstable_set> _sstable_set;
    // used to incrementally calculate max purgeable timestamp, as we iterate through decorated keys.
    std::optional<sstable_set::incremental_selector> _selector;
    std::unordered_set<shared_sstable> _compacting_for_max_purgeable_func;
    // optional owned_ranges vector for cleanup;
    const owned_ranges_ptr _owned_ranges = {};
    // required for reshard compaction.
    const dht::sharder* _sharder = nullptr;
    const std::optional<dht::incremental_owned_ranges_checker> _owned_ranges_checker;
    // Garbage collected sstables that are sealed but were not added to SSTable set yet.
    std::vector<shared_sstable> _unused_garbage_collected_sstables;
    // Garbage collected sstables that were added to SSTable set and should be eventually removed from it.
    std::vector<shared_sstable> _used_garbage_collected_sstables;
    utils::observable<> _stop_request_observable;
private:
    // Keeps track of monitors for input sstable.
    // If _update_backlog_tracker is set to true, monitors are responsible for adjusting backlog as compaction progresses.
    compaction_progress_monitor& _progress_monitor;
    compaction_data& init_compaction_data(compaction_data& cdata, const compaction_descriptor& descriptor) const {
        cdata.compaction_fan_in = descriptor.fan_in();
        return cdata;
    }

    // Called in a seastar thread
    dht::partition_range_vector
    get_ranges_for_invalidation(const std::vector<shared_sstable>& sstables) {
        // If owned ranges is disengaged, it means no cleanup work was done and
        // so nothing needs to be invalidated.
        if (!_owned_ranges) {
            return dht::partition_range_vector{};
        }
        auto owned_ranges = dht::to_partition_ranges(*_owned_ranges, utils::can_yield::yes);

        auto non_owned_ranges = boost::copy_range<dht::partition_range_vector>(sstables
                | boost::adaptors::transformed([] (const shared_sstable& sst) {
            seastar::thread::maybe_yield();
            return dht::partition_range::make({sst->get_first_decorated_key(), true},
                                              {sst->get_last_decorated_key(), true});
        }));

        return dht::subtract_ranges(*_schema, non_owned_ranges, std::move(owned_ranges)).get();
    }
protected:
    compaction(table_state& table_s, compaction_descriptor descriptor, compaction_data& cdata, compaction_progress_monitor& progress_monitor, use_backlog_tracker use_backlog_tracker)
        : _cdata(init_compaction_data(cdata, descriptor))
        , _table_s(table_s)
        , _sstable_creator(std::move(descriptor.creator))
        , _schema(_table_s.schema())
        , _permit(_table_s.make_compaction_reader_permit())
        , _sstables(std::move(descriptor.sstables))
        , _type(descriptor.options.type())
        , _max_sstable_size(descriptor.max_sstable_bytes)
        , _sstable_level(descriptor.level)
        , _can_split_large_partition(descriptor.can_split_large_partition)
        , _replacer(std::move(descriptor.replacer))
        , _run_identifier(descriptor.run_identifier)
        , _sstable_set(std::move(descriptor.all_sstables_snapshot))
        , _selector(_sstable_set ? _sstable_set->make_incremental_selector() : std::optional<sstable_set::incremental_selector>{})
        , _compacting_for_max_purgeable_func(std::unordered_set<shared_sstable>(_sstables.begin(), _sstables.end()))
        , _owned_ranges(std::move(descriptor.owned_ranges))
        , _sharder(descriptor.sharder)
        , _owned_ranges_checker(_owned_ranges ? std::optional<dht::incremental_owned_ranges_checker>(*_owned_ranges) : std::nullopt)
        , _progress_monitor(progress_monitor)
    {
        std::unordered_set<run_id> ssts_run_ids;
        _contains_multi_fragment_runs = std::any_of(_sstables.begin(), _sstables.end(), [&ssts_run_ids] (shared_sstable& sst) {
            return !ssts_run_ids.insert(sst->run_identifier()).second;
        });
        _progress_monitor.set_generator(std::make_unique<compaction_read_monitor_generator>(_table_s, use_backlog_tracker));
    }

    read_monitor_generator& unwrap_monitor_generator() const {
        if (_progress_monitor._generator) {
            return *_progress_monitor._generator;
        }
        return default_read_monitor_generator();
    }

    virtual uint64_t partitions_per_sstable() const {
        // some tests use _max_sstable_size == 0 for force many one partition per sstable
        auto max_sstable_size = std::max<uint64_t>(_max_sstable_size, 1);
        uint64_t estimated_sstables = std::max(1UL, uint64_t(ceil(double(_compacting_data_file_size) / max_sstable_size)));
        return std::min(uint64_t(ceil(double(_estimated_partitions) / estimated_sstables)),
                        _table_s.get_compaction_strategy().adjust_partition_estimate(_ms_metadata, _estimated_partitions, _schema));
    }

    void setup_new_sstable(shared_sstable& sst) {
        _all_new_sstables.push_back(sst);
        _new_partial_sstables.insert(sst);
        for (auto ancestor : _input_sstable_generations) {
            sst->add_ancestor(ancestor);
        }
    }

    void finish_new_sstable(compaction_writer* writer) {
        writer->writer.consume_end_of_stream();
        writer->sst->open_data().get();
        _end_size += writer->sst->bytes_on_disk();
        _new_unused_sstables.push_back(writer->sst);
        _new_partial_sstables.erase(writer->sst);
    }

    sstable_writer_config make_sstable_writer_config(compaction_type type) {
        auto s = compaction_name(type);
        std::transform(s.begin(), s.end(), s.begin(), [] (char c) {
            return std::tolower(c);
        });
        sstable_writer_config cfg = _table_s.configure_writer(std::move(s));
        cfg.max_sstable_size = _max_sstable_size;
        cfg.monitor = &default_write_monitor();
        cfg.run_identifier = _run_identifier;
        cfg.replay_position = _rp;
        cfg.sstable_level = _sstable_level;
        return cfg;
    }

    api::timestamp_type maximum_timestamp() const {
        auto m = std::max_element(_sstables.begin(), _sstables.end(), [] (const shared_sstable& sst1, const shared_sstable& sst2) {
            return sst1->get_stats_metadata().max_timestamp < sst2->get_stats_metadata().max_timestamp;
        });
        return (*m)->get_stats_metadata().max_timestamp;
    }

    encoding_stats get_encoding_stats() const {
        return _stats_collector.get();
    }

    compaction_completion_desc
    get_compaction_completion_desc(std::vector<shared_sstable> input_sstables, std::vector<shared_sstable> output_sstables) {
        auto ranges_for_for_invalidation = get_ranges_for_invalidation(input_sstables);
        return compaction_completion_desc{std::move(input_sstables), std::move(output_sstables), std::move(ranges_for_for_invalidation)};
    }

    // Tombstone expiration is enabled based on the presence of sstable set.
    // If it's not present, we cannot purge tombstones without the risk of resurrecting data.
    bool tombstone_expiration_enabled() const {
        return bool(_sstable_set) && _table_s.tombstone_gc_enabled();
    }

    compaction_writer create_gc_compaction_writer(run_id gc_run) const {
        auto sst = _sstable_creator(this_shard_id());

        auto monitor = std::make_unique<compaction_write_monitor>(sst, _table_s, maximum_timestamp(), _sstable_level);
        sstable_writer_config cfg = _table_s.configure_writer("garbage_collection");
        cfg.run_identifier = gc_run;
        cfg.monitor = monitor.get();
        uint64_t estimated_partitions = std::max(1UL, uint64_t(ceil(partitions_per_sstable() * _estimated_droppable_tombstone_ratio)));
        auto writer = sst->get_writer(*schema(), estimated_partitions, cfg, get_encoding_stats());
        return compaction_writer(std::move(monitor), std::move(writer), std::move(sst));
    }

    void stop_gc_compaction_writer(compaction_writer* c_writer) {
        c_writer->writer.consume_end_of_stream();
        auto sst = c_writer->sst;
        sst->open_data().get();
        _unused_garbage_collected_sstables.push_back(std::move(sst));
    }

    // Writes a temporary sstable run containing only garbage collected data.
    // Whenever regular compaction writer seals a new sstable, this writer will flush a new sstable as well,
    // right before there's an attempt to release exhausted sstables earlier.
    // Generated sstables will be temporarily added to table to make sure that a compaction crash will not
    // result in data resurrection.
    // When compaction finishes, all the temporary sstables generated here will be deleted and removed
    // from table's sstable set.
    compacted_fragments_writer get_gc_compacted_fragments_writer() {
        // because the temporary sstable run can overlap with the non-gc sstables run created by
        // get_compacted_fragments_writer(), we have to use a different run_id. the gc_run_id is
        // created here as:
        // 1. it can be shared across all sstables created by this writer
        // 2. it is optional, as gc writer is not always used
        auto gc_run = run_id::create_random_id();
        return compacted_fragments_writer(*this,
             [this, gc_run] (const dht::decorated_key&) { return create_gc_compaction_writer(gc_run); },
             [this] (compaction_writer* cw) { stop_gc_compaction_writer(cw); },
             _stop_request_observable);
    }

    // Retrieves all unused garbage collected sstables that will be subsequently added
    // to the SSTable set, and mark them as used.
    std::vector<shared_sstable> consume_unused_garbage_collected_sstables() {
        auto unused = std::exchange(_unused_garbage_collected_sstables, {});
        _used_garbage_collected_sstables.insert(_used_garbage_collected_sstables.end(), unused.begin(), unused.end());
        return unused;
    }

    const std::vector<shared_sstable>& used_garbage_collected_sstables() const {
        return _used_garbage_collected_sstables;
    }

    virtual bool enable_garbage_collected_sstable_writer() const noexcept {
        return _contains_multi_fragment_runs && _max_sstable_size != std::numeric_limits<uint64_t>::max() && bool(_replacer);
    }
public:
    compaction& operator=(const compaction&) = delete;
    compaction(const compaction&) = delete;

    compaction(compaction&& other) = delete;
    compaction& operator=(compaction&& other) = delete;

    virtual ~compaction() {
        _progress_monitor.reset_generator();
    }
private:
    // Default range sstable reader that will only return mutation that belongs to current shard.
    virtual mutation_reader make_sstable_reader(schema_ptr s,
                                                        reader_permit permit,
                                                        const dht::partition_range& range,
                                                        const query::partition_slice& slice,
                                                        tracing::trace_state_ptr,
                                                        streamed_mutation::forwarding fwd,
                                                        mutation_reader::forwarding) const = 0;

    mutation_reader setup_sstable_reader() const {
        if (!_owned_ranges_checker) {
            return make_sstable_reader(_schema,
                                       _permit,
                                       query::full_partition_range,
                                       _schema->full_slice(),
                                       tracing::trace_state_ptr(),
                                       ::streamed_mutation::forwarding::no,
                                       ::mutation_reader::forwarding::no);
        }

        auto source = mutation_source([this] (schema_ptr s,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                tracing::trace_state_ptr trace_state,
                streamed_mutation::forwarding fwd,
                mutation_reader::forwarding fwd_mr) {
            log_trace("Creating sstable set reader with range {}", range);
            return make_sstable_reader(std::move(s),
                                       std::move(permit),
                                       range,
                                       slice,
                                       std::move(trace_state),
                                       fwd,
                                       fwd_mr);
        });

        auto owned_range_generator = [this] () -> std::optional<dht::partition_range> {
            auto r = _owned_ranges_checker->next_owned_range();
            if (r == nullptr) {
                return std::nullopt;
            }
            log_trace("Skipping to the next owned range {}", *r);
            return dht::to_partition_range(*r);
        };

        return make_flat_multi_range_reader(_schema, _permit, std::move(source),
                                            std::move(owned_range_generator),
                                            _schema->full_slice(),
                                            tracing::trace_state_ptr());
    }

    virtual sstables::sstable_set make_sstable_set_for_input() const {
        return _table_s.get_compaction_strategy().make_sstable_set(_schema);
    }

    future<> setup() {
        auto ssts = make_lw_shared<sstables::sstable_set>(make_sstable_set_for_input());
        auto fully_expired = _table_s.fully_expired_sstables(_sstables, gc_clock::now());
        min_max_tracker<api::timestamp_type> timestamp_tracker;

        double sum_of_estimated_droppable_tombstone_ratio = 0;
        _input_sstable_generations.reserve(_sstables.size());
        for (auto& sst : _sstables) {
            co_await coroutine::maybe_yield();
            auto& sst_stats = sst->get_stats_metadata();
            timestamp_tracker.update(sst_stats.min_timestamp);
            timestamp_tracker.update(sst_stats.max_timestamp);

            // Compacted sstable keeps track of its ancestors.
            _input_sstable_generations.push_back(sst->generation());
            _start_size += sst->bytes_on_disk();
            _cdata.total_partitions += sst->get_estimated_key_count();

            // Do not actually compact a sstable that is fully expired and can be safely
            // dropped without resurrecting old data.
            if (tombstone_expiration_enabled() && fully_expired.contains(sst)) {
                log_debug("Fully expired sstable {} will be dropped on compaction completion", sst->get_filename());
                continue;
            }
            _stats_collector.update(sst->get_encoding_stats_for_compaction());

            _cdata.compaction_size += sst->data_size();
            // We also capture the sstable, so we keep it alive while the read isn't done
            ssts->insert(sst);
            // FIXME: If the sstables have cardinality estimation bitmaps, use that
            // for a better estimate for the number of partitions in the merged
            // sstable than just adding up the lengths of individual sstables.
            _estimated_partitions += sst->get_estimated_key_count();
            sum_of_estimated_droppable_tombstone_ratio += sst->estimate_droppable_tombstone_ratio(gc_clock::now(), _table_s.get_tombstone_gc_state(), _schema);
            _compacting_data_file_size += sst->ondisk_data_size();
            _compacting_max_timestamp = std::max(_compacting_max_timestamp, sst->get_stats_metadata().max_timestamp);
            if (sst->originated_on_this_node().value_or(false) && sst_stats.position.shard_id() == this_shard_id()) {
                _rp = std::max(_rp, sst_stats.position);
            }
        }
        log_info("{} [{}]", report_start_desc(), fmt::join(_sstables | boost::adaptors::transformed([] (auto sst) { return to_string(sst, true); }), ","));
        if (ssts->size() < _sstables.size()) {
            log_debug("{} out of {} input sstables are fully expired sstables that will not be actually compacted",
                      _sstables.size() - ssts->size(), _sstables.size());
        }
        // _estimated_droppable_tombstone_ratio could exceed 1.0 in certain cases, so limit it to 1.0.
        _estimated_droppable_tombstone_ratio = std::min(1.0, sum_of_estimated_droppable_tombstone_ratio / ssts->size());

        _compacting = std::move(ssts);

        _ms_metadata.min_timestamp = timestamp_tracker.min();
        _ms_metadata.max_timestamp = timestamp_tracker.max();
    }

    // This consumer will perform mutation compaction on producer side using
    // compacting_reader. It's useful for allowing data from different buckets
    // to be compacted together.
    future<> consume_without_gc_writer(gc_clock::time_point compaction_time) {
        auto consumer = make_interposer_consumer([this] (mutation_reader reader) mutable {
            return seastar::async([this, reader = std::move(reader)] () mutable {
                auto close_reader = deferred_close(reader);
                auto cfc = get_compacted_fragments_writer();
                reader.consume_in_thread(std::move(cfc));
            });
        });
        const auto& gc_state = _table_s.get_tombstone_gc_state();
        return consumer(make_compacting_reader(setup_sstable_reader(), compaction_time, max_purgeable_func(), gc_state));
    }

    future<> consume() {
        auto now = gc_clock::now();
        // consume_without_gc_writer(), which uses compacting_reader, is ~3% slower.
        // let's only use it when GC writer is disabled and interposer consumer is enabled, as we
        // wouldn't like others to pay the penalty for something they don't need.
        if (!enable_garbage_collected_sstable_writer() && use_interposer_consumer()) {
            return consume_without_gc_writer(now);
        }
        auto consumer = make_interposer_consumer([this, now] (mutation_reader reader) mutable
        {
            return seastar::async([this, reader = std::move(reader), now] () mutable {
                auto close_reader = deferred_close(reader);

                if (enable_garbage_collected_sstable_writer()) {
                    using compact_mutations = compact_for_compaction_v2<compacted_fragments_writer, compacted_fragments_writer>;
                    auto cfc = compact_mutations(*schema(), now,
                        max_purgeable_func(),
                        _table_s.get_tombstone_gc_state(),
                        get_compacted_fragments_writer(),
                        get_gc_compacted_fragments_writer());

                    reader.consume_in_thread(std::move(cfc));
                    return;
                }
                using compact_mutations = compact_for_compaction_v2<compacted_fragments_writer, noop_compacted_fragments_consumer>;
                auto cfc = compact_mutations(*schema(), now,
                    max_purgeable_func(),
                    _table_s.get_tombstone_gc_state(),
                    get_compacted_fragments_writer(),
                    noop_compacted_fragments_consumer());
                reader.consume_in_thread(std::move(cfc));
            });
        });
        return consumer(setup_sstable_reader());
    }

    virtual reader_consumer_v2 make_interposer_consumer(reader_consumer_v2 end_consumer) {
        return _table_s.get_compaction_strategy().make_interposer_consumer(_ms_metadata, std::move(end_consumer));
    }

    virtual bool use_interposer_consumer() const {
        return _table_s.get_compaction_strategy().use_interposer_consumer();
    }
protected:
    virtual compaction_result finish(std::chrono::time_point<db_clock> started_at, std::chrono::time_point<db_clock> ended_at) {
        compaction_result ret {
            .new_sstables = std::move(_all_new_sstables),
            .stats {
                .ended_at = ended_at,
                .start_size = _start_size,
                .end_size = _end_size,
                .bloom_filter_checks = _bloom_filter_checks,
            },
        };

        auto ratio = double(_end_size) / double(_start_size);
        auto duration = std::chrono::duration<float>(ended_at - started_at);
        // Don't report NaN or negative number.

        on_end_of_compaction();

        // FIXME: there is some missing information in the log message below.
        // look at CompactionTask::runMayThrow() in origin for reference.
        // - add support to merge summary (message: Partition merge counts were {%s}.).
        // - there is no easy way, currently, to know the exact number of total partitions.
        // By the time being, using estimated key count.
        log_info("{} {} sstables to [{}]. {} to {} (~{}% of original) in {}ms = {}. ~{} total partitions merged to {}.",
                report_finish_desc(), _input_sstable_generations.size(),
                fmt::join(ret.new_sstables | boost::adaptors::transformed([] (auto sst) { return to_string(sst, false); }), ","),
                utils::pretty_printed_data_size(_start_size), utils::pretty_printed_data_size(_end_size), int(ratio * 100),
                std::chrono::duration_cast<std::chrono::milliseconds>(duration).count(), utils::pretty_printed_throughput(_start_size, duration),
                _cdata.total_partitions, _cdata.total_keys_written);

        return ret;
    }
private:
    void on_interrupt(std::exception_ptr ex) {
        log_info("{} of {} sstables interrupted due to: {}, at {}", report_start_desc(), _input_sstable_generations.size(), ex, current_backtrace());
        delete_sstables_for_interrupted_compaction();
    }

    virtual std::string_view report_start_desc() const = 0;
    virtual std::string_view report_finish_desc() const = 0;

    std::function<api::timestamp_type(const dht::decorated_key&)> max_purgeable_func() {
        if (!tombstone_expiration_enabled()) {
            return [] (const dht::decorated_key& dk) {
                return api::min_timestamp;
            };
        }
        return [this] (const dht::decorated_key& dk) {
            return get_max_purgeable_timestamp(_table_s, *_selector, _compacting_for_max_purgeable_func, dk, _bloom_filter_checks, _compacting_max_timestamp);
        };
    }

    virtual void on_new_partition() {}

    virtual void on_end_of_compaction() {};

    // create a writer based on decorated key.
    virtual compaction_writer create_compaction_writer(const dht::decorated_key& dk) = 0;
    // stop current writer
    virtual void stop_sstable_writer(compaction_writer* writer) = 0;

    compacted_fragments_writer get_compacted_fragments_writer() {
        return compacted_fragments_writer(*this,
            [this] (const dht::decorated_key& dk) { return create_compaction_writer(dk); },
            [this] (compaction_writer* cw) { stop_sstable_writer(cw); });
    }

    const schema_ptr& schema() const {
        return _schema;
    }

    void delete_sstables_for_interrupted_compaction() {
        // Delete either partially or fully written sstables of a compaction that
        // was either stopped abruptly (e.g. out of disk space) or deliberately
        // (e.g. nodetool stop COMPACTION).
        for (auto& sst : boost::range::join(_new_partial_sstables, _new_unused_sstables)) {
            log_debug("Deleting sstable {} of interrupted compaction for {}.{}", sst->get_filename(), _schema->ks_name(), _schema->cf_name());
            sst->mark_for_deletion();
        }
    }
protected:
    template <typename... Args>
    void log(log_level level, std::string_view fmt, const Args&... args) const {
        if (clogger.is_enabled(level)) {
            auto msg = fmt::format(fmt::runtime(fmt), args...);
            clogger.log(level, "[{} {}.{} {}] {}", _type, _schema->ks_name(), _schema->cf_name(), _cdata.compaction_uuid, msg);
        }
    }

    template <typename... Args>
    void log_error(std::string_view fmt, Args&&... args) const {
        log(log_level::error, std::move(fmt), std::forward<Args>(args)...);
    }

    template <typename... Args>
    void log_warning(std::string_view fmt, Args&&... args) const {
        log(log_level::warn, std::move(fmt), std::forward<Args>(args)...);
    }

    template <typename... Args>
    void log_info(std::string_view fmt, Args&&... args) const {
        log(log_level::info, std::move(fmt), std::forward<Args>(args)...);
    }

    template <typename... Args>
    void log_debug(std::string_view fmt, Args&&... args) const {
        log(log_level::debug, std::move(fmt), std::forward<Args>(args)...);
    }

    template <typename... Args>
    void log_trace(std::string_view fmt, Args&&... args) const {
        log(log_level::trace, std::move(fmt), std::forward<Args>(args)...);
    }
public:
    static future<compaction_result> run(std::unique_ptr<compaction> c);

    friend class compacted_fragments_writer;
};

compacted_fragments_writer::compacted_fragments_writer(compacted_fragments_writer&& other)
        : _c(other._c)
        , _compaction_writer(std::move(other._compaction_writer))
        , _create_compaction_writer(std::move(other._create_compaction_writer))
        , _stop_compaction_writer(std::move(other._stop_compaction_writer)) {
    if (std::exchange(other._stop_request_observer, std::nullopt)) {
        _stop_request_observer = make_stop_request_observer(_c._stop_request_observable);
    }
}

void compacted_fragments_writer::maybe_abort_compaction() {
    if (_c._cdata.is_stop_requested()) [[unlikely]] {
        // Compaction manager will catch this exception and re-schedule the compaction.
        throw compaction_stopped_exception(_c._schema->ks_name(), _c._schema->cf_name(), _c._cdata.stop_requested);
    }
}

void compacted_fragments_writer::stop_current_writer() {
    // stop sstable writer being currently used.
    _stop_compaction_writer(&*_compaction_writer);
    _compaction_writer = std::nullopt;
}

bool compacted_fragments_writer::can_split_large_partition() const {
    return _c._can_split_large_partition;
}

void compacted_fragments_writer::track_last_position(position_in_partition_view pos) {
    if (can_split_large_partition()) {
        _current_partition.last_pos = pos;
    }
}

void compacted_fragments_writer::split_large_partition() {
    // Closes the active range tombstone if needed, before emitting partition end.
    // after_key(last_pos) is used for both closing and re-opening the active tombstone, which
    // will result in current fragment storing an inclusive end bound for last pos, and the
    // next fragment storing an exclusive start bound for last pos. This is very important
    // for not losing information on the range tombstone.
    auto after_last_pos = position_in_partition::after_key(*_c.schema(), _current_partition.last_pos.key());
    if (_current_partition.current_emitted_tombstone) {
        auto rtc = range_tombstone_change(after_last_pos, tombstone{});
        _c.log_debug("Closing active tombstone {} with {} for partition {}", _current_partition.current_emitted_tombstone, rtc, *_current_partition.dk);
        _compaction_writer->writer.consume(std::move(rtc));
    }
    _c.log_debug("Splitting large partition {} in order to respect SSTable size limit of {}", *_current_partition.dk, utils::pretty_printed_data_size(_c._max_sstable_size));
    // Close partition in current writer, and open it again in a new writer.
    do_consume_end_of_partition();
    stop_current_writer();
    do_consume_new_partition(*_current_partition.dk);
    // Replicate partition tombstone to every fragment, allowing the SSTable run reader
    // to open a single fragment during the read.
    if (_current_partition.tombstone) {
        consume(_current_partition.tombstone);
    }
    if (_current_partition.current_emitted_tombstone) {
        _compaction_writer->writer.consume(range_tombstone_change(after_last_pos, _current_partition.current_emitted_tombstone));
    }
    _current_partition.is_splitting_partition = false;
}

void compacted_fragments_writer::do_consume_new_partition(const dht::decorated_key& dk) {
    maybe_abort_compaction();
    if (!_compaction_writer) {
        _compaction_writer = _create_compaction_writer(dk);
    }

    _c.on_new_partition();
    _compaction_writer->writer.consume_new_partition(dk);
    _unclosed_partition = true;
}

stop_iteration compacted_fragments_writer::do_consume_end_of_partition() {
    _unclosed_partition = false;
    return _compaction_writer->writer.consume_end_of_partition();
}

void compacted_fragments_writer::consume_new_partition(const dht::decorated_key& dk) {
    _current_partition = {
        .dk = dk,
        .tombstone = tombstone(),
        .current_emitted_tombstone = tombstone(),
        .last_pos = position_in_partition::for_partition_start(),
        .is_splitting_partition = false
    };
    do_consume_new_partition(dk);
    _c._cdata.total_keys_written++;
}

void compacted_fragments_writer::consume(tombstone t) {
    _current_partition.tombstone = t;
    _compaction_writer->writer.consume(t);
}

stop_iteration compacted_fragments_writer::consume(clustering_row&& cr, row_tombstone, bool) {
    maybe_abort_compaction();
    if (_current_partition.is_splitting_partition) [[unlikely]] {
        split_large_partition();
    }
    track_last_position(cr.position());
    auto ret = _compaction_writer->writer.consume(std::move(cr));
    if (can_split_large_partition() && ret == stop_iteration::yes) [[unlikely]] {
        _current_partition.is_splitting_partition = true;
    }
    return stop_iteration::no;
}

stop_iteration compacted_fragments_writer::consume(range_tombstone_change&& rtc) {
    maybe_abort_compaction();
    _current_partition.current_emitted_tombstone = rtc.tombstone();
    track_last_position(rtc.position());
    return _compaction_writer->writer.consume(std::move(rtc));
}

stop_iteration compacted_fragments_writer::consume_end_of_partition() {
    auto ret = do_consume_end_of_partition();
    if (ret == stop_iteration::yes) {
        stop_current_writer();
    }
    return ret;
}

void compacted_fragments_writer::consume_end_of_stream() {
    if (_compaction_writer) {
        _stop_compaction_writer(&*_compaction_writer);
        _compaction_writer = std::nullopt;
    }
}

class regular_compaction : public compaction {
    seastar::semaphore _replacer_lock = {1};
public:
    regular_compaction(table_state& table_s, compaction_descriptor descriptor, compaction_data& cdata, compaction_progress_monitor& progress_monitor, use_backlog_tracker use_backlog_tracker = use_backlog_tracker::yes)
        : compaction(table_s, std::move(descriptor), cdata, progress_monitor, use_backlog_tracker)
    {
    }

    mutation_reader make_sstable_reader(schema_ptr s,
                                                reader_permit permit,
                                                const dht::partition_range& range,
                                                const query::partition_slice& slice,
                                                tracing::trace_state_ptr trace,
                                                streamed_mutation::forwarding sm_fwd,
                                                mutation_reader::forwarding mr_fwd) const override {
        return _compacting->make_local_shard_sstable_reader(std::move(s),
                std::move(permit),
                range,
                slice,
                std::move(trace),
                sm_fwd,
                mr_fwd,
                unwrap_monitor_generator());
    }

    std::string_view report_start_desc() const override {
        return "Compacting";
    }

    std::string_view report_finish_desc() const override {
        return "Compacted";
    }

    virtual compaction_writer create_compaction_writer(const dht::decorated_key& dk) override {
        auto sst = _sstable_creator(this_shard_id());
        setup_new_sstable(sst);

        auto monitor = std::make_unique<compaction_write_monitor>(sst, _table_s, maximum_timestamp(), _sstable_level);
        sstable_writer_config cfg = make_sstable_writer_config(_type);
        cfg.monitor = monitor.get();
        return compaction_writer{std::move(monitor), sst->get_writer(*_schema, partitions_per_sstable(), cfg, get_encoding_stats()), sst};
    }

    virtual void stop_sstable_writer(compaction_writer* writer) override {
        if (writer) {
            finish_new_sstable(writer);
            maybe_replace_exhausted_sstables_by_sst(writer->sst);
        }
    }

    void on_new_partition() override {
        update_pending_ranges();
    }

    virtual void on_end_of_compaction() override {
        replace_remaining_exhausted_sstables();
    }
private:
    void maybe_replace_exhausted_sstables_by_sst(shared_sstable sst) {
        // Skip earlier replacement of exhausted sstables if compaction works with only single-fragment runs,
        // meaning incremental compaction is disabled for this compaction.
        if (!enable_garbage_collected_sstable_writer()) {
            return;
        }
        auto permit = seastar::get_units(_replacer_lock, 1).get();
        // Replace exhausted sstable(s), if any, by new one(s) in the column family.
        auto not_exhausted = [s = _schema, &dk = sst->get_last_decorated_key()] (shared_sstable& sst) {
            return sst->get_last_decorated_key().tri_compare(*s, dk) > 0;
        };
        auto exhausted = std::partition(_sstables.begin(), _sstables.end(), not_exhausted);

        if (exhausted != _sstables.end()) {
            // The goal is that exhausted sstables will be deleted as soon as possible,
            // so we need to release reference to them.
            std::for_each(exhausted, _sstables.end(), [this] (shared_sstable& sst) {
                _compacting_for_max_purgeable_func.erase(sst);
                // Fully expired sstable is not actually compacted, therefore it's not present in the compacting set.
                _compacting->erase(sst);
            });
            // Make sure SSTable created by garbage collected writer is made available
            // before exhausted SSTable is released, so to prevent data resurrection.
            _stop_request_observable();

            // Added Garbage collected SSTables to list of unused SSTables that will be added
            // to SSTable set. GC SSTables should be added before compaction completes because
            // a failure could result in data resurrection if data is not made available.
            auto unused_gc_sstables = consume_unused_garbage_collected_sstables();
            _new_unused_sstables.insert(_new_unused_sstables.end(), unused_gc_sstables.begin(), unused_gc_sstables.end());

            auto exhausted_ssts = std::vector<shared_sstable>(exhausted, _sstables.end());
            log_debug("Replacing earlier exhausted sstable(s) [{}] by new sstable(s) [{}]",
                fmt::join(exhausted_ssts | boost::adaptors::transformed([] (auto sst) { return to_string(sst, false); }), ","),
                fmt::join(_new_unused_sstables | boost::adaptors::transformed([] (auto sst) { return to_string(sst, true); }), ","));
            _replacer(get_compaction_completion_desc(exhausted_ssts, std::move(_new_unused_sstables)));
            _sstables.erase(exhausted, _sstables.end());
            dynamic_cast<compaction_read_monitor_generator&>(unwrap_monitor_generator()).remove_exhausted_sstables(exhausted_ssts);
        }
    }

    void replace_remaining_exhausted_sstables() {
        if (!_sstables.empty() || !used_garbage_collected_sstables().empty()) {
            std::vector<shared_sstable> old_sstables;
            std::move(_sstables.begin(), _sstables.end(), std::back_inserter(old_sstables));

            // Remove Garbage Collected SSTables from the SSTable set if any was previously added.
            auto& used_gc_sstables = used_garbage_collected_sstables();
            old_sstables.insert(old_sstables.end(), used_gc_sstables.begin(), used_gc_sstables.end());

            _replacer(get_compaction_completion_desc(std::move(old_sstables), std::move(_new_unused_sstables)));
         }
    }

    void update_pending_ranges() {
        auto pending_replacements = std::exchange(_cdata.pending_replacements, {});
        if (!_sstable_set || _sstable_set->all()->empty() || pending_replacements.empty()) { // set can be empty for testing scenario.
            return;
        }
        // Releases reference to sstables compacted by this compaction or another, both of which belongs
        // to the same column family
        for (auto& pending_replacement : pending_replacements) {
            for (auto& sst : pending_replacement.removed) {
                // Set may not contain sstable to be removed because this compaction may have started
                // before the creation of that sstable.
                if (!_sstable_set->all()->contains(sst)) {
                    continue;
                }
                _sstable_set->erase(sst);
            }
            for (auto& sst : pending_replacement.added) {
                _sstable_set->insert(sst);
            }
        }
        _selector.emplace(_sstable_set->make_incremental_selector());
    }
};

class reshape_compaction : public regular_compaction {
private:
    bool has_sstable_replacer() const noexcept {
        return bool(_replacer);
    }
public:
    reshape_compaction(table_state& table_s, compaction_descriptor descriptor, compaction_data& cdata, compaction_progress_monitor& progress_monitor)
        : regular_compaction(table_s, std::move(descriptor), cdata, progress_monitor, use_backlog_tracker::no) {
    }

    virtual sstables::sstable_set make_sstable_set_for_input() const override {
        return sstables::make_partitioned_sstable_set(_schema, false);
    }

    // Unconditionally enable incremental compaction if the strategy specifies a max output size, e.g. LCS.
    virtual bool enable_garbage_collected_sstable_writer() const noexcept override {
        return _max_sstable_size != std::numeric_limits<uint64_t>::max() && bool(_replacer);
    }

    mutation_reader make_sstable_reader(schema_ptr s,
                                                reader_permit permit,
                                                const dht::partition_range& range,
                                                const query::partition_slice& slice,
                                                tracing::trace_state_ptr trace,
                                                streamed_mutation::forwarding sm_fwd,
                                                mutation_reader::forwarding mr_fwd) const override {
        return _compacting->make_local_shard_sstable_reader(std::move(s),
                std::move(permit),
                range,
                slice,
                std::move(trace),
                sm_fwd,
                mr_fwd,
                unwrap_monitor_generator());
    }

    std::string_view report_start_desc() const override {
        return "Reshaping";
    }

    std::string_view report_finish_desc() const override {
        return "Reshaped";
    }

    virtual compaction_writer create_compaction_writer(const dht::decorated_key& dk) override {
        auto sst = _sstable_creator(this_shard_id());
        setup_new_sstable(sst);

        sstable_writer_config cfg = make_sstable_writer_config(compaction_type::Reshape);
        return compaction_writer{sst->get_writer(*_schema, partitions_per_sstable(), cfg, get_encoding_stats()), sst};
    }

    virtual void stop_sstable_writer(compaction_writer* writer) override {
        if (writer) {
            if (has_sstable_replacer()) {
                regular_compaction::stop_sstable_writer(writer);
            } else {
                finish_new_sstable(writer);
            }
        }
    }

    virtual void on_end_of_compaction() override {
        if (has_sstable_replacer()) {
            regular_compaction::on_end_of_compaction();
        }
    }
};

class cleanup_compaction final : public regular_compaction {
public:
    cleanup_compaction(table_state& table_s, compaction_descriptor descriptor, compaction_data& cdata, compaction_progress_monitor& progress_monitor)
        : regular_compaction(table_s, std::move(descriptor), cdata, progress_monitor)
    {
    }

    std::string_view report_start_desc() const override {
        return "Cleaning";
    }

    std::string_view report_finish_desc() const override {
        return "Cleaned";
    }
};

class split_compaction final : public regular_compaction {
    compaction_type_options::split _options;
public:
    split_compaction(table_state& table_s, compaction_descriptor descriptor, compaction_data& cdata, compaction_type_options::split options,
                         compaction_progress_monitor& progress_monitor)
            : regular_compaction(table_s, std::move(descriptor), cdata, progress_monitor)
            , _options(std::move(options))
    {
    }

    reader_consumer_v2 make_interposer_consumer(reader_consumer_v2 end_consumer) override {
        return [this, end_consumer = std::move(end_consumer)] (mutation_reader reader) mutable -> future<> {
            return mutation_writer::segregate_by_token_group(std::move(reader),
                    _options.classifier,
                    _table_s.get_compaction_strategy().make_interposer_consumer(_ms_metadata, std::move(end_consumer)));
        };
    }

    bool use_interposer_consumer() const override {
        return true;
    }

    std::string_view report_start_desc() const override {
        return "Splitting";
    }

    std::string_view report_finish_desc() const override {
        return "Split";
    }
};

class scrub_compaction final : public regular_compaction {
public:
    static void report_validation_error(compaction_type type, const ::schema& schema, sstring what, std::string_view action = "") {
        clogger.error("[{} compaction {}.{}] {}{}{}", type, schema.ks_name(), schema.cf_name(), what, action.empty() ? "" : "; ", action);
    }

private:

    class reader : public mutation_reader::impl {
        using skip = bool_class<class skip_tag>;
    private:
        compaction_type_options::scrub::mode _scrub_mode;
        mutation_reader _reader;
        mutation_fragment_stream_validator _validator;
        bool _skip_to_next_partition = false;
        uint64_t& _validation_errors;

    private:
        void maybe_abort_scrub(std::function<void()> report_error) {
            if (_scrub_mode == compaction_type_options::scrub::mode::abort) {
                report_error();
                throw compaction_aborted_exception(_schema->ks_name(), _schema->cf_name(), "scrub compaction found invalid data");
            }
            ++_validation_errors;
        }

        void on_unexpected_partition_start(const mutation_fragment_v2& ps, sstring error) {
            auto report_fn = [this, error] (std::string_view action = "") {
                report_validation_error(compaction_type::Scrub, *_schema, error, action);
            };
            maybe_abort_scrub(report_fn);
            report_fn("Rectifying by adding assumed missing partition-end");

            auto pe = mutation_fragment_v2(*_schema, _permit, partition_end{});
            if (!_validator(pe)) {
                throw compaction_aborted_exception(
                        _schema->ks_name(),
                        _schema->cf_name(),
                        "scrub compaction failed to rectify unexpected partition-start, validator rejects the injected partition-end");
            }
            push_mutation_fragment(std::move(pe));

            if (!_validator(ps)) {
                throw compaction_aborted_exception(
                        _schema->ks_name(),
                        _schema->cf_name(),
                        "scrub compaction failed to rectify unexpected partition-start, validator rejects it even after the injected partition-end");
            }
        }

        skip on_invalid_partition(const dht::decorated_key& new_key, sstring error) {
            auto report_fn = [this, error] (std::string_view action = "") {
                report_validation_error(compaction_type::Scrub, *_schema, error, action);
            };
            maybe_abort_scrub(report_fn);
            if (_scrub_mode == compaction_type_options::scrub::mode::segregate) {
                report_fn("Detected");
                _validator.reset(new_key);
                // Let the segregating interposer consumer handle this.
                return skip::no;
            }
            report_fn("Skipping");
            _skip_to_next_partition = true;
            return skip::yes;
        }

        skip on_invalid_mutation_fragment(const mutation_fragment_v2& mf, sstring error) {
            auto report_fn = [this, error] (std::string_view action = "") {
                report_validation_error(compaction_type::Scrub, *_schema, error, action);
            };
            maybe_abort_scrub(report_fn);

            const auto& key = _validator.previous_partition_key();

            if (_validator.current_tombstone()) {
                throw compaction_aborted_exception(
                        _schema->ks_name(),
                        _schema->cf_name(),
                        "scrub compaction cannot handle invalid fragments with an active range tombstone change");
            }

            // If the unexpected fragment is a partition end, we just drop it.
            // The only case a partition end is invalid is when it comes after
            // another partition end, and we can just drop it in that case.
            if (!mf.is_end_of_partition() && _scrub_mode == compaction_type_options::scrub::mode::segregate) {
                report_fn("Injecting partition start/end to segregate out-of-order fragment");
                push_mutation_fragment(*_schema, _permit, partition_end{});

                // We loose the partition tombstone if any, but it will be
                // picked up when compaction merges these partitions back.
                push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, partition_start(key, {})));

                _validator.reset(mf);

                // Let the segregating interposer consumer handle this.
                return skip::no;
            }

            report_fn("Skipping");

            return skip::yes;
        }

        void on_invalid_end_of_stream(sstring error) {
            auto report_fn = [this, error] (std::string_view action = "") {
                report_validation_error(compaction_type::Scrub, *_schema, error, action);
            };
            maybe_abort_scrub(report_fn);
            // Handle missing partition_end
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, partition_end{}));
            report_fn("Rectifying by adding missing partition-end to the end of the stream");
        }

        void fill_buffer_from_underlying() {
            utils::get_local_injector().inject("rest_api_keyspace_scrub_abort", [] { throw compaction_aborted_exception("", "", "scrub compaction found invalid data"); });
            while (!_reader.is_buffer_empty() && !is_buffer_full()) {
                auto mf = _reader.pop_mutation_fragment();
                if (mf.is_partition_start()) {
                    // First check that fragment kind monotonicity stands.
                    // When skipping to another partition the fragment
                    // monotonicity of the partition-start doesn't have to be
                    // and shouldn't be verified. We know the last fragment the
                    // validator saw is a partition-start, passing it another one
                    // will confuse it.
                    if (!_skip_to_next_partition) {
                        if (auto res = _validator(mf); !res) {
                            on_unexpected_partition_start(mf, res.what());
                        }
                        // Continue processing this partition start.
                    }
                    _skip_to_next_partition = false;
                    // Then check that the partition monotonicity stands.
                    const auto& dk = mf.as_partition_start().key();
                    if (auto res = _validator(dk); !res) {
                        if (on_invalid_partition(dk, res.what()) == skip::yes) {
                            continue;
                        }
                    }
                } else if (_skip_to_next_partition) {
                    continue;
                } else {
                    if (auto res = _validator(mf); !res) {
                        if (on_invalid_mutation_fragment(mf, res.what()) == skip::yes) {
                            continue;
                        }
                    }
                }
                push_mutation_fragment(std::move(mf));
            }

            _end_of_stream = _reader.is_end_of_stream() && _reader.is_buffer_empty();

            if (_end_of_stream) {
                if (auto res = _validator.on_end_of_stream(); !res) {
                    on_invalid_end_of_stream(res.what());
                }
            }
        }

    public:
        reader(mutation_reader underlying, compaction_type_options::scrub::mode scrub_mode, uint64_t& validation_errors)
            : impl(underlying.schema(), underlying.permit())
            , _scrub_mode(scrub_mode)
            , _reader(std::move(underlying))
            , _validator(*_schema)
            , _validation_errors(validation_errors)
        { }
        virtual future<> fill_buffer() override {
            if (_end_of_stream) {
                return make_ready_future<>();
            }
            return repeat([this] {
                return _reader.fill_buffer().then([this] {
                    fill_buffer_from_underlying();
                    return stop_iteration(is_buffer_full() || _end_of_stream);
                });
            }).handle_exception([this] (std::exception_ptr e) {
                try {
                    std::rethrow_exception(std::move(e));
                } catch (const compaction_job_exception&) {
                    // Propagate these unchanged.
                    throw;
                } catch (const storage_io_error&) {
                    // Propagate these unchanged.
                    throw;
                } catch (...) {
                    // We don't want failed scrubs to be retried.
                    throw compaction_aborted_exception(
                            _schema->ks_name(),
                            _schema->cf_name(),
                            format("scrub compaction failed due to unrecoverable error: {}", std::current_exception()));
                }
            });
        }
        virtual future<> next_partition() override {
            return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
        }
        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
        }
        virtual future<> fast_forward_to(position_range pr) override {
            return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
        }
        virtual future<> close() noexcept override {
            return _reader.close();
        }
    };

private:
    compaction_type_options::scrub _options;
    std::string _scrub_start_description;
    mutable std::string _scrub_finish_description;
    uint64_t _bucket_count = 0;
    mutable uint64_t _validation_errors = 0;

public:
    scrub_compaction(table_state& table_s, compaction_descriptor descriptor, compaction_data& cdata, compaction_type_options::scrub options, compaction_progress_monitor& progress_monitor)
        : regular_compaction(table_s, std::move(descriptor), cdata, progress_monitor, use_backlog_tracker::no)
        , _options(options)
        , _scrub_start_description(fmt::format("Scrubbing in {} mode", _options.operation_mode))
        , _scrub_finish_description(fmt::format("Finished scrubbing in {} mode", _options.operation_mode)) {
    }

    std::string_view report_start_desc() const override {
        return _scrub_start_description;
    }

    std::string_view report_finish_desc() const override {
        if (_options.operation_mode == compaction_type_options::scrub::mode::segregate) {
            _scrub_finish_description = fmt::format("Finished scrubbing in {} mode{}", _options.operation_mode, _bucket_count ? fmt::format(" (segregated input into {} bucket(s))", _bucket_count) : "");
        }
        return _scrub_finish_description;
    }

    mutation_reader make_sstable_reader(schema_ptr s,
                                                reader_permit permit,
                                                const dht::partition_range& range,
                                                const query::partition_slice& slice,
                                                tracing::trace_state_ptr trace,
                                                streamed_mutation::forwarding sm_fwd,
                                                mutation_reader::forwarding mr_fwd) const override {
        if (!range.is_full()) {
            on_internal_error(clogger, fmt::format("Scrub compaction in mode {} expected full partition range, but got {} instead", _options.operation_mode, range));
        }
        auto crawling_reader = _compacting->make_crawling_reader(std::move(s), std::move(permit), nullptr, unwrap_monitor_generator());
        return make_mutation_reader<reader>(std::move(crawling_reader), _options.operation_mode, _validation_errors);
    }

    uint64_t partitions_per_sstable() const override {
        const auto original_estimate = compaction::partitions_per_sstable();
        if (_bucket_count <= 1) {
            return original_estimate;
        } else {
            const auto shift = std::min(uint64_t(63), _bucket_count - 1);
            return std::max(uint64_t(1), original_estimate >> shift);
        }
    }

    reader_consumer_v2 make_interposer_consumer(reader_consumer_v2 end_consumer) override {
        if (!use_interposer_consumer()) {
            return end_consumer;
        }
        return [this, end_consumer = std::move(end_consumer)] (mutation_reader reader) mutable -> future<> {
            auto cfg = mutation_writer::segregate_config{memory::stats().total_memory() / 10};
            return mutation_writer::segregate_by_partition(std::move(reader), cfg,
                    [consumer = std::move(end_consumer), this] (mutation_reader rd) {
                ++_bucket_count;
                return consumer(std::move(rd));
            });
        };
    }

    bool use_interposer_consumer() const override {
        return _options.operation_mode == compaction_type_options::scrub::mode::segregate;
    }

    compaction_result finish(std::chrono::time_point<db_clock> started_at, std::chrono::time_point<db_clock> ended_at) override {
        auto ret = compaction::finish(started_at, ended_at);
        ret.stats.validation_errors = _validation_errors;
        return ret;
    }

    friend mutation_reader make_scrubbing_reader(mutation_reader rd, compaction_type_options::scrub::mode scrub_mode, uint64_t& validation_errors);
};

mutation_reader make_scrubbing_reader(mutation_reader rd, compaction_type_options::scrub::mode scrub_mode, uint64_t& validation_errors) {
    return make_mutation_reader<scrub_compaction::reader>(std::move(rd), scrub_mode, validation_errors);
}

class resharding_compaction final : public compaction {
    // Partition count estimation for a shard S:
    //
    // TE, the total estimated partition count for a shard S, is defined as
    // TE = Sum(i = 0...N) { Ei / Si }.
    //
    // where i is an input sstable that belongs to shard S,
    //       Ei is the estimated partition count for sstable i,
    //       Si is the total number of shards that own sstable i.
    //
    struct estimated_values {
        uint64_t estimated_size = 0;
        uint64_t estimated_partitions = 0;
    };
    std::vector<estimated_values> _estimation_per_shard;
    std::vector<run_id> _run_identifiers;
private:
    // return estimated partitions per sstable for a given shard
    uint64_t partitions_per_sstable(shard_id s) const {
        uint64_t estimated_sstables = std::max(uint64_t(1), uint64_t(ceil(double(_estimation_per_shard[s].estimated_size) / _max_sstable_size)));
        return std::min(uint64_t(ceil(double(_estimation_per_shard[s].estimated_partitions) / estimated_sstables)),
                _table_s.get_compaction_strategy().adjust_partition_estimate(_ms_metadata, _estimation_per_shard[s].estimated_partitions, _schema));
    }
public:
    resharding_compaction(table_state& table_s, sstables::compaction_descriptor descriptor, compaction_data& cdata, compaction_progress_monitor& progress_monitor)
        : compaction(table_s, std::move(descriptor), cdata, progress_monitor, use_backlog_tracker::no)
        , _estimation_per_shard(smp::count)
        , _run_identifiers(smp::count)
    {
        for (auto& sst : _sstables) {
            const auto& shards = sst->get_shards_for_this_sstable();
            auto size = sst->bytes_on_disk();
            auto estimated_partitions = sst->get_estimated_key_count();
            for (auto& s : shards) {
                _estimation_per_shard[s].estimated_size += std::max(uint64_t(1), uint64_t(ceil(double(size) / shards.size())));
                _estimation_per_shard[s].estimated_partitions += std::max(uint64_t(1), uint64_t(ceil(double(estimated_partitions) / shards.size())));
            }
        }
        for (auto i : boost::irange(0u, smp::count)) {
            _run_identifiers[i] = run_id::create_random_id();
        }
    }

    ~resharding_compaction() { }

    // Use reader that makes sure no non-local mutation will not be filtered out.
    mutation_reader make_sstable_reader(schema_ptr s,
                                                reader_permit permit,
                                                const dht::partition_range& range,
                                                const query::partition_slice& slice,
                                                tracing::trace_state_ptr trace,
                                                streamed_mutation::forwarding sm_fwd,
                                                mutation_reader::forwarding mr_fwd) const override {
        return _compacting->make_range_sstable_reader(std::move(s),
                std::move(permit),
                range,
                slice,
                nullptr,
                sm_fwd,
                mr_fwd,
                unwrap_monitor_generator());

    }

    reader_consumer_v2 make_interposer_consumer(reader_consumer_v2 end_consumer) override {
        return [end_consumer = std::move(end_consumer)] (mutation_reader reader) mutable -> future<> {
            return mutation_writer::segregate_by_shard(std::move(reader), std::move(end_consumer));
        };
    }

    bool use_interposer_consumer() const override {
        return true;
    }

    std::string_view report_start_desc() const override {
        return "Resharding";
    }

    std::string_view report_finish_desc() const override {
        return "Resharded";
    }

    compaction_writer create_compaction_writer(const dht::decorated_key& dk) override {
        auto shards = _sharder->shard_for_writes(dk.token());
        if (shards.size() != 1) {
            // Resharding is not supposed to run on tablets, so this case does not have to be supported.
            on_internal_error(clogger, fmt::format("Got {} shards for token {} in table {}.{}", shards.size(), dk.token(), _schema->ks_name(), _schema->cf_name()));
        }
        auto shard = shards[0];
        auto sst = _sstable_creator(shard);
        setup_new_sstable(sst);

        auto cfg = make_sstable_writer_config(compaction_type::Reshard);
        // sstables generated for a given shard will share the same run identifier.
        cfg.run_identifier = _run_identifiers.at(shard);
        return compaction_writer{sst->get_writer(*_schema, partitions_per_sstable(shard), cfg, get_encoding_stats(), shard), sst};
    }

    void stop_sstable_writer(compaction_writer* writer) override {
        if (writer) {
            finish_new_sstable(writer);
        }
    }
};

future<compaction_result> compaction::run(std::unique_ptr<compaction> c) {
    return seastar::async([c = std::move(c)] () mutable {
        c->setup().get();
        auto consumer = c->consume();

        auto start_time = db_clock::now();
        try {
           consumer.get();
        } catch (...) {
            c->on_interrupt(std::current_exception());
            c = nullptr; // make sure writers are stopped while running in thread context. This is because of calls to file.close().get();
            throw;
        }

        return c->finish(std::move(start_time), db_clock::now());
    });
}

compaction_type compaction_type_options::type() const {
    // Maps options_variant indexes to the corresponding compaction_type member.
    static const compaction_type index_to_type[] = {
        compaction_type::Compaction,
        compaction_type::Cleanup,
        compaction_type::Upgrade,
        compaction_type::Scrub,
        compaction_type::Reshard,
        compaction_type::Reshape,
        compaction_type::Split,
    };
    static_assert(std::variant_size_v<compaction_type_options::options_variant> == std::size(index_to_type));
    return index_to_type[_options.index()];
}

static std::unique_ptr<compaction> make_compaction(table_state& table_s, sstables::compaction_descriptor descriptor, compaction_data& cdata, compaction_progress_monitor& progress_monitor) {
    struct {
        table_state& table_s;
        sstables::compaction_descriptor&& descriptor;
        compaction_data& cdata;
        compaction_progress_monitor& progress_monitor;

        std::unique_ptr<compaction> operator()(compaction_type_options::reshape) {
            return std::make_unique<reshape_compaction>(table_s, std::move(descriptor), cdata, progress_monitor);
        }
        std::unique_ptr<compaction> operator()(compaction_type_options::reshard) {
            return std::make_unique<resharding_compaction>(table_s, std::move(descriptor), cdata, progress_monitor);
        }
        std::unique_ptr<compaction> operator()(compaction_type_options::regular) {
            return std::make_unique<regular_compaction>(table_s, std::move(descriptor), cdata, progress_monitor);
        }
        std::unique_ptr<compaction> operator()(compaction_type_options::cleanup) {
            return std::make_unique<cleanup_compaction>(table_s, std::move(descriptor), cdata, progress_monitor);
        }
        std::unique_ptr<compaction> operator()(compaction_type_options::upgrade) {
            return std::make_unique<cleanup_compaction>(table_s, std::move(descriptor), cdata, progress_monitor);
        }
        std::unique_ptr<compaction> operator()(compaction_type_options::scrub scrub_options) {
            return std::make_unique<scrub_compaction>(table_s, std::move(descriptor), cdata, scrub_options, progress_monitor);
        }
        std::unique_ptr<compaction> operator()(compaction_type_options::split split_options) {
            return std::make_unique<split_compaction>(table_s, std::move(descriptor), cdata, std::move(split_options), progress_monitor);
        }
    } visitor_factory{table_s, std::move(descriptor), cdata, progress_monitor};

    return descriptor.options.visit(visitor_factory);
}

static future<compaction_result> scrub_sstables_validate_mode(sstables::compaction_descriptor descriptor, compaction_data& cdata, table_state& table_s, read_monitor_generator& monitor_generator) {
    auto schema = table_s.schema();
    auto permit = table_s.make_compaction_reader_permit();

    uint64_t validation_errors = 0;
    cdata.compaction_size = boost::accumulate(descriptor.sstables | boost::adaptors::transformed([] (auto& sst) { return sst->data_size(); }), int64_t(0));

    for (const auto& sst : descriptor.sstables) {
        clogger.info("Scrubbing in validate mode {}", sst->get_filename());

        validation_errors += co_await sst->validate(permit, cdata.abort, [&schema] (sstring what) {
            scrub_compaction::report_validation_error(compaction_type::Scrub, *schema, what);
        }, monitor_generator(sst));
        // Did validation actually finish because aborted?
        if (cdata.is_stop_requested()) {
            // Compaction manager will catch this exception and re-schedule the compaction.
            throw compaction_stopped_exception(schema->ks_name(), schema->cf_name(), cdata.stop_requested);
        }

        clogger.info("Finished scrubbing in validate mode {} - sstable is {}", sst->get_filename(), validation_errors == 0 ? "valid" : "invalid");
    }

    using scrub = sstables::compaction_type_options::scrub;
    if (validation_errors != 0 && descriptor.options.as<scrub>().quarantine_sstables == scrub::quarantine_invalid_sstables::yes) {
        for (auto& sst : descriptor.sstables) {
            co_await sst->change_state(sstables::sstable_state::quarantine);
        }
    }

    co_return compaction_result {
        .new_sstables = {},
        .stats = {
            .ended_at = db_clock::now(),
            .validation_errors = validation_errors,
        },
    };
}

future<compaction_result> scrub_sstables_validate_mode(sstables::compaction_descriptor descriptor, compaction_data& cdata, table_state& table_s, compaction_progress_monitor& progress_monitor) {
    progress_monitor.set_generator(std::make_unique<compaction_read_monitor_generator>(table_s, use_backlog_tracker::no));
    auto d = defer([&] { progress_monitor.reset_generator(); });
    auto res = co_await scrub_sstables_validate_mode(descriptor, cdata, table_s, *progress_monitor._generator);
    co_return res;
}

future<compaction_result>
compact_sstables(sstables::compaction_descriptor descriptor, compaction_data& cdata, table_state& table_s, compaction_progress_monitor& progress_monitor) {
    if (descriptor.sstables.empty()) {
        return make_exception_future<compaction_result>(std::runtime_error(format("Called {} compaction with empty set on behalf of {}.{}",
                compaction_name(descriptor.options.type()), table_s.schema()->ks_name(), table_s.schema()->cf_name())));
    }
    if (descriptor.options.type() == compaction_type::Scrub
            && std::get<compaction_type_options::scrub>(descriptor.options.options()).operation_mode == compaction_type_options::scrub::mode::validate) {
        // Bypass the usual compaction machinery for dry-mode scrub
        return scrub_sstables_validate_mode(std::move(descriptor), cdata, table_s, progress_monitor);
    }
    return compaction::run(make_compaction(table_s, std::move(descriptor), cdata, progress_monitor));
}

std::unordered_set<sstables::shared_sstable>
get_fully_expired_sstables(const table_state& table_s, const std::vector<sstables::shared_sstable>& compacting, gc_clock::time_point compaction_time) {
    clogger.debug("Checking droppable sstables in {}.{}", table_s.schema()->ks_name(), table_s.schema()->cf_name());

    if (compacting.empty()) {
        return {};
    }

    std::unordered_set<sstables::shared_sstable> candidates;
    auto uncompacting_sstables = get_uncompacting_sstables(table_s, compacting);
    // Get list of uncompacting sstables that overlap the ones being compacted.
    std::vector<sstables::shared_sstable> overlapping = leveled_manifest::overlapping(*table_s.schema(), compacting, uncompacting_sstables);
    int64_t min_timestamp = std::numeric_limits<int64_t>::max();

    for (auto& sstable : overlapping) {
        auto gc_before = sstable->get_gc_before_for_fully_expire(compaction_time, table_s.get_tombstone_gc_state(), table_s.schema());
        if (sstable->get_max_local_deletion_time() >= gc_before) {
            min_timestamp = std::min(min_timestamp, sstable->get_stats_metadata().min_timestamp);
        }
    }

    auto compacted_undeleted_gens = boost::copy_range<std::unordered_set<generation_type>>(table_s.compacted_undeleted_sstables()
        | boost::adaptors::transformed(std::mem_fn(&sstables::sstable::generation)));
    auto has_undeleted_ancestor = [&compacted_undeleted_gens] (auto& candidate) {
        // Get ancestors from sstable which is empty after restart. It works for this purpose because
        // we only need to check that a sstable compacted *in this instance* hasn't an ancestor undeleted.
        // Not getting it from sstable metadata because mc format hasn't it available.
        return boost::algorithm::any_of(candidate->compaction_ancestors(), [&compacted_undeleted_gens] (const generation_type& gen) {
            return compacted_undeleted_gens.contains(gen);
        });
    };

    // SStables that do not contain live data is added to list of possibly expired sstables.
    for (auto& candidate : compacting) {
        auto gc_before = candidate->get_gc_before_for_fully_expire(compaction_time, table_s.get_tombstone_gc_state(), table_s.schema());
        clogger.debug("Checking if candidate of generation {} and max_deletion_time {} is expired, gc_before is {}",
                    candidate->generation(), candidate->get_stats_metadata().max_local_deletion_time, gc_before);
        // A fully expired sstable which has an ancestor undeleted shouldn't be compacted because
        // expired data won't be purged because undeleted sstables are taken into account when
        // calculating max purgeable timestamp, and not doing it could lead to a compaction loop.
        if (candidate->get_max_local_deletion_time() < gc_before && !has_undeleted_ancestor(candidate)) {
            clogger.debug("Adding candidate of generation {} to list of possibly expired sstables", candidate->generation());
            candidates.insert(candidate);
        } else {
            min_timestamp = std::min(min_timestamp, candidate->get_stats_metadata().min_timestamp);
        }
    }

    auto it = candidates.begin();
    while (it != candidates.end()) {
        auto& candidate = *it;
        // Remove from list any candidate that may contain a tombstone that covers older data.
        if (candidate->get_stats_metadata().max_timestamp >= min_timestamp) {
            it = candidates.erase(it);
        } else {
            clogger.debug("Dropping expired SSTable {} (maxLocalDeletionTime={})",
                    candidate->get_filename(), candidate->get_stats_metadata().max_local_deletion_time);
            it++;
        }
    }
    clogger.debug("Checking droppable sstables in {}.{}, candidates={}", table_s.schema()->ks_name(), table_s.schema()->cf_name(), candidates.size());
    return candidates;
}

unsigned compaction_descriptor::fan_in() const {
    return boost::copy_range<std::unordered_set<run_id>>(sstables | boost::adaptors::transformed(std::mem_fn(&sstables::sstable::run_identifier))).size();
}

uint64_t compaction_descriptor::sstables_size() const {
    return boost::accumulate(sstables | boost::adaptors::transformed(std::mem_fn(&sstables::sstable::data_size)), uint64_t(0));
}

}

auto fmt::formatter<sstables::compaction_type>::format(sstables::compaction_type type, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}", to_string(type));
}

auto fmt::formatter<sstables::compaction_type_options::scrub::mode>::format(sstables::compaction_type_options::scrub::mode mode, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}", to_string(mode));
}

auto fmt::formatter<sstables::compaction_type_options::scrub::quarantine_mode>::format(sstables::compaction_type_options::scrub::quarantine_mode mode, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}", to_string(mode));
}
