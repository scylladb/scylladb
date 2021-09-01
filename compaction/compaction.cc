/*
 * Copyright (C) 2015-present ScyllaDB
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

#include "sstables/sstables.hh"
#include "sstables/sstable_writer.hh"
#include "sstables/progress_monitor.hh"
#include "sstables/sstables_manager.hh"
#include "compaction.hh"
#include "compaction_manager.hh"
#include "database.hh"
#include "mutation_reader.hh"
#include "schema.hh"
#include "db/system_keyspace.hh"
#include "service/priority_manager.hh"
#include "db_clock.hh"
#include "mutation_compactor.hh"
#include "leveled_manifest.hh"
#include "dht/token.hh"
#include "mutation_writer/shard_based_splitting_writer.hh"
#include "mutation_writer/partition_based_splitting_writer.hh"
#include "mutation_source_metadata.hh"
#include "mutation_fragment_stream_validator.hh"
#include "utils/UUID_gen.hh"

namespace sstables {

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

static std::string_view to_string(compaction_type type) {
    switch (type) {
    case compaction_type::Compaction: return "Compact";
    case compaction_type::Cleanup: return "Cleanup";
    case compaction_type::Validation: return "Validate";
    case compaction_type::Scrub: return "Scrub";
    case compaction_type::Index_build: return "Index_build";
    case compaction_type::Reshard: return "Reshard";
    case compaction_type::Upgrade: return "Upgrade";
    case compaction_type::Reshape: return "Reshape";
    }
    on_internal_error_noexcept(clogger, format("Invalid compaction type {}", int(type)));
    return "(invalid)";
}

std::ostream& operator<<(std::ostream& os, compaction_type type) {
    os << to_string(type);
    return os;
}

std::string_view to_string(compaction_options::scrub::mode scrub_mode) {
    switch (scrub_mode) {
        case compaction_options::scrub::mode::abort:
            return "abort";
        case compaction_options::scrub::mode::skip:
            return "skip";
        case compaction_options::scrub::mode::segregate:
            return "segregate";
        case compaction_options::scrub::mode::validate:
            return "validate";
    }
    on_internal_error_noexcept(clogger, format("Invalid scrub mode {}", int(scrub_mode)));
    return "(invalid)";
}

std::ostream& operator<<(std::ostream& os, compaction_options::scrub::mode scrub_mode) {
    return os << to_string(scrub_mode);
}

std::ostream& operator<<(std::ostream& os, pretty_printed_data_size data) {
    static constexpr const char* suffixes[] = { " bytes", "kB", "MB", "GB", "TB", "PB" };

    unsigned exp = 0;
    while ((data._size >= 1000) && (exp < sizeof(suffixes))) {
        exp++;
        data._size /= 1000;
    }

    os << data._size << suffixes[exp];
    return os;
}

std::ostream& operator<<(std::ostream& os, pretty_printed_throughput tp) {
    uint64_t throughput = tp._duration.count() > 0 ? tp._size / tp._duration.count() : 0;
    os << pretty_printed_data_size(throughput) << "/s";
    return os;
}

static api::timestamp_type get_max_purgeable_timestamp(const column_family& cf, sstable_set::incremental_selector& selector,
        const std::unordered_set<shared_sstable>& compacting_set, const dht::decorated_key& dk) {
    auto timestamp = api::max_timestamp;
    std::optional<utils::hashed_key> hk;
    for (auto&& sst : boost::range::join(selector.select(dk).sstables, cf.compacted_undeleted_sstables())) {
        if (compacting_set.contains(sst)) {
            continue;
        }
        if (!hk) {
            hk = sstables::sstable::make_hashed_key(*cf.schema(), dk.key());
        }
        if (sst->filter_has_key(*hk)) {
            timestamp = std::min(timestamp, sst->get_stats_metadata().min_timestamp);
        }
    }
    return timestamp;
}

class incremental_owned_ranges_checker {
    const dht::token_range_vector& _sorted_owned_ranges;
    mutable dht::token_range_vector::const_iterator _it;
public:
    incremental_owned_ranges_checker(const dht::token_range_vector& sorted_owned_ranges)
        : _sorted_owned_ranges(sorted_owned_ranges)
        , _it(_sorted_owned_ranges.begin()) {
    }

    // Must be called with increasing token values.
    bool belongs_to_current_node(const dht::token& t) const {
        // While token T is after a range Rn, advance the iterator.
        // iterator will be stopped at a range which either overlaps with T (if T belongs to node),
        // or at a range which is after T (if T doesn't belong to this node).
        while (_it != _sorted_owned_ranges.end() && _it->after(t, dht::token_comparator())) {
            _it++;
        }

        return _it != _sorted_owned_ranges.end() && _it->contains(t, dht::token_comparator());
    }
};

static std::vector<shared_sstable> get_uncompacting_sstables(column_family& cf, std::vector<shared_sstable> sstables) {
    auto all_sstables = boost::copy_range<std::vector<shared_sstable>>(*cf.get_sstables_including_compacted_undeleted());
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
    column_family& _cf;
    const sstables::writer_offset_tracker* _tracker = nullptr;
    uint64_t _progress_seen = 0;
    api::timestamp_type _maximum_timestamp;
    unsigned _sstable_level;
public:
    compaction_write_monitor(sstables::shared_sstable sst, column_family& cf, api::timestamp_type max_timestamp, unsigned sstable_level)
        : _sst(sst)
        , _cf(cf)
        , _maximum_timestamp(max_timestamp)
        , _sstable_level(sstable_level)
    {}

    ~compaction_write_monitor() {
        if (_sst) {
            _cf.get_compaction_strategy().get_backlog_tracker().revert_charges(_sst);
        }
    }

    virtual void on_write_started(const sstables::writer_offset_tracker& tracker) override {
        _tracker = &tracker;
        _cf.get_compaction_strategy().get_backlog_tracker().register_partially_written_sstable(_sst, *this);
    }

    virtual void on_data_write_completed() override {
        if (_tracker) {
            _progress_seen = _tracker->offset;
            _tracker = nullptr;
        }
    }

    virtual uint64_t written() const {
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

class compacting_sstable_writer {
    compaction& _c;
    std::optional<compaction_writer> _compaction_writer = {};
private:
    inline void maybe_abort_compaction();
public:
    explicit compacting_sstable_writer(compaction& c) : _c(c) { }
    void consume_new_partition(const dht::decorated_key& dk);

    void consume(tombstone t) { _compaction_writer->writer.consume(t); }
    stop_iteration consume(static_row&& sr, tombstone, bool) {
        maybe_abort_compaction();
        return _compaction_writer->writer.consume(std::move(sr));
    }
    stop_iteration consume(clustering_row&& cr, row_tombstone, bool) {
        maybe_abort_compaction();
        return _compaction_writer->writer.consume(std::move(cr));
    }
    stop_iteration consume(range_tombstone&& rt) {
        maybe_abort_compaction();
        return _compaction_writer->writer.consume(std::move(rt));
    }

    stop_iteration consume_end_of_partition();
    void consume_end_of_stream();
};

struct compaction_read_monitor_generator final : public read_monitor_generator {
    class compaction_read_monitor final : public  sstables::read_monitor, public backlog_read_progress_manager {
        sstables::shared_sstable _sst;
        column_family& _cf;
        const sstables::reader_position_tracker* _tracker = nullptr;
        uint64_t _last_position_seen = 0;
    public:
        virtual void on_read_started(const sstables::reader_position_tracker& tracker) override {
            _tracker = &tracker;
            _cf.get_compaction_strategy().get_backlog_tracker().register_compacting_sstable(_sst, *this);
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

        void remove_sstable(bool is_tracking) {
            if (is_tracking && _sst) {
                _cf.get_compaction_strategy().get_backlog_tracker().remove_sstable(_sst);
            } else if (_sst) {
                _cf.get_compaction_strategy().get_backlog_tracker().revert_charges(_sst);
            }
            _sst = {};
        }

        compaction_read_monitor(sstables::shared_sstable sst, column_family &cf)
            : _sst(std::move(sst)), _cf(cf) { }

        ~compaction_read_monitor() {
            // We failed to finish handling this SSTable, so we have to update the backlog_tracker
            // about it.
            if (_sst) {
                _cf.get_compaction_strategy().get_backlog_tracker().revert_charges(_sst);
            }
        }

        friend class compaction_read_monitor_generator;
    };

    virtual sstables::read_monitor& operator()(sstables::shared_sstable sst) override {
        _generated_monitors.emplace_back(std::move(sst), _cf);
        return _generated_monitors.back();
    }

    explicit compaction_read_monitor_generator(column_family& cf)
        : _cf(cf) {}

    void remove_sstables(bool is_tracking) {
        for (auto& rm : _generated_monitors) {
            rm.remove_sstable(is_tracking);
        }
    }

    void remove_sstable(bool is_tracking, sstables::shared_sstable& sst) {
        for (auto& rm : _generated_monitors) {
            if (rm._sst == sst) {
                rm.remove_sstable(is_tracking);
                break;
            }
        }
    }
private:
     column_family& _cf;
     std::deque<compaction_read_monitor> _generated_monitors;
};

// Writes a temporary sstable run containing only garbage collected data.
// Whenever regular compaction writer seals a new sstable, this writer will flush a new sstable as well,
// right before there's an attempt to release exhausted sstables earlier.
// Generated sstables will be temporarily added to table to make sure that a compaction crash will not
// result in data resurrection.
// When compaction finishes, all the temporary sstables generated here will be deleted and removed
// from table's sstable set.
class garbage_collected_sstable_writer {
public:
    // Data for GC writer is stored separately to allow compaction class to communicate directly
    // with garbage_collected_sstable_writer which is moved into mutation_compaction, making it
    // unreachable after the compaction process has started.
    class data {
        compaction* _c = nullptr;
        // Garbage collected sstables that are sealed but were not added to SSTable set yet.
        std::vector<shared_sstable> _unused_garbage_collected_sstables;
        // Garbage collected sstables that were added to SSTable set and should be eventually removed from it.
        std::vector<shared_sstable> _used_garbage_collected_sstables;
        std::optional<compaction_writer> _compaction_writer;
        utils::UUID _run_identifier = utils::make_random_uuid();
        sstable_writer& writer() {
            return _compaction_writer->writer;
        }

    public:
        explicit data(compaction& c) : _c(&c) {
        }

        data& operator=(const data&) = delete;
        data(const data&) = delete;

        void maybe_create_new_sstable_writer();
        void finish_sstable_writer();

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

        friend class garbage_collected_sstable_writer;
    };
private:
    garbage_collected_sstable_writer::data* _data = nullptr;
    sstable_writer& writer() {
        return _data->writer();
    }
public:
    explicit garbage_collected_sstable_writer() = default;
    explicit garbage_collected_sstable_writer(garbage_collected_sstable_writer::data& data) : _data(&data) {}

    garbage_collected_sstable_writer& operator=(const garbage_collected_sstable_writer&) = delete;
    garbage_collected_sstable_writer(const garbage_collected_sstable_writer&) = delete;

    garbage_collected_sstable_writer(garbage_collected_sstable_writer&& other) = default;
    garbage_collected_sstable_writer& operator=(garbage_collected_sstable_writer&& other) = default;

    void consume_new_partition(const dht::decorated_key& dk) {
        _data->maybe_create_new_sstable_writer();
        writer().consume_new_partition(dk);
    }

    void consume(tombstone t) { writer().consume(t); }
    stop_iteration consume(static_row&& sr, tombstone, bool) { return writer().consume(std::move(sr)); }
    stop_iteration consume(clustering_row&& cr, row_tombstone, bool) { return writer().consume(std::move(cr)); }
    stop_iteration consume(range_tombstone&& rt) { return writer().consume(std::move(rt)); }

    stop_iteration consume_end_of_partition() {
        writer().consume_end_of_partition();
        return stop_iteration::no;
    }

    void consume_end_of_stream() {
        _data->finish_sstable_writer();
    }
};

class formatted_sstables_list {
    bool _include_origin = true;
    std::vector<sstring> _ssts;
public:
    formatted_sstables_list() = default;
    explicit formatted_sstables_list(const std::vector<shared_sstable>& ssts, bool include_origin) : _include_origin(include_origin) {
        _ssts.reserve(ssts.size());
        for (const auto& sst : ssts) {
            *this += sst;
        }
    }
    formatted_sstables_list& operator+=(const shared_sstable& sst) {
        if (_include_origin) {
            _ssts.emplace_back(format("{}:level={:d}:origin={}", sst->get_filename(), sst->get_sstable_level(), sst->get_origin()));
        } else {
            _ssts.emplace_back(format("{}:level={:d}", sst->get_filename(), sst->get_sstable_level()));
        }
        return *this;
    }
    friend std::ostream& operator<<(std::ostream& os, const formatted_sstables_list& lst);
};

std::ostream& operator<<(std::ostream& os, const formatted_sstables_list& lst) {
    os << "[";
    os << boost::algorithm::join(lst._ssts, ",");
    os << "]";
    return os;
}

class compaction {
protected:
    column_family& _cf;
    compaction_sstable_creator_fn _sstable_creator;
    schema_ptr _schema;
    reader_permit _permit;
    std::vector<shared_sstable> _sstables;
    // Unused sstables are tracked because if compaction is interrupted we can only delete them.
    // Deleting used sstables could potentially result in data loss.
    std::vector<shared_sstable> _new_unused_sstables;
    lw_shared_ptr<sstable_set> _compacting;
    uint64_t _max_sstable_size;
    uint32_t _sstable_level;
    lw_shared_ptr<compaction_info> _info;
    uint64_t _estimated_partitions = 0;
    std::vector<unsigned long> _ancestors;
    db::replay_position _rp;
    encoding_stats_collector _stats_collector;
    bool _contains_multi_fragment_runs = false;
    mutation_source_metadata _ms_metadata = {};
    garbage_collected_sstable_writer::data _gc_sstable_writer_data;
    compaction_sstable_replacer_fn _replacer;
    utils::UUID _run_identifier;
    ::io_priority_class _io_priority;
    // optional clone of sstable set to be used for expiration purposes, so it will be set if expiration is enabled.
    std::optional<sstable_set> _sstable_set;
    // used to incrementally calculate max purgeable timestamp, as we iterate through decorated keys.
    std::optional<sstable_set::incremental_selector> _selector;
    std::unordered_set<shared_sstable> _compacting_for_max_purgeable_func;
public:
    static lw_shared_ptr<compaction_info> create_compaction_info(column_family& cf, const compaction_descriptor& descriptor) {
        auto info = make_lw_shared<compaction_info>();
        info->ks_name = cf.schema()->ks_name();
        info->cf_name = cf.schema()->cf_name();
        info->type = descriptor.options.type();
        info->run_identifier = descriptor.run_identifier;
        info->cf = &cf;
        info->compaction_uuid = utils::UUID_gen::get_time_UUID();
        return info;
    }

protected:
    compaction(column_family& cf, compaction_descriptor descriptor)
        : _cf(cf)
        , _sstable_creator(std::move(descriptor.creator))
        , _schema(cf.schema())
        , _permit(_cf.compaction_concurrency_semaphore().make_tracking_only_permit(_cf.schema().get(), "compaction", db::no_timeout))
        , _sstables(std::move(descriptor.sstables))
        , _max_sstable_size(descriptor.max_sstable_bytes)
        , _sstable_level(descriptor.level)
        , _info(create_compaction_info(cf, descriptor))
        , _gc_sstable_writer_data(*this)
        , _replacer(std::move(descriptor.replacer))
        , _run_identifier(descriptor.run_identifier)
        , _io_priority(descriptor.io_priority)
        , _sstable_set(std::move(descriptor.all_sstables_snapshot))
        , _selector(_sstable_set ? _sstable_set->make_incremental_selector() : std::optional<sstable_set::incremental_selector>{})
        , _compacting_for_max_purgeable_func(std::unordered_set<shared_sstable>(_sstables.begin(), _sstables.end()))
    {
        for (auto& sst : _sstables) {
            _stats_collector.update(sst->get_encoding_stats_for_compaction());
        }
        std::unordered_set<utils::UUID> ssts_run_ids;
        _contains_multi_fragment_runs = std::any_of(_sstables.begin(), _sstables.end(), [&ssts_run_ids] (shared_sstable& sst) {
            return !ssts_run_ids.insert(sst->run_identifier()).second;
        });
        _cf.get_compaction_manager().register_compaction(_info);
    }

    uint64_t partitions_per_sstable() const {
        // some tests use _max_sstable_size == 0 for force many one partition per sstable
        auto max_sstable_size = std::max<uint64_t>(_max_sstable_size, 1);
        uint64_t estimated_sstables = std::max(1UL, uint64_t(ceil(double(_info->start_size) / max_sstable_size)));
        return std::min(uint64_t(ceil(double(_estimated_partitions) / estimated_sstables)),
                        _cf.get_compaction_strategy().adjust_partition_estimate(_ms_metadata, _estimated_partitions));
    }

    void setup_new_sstable(shared_sstable& sst) {
        _info->new_sstables.push_back(sst);
        _new_unused_sstables.push_back(sst);
        for (auto ancestor : _ancestors) {
            sst->add_ancestor(ancestor);
        }
    }

    void finish_new_sstable(compaction_writer* writer) {
        writer->writer.consume_end_of_stream();
        writer->sst->open_data().get0();
        _info->end_size += writer->sst->bytes_on_disk();
    }

    sstable_writer_config make_sstable_writer_config(compaction_type type) {
        auto s = compaction_name(type);
        std::transform(s.begin(), s.end(), s.begin(), [] (char c) {
            return std::tolower(c);
        });
        sstable_writer_config cfg = _cf.get_sstables_manager().configure_writer(std::move(s));
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

    virtual compaction_completion_desc
    get_compaction_completion_desc(std::vector<shared_sstable> input_sstables, std::vector<shared_sstable> output_sstables) {
        return compaction_completion_desc{std::move(input_sstables), std::move(output_sstables)};
    }

    // Tombstone expiration is enabled based on the presence of sstable set.
    // If it's not present, we cannot purge tombstones without the risk of resurrecting data.
    bool tombstone_expiration_enabled() const {
        return bool(_sstable_set);
    }
public:
    compaction& operator=(const compaction&) = delete;
    compaction(const compaction&) = delete;

    compaction(compaction&& other) = delete;
    compaction& operator=(compaction&& other) = delete;

    virtual ~compaction() {
        if (_info) {
            _cf.get_compaction_manager().deregister_compaction(_info);
        }
    }
private:
    // Default range sstable reader that will only return mutation that belongs to current shard.
    virtual flat_mutation_reader make_sstable_reader() const = 0;

    virtual sstables::sstable_set make_sstable_set_for_input() const {
        return _cf.get_compaction_strategy().make_sstable_set(_schema);
    }

    template <typename GCConsumer>
    requires CompactedFragmentsConsumer<GCConsumer>
    future<> setup(GCConsumer gc_consumer) {
        auto ssts = make_lw_shared<sstables::sstable_set>(make_sstable_set_for_input());
        formatted_sstables_list formatted_msg;
        auto fully_expired = get_fully_expired_sstables(_cf, _sstables, gc_clock::now() - _schema->gc_grace_seconds());
        min_max_tracker<api::timestamp_type> timestamp_tracker;

        for (auto& sst : _sstables) {
            auto& sst_stats = sst->get_stats_metadata();
            timestamp_tracker.update(sst_stats.min_timestamp);
            timestamp_tracker.update(sst_stats.max_timestamp);

            // Compacted sstable keeps track of its ancestors.
            _ancestors.push_back(sst->generation());
            _info->start_size += sst->bytes_on_disk();
            _info->total_partitions += sst->get_estimated_key_count();
            formatted_msg += sst;

            // Do not actually compact a sstable that is fully expired and can be safely
            // dropped without ressurrecting old data.
            if (tombstone_expiration_enabled() && fully_expired.contains(sst)) {
                on_skipped_expired_sstable(sst);
                continue;
            }

            // We also capture the sstable, so we keep it alive while the read isn't done
            ssts->insert(sst);
            // FIXME: If the sstables have cardinality estimation bitmaps, use that
            // for a better estimate for the number of partitions in the merged
            // sstable than just adding up the lengths of individual sstables.
            _estimated_partitions += sst->get_estimated_key_count();
            // TODO:
            // Note that this is not fully correct. Since we might be merging sstables that originated on
            // another shard (#cpu changed), we might be comparing RP:s with differing shard ids,
            // which might vary in "comparable" size quite a bit. However, since the worst that happens
            // is that we might miss a high water mark for the commit log replayer,
            // this is kind of ok, esp. since we will hopefully not be trying to recover based on
            // compacted sstables anyway (CL should be clean by then).
            _rp = std::max(_rp, sst_stats.position);
        }
        _info->sstables = _sstables.size();
        log_info("{} {}", report_start_desc(), formatted_msg);
        if (ssts->all()->size() < _sstables.size()) {
            log_debug("{} out of {} input sstables are fully expired sstables that will not be actually compacted",
                      _sstables.size() - ssts->all()->size(), _sstables.size());
        }

        _compacting = std::move(ssts);

        _ms_metadata.min_timestamp = timestamp_tracker.min();
        _ms_metadata.max_timestamp = timestamp_tracker.max();
        auto now = gc_clock::now();
        auto consumer = make_interposer_consumer([this, gc_consumer = std::move(gc_consumer), now] (flat_mutation_reader reader) mutable
        {
            return seastar::async([this, reader = std::move(reader), gc_consumer = std::move(gc_consumer), now] () mutable {
                auto close_reader = deferred_close(reader);

                using compact_mutations = compact_for_compaction<compacting_sstable_writer, GCConsumer>;
                auto cfc = make_stable_flattened_mutations_consumer<compact_mutations>(*schema(), now,
                                         max_purgeable_func(),
                                         get_compacting_sstable_writer(),
                                         std::move(gc_consumer));

                reader.consume_in_thread(std::move(cfc));
            });
        });
        return consumer(make_sstable_reader());
    }

    virtual reader_consumer make_interposer_consumer(reader_consumer end_consumer) {
        return _cf.get_compaction_strategy().make_interposer_consumer(_ms_metadata, std::move(end_consumer));
    }

    virtual bool use_interposer_consumer() const {
        return _cf.get_compaction_strategy().use_interposer_consumer();
    }

    compaction_info finish(std::chrono::time_point<db_clock> started_at, std::chrono::time_point<db_clock> ended_at) {
        _info->ended_at = std::chrono::duration_cast<std::chrono::milliseconds>(ended_at.time_since_epoch()).count();
        auto ratio = double(_info->end_size) / double(_info->start_size);
        auto duration = std::chrono::duration<float>(ended_at - started_at);
        // Don't report NaN or negative number.

        on_end_of_compaction();

        formatted_sstables_list new_sstables_msg(_info->new_sstables, false);

        // FIXME: there is some missing information in the log message below.
        // look at CompactionTask::runMayThrow() in origin for reference.
        // - add support to merge summary (message: Partition merge counts were {%s}.).
        // - there is no easy way, currently, to know the exact number of total partitions.
        // By the time being, using estimated key count.
        log_info("{} {} sstables to {}. {} to {} (~{}% of original) in {}ms = {}. ~{} total partitions merged to {}.",
                report_finish_desc(),
                _info->sstables, new_sstables_msg, pretty_printed_data_size(_info->start_size), pretty_printed_data_size(_info->end_size), int(ratio * 100),
                std::chrono::duration_cast<std::chrono::milliseconds>(duration).count(), pretty_printed_throughput(_info->end_size, duration),
                _info->total_partitions, _info->total_keys_written);

        backlog_tracker_adjust_charges();

        auto info = std::move(_info);
        _cf.get_compaction_manager().deregister_compaction(info);
        return std::move(*info);
    }

    virtual std::string_view report_start_desc() const = 0;
    virtual std::string_view report_finish_desc() const = 0;
    virtual void backlog_tracker_adjust_charges() { };

    std::function<api::timestamp_type(const dht::decorated_key&)> max_purgeable_func() {
        if (!tombstone_expiration_enabled()) {
            return [] (const dht::decorated_key& dk) {
                return api::min_timestamp;
            };
        }
        return [this] (const dht::decorated_key& dk) {
            return get_max_purgeable_timestamp(_cf, *_selector, _compacting_for_max_purgeable_func, dk);
        };
    }

    virtual void on_new_partition() {}

    virtual void on_end_of_compaction() {};

    // Inform about every expired sstable that was skipped during setup phase
    virtual void on_skipped_expired_sstable(shared_sstable sstable) {}

    // create a writer based on decorated key.
    virtual compaction_writer create_compaction_writer(const dht::decorated_key& dk) = 0;
    // stop current writer
    virtual void stop_sstable_writer(compaction_writer* writer) = 0;

    compacting_sstable_writer get_compacting_sstable_writer() {
        return compacting_sstable_writer(*this);
    }

    const schema_ptr& schema() const {
        return _schema;
    }

    void delete_sstables_for_interrupted_compaction() {
        // Delete either partially or fully written sstables of a compaction that
        // was either stopped abruptly (e.g. out of disk space) or deliberately
        // (e.g. nodetool stop COMPACTION).
        for (auto& sst : _new_unused_sstables) {
            log_debug("Deleting sstable {} of interrupted compaction for {}.{}", sst->get_filename(), _info->ks_name, _info->cf_name);
            sst->mark_for_deletion();
        }
    }
protected:
    template <typename... Args>
    void log(log_level level, std::string_view fmt, const Args&... args) const {
        if (clogger.is_enabled(level)) {
            auto msg = fmt::format(fmt, args...);
            clogger.log(level, "[{} {}.{} {}] {}", _info->type, _info->ks_name, _info->cf_name, _info->compaction_uuid, msg);
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
    garbage_collected_sstable_writer make_garbage_collected_sstable_writer() {
        return garbage_collected_sstable_writer(_gc_sstable_writer_data);
    }

    bool enable_garbage_collected_sstable_writer() const {
        // FIXME: Disable GC writer if interposer consumer is enabled until they both can work simultaneously.
        // More details can be found at https://github.com/scylladb/scylla/issues/6472
        return _contains_multi_fragment_runs && !use_interposer_consumer();
    }

    template <typename GCConsumer = noop_compacted_fragments_consumer>
    requires CompactedFragmentsConsumer<GCConsumer>
    static future<compaction_info> run(std::unique_ptr<compaction> c, GCConsumer gc_consumer = GCConsumer());

    friend class compacting_sstable_writer;
    friend class garbage_collected_sstable_writer;
    friend class garbage_collected_sstable_writer::data;
};

void compacting_sstable_writer::maybe_abort_compaction() {
    if (_c._info->is_stop_requested()) [[unlikely]] {
        // Compaction manager will catch this exception and re-schedule the compaction.
        throw compaction_stop_exception(_c._info->ks_name, _c._info->cf_name, _c._info->stop_requested);
    }
}

void compacting_sstable_writer::consume_new_partition(const dht::decorated_key& dk) {
    maybe_abort_compaction();
    if (!_compaction_writer) {
        _compaction_writer = _c.create_compaction_writer(dk);
    }

    _c.on_new_partition();
    _compaction_writer->writer.consume_new_partition(dk);
    _c._info->total_keys_written++;
}

stop_iteration compacting_sstable_writer::consume_end_of_partition() {
    auto ret = _compaction_writer->writer.consume_end_of_partition();
    if (ret == stop_iteration::yes) {
        // stop sstable writer being currently used.
        _c.stop_sstable_writer(&*_compaction_writer);
        _compaction_writer = std::nullopt;
    }
    return ret;
}

void compacting_sstable_writer::consume_end_of_stream() {
    if (_compaction_writer) {
        _c.stop_sstable_writer(&*_compaction_writer);
        _compaction_writer = std::nullopt;
    }
}

void garbage_collected_sstable_writer::data::maybe_create_new_sstable_writer() {
    if (!_compaction_writer) {
        auto sst = _c->_sstable_creator(this_shard_id());

        auto&& priority = _c->_io_priority;
        auto monitor = std::make_unique<compaction_write_monitor>(sst, _c->_cf, _c->maximum_timestamp(), _c->_sstable_level);
        sstable_writer_config cfg = _c->_cf.get_sstables_manager().configure_writer("garbage_collection");
        cfg.run_identifier = _run_identifier;
        cfg.monitor = monitor.get();
        auto writer = sst->get_writer(*_c->schema(), _c->partitions_per_sstable(), cfg, _c->get_encoding_stats(), priority);
        _compaction_writer.emplace(std::move(monitor), std::move(writer), std::move(sst));
    }
}

void garbage_collected_sstable_writer::data::finish_sstable_writer() {
    if (_compaction_writer) {
        writer().consume_end_of_stream();
        auto sst = std::move(_compaction_writer->sst);
        sst->open_data().get0();
        _compaction_writer = std::nullopt;
        _unused_garbage_collected_sstables.push_back(std::move(sst));
    }
}

class reshape_compaction : public compaction {
public:
    reshape_compaction(column_family& cf, compaction_descriptor descriptor)
        : compaction(cf, std::move(descriptor)) {
    }

    virtual sstables::sstable_set make_sstable_set_for_input() const override {
        return sstables::make_partitioned_sstable_set(_schema, make_lw_shared<sstable_list>(sstable_list{}), false);
    }

    flat_mutation_reader make_sstable_reader() const override {
        return _compacting->make_local_shard_sstable_reader(_schema,
                _permit,
                query::full_partition_range,
                _schema->full_slice(),
                _io_priority,
                tracing::trace_state_ptr(),
                ::streamed_mutation::forwarding::no,
                ::mutation_reader::forwarding::no,
                default_read_monitor_generator());
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
        return compaction_writer{sst->get_writer(*_schema, partitions_per_sstable(), cfg, get_encoding_stats(), _io_priority), sst};
    }

    virtual void stop_sstable_writer(compaction_writer* writer) override {
        if (writer) {
            finish_new_sstable(writer);
        }
    }
};

class regular_compaction : public compaction {
    // sstable being currently written.
    mutable compaction_read_monitor_generator _monitor_generator;
    std::vector<shared_sstable> _unused_sstables = {};
public:
    regular_compaction(column_family& cf, compaction_descriptor descriptor)
        : compaction(cf, std::move(descriptor))
        , _monitor_generator(_cf)
    {
    }

    flat_mutation_reader make_sstable_reader() const override {
        return _compacting->make_local_shard_sstable_reader(_schema,
                _permit,
                query::full_partition_range,
                _schema->full_slice(),
                _io_priority,
                tracing::trace_state_ptr(),
                ::streamed_mutation::forwarding::no,
                ::mutation_reader::forwarding::no,
                _monitor_generator);
    }

    std::string_view report_start_desc() const override {
        return "Compacting";
    }

    std::string_view report_finish_desc() const override {
        return "Compacted";
    }

    void backlog_tracker_adjust_charges() override {
        _monitor_generator.remove_sstables(_info->tracking);
        auto& tracker = _cf.get_compaction_strategy().get_backlog_tracker();
        for (auto& sst : _unused_sstables) {
            tracker.add_sstable(sst);
        }
        _unused_sstables.clear();
    }

    virtual compaction_writer create_compaction_writer(const dht::decorated_key& dk) override {
        auto sst = _sstable_creator(this_shard_id());
        setup_new_sstable(sst);
        _unused_sstables.push_back(sst);

        auto monitor = std::make_unique<compaction_write_monitor>(sst, _cf, maximum_timestamp(), _sstable_level);
        sstable_writer_config cfg = make_sstable_writer_config(_info->type);
        cfg.monitor = monitor.get();
        return compaction_writer{std::move(monitor), sst->get_writer(*_schema, partitions_per_sstable(), cfg, get_encoding_stats(), _io_priority), sst};
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

    virtual void on_skipped_expired_sstable(shared_sstable sstable) override {
        // manually register expired sstable into monitor, as it's not being actually compacted
        // this will allow expired sstable to be removed from tracker once compaction completes
        _monitor_generator(std::move(sstable));
    }
private:
    void backlog_tracker_incrementally_adjust_charges(std::vector<shared_sstable> exhausted_sstables) {
        //
        // Notify backlog tracker of an early sstable replacement triggered by incremental compaction approach.
        // Backlog tracker will be told that the exhausted sstables aren't being compacted anymore, and the
        // new sstables, which replaced the exhausted ones, are not partially written sstables and they can
        // be added to tracker like any other regular sstable in the table's set.
        // This way we prevent bogus calculation of backlog due to lack of charge adjustment whenever there's
        // an early sstable replacement.
        //

        for (auto& sst : exhausted_sstables) {
            _monitor_generator.remove_sstable(_info->tracking, sst);
        }
        auto& tracker = _cf.get_compaction_strategy().get_backlog_tracker();
        for (auto& sst : _unused_sstables) {
            tracker.add_sstable(sst);
         }
        _unused_sstables.clear();
    }

    void maybe_replace_exhausted_sstables_by_sst(shared_sstable sst) {
        // Skip earlier replacement of exhausted sstables if compaction works with only single-fragment runs,
        // meaning incremental compaction is disabled for this compaction.
        if (!enable_garbage_collected_sstable_writer()) {
            return;
        }
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
            // before exhausted SSTable is released, so as to prevent data resurrection.
            _gc_sstable_writer_data.finish_sstable_writer();
            // Added Garbage collected SSTables to list of unused SSTables that will be added
            // to SSTable set. GC SSTables should be added before compaction completes because
            // a failure could result in data resurrection if data is not made available.
            auto unused_gc_sstables = _gc_sstable_writer_data.consume_unused_garbage_collected_sstables();
            _new_unused_sstables.insert(_new_unused_sstables.end(), unused_gc_sstables.begin(), unused_gc_sstables.end());

            auto exhausted_ssts = std::vector<shared_sstable>(exhausted, _sstables.end());
            _replacer(get_compaction_completion_desc(exhausted_ssts, std::move(_new_unused_sstables)));
            _sstables.erase(exhausted, _sstables.end());
            backlog_tracker_incrementally_adjust_charges(std::move(exhausted_ssts));
        }
    }

    void replace_remaining_exhausted_sstables() {
        if (!_sstables.empty() || !_gc_sstable_writer_data.used_garbage_collected_sstables().empty()) {
            std::vector<shared_sstable> old_sstables;
            std::move(_sstables.begin(), _sstables.end(), std::back_inserter(old_sstables));

            // Remove Garbage Collected SSTables from the SSTable set if any was previously added.
            auto& used_garbage_collected_sstables = _gc_sstable_writer_data.used_garbage_collected_sstables();
            old_sstables.insert(old_sstables.end(), used_garbage_collected_sstables.begin(), used_garbage_collected_sstables.end());

            _replacer(get_compaction_completion_desc(std::move(old_sstables), std::move(_new_unused_sstables)));
         }
    }

    void update_pending_ranges() {
        if (!_sstable_set || _sstable_set->all()->empty() || _info->pending_replacements.empty()) { // set can be empty for testing scenario.
            return;
        }
        // Releases reference to sstables compacted by this compaction or another, both of which belongs
        // to the same column family
        for (auto& pending_replacement : _info->pending_replacements) {
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
        _info->pending_replacements.clear();
    }
};

class cleanup_compaction final : public regular_compaction {
    dht::token_range_vector _owned_ranges;
    incremental_owned_ranges_checker _owned_ranges_checker;
private:
    // Called in a seastar thread
    dht::partition_range_vector
    get_ranges_for_invalidation(const std::vector<shared_sstable>& sstables) {
        auto owned_ranges = dht::to_partition_ranges(_owned_ranges, utils::can_yield::yes);

        auto non_owned_ranges = boost::copy_range<dht::partition_range_vector>(sstables
                | boost::adaptors::transformed([] (const shared_sstable& sst) {
            seastar::thread::maybe_yield();
            return dht::partition_range::make({sst->get_first_decorated_key(), true},
                                              {sst->get_last_decorated_key(), true});
        }));
        // optimize set of potentially overlapping ranges by deoverlapping them.
        non_owned_ranges = dht::partition_range::deoverlap(std::move(non_owned_ranges), dht::ring_position_comparator(*_schema));

        // subtract *each* owned range from the partition range of *each* sstable*,
        // such that we'll be left only with a set of non-owned ranges.
        for (auto& owned_range : owned_ranges) {
            dht::partition_range_vector new_non_owned_ranges;
            for (auto& non_owned_range : non_owned_ranges) {
                auto ret = non_owned_range.subtract(owned_range, dht::ring_position_comparator(*_schema));
                new_non_owned_ranges.insert(new_non_owned_ranges.end(), ret.begin(), ret.end());
                seastar::thread::maybe_yield();
            }
            non_owned_ranges = std::move(new_non_owned_ranges);
        }
        return non_owned_ranges;
    }
protected:
    virtual compaction_completion_desc
    get_compaction_completion_desc(std::vector<shared_sstable> input_sstables, std::vector<shared_sstable> output_sstables) override {
        auto ranges_for_for_invalidation = get_ranges_for_invalidation(input_sstables);
        return compaction_completion_desc{std::move(input_sstables), std::move(output_sstables), std::move(ranges_for_for_invalidation)};
    }

private:
    cleanup_compaction(database& db, column_family& cf, compaction_descriptor descriptor)
        : regular_compaction(cf, std::move(descriptor))
        , _owned_ranges(db.get_keyspace_local_ranges(_schema->ks_name()))
        , _owned_ranges_checker(_owned_ranges)
    {
    }

public:
    cleanup_compaction(column_family& cf, compaction_descriptor descriptor, compaction_options::cleanup opts)
        : cleanup_compaction(opts.db, cf, std::move(descriptor)) {}
    cleanup_compaction(column_family& cf, compaction_descriptor descriptor, compaction_options::upgrade opts)
        : cleanup_compaction(opts.db, cf, std::move(descriptor)) {}

    flat_mutation_reader make_sstable_reader() const override {
        return make_filtering_reader(regular_compaction::make_sstable_reader(), make_partition_filter());
    }

    std::string_view report_start_desc() const override {
        return "Cleaning";
    }

    std::string_view report_finish_desc() const override {
        return "Cleaned";
    }

    flat_mutation_reader::filter make_partition_filter() const {
        return [this] (const dht::decorated_key& dk) {
#ifdef SEASTAR_DEBUG
            // sstables should never be shared with other shards at this point.
            assert(dht::shard_of(*_schema, dk.token()) == this_shard_id());
#endif

            if (!_owned_ranges_checker.belongs_to_current_node(dk.token())) {
                log_trace("Token {} does not belong to this node, skipping", dk.token());
                return false;
            }
            return true;
        };
    }
};

class scrub_compaction final : public regular_compaction {
public:
    static void report_invalid_partition(compaction_type type, mutation_fragment_stream_validator& validator, const dht::decorated_key& new_key,
            std::string_view action = "") {
        const auto& schema = validator.schema();
        const auto& current_key = validator.previous_partition_key();
        clogger.error("[{} compaction {}.{}] Invalid partition {} ({}), partition is out-of-order compared to previous partition {} ({}){}{}",
                type,
                schema.ks_name(),
                schema.cf_name(),
                new_key.key().with_schema(schema),
                new_key,
                current_key.key().with_schema(schema),
                current_key,
                action.empty() ? "" : "; ",
                action);
    }
    static void report_invalid_partition_start(compaction_type type, mutation_fragment_stream_validator& validator, const dht::decorated_key& new_key,
            std::string_view action = "") {
        const auto& schema = validator.schema();
        const auto& current_key = validator.previous_partition_key();
        clogger.error("[{} compaction {}.{}] Invalid partition start for partition {} ({}), previous partition {} ({}) didn't end with a partition-end fragment{}{}",
                type,
                schema.ks_name(),
                schema.cf_name(),
                new_key.key().with_schema(schema),
                new_key,
                current_key.key().with_schema(schema),
                current_key,
                action.empty() ? "" : "; ",
                action);
    }
    static void report_invalid_mutation_fragment(compaction_type type, mutation_fragment_stream_validator& validator, const mutation_fragment& mf,
            std::string_view action = "") {
        const auto& schema = validator.schema();
        const auto& key = validator.previous_partition_key();
        const auto prev_pos = validator.previous_position();
        clogger.error("[{} compaction {}.{}] Invalid {} fragment{} ({}) in partition {} ({}),"
                " fragment is out-of-order compared to previous {} fragment{} ({}){}{}",
                type,
                schema.ks_name(),
                schema.cf_name(),
                mf.mutation_fragment_kind(),
                mf.has_key() ? format(" with key {}", mf.key().with_schema(schema)) : "",
                mf.position(),
                key.key().with_schema(schema),
                key,
                prev_pos.region(),
                prev_pos.has_key() ? format(" with key {}", prev_pos.key().with_schema(schema)) : "",
                prev_pos,
                action.empty() ? "" : "; ",
                action);
    }
    static void report_invalid_end_of_stream(compaction_type type, mutation_fragment_stream_validator& validator, std::string_view action = "") {
        const auto& schema = validator.schema();
        const auto& key = validator.previous_partition_key();
        clogger.error("[{} compaction {}.{}] Invalid end-of-stream, last partition {} ({}) didn't end with a partition-end fragment{}{}",
                type, schema.ks_name(), schema.cf_name(), key.key().with_schema(schema), key, action.empty() ? "" : "; ", action);
    }

private:
    class reader : public flat_mutation_reader::impl {
        using skip = bool_class<class skip_tag>;
    private:
        compaction_options::scrub::mode _scrub_mode;
        flat_mutation_reader _reader;
        mutation_fragment_stream_validator _validator;
        bool _skip_to_next_partition = false;

    private:
        void maybe_abort_scrub() {
            if (_scrub_mode == compaction_options::scrub::mode::abort) {
                throw compaction_stop_exception(_schema->ks_name(), _schema->cf_name(), "scrub compaction found invalid data", false);
            }
        }

        void on_unexpected_partition_start(const mutation_fragment& ps) {
            maybe_abort_scrub();
            report_invalid_partition_start(compaction_type::Scrub, _validator, ps.as_partition_start().key(),
                    "Rectifying by adding assumed missing partition-end");

            auto pe = mutation_fragment(*_schema, _permit, partition_end{});
            if (!_validator(pe)) {
                throw compaction_stop_exception(
                        _schema->ks_name(),
                        _schema->cf_name(),
                        "scrub compaction failed to rectify unexpected partition-start, validator rejects the injected partition-end",
                        false);
            }
            push_mutation_fragment(std::move(pe));

            if (!_validator(ps)) {
                throw compaction_stop_exception(
                        _schema->ks_name(),
                        _schema->cf_name(),
                        "scrub compaction failed to rectify unexpected partition-start, validator rejects it even after the injected partition-end",
                        false);
            }
        }

        skip on_invalid_partition(const dht::decorated_key& new_key) {
            maybe_abort_scrub();
            if (_scrub_mode == compaction_options::scrub::mode::segregate) {
                report_invalid_partition(compaction_type::Scrub, _validator, new_key, "Detected");
                _validator.reset(new_key);
                // Let the segregating interposer consumer handle this.
                return skip::no;
            }
            report_invalid_partition(compaction_type::Scrub, _validator, new_key, "Skipping");
            _skip_to_next_partition = true;
            return skip::yes;
        }

        skip on_invalid_mutation_fragment(const mutation_fragment& mf) {
            maybe_abort_scrub();

            const auto& key = _validator.previous_partition_key();

            // If the unexpected fragment is a partition end, we just drop it.
            // The only case a partition end is invalid is when it comes after
            // another partition end, and we can just drop it in that case.
            if (!mf.is_end_of_partition() && _scrub_mode == compaction_options::scrub::mode::segregate) {
                report_invalid_mutation_fragment(compaction_type::Scrub, _validator, mf,
                        "Injecting partition start/end to segregate out-of-order fragment");
                push_mutation_fragment(*_schema, _permit, partition_end{});

                // We loose the partition tombstone if any, but it will be
                // picked up when compaction merges these partitions back.
                push_mutation_fragment(mutation_fragment(*_schema, _permit, partition_start(key, {})));

                _validator.reset(mf);

                // Let the segregating interposer consumer handle this.
                return skip::no;
            }

            report_invalid_mutation_fragment(compaction_type::Scrub, _validator, mf, "Skipping");

            return skip::yes;
        }

        void on_invalid_end_of_stream() {
            maybe_abort_scrub();
            // Handle missing partition_end
            push_mutation_fragment(mutation_fragment(*_schema, _permit, partition_end{}));
            report_invalid_end_of_stream(compaction_type::Scrub, _validator, "Rectifying by adding missing partition-end to the end of the stream");
        }

        void fill_buffer_from_underlying() {
            while (!_reader.is_buffer_empty() && !is_buffer_full()) {
                auto mf = _reader.pop_mutation_fragment();
                if (mf.is_partition_start()) {
                    // First check that fragment kind monotonicity stands.
                    // When skipping to another partition the fragment
                    // monotonicity of the partition-start doesn't have to be
                    // and shouldn't be verified. We know the last fragment the
                    // validator saw is a partition-start, passing it another one
                    // will confuse it.
                    if (!_skip_to_next_partition && !_validator(mf)) {
                        on_unexpected_partition_start(mf);
                        // Continue processing this partition start.
                    }
                    _skip_to_next_partition = false;
                    // Then check that the partition monotonicity stands.
                    const auto& dk = mf.as_partition_start().key();
                    if (!_validator(dk) && on_invalid_partition(dk) == skip::yes) {
                        continue;
                    }
                } else if (_skip_to_next_partition) {
                    continue;
                } else {
                    if (!_validator(mf) && on_invalid_mutation_fragment(mf) == skip::yes) {
                        continue;
                    }
                }
                push_mutation_fragment(std::move(mf));
            }

            _end_of_stream = _reader.is_end_of_stream() && _reader.is_buffer_empty();

            if (_end_of_stream) {
                if (!_validator.on_end_of_stream()) {
                    on_invalid_end_of_stream();
                }
            }
        }

    public:
        reader(flat_mutation_reader underlying, compaction_options::scrub::mode scrub_mode)
            : impl(underlying.schema(), underlying.permit())
            , _scrub_mode(scrub_mode)
            , _reader(std::move(underlying))
            , _validator(*_schema)
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
                } catch (const compaction_stop_exception&) {
                    // Propagate these unchanged.
                    throw;
                } catch (const storage_io_error&) {
                    // Propagate these unchanged.
                    throw;
                } catch (...) {
                    // We don't want failed scrubs to be retried.
                    throw compaction_stop_exception(
                            _schema->ks_name(),
                            _schema->cf_name(),
                            format("scrub compaction failed due to unrecoverable error: {}", std::current_exception()),
                            false);
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
    compaction_options::scrub _options;
    std::string _scrub_start_description;
    std::string _scrub_finish_description;

public:
    scrub_compaction(column_family& cf, compaction_descriptor descriptor, compaction_options::scrub options)
        : regular_compaction(cf, std::move(descriptor))
        , _options(options)
        , _scrub_start_description(fmt::format("Scrubbing in {} mode", _options.operation_mode))
        , _scrub_finish_description(fmt::format("Finished scrubbing in {} mode", _options.operation_mode)) {
    }

    std::string_view report_start_desc() const override {
        return _scrub_start_description;
    }

    std::string_view report_finish_desc() const override {
        return _scrub_finish_description;
    }

    flat_mutation_reader make_sstable_reader() const override {
        auto crawling_reader = _compacting->make_crawling_reader(_schema, _permit, _io_priority, nullptr);
        return make_flat_mutation_reader<reader>(std::move(crawling_reader), _options.operation_mode);
    }

    reader_consumer make_interposer_consumer(reader_consumer end_consumer) override {
        return [this, end_consumer = std::move(end_consumer)] (flat_mutation_reader reader) mutable -> future<> {
            return mutation_writer::segregate_by_partition(std::move(reader), std::move(end_consumer));
        };
    }

    bool use_interposer_consumer() const override {
        return _options.operation_mode == compaction_options::scrub::mode::segregate;
    }

    friend flat_mutation_reader make_scrubbing_reader(flat_mutation_reader rd, compaction_options::scrub::mode scrub_mode);
};

flat_mutation_reader make_scrubbing_reader(flat_mutation_reader rd, compaction_options::scrub::mode scrub_mode) {
    return make_flat_mutation_reader<scrub_compaction::reader>(std::move(rd), scrub_mode);
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
    std::vector<utils::UUID> _run_identifiers;
private:
    // return estimated partitions per sstable for a given shard
    uint64_t partitions_per_sstable(shard_id s) const {
        uint64_t estimated_sstables = std::max(uint64_t(1), uint64_t(ceil(double(_estimation_per_shard[s].estimated_size) / _max_sstable_size)));
        return std::min(uint64_t(ceil(double(_estimation_per_shard[s].estimated_partitions) / estimated_sstables)),
                _cf.get_compaction_strategy().adjust_partition_estimate(_ms_metadata, _estimation_per_shard[s].estimated_partitions));
    }
public:
    resharding_compaction(column_family& cf, sstables::compaction_descriptor descriptor)
        : compaction(cf, std::move(descriptor))
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
            _run_identifiers[i] = utils::make_random_uuid();
        }
    }

    ~resharding_compaction() { }

    // Use reader that makes sure no non-local mutation will not be filtered out.
    flat_mutation_reader make_sstable_reader() const override {
        return _compacting->make_range_sstable_reader(_schema,
                _permit,
                query::full_partition_range,
                _schema->full_slice(),
                _io_priority,
                nullptr,
                ::streamed_mutation::forwarding::no,
                ::mutation_reader::forwarding::no);

    }

    reader_consumer make_interposer_consumer(reader_consumer end_consumer) override {
        return [this, end_consumer = std::move(end_consumer)] (flat_mutation_reader reader) mutable -> future<> {
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

    void backlog_tracker_adjust_charges() override { }

    compaction_writer create_compaction_writer(const dht::decorated_key& dk) override {
        auto shard = dht::shard_of(*_schema, dk.token());
        auto sst = _sstable_creator(shard);
        setup_new_sstable(sst);

        auto cfg = make_sstable_writer_config(compaction_type::Reshard);
        // sstables generated for a given shard will share the same run identifier.
        cfg.run_identifier = _run_identifiers.at(shard);
        return compaction_writer{sst->get_writer(*_schema, partitions_per_sstable(shard), cfg, get_encoding_stats(), _io_priority, shard), sst};
    }

    void on_new_partition() override {}

    virtual void on_end_of_compaction() override {}

    void stop_sstable_writer(compaction_writer* writer) override {
        if (writer) {
            finish_new_sstable(writer);
        }
    }
};

template <typename GCConsumer>
requires CompactedFragmentsConsumer<GCConsumer>
future<compaction_info> compaction::run(std::unique_ptr<compaction> c, GCConsumer gc_consumer) {
    return seastar::async([c = std::move(c), gc_consumer = std::move(gc_consumer)] () mutable {
        auto consumer = c->setup(std::move(gc_consumer));
        auto start_time = db_clock::now();
        try {
           consumer.get();
        } catch (...) {
            c->delete_sstables_for_interrupted_compaction();
            c = nullptr; // make sure writers are stopped while running in thread context. This is because of calls to file.close().get();
            throw;
        }

        return c->finish(std::move(start_time), db_clock::now());
    });
}

compaction_type compaction_options::type() const {
    // Maps options_variant indexes to the corresponding compaction_type member.
    static const compaction_type index_to_type[] = {
        compaction_type::Compaction,
        compaction_type::Cleanup,
        compaction_type::Upgrade,
        compaction_type::Scrub,
        compaction_type::Reshard,
        compaction_type::Reshape,
    };
    static_assert(std::variant_size_v<compaction_options::options_variant> == std::size(index_to_type));
    return index_to_type[_options.index()];
}

static std::unique_ptr<compaction> make_compaction(column_family& cf, sstables::compaction_descriptor descriptor) {
    struct {
        column_family& cf;
        sstables::compaction_descriptor&& descriptor;

        std::unique_ptr<compaction> operator()(compaction_options::reshape) {
            return std::make_unique<reshape_compaction>(cf, std::move(descriptor));
        }
        std::unique_ptr<compaction> operator()(compaction_options::reshard) {
            return std::make_unique<resharding_compaction>(cf, std::move(descriptor));
        }
        std::unique_ptr<compaction> operator()(compaction_options::regular) {
            return std::make_unique<regular_compaction>(cf, std::move(descriptor));
        }
        std::unique_ptr<compaction> operator()(compaction_options::cleanup options) {
            return std::make_unique<cleanup_compaction>(cf, std::move(descriptor), std::move(options));
        }
        std::unique_ptr<compaction> operator()(compaction_options::upgrade options) {
            return std::make_unique<cleanup_compaction>(cf, std::move(descriptor), std::move(options));
        }
        std::unique_ptr<compaction> operator()(compaction_options::scrub scrub_options) {
            return std::make_unique<scrub_compaction>(cf, std::move(descriptor), scrub_options);
        }
    } visitor_factory{cf, std::move(descriptor)};

    return descriptor.options.visit(visitor_factory);
}

future<bool> scrub_validate_mode_validate_reader(flat_mutation_reader reader, const compaction_info& info) {
    auto schema = reader.schema();

    bool valid = true;
    std::exception_ptr ex;

    try {
        auto validator = mutation_fragment_stream_validator(*schema);

        while (auto mf_opt = co_await reader()) {
            if (info.is_stop_requested()) [[unlikely]] {
                // Compaction manager will catch this exception and re-schedule the compaction.
                co_return coroutine::make_exception(compaction_stop_exception(info.ks_name, info.cf_name, info.stop_requested));
            }

            const auto& mf = *mf_opt;

            if (mf.is_partition_start()) {
                const auto& ps = mf.as_partition_start();
                if (!validator(mf)) {
                    scrub_compaction::report_invalid_partition_start(compaction_type::Scrub, validator, ps.key());
                    validator.reset(mf);
                    valid = false;
                }
                if (!validator(ps.key())) {
                    scrub_compaction::report_invalid_partition(compaction_type::Scrub, validator, ps.key());
                    validator.reset(ps.key());
                    valid = false;
                }
            } else {
                if (!validator(mf)) {
                    scrub_compaction::report_invalid_mutation_fragment(compaction_type::Scrub, validator, mf);
                    validator.reset(mf);
                    valid = false;
                }
            }
        }
        if (!validator.on_end_of_stream()) {
            scrub_compaction::report_invalid_end_of_stream(compaction_type::Scrub, validator);
            valid = false;
        }
    } catch (...) {
        ex = std::current_exception();
    }

    co_await reader.close();

    if (ex) {
        co_return coroutine::exception(std::move(ex));
    }

    co_return valid;
}

static future<compaction_info> scrub_sstables_validate_mode(sstables::compaction_descriptor descriptor, column_family& cf) {
    auto schema = cf.schema();

    formatted_sstables_list sstables_list_msg;
    auto sstables = make_lw_shared<sstables::sstable_set>(sstables::make_partitioned_sstable_set(schema, make_lw_shared<sstable_list>(sstable_list{}), false));
    for (const auto& sst : descriptor.sstables) {
        sstables_list_msg += sst;
        sstables->insert(sst);
    }

    auto info = compaction::create_compaction_info(cf, descriptor);
    info->sstables = descriptor.sstables.size();
    cf.get_compaction_manager().register_compaction(info);
    auto deregister_compaction = defer([&cf, info] () noexcept {
        try {
            cf.get_compaction_manager().deregister_compaction(info);
        } catch (...) {
            clogger.warn("Could not deregister compaction: {}. Ignored.", std::current_exception());
        }
    });

    clogger.info("Scrubbing in validate mode {}", sstables_list_msg);

    auto permit = cf.compaction_concurrency_semaphore().make_tracking_only_permit(schema.get(), "scrub:validate", db::no_timeout);
    auto reader = sstables->make_crawling_reader(schema, permit, descriptor.io_priority, nullptr);

    const auto valid = co_await scrub_validate_mode_validate_reader(std::move(reader), *info);

    clogger.info("Finished scrubbing in validate mode {} - sstable(s) are {}", sstables_list_msg, valid ? "valid" : "invalid");

    co_return *info;
}

future<compaction_info>
compact_sstables(sstables::compaction_descriptor descriptor, column_family& cf) {
    if (descriptor.sstables.empty()) {
        return make_exception_future<compaction_info>(std::runtime_error(format("Called {} compaction with empty set on behalf of {}.{}",
                compaction_name(descriptor.options.type()), cf.schema()->ks_name(), cf.schema()->cf_name())));
    }
    if (descriptor.options.type() == compaction_type::Scrub
            && std::get<compaction_options::scrub>(descriptor.options.options()).operation_mode == compaction_options::scrub::mode::validate) {
        // Bypass the usual compaction machinery for dry-mode scrub
        return scrub_sstables_validate_mode(std::move(descriptor), cf);
    }
    auto c = make_compaction(cf, std::move(descriptor));
    if (c->enable_garbage_collected_sstable_writer()) {
        auto gc_writer = c->make_garbage_collected_sstable_writer();
        return compaction::run(std::move(c), std::move(gc_writer));
    }
    return compaction::run(std::move(c));
}

std::unordered_set<sstables::shared_sstable>
get_fully_expired_sstables(column_family& cf, const std::vector<sstables::shared_sstable>& compacting, gc_clock::time_point gc_before) {
    clogger.debug("Checking droppable sstables in {}.{}", cf.schema()->ks_name(), cf.schema()->cf_name());

    if (compacting.empty()) {
        return {};
    }

    std::unordered_set<sstables::shared_sstable> candidates;
    auto uncompacting_sstables = get_uncompacting_sstables(cf, compacting);
    // Get list of uncompacting sstables that overlap the ones being compacted.
    std::vector<sstables::shared_sstable> overlapping = leveled_manifest::overlapping(*cf.schema(), compacting, uncompacting_sstables);
    int64_t min_timestamp = std::numeric_limits<int64_t>::max();

    for (auto& sstable : overlapping) {
        if (sstable->get_max_local_deletion_time() >= gc_before) {
            min_timestamp = std::min(min_timestamp, sstable->get_stats_metadata().min_timestamp);
        }
    }

    auto compacted_undeleted_gens = boost::copy_range<std::unordered_set<int64_t>>(cf.compacted_undeleted_sstables()
        | boost::adaptors::transformed(std::mem_fn(&sstables::sstable::generation)));
    auto has_undeleted_ancestor = [&compacted_undeleted_gens] (auto& candidate) {
        // Get ancestors from sstable which is empty after restart. It works for this purpose because
        // we only need to check that a sstable compacted *in this instance* hasn't an ancestor undeleted.
        // Not getting it from sstable metadata because mc format hasn't it available.
        return boost::algorithm::any_of(candidate->compaction_ancestors(), [&compacted_undeleted_gens] (auto gen) {
            return compacted_undeleted_gens.contains(gen);
        });
    };

    // SStables that do not contain live data is added to list of possibly expired sstables.
    for (auto& candidate : compacting) {
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
            clogger.debug("Dropping expired SSTable {} (maxLocalDeletionTime={}, gcBefore={})",
                    candidate->get_filename(), candidate->get_stats_metadata().max_local_deletion_time, gc_before);
            it++;
        }
    }
    return candidates;
}

}
