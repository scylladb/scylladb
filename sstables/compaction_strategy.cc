/*
 * Copyright (C) 2016 ScyllaDB
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
#include <chrono>
#include <seastar/core/shared_ptr.hh>
#include "sstables.hh"
#include "compaction.hh"
#include "database.hh"
#include "compaction_strategy.hh"
#include "compaction_strategy_impl.hh"
#include "schema.hh"
#include "sstable_set.hh"
#include "compatible_ring_position.hh"
#include <boost/range/algorithm/find.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/icl/interval_map.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include "size_tiered_compaction_strategy.hh"
#include "date_tiered_compaction_strategy.hh"
#include "leveled_compaction_strategy.hh"
#include "time_window_compaction_strategy.hh"
#include "sstables/compaction_backlog_manager.hh"
#include "sstables/size_tiered_backlog_tracker.hh"

logging::logger date_tiered_manifest::logger = logging::logger("DateTieredCompactionStrategy");
logging::logger leveled_manifest::logger("LeveledManifest");

namespace sstables {

extern logging::logger clogger;

void sstable_run::insert(shared_sstable sst) {
    _all.insert(std::move(sst));
}

void sstable_run::erase(shared_sstable sst) {
    _all.erase(sst);
}

uint64_t sstable_run::data_size() const {
    return boost::accumulate(_all | boost::adaptors::transformed(std::mem_fn(&sstable::data_size)), uint64_t(0));
}

std::ostream& operator<<(std::ostream& os, const sstables::sstable_run& run) {
    os << "Run = {\n";
    if (run.all().empty()) {
        os << "  Identifier: not found\n";
    } else {
        os << format("  Identifier: {}\n", (*run.all().begin())->run_identifier());
    }

    auto frags = boost::copy_range<std::vector<shared_sstable>>(run.all());
    boost::sort(frags, [] (const shared_sstable& x, const shared_sstable& y) {
        return x->get_first_decorated_key().token() < y->get_first_decorated_key().token();
    });
    os << "  Fragments = {\n";
    for (auto& frag : frags) {
        os << format("    {}={}:{}\n", frag->generation(), frag->get_first_decorated_key().token(), frag->get_last_decorated_key().token());
    }
    os << "  }\n}\n";
    return os;
}

class incremental_selector_impl {
public:
    virtual ~incremental_selector_impl() {}
    virtual std::tuple<dht::partition_range, std::vector<shared_sstable>, dht::ring_position_view> select(const dht::ring_position_view&) = 0;
};

class sstable_set_impl {
public:
    virtual ~sstable_set_impl() {}
    virtual std::unique_ptr<sstable_set_impl> clone() const = 0;
    virtual std::vector<shared_sstable> select(const dht::partition_range& range) const = 0;
    virtual void insert(shared_sstable sst) = 0;
    virtual void erase(shared_sstable sst) = 0;
    virtual std::unique_ptr<incremental_selector_impl> make_incremental_selector() const = 0;
};

sstable_set::sstable_set(std::unique_ptr<sstable_set_impl> impl, schema_ptr s, lw_shared_ptr<sstable_list> all)
        : _impl(std::move(impl))
        , _schema(std::move(s))
        , _all(std::move(all)) {
}

sstable_set::sstable_set(const sstable_set& x)
        : _impl(x._impl->clone())
        , _schema(x._schema)
        , _all(make_lw_shared(sstable_list(*x._all)))
        , _all_runs(x._all_runs) {
}

sstable_set::sstable_set(sstable_set&&) noexcept = default;

sstable_set&
sstable_set::operator=(const sstable_set& x) {
    if (this != &x) {
        auto tmp = sstable_set(x);
        *this = std::move(tmp);
    }
    return *this;
}

sstable_set&
sstable_set::operator=(sstable_set&&) noexcept = default;

std::vector<shared_sstable>
sstable_set::select(const dht::partition_range& range) const {
    return _impl->select(range);
}

std::vector<sstable_run>
sstable_set::select(const std::vector<shared_sstable>& sstables) const {
    auto run_ids = boost::copy_range<std::unordered_set<utils::UUID>>(sstables | boost::adaptors::transformed(std::mem_fn(&sstable::run_identifier)));
    return boost::copy_range<std::vector<sstable_run>>(run_ids | boost::adaptors::transformed([this] (utils::UUID run_id) {
        return _all_runs.at(run_id);
    }));
}

void
sstable_set::insert(shared_sstable sst) {
    _impl->insert(sst);
    try {
        _all->insert(sst);
        try {
            _all_runs[sst->run_identifier()].insert(sst);
        } catch (...) {
            _all->erase(sst);
            throw;
        }
    } catch (...) {
        _impl->erase(sst);
        throw;
    }
}

void
sstable_set::erase(shared_sstable sst) {
    _impl->erase(sst);
    _all->erase(sst);
    _all_runs[sst->run_identifier()].erase(sst);
}

sstable_set::~sstable_set() = default;

sstable_set::incremental_selector::incremental_selector(std::unique_ptr<incremental_selector_impl> impl, const schema& s)
    : _impl(std::move(impl))
    , _cmp(s) {
}

sstable_set::incremental_selector::~incremental_selector() = default;

sstable_set::incremental_selector::incremental_selector(sstable_set::incremental_selector&&) noexcept = default;

sstable_set::incremental_selector::selection
sstable_set::incremental_selector::select(const dht::ring_position_view& pos) const {
    if (!_current_range_view || !_current_range_view->contains(pos, _cmp)) {
        std::tie(_current_range, _current_sstables, _current_next_position) = _impl->select(pos);
        _current_range_view = _current_range->transform([] (const dht::ring_position& rp) { return dht::ring_position_view(rp); });
    }
    return {_current_sstables, _current_next_position};
}

sstable_set::incremental_selector
sstable_set::make_incremental_selector() const {
    return incremental_selector(_impl->make_incremental_selector(), *_schema);
}

// default sstable_set, not specialized for anything
class bag_sstable_set : public sstable_set_impl {
    // erasing is slow, but select() is fast
    std::vector<shared_sstable> _sstables;
public:
    virtual std::unique_ptr<sstable_set_impl> clone() const override {
        return std::make_unique<bag_sstable_set>(*this);
    }
    virtual std::vector<shared_sstable> select(const dht::partition_range& range = query::full_partition_range) const override {
        return _sstables;
    }
    virtual void insert(shared_sstable sst) override {
        _sstables.push_back(std::move(sst));
    }
    virtual void erase(shared_sstable sst) override {
        auto it = boost::range::find(_sstables, sst);
        if (it != _sstables.end()){
            _sstables.erase(it);
        }
    }
    virtual std::unique_ptr<incremental_selector_impl> make_incremental_selector() const override;
    class incremental_selector;
};

class bag_sstable_set::incremental_selector : public incremental_selector_impl {
    const std::vector<shared_sstable>& _sstables;
public:
    incremental_selector(const std::vector<shared_sstable>& sstables)
        : _sstables(sstables) {
    }
    virtual std::tuple<dht::partition_range, std::vector<shared_sstable>, dht::ring_position_view> select(const dht::ring_position_view&) override {
        return std::make_tuple(dht::partition_range::make_open_ended_both_sides(), _sstables, dht::ring_position_view::max());
    }
};

std::unique_ptr<incremental_selector_impl> bag_sstable_set::make_incremental_selector() const {
    return std::make_unique<incremental_selector>(_sstables);
}

// specialized when sstables are partitioned in the token range space
// e.g. leveled compaction strategy
class partitioned_sstable_set : public sstable_set_impl {
    using value_set = std::unordered_set<shared_sstable>;
    using interval_map_type = boost::icl::interval_map<compatible_ring_position_or_view, value_set>;
    using interval_type = interval_map_type::interval_type;
    using map_iterator = interval_map_type::const_iterator;
private:
    schema_ptr _schema;
    std::vector<shared_sstable> _unleveled_sstables;
    interval_map_type _leveled_sstables;
    // Change counter on interval map for leveled sstables which is used by
    // incremental selector to determine whether or not to invalidate iterators.
    uint64_t _leveled_sstables_change_cnt = 0;
    bool _use_level_metadata = false;
private:
    static interval_type make_interval(const schema& s, const dht::partition_range& range) {
        return interval_type::closed(
                compatible_ring_position_or_view(s, dht::ring_position_view(range.start()->value())),
                compatible_ring_position_or_view(s, dht::ring_position_view(range.end()->value())));
    }
    interval_type make_interval(const dht::partition_range& range) const {
        return make_interval(*_schema, range);
    }
    static interval_type make_interval(const schema_ptr& s, const sstable& sst) {
        return interval_type::closed(
                compatible_ring_position_or_view(s, dht::ring_position(sst.get_first_decorated_key())),
                compatible_ring_position_or_view(s, dht::ring_position(sst.get_last_decorated_key())));
    }
    interval_type make_interval(const sstable& sst) {
        return make_interval(_schema, sst);
    }
    interval_type singular(const dht::ring_position& rp) const {
        // We should use the view here, since this is used for queries.
        auto rpv = dht::ring_position_view(rp);
        auto crp = compatible_ring_position_or_view(*_schema, std::move(rpv));
        return interval_type::closed(crp, crp);
    }
    std::pair<map_iterator, map_iterator> query(const dht::partition_range& range) const {
        if (range.start() && range.end()) {
            return _leveled_sstables.equal_range(make_interval(range));
        }
        else if (range.start() && !range.end()) {
            auto start = singular(range.start()->value());
            return { _leveled_sstables.lower_bound(start), _leveled_sstables.end() };
        } else if (!range.start() && range.end()) {
            auto end = singular(range.end()->value());
            return { _leveled_sstables.begin(), _leveled_sstables.upper_bound(end) };
        } else {
            return { _leveled_sstables.begin(), _leveled_sstables.end() };
        }
    }
    // SSTables are stored separately to avoid interval map's fragmentation issue when level 0 falls behind.
    bool store_as_unleveled(const shared_sstable& sst) const {
        return _use_level_metadata && sst->get_sstable_level() == 0;
    }
public:
    static dht::ring_position to_ring_position(const compatible_ring_position_or_view& crp) {
        // Ring position views, representing bounds of sstable intervals are
        // guaranteed to have key() != nullptr;
        const auto& pos = crp.position();
        return dht::ring_position(pos.token(), *pos.key());
    }
    static dht::partition_range to_partition_range(const interval_type& i) {
        return dht::partition_range::make(
                {to_ring_position(i.lower()), boost::icl::is_left_closed(i.bounds())},
                {to_ring_position(i.upper()), boost::icl::is_right_closed(i.bounds())});
    }
    static dht::partition_range to_partition_range(const dht::ring_position_view& pos, const interval_type& i) {
        auto lower_bound = [&] {
            if (pos.key()) {
                return dht::partition_range::bound(dht::ring_position(pos.token(), *pos.key()),
                        pos.is_after_key() == dht::ring_position_view::after_key::no);
            } else {
                return dht::partition_range::bound(dht::ring_position(pos.token(), pos.get_token_bound()), true);
            }
        }();
        auto upper_bound = dht::partition_range::bound(to_ring_position(i.lower()), !boost::icl::is_left_closed(i.bounds()));
        return dht::partition_range::make(std::move(lower_bound), std::move(upper_bound));
    }
    explicit partitioned_sstable_set(schema_ptr schema, bool use_level_metadata = true)
            : _schema(std::move(schema))
            , _use_level_metadata(use_level_metadata) {
    }
    virtual std::unique_ptr<sstable_set_impl> clone() const override {
        return std::make_unique<partitioned_sstable_set>(*this);
    }
    virtual std::vector<shared_sstable> select(const dht::partition_range& range) const override {
        auto ipair = query(range);
        auto b = std::move(ipair.first);
        auto e = std::move(ipair.second);
        value_set result;
        while (b != e) {
            boost::copy(b++->second, std::inserter(result, result.end()));
        }
        auto r = _unleveled_sstables;
        r.insert(r.end(), result.begin(), result.end());
        return r;
    }
    virtual void insert(shared_sstable sst) override {
        if (store_as_unleveled(sst)) {
            _unleveled_sstables.push_back(std::move(sst));
        } else {
            _leveled_sstables_change_cnt++;
            _leveled_sstables.add({make_interval(*sst), value_set({sst})});
        }
    }
    virtual void erase(shared_sstable sst) override {
        if (store_as_unleveled(sst)) {
            _unleveled_sstables.erase(std::remove(_unleveled_sstables.begin(), _unleveled_sstables.end(), sst), _unleveled_sstables.end());
        } else {
            _leveled_sstables_change_cnt++;
            _leveled_sstables.subtract({make_interval(*sst), value_set({sst})});
        }
    }
    virtual std::unique_ptr<incremental_selector_impl> make_incremental_selector() const override;
    class incremental_selector;
};

class partitioned_sstable_set::incremental_selector : public incremental_selector_impl {
    schema_ptr _schema;
    const std::vector<shared_sstable>& _unleveled_sstables;
    const interval_map_type& _leveled_sstables;
    const uint64_t& _leveled_sstables_change_cnt;
    uint64_t _last_known_leveled_sstables_change_cnt;
    map_iterator _it;
    // Only to back the dht::ring_position_view returned from select().
    dht::ring_position _next_position;
private:
    dht::ring_position_view next_position(map_iterator it) {
        if (it == _leveled_sstables.end()) {
            _next_position = dht::ring_position::max();
            return dht::ring_position_view::max();
        } else {
            _next_position = partitioned_sstable_set::to_ring_position(it->first.lower());
            return dht::ring_position_view(_next_position, dht::ring_position_view::after_key(!boost::icl::is_left_closed(it->first.bounds())));
        }
    }
    static bool is_before_interval(const compatible_ring_position_or_view& crp, const interval_type& interval) {
        if (boost::icl::is_left_closed(interval.bounds())) {
            return crp < interval.lower();
        } else {
            return crp <= interval.lower();
        }
    }
    void maybe_invalidate_iterator(const compatible_ring_position_or_view& crp) {
        if (_last_known_leveled_sstables_change_cnt != _leveled_sstables_change_cnt) {
            _it = _leveled_sstables.lower_bound(interval_type::closed(crp, crp));
            _last_known_leveled_sstables_change_cnt = _leveled_sstables_change_cnt;
        }
    }
public:
    incremental_selector(schema_ptr schema, const std::vector<shared_sstable>& unleveled_sstables, const interval_map_type& leveled_sstables,
                         const uint64_t& leveled_sstables_change_cnt)
        : _schema(std::move(schema))
        , _unleveled_sstables(unleveled_sstables)
        , _leveled_sstables(leveled_sstables)
        , _leveled_sstables_change_cnt(leveled_sstables_change_cnt)
        , _last_known_leveled_sstables_change_cnt(leveled_sstables_change_cnt)
        , _it(leveled_sstables.begin())
        , _next_position(dht::ring_position::min()) {
    }
    virtual std::tuple<dht::partition_range, std::vector<shared_sstable>, dht::ring_position_view> select(const dht::ring_position_view& pos) override {
        auto crp = compatible_ring_position_or_view(*_schema, pos);
        auto ssts = _unleveled_sstables;
        using namespace dht;

        maybe_invalidate_iterator(crp);

        while (_it != _leveled_sstables.end()) {
            if (boost::icl::contains(_it->first, crp)) {
                ssts.insert(ssts.end(), _it->second.begin(), _it->second.end());
                return std::make_tuple(partitioned_sstable_set::to_partition_range(_it->first), std::move(ssts), next_position(std::next(_it)));
            }
            // We don't want to skip current interval if pos lies before it.
            if (is_before_interval(crp, _it->first)) {
                return std::make_tuple(partitioned_sstable_set::to_partition_range(pos, _it->first), std::move(ssts), next_position(_it));
            }
            _it++;
        }
        return std::make_tuple(partition_range::make_open_ended_both_sides(), std::move(ssts), ring_position_view::max());
    }
};

std::unique_ptr<incremental_selector_impl> partitioned_sstable_set::make_incremental_selector() const {
    return std::make_unique<incremental_selector>(_schema, _unleveled_sstables, _leveled_sstables, _leveled_sstables_change_cnt);
}

std::unique_ptr<sstable_set_impl> compaction_strategy_impl::make_sstable_set(schema_ptr schema) const {
    return std::make_unique<bag_sstable_set>();
}

std::unique_ptr<sstable_set_impl> leveled_compaction_strategy::make_sstable_set(schema_ptr schema) const {
    return std::make_unique<partitioned_sstable_set>(std::move(schema));
}

std::unique_ptr<sstable_set_impl> make_partitioned_sstable_set(schema_ptr schema, bool use_level_metadata) {
    return std::make_unique<partitioned_sstable_set>(std::move(schema), use_level_metadata);
}

compaction_descriptor compaction_strategy_impl::get_major_compaction_job(column_family& cf, std::vector<sstables::shared_sstable> candidates) {
    return compaction_descriptor(std::move(candidates), cf.get_sstable_set(), service::get_local_compaction_priority());
}

bool compaction_strategy_impl::worth_dropping_tombstones(const shared_sstable& sst, gc_clock::time_point gc_before) {
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
    return sst->estimate_droppable_tombstone_ratio(gc_before) >= _tombstone_threshold;
}

std::vector<resharding_descriptor>
compaction_strategy_impl::get_resharding_jobs(column_family& cf, std::vector<sstables::shared_sstable> candidates) {
    std::vector<resharding_descriptor> jobs;
    shard_id reshard_at_current = 0;

    clogger.debug("Trying to get resharding jobs for {}.{}...", cf.schema()->ks_name(), cf.schema()->cf_name());
    for (auto& candidate : candidates) {
        auto level = candidate->get_sstable_level();
        jobs.push_back(resharding_descriptor{{std::move(candidate)}, std::numeric_limits<uint64_t>::max(), reshard_at_current++ % smp::count, level});
    }
    return jobs;
}

uint64_t compaction_strategy_impl::adjust_partition_estimate(const mutation_source_metadata& ms_meta, uint64_t partition_estimate) {
    return partition_estimate;
}

reader_consumer compaction_strategy_impl::make_interposer_consumer(const mutation_source_metadata& ms_meta, reader_consumer end_consumer) {
    return end_consumer;
}

} // namespace sstables

size_tiered_backlog_tracker::inflight_component
size_tiered_backlog_tracker::partial_backlog(const compaction_backlog_tracker::ongoing_writes& ongoing_writes) const {
    inflight_component in;
    for (auto const& swp :  ongoing_writes) {
        auto written = swp.second->written();
        if (written > 0) {
            in.total_bytes += written;
            in.contribution += written * log4(written);
        }
    }
    return in;
}

size_tiered_backlog_tracker::inflight_component
size_tiered_backlog_tracker::compacted_backlog(const compaction_backlog_tracker::ongoing_compactions& ongoing_compactions) const {
    inflight_component in;
    for (auto const& crp : ongoing_compactions) {
        auto compacted = crp.second->compacted();
        in.total_bytes += compacted;
        in.contribution += compacted * log4((crp.first->data_size()));
    }
    return in;
}

double size_tiered_backlog_tracker::backlog(const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const {
    inflight_component partial = partial_backlog(ow);
    inflight_component compacted = compacted_backlog(oc);

    auto total_bytes = _total_bytes + partial.total_bytes - compacted.total_bytes;
    if ((total_bytes <= 0)) {
        return 0;
    }
    auto sstables_contribution = _sstables_backlog_contribution + partial.contribution - compacted.contribution;
    auto b = (total_bytes * log4(total_bytes)) - sstables_contribution;
    return b > 0 ? b : 0;
}

void size_tiered_backlog_tracker::add_sstable(sstables::shared_sstable sst) {
    if (sst->data_size() > 0) {
        _total_bytes += sst->data_size();
        _sstables_backlog_contribution += sst->data_size() * log4(sst->data_size());
    }
}

void size_tiered_backlog_tracker::remove_sstable(sstables::shared_sstable sst) {
    if (sst->data_size() > 0) {
        _total_bytes -= sst->data_size();
        _sstables_backlog_contribution -= sst->data_size() * log4(sst->data_size());
    }
}

namespace sstables {

// The backlog for TWCS is just the sum of the individual backlogs in each time window.
// We'll keep various SizeTiered backlog tracker objects-- one per window for the static SSTables.
// We then scan the current compacting and in-progress writes and matching them to existing time
// windows.
//
// With the above we have everything we need to just calculate the backlogs individually and sum
// them. Just need to be careful that for the current in progress backlog we may have to create
// a new object for the partial write at this time.
class time_window_backlog_tracker final : public compaction_backlog_tracker::impl {
    time_window_compaction_strategy_options _twcs_options;
    std::unordered_map<api::timestamp_type, size_tiered_backlog_tracker> _windows;

    api::timestamp_type lower_bound_of(api::timestamp_type timestamp) const {
        timestamp_type ts = time_window_compaction_strategy::to_timestamp_type(_twcs_options.timestamp_resolution, timestamp);
        return time_window_compaction_strategy::get_window_lower_bound(_twcs_options.sstable_window_size, ts);
    }
public:
    time_window_backlog_tracker(time_window_compaction_strategy_options options)
        : _twcs_options(options)
    {}

    virtual double backlog(const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const override {
        std::unordered_map<api::timestamp_type, compaction_backlog_tracker::ongoing_writes> writes_per_window;
        std::unordered_map<api::timestamp_type, compaction_backlog_tracker::ongoing_compactions> compactions_per_window;
        double b = 0;

        for (auto& wp : ow) {
            auto bound = lower_bound_of(wp.second->maximum_timestamp());
            writes_per_window[bound].insert(wp);
        }

        for (auto& cp : oc) {
            auto bound = lower_bound_of(cp.first->get_stats_metadata().max_timestamp);
            compactions_per_window[bound].insert(cp);
        }

        auto no_ow = compaction_backlog_tracker::ongoing_writes();
        auto no_oc = compaction_backlog_tracker::ongoing_compactions();
        // Match the in-progress backlogs to existing windows. Compactions should always match an
        // existing windows. Writes in progress can fall into an non-existent window.
        for (auto& windows : _windows) {
            auto bound = windows.first;
            auto* ow_this_window = &no_ow;
            auto itw = writes_per_window.find(bound);
            if (itw != writes_per_window.end()) {
                ow_this_window = &itw->second;
            }
            auto* oc_this_window = &no_oc;
            auto itc = compactions_per_window.find(bound);
            if (itc != compactions_per_window.end()) {
                oc_this_window = &itc->second;
            }
            b += windows.second.backlog(*ow_this_window, *oc_this_window);
            if (itw != writes_per_window.end()) {
                // We will erase here so we can keep track of which
                // writes belong to existing windows. Writes that don't belong to any window
                // are writes in progress to new windows and will be accounted in the final
                // loop before we return
                writes_per_window.erase(itw);
            }
        }

        // Partial writes that don't belong to any window are accounted here.
        for (auto& current : writes_per_window) {
            b += size_tiered_backlog_tracker().backlog(current.second, no_oc);
        }
        return b;
    }

    virtual void add_sstable(sstables::shared_sstable sst) override {
        auto bound = lower_bound_of(sst->get_stats_metadata().max_timestamp);
        _windows[bound].add_sstable(sst);
    }

    virtual void remove_sstable(sstables::shared_sstable sst) override {
        auto bound = lower_bound_of(sst->get_stats_metadata().max_timestamp);
        auto it = _windows.find(bound);
        if (it != _windows.end()) {
            it->second.remove_sstable(sst);
            if (it->second.total_bytes() <= 0) {
                _windows.erase(it);
            }
        }
    }
};

class leveled_compaction_backlog_tracker final : public compaction_backlog_tracker::impl {
    // Because we can do SCTS in L0, we will account for that in the backlog.
    // Whatever backlog we accumulate here will be added to the main backlog.
    size_tiered_backlog_tracker _l0_scts;
    std::vector<uint64_t> _size_per_level;
    uint64_t _max_sstable_size;
public:
    leveled_compaction_backlog_tracker(int32_t max_sstable_size_in_mb)
        : _size_per_level(leveled_manifest::MAX_LEVELS, uint64_t(0))
        , _max_sstable_size(max_sstable_size_in_mb * 1024 * 1024)
    {}

    virtual double backlog(const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const override {
        std::vector<uint64_t> effective_size_per_level = _size_per_level;
        compaction_backlog_tracker::ongoing_writes l0_partial_writes;
        compaction_backlog_tracker::ongoing_compactions l0_compacted;

        for (auto& op : ow) {
            auto level = op.second->level();
            if (level == 0) {
                l0_partial_writes.insert(op);
            }
            effective_size_per_level[level] += op.second->written();
        }

        for (auto& cp : oc) {
            auto level = cp.first->get_sstable_level();
            if (level == 0) {
                l0_compacted.insert(cp);
            }
            effective_size_per_level[level] -= cp.second->compacted();
        }

        double b = _l0_scts.backlog(l0_partial_writes, l0_compacted);
        // Backlog for a level: size_of_level * (max_level - n) * fan_out
        //
        // The fan_out is usually 10. But if the level above us is not
        // fully populated-- which can happen when a level is still being born, we don't want that
        // to jump abruptly. So what we will do instead is to define the fan out as the minimum
        // between 10 and the number of sstables that are estimated to be there.
        //
        // Because of that, it's easier to write this code as an accumulator loop. If we are level
        // L, for each level L + n, n > 0, we accumulate sizeof(L) * fan_out_of(L+n)
        for (size_t level = 0; level < _size_per_level.size() - 1; ++level) {
            auto lsize = effective_size_per_level[level];
            for (size_t next = level + 1; next < _size_per_level.size() - 1; ++next) {
                auto lsize_next = effective_size_per_level[next];
                b += std::min(double(leveled_manifest::leveled_fan_out), double(lsize_next) / _max_sstable_size)  * lsize;
            }
        }
        return b;
    }

    virtual void add_sstable(sstables::shared_sstable sst) override {
        auto level = sst->get_sstable_level();
        _size_per_level[level] += sst->data_size();
        if (level == 0) {
            _l0_scts.add_sstable(sst);
        }
    }

    virtual void remove_sstable(sstables::shared_sstable sst) override {
        auto level = sst->get_sstable_level();
        _size_per_level[level] -= sst->data_size();
        if (level == 0) {
            _l0_scts.remove_sstable(sst);
        }
    }
};

bool compaction_strategy::can_compact_partial_runs() const {
    return _compaction_strategy_impl->can_compact_partial_runs();
}


struct unimplemented_backlog_tracker final : public compaction_backlog_tracker::impl {
    virtual double backlog(const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const override {
        return compaction_controller::disable_backlog;
    }
    virtual void add_sstable(sstables::shared_sstable sst) override { }
    virtual void remove_sstable(sstables::shared_sstable sst) override { }
};

struct null_backlog_tracker final : public compaction_backlog_tracker::impl {
    virtual double backlog(const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const override {
        return 0;
    }
    virtual void add_sstable(sstables::shared_sstable sst) override { }
    virtual void remove_sstable(sstables::shared_sstable sst)  override { }
};

// Just so that if we have more than one CF with NullStrategy, we don't create a lot
// of objects to iterate over for no reason
// Still thread local because of make_unique. But this will disappear soon
static thread_local compaction_backlog_tracker null_backlog_tracker(std::make_unique<null_backlog_tracker>());
compaction_backlog_tracker& get_null_backlog_tracker() {
    return null_backlog_tracker;
}

//
// Null compaction strategy is the default compaction strategy.
// As the name implies, it does nothing.
//
class null_compaction_strategy : public compaction_strategy_impl {
public:
    virtual compaction_descriptor get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) override {
        return sstables::compaction_descriptor();
    }

    virtual int64_t estimated_pending_compactions(column_family& cf) const override {
        return 0;
    }

    virtual compaction_strategy_type type() const {
        return compaction_strategy_type::null;
    }

    virtual compaction_backlog_tracker& get_backlog_tracker() override {
        return get_null_backlog_tracker();
    }
};

leveled_compaction_strategy::leveled_compaction_strategy(const std::map<sstring, sstring>& options)
        : compaction_strategy_impl(options)
        , _max_sstable_size_in_mb(calculate_max_sstable_size_in_mb(compaction_strategy_impl::get_value(options, SSTABLE_SIZE_OPTION)))
        , _stcs_options(options)
        , _backlog_tracker(std::make_unique<leveled_compaction_backlog_tracker>(_max_sstable_size_in_mb))
{
    _compaction_counter.resize(leveled_manifest::MAX_LEVELS);
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
            "improves up to 160MB", max_size);
    }
    return max_size;
}

time_window_compaction_strategy::time_window_compaction_strategy(const std::map<sstring, sstring>& options)
    : compaction_strategy_impl(options)
    , _options(options)
    , _stcs_options(options)
    , _backlog_tracker(std::make_unique<time_window_backlog_tracker>(_options))
{
    if (!options.count(TOMBSTONE_COMPACTION_INTERVAL_OPTION) && !options.count(TOMBSTONE_THRESHOLD_OPTION)) {
        _disable_tombstone_compaction = true;
        clogger.debug("Disabling tombstone compactions for TWCS");
    } else {
        clogger.debug("Enabling tombstone compactions for TWCS");
    }
    _use_clustering_key_filter = true;
}

} // namespace sstables

std::vector<sstables::shared_sstable>
date_tiered_manifest::get_next_sstables(column_family& cf, std::vector<sstables::shared_sstable>& uncompacting, gc_clock::time_point gc_before) {
    if (cf.get_sstables()->empty()) {
        return {};
    }

    // Find fully expired SSTables. Those will be included no matter what.
    auto expired = get_fully_expired_sstables(cf, uncompacting, gc_before);

    if (!expired.empty()) {
        auto is_expired = [&] (const sstables::shared_sstable& s) { return expired.find(s) != expired.end(); };
        uncompacting.erase(boost::remove_if(uncompacting, is_expired), uncompacting.end());
    }

    auto compaction_candidates = get_next_non_expired_sstables(cf, uncompacting, gc_before);
    if (!expired.empty()) {
        compaction_candidates.insert(compaction_candidates.end(), expired.begin(), expired.end());
    }
    return compaction_candidates;
}

int64_t date_tiered_manifest::get_estimated_tasks(column_family& cf) const {
    int base = cf.schema()->min_compaction_threshold();
    int64_t now = get_now(cf);
    std::vector<sstables::shared_sstable> sstables;
    int64_t n = 0;

    sstables.reserve(cf.sstables_count());
    for (auto& entry : *cf.get_sstables()) {
        sstables.push_back(entry);
    }
    auto candidates = filter_old_sstables(sstables, _options.max_sstable_age, now);
    auto buckets = get_buckets(create_sst_and_min_timestamp_pairs(candidates), _options.base_time, base, now);

    for (auto& bucket : buckets) {
        if (bucket.size() >= size_t(cf.schema()->min_compaction_threshold())) {
            n += std::ceil(double(bucket.size()) / cf.schema()->max_compaction_threshold());
        }
    }
    return n;
}

std::vector<sstables::shared_sstable>
date_tiered_manifest::get_next_non_expired_sstables(column_family& cf, std::vector<sstables::shared_sstable>& non_expiring_sstables, gc_clock::time_point gc_before) {
    int base = cf.schema()->min_compaction_threshold();
    int64_t now = get_now(cf);
    auto most_interesting = get_compaction_candidates(cf, non_expiring_sstables, now, base);

    return most_interesting;

    // FIXME: implement functionality below that will look for a single sstable with worth dropping tombstone,
    // iff strategy didn't find anything to compact. So it's not essential.
#if 0
    // if there is no sstable to compact in standard way, try compacting single sstable whose droppable tombstone
    // ratio is greater than threshold.

    List<SSTableReader> sstablesWithTombstones = Lists.newArrayList();
    for (SSTableReader sstable : nonExpiringSSTables)
    {
        if (worthDroppingTombstones(sstable, gcBefore))
            sstablesWithTombstones.add(sstable);
    }
    if (sstablesWithTombstones.isEmpty())
        return Collections.emptyList();

    return Collections.singletonList(Collections.min(sstablesWithTombstones, new SSTableReader.SizeComparator()));
#endif
}

std::vector<sstables::shared_sstable>
date_tiered_manifest::get_compaction_candidates(column_family& cf, std::vector<sstables::shared_sstable> candidate_sstables, int64_t now, int base) {
    int min_threshold = cf.schema()->min_compaction_threshold();
    int max_threshold = cf.schema()->max_compaction_threshold();
    auto candidates = filter_old_sstables(candidate_sstables, _options.max_sstable_age, now);

    auto buckets = get_buckets(create_sst_and_min_timestamp_pairs(candidates), _options.base_time, base, now);

    return newest_bucket(buckets, min_threshold, max_threshold, now, _options.base_time);
}

int64_t date_tiered_manifest::get_now(column_family& cf) {
    int64_t max_timestamp = 0;
    for (auto& sst : *cf.get_sstables()) {
        int64_t candidate = sst->get_stats_metadata().max_timestamp;
        max_timestamp = candidate > max_timestamp ? candidate : max_timestamp;
    }
    return max_timestamp;
}

std::vector<sstables::shared_sstable>
date_tiered_manifest::filter_old_sstables(std::vector<sstables::shared_sstable> sstables, api::timestamp_type max_sstable_age, int64_t now) {
    if (max_sstable_age == 0) {
        return sstables;
    }
    int64_t cutoff = now - max_sstable_age;

    sstables.erase(std::remove_if(sstables.begin(), sstables.end(), [cutoff] (auto& sst) {
        return sst->get_stats_metadata().max_timestamp < cutoff;
    }), sstables.end());

    return sstables;
}

std::vector<std::pair<sstables::shared_sstable,int64_t>>
date_tiered_manifest::create_sst_and_min_timestamp_pairs(const std::vector<sstables::shared_sstable>& sstables) {
    std::vector<std::pair<sstables::shared_sstable,int64_t>> sstable_min_timestamp_pairs;
    sstable_min_timestamp_pairs.reserve(sstables.size());
    for (auto& sst : sstables) {
        sstable_min_timestamp_pairs.emplace_back(sst, sst->get_stats_metadata().min_timestamp);
    }
    return sstable_min_timestamp_pairs;
}

namespace sstables {

date_tiered_compaction_strategy::date_tiered_compaction_strategy(const std::map<sstring, sstring>& options)
    : compaction_strategy_impl(options)
    , _manifest(options)
    , _backlog_tracker(std::make_unique<unimplemented_backlog_tracker>())
{
    clogger.warn("DateTieredCompactionStrategy is deprecated. Usually cases for which it is used are better handled by TimeWindowCompactionStrategy."
            " Please change your compaction strategy to TWCS as DTCS will be retired in the near future");

    // tombstone compaction is disabled by default because:
    // - deletion shouldn't be used with DTCS; rather data is deleted through TTL.
    // - with time series workloads, it's usually better to wait for whole sstable to be expired rather than
    // compacting a single sstable when it's more than 20% (default value) expired.
    // For more details, see CASSANDRA-9234
    if (!options.count(TOMBSTONE_COMPACTION_INTERVAL_OPTION) && !options.count(TOMBSTONE_THRESHOLD_OPTION)) {
        _disable_tombstone_compaction = true;
        date_tiered_manifest::logger.debug("Disabling tombstone compactions for DTCS");
    } else {
        date_tiered_manifest::logger.debug("Enabling tombstone compactions for DTCS");
    }

    _use_clustering_key_filter = true;
}

compaction_descriptor date_tiered_compaction_strategy::get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) {
    auto gc_before = gc_clock::now() - cfs.schema()->gc_grace_seconds();
    auto sstables = _manifest.get_next_sstables(cfs, candidates, gc_before);

    if (!sstables.empty()) {
        date_tiered_manifest::logger.debug("datetiered: Compacting {} out of {} sstables", sstables.size(), candidates.size());
        return sstables::compaction_descriptor(std::move(sstables), cfs.get_sstable_set(), service::get_local_compaction_priority());
    }

    // filter out sstables which droppable tombstone ratio isn't greater than the defined threshold.
    auto e = boost::range::remove_if(candidates, [this, &gc_before] (const sstables::shared_sstable& sst) -> bool {
        return !worth_dropping_tombstones(sst, gc_before);
    });
    candidates.erase(e, candidates.end());
    if (candidates.empty()) {
        return sstables::compaction_descriptor();
    }
    // find oldest sstable which is worth dropping tombstones because they are more unlikely to
    // shadow data from other sstables, and it also tends to be relatively big.
    auto it = std::min_element(candidates.begin(), candidates.end(), [] (auto& i, auto& j) {
        return i->get_stats_metadata().min_timestamp < j->get_stats_metadata().min_timestamp;
    });
    return sstables::compaction_descriptor({ *it }, cfs.get_sstable_set(), service::get_local_compaction_priority());
}

size_tiered_compaction_strategy::size_tiered_compaction_strategy(const std::map<sstring, sstring>& options)
    : compaction_strategy_impl(options)
    , _options(options)
    , _backlog_tracker(std::make_unique<size_tiered_backlog_tracker>())
{}

size_tiered_compaction_strategy::size_tiered_compaction_strategy(const size_tiered_compaction_strategy_options& options)
    : _options(options)
    , _backlog_tracker(std::make_unique<size_tiered_backlog_tracker>())
{}

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

compaction_descriptor compaction_strategy::get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) {
    return _compaction_strategy_impl->get_sstables_for_compaction(cfs, std::move(candidates));
}

compaction_descriptor compaction_strategy::get_major_compaction_job(column_family& cf, std::vector<sstables::shared_sstable> candidates) {
    return _compaction_strategy_impl->get_major_compaction_job(cf, std::move(candidates));
}

std::vector<resharding_descriptor> compaction_strategy::get_resharding_jobs(column_family& cf, std::vector<sstables::shared_sstable> candidates) {
    return _compaction_strategy_impl->get_resharding_jobs(cf, std::move(candidates));
}

void compaction_strategy::notify_completion(const std::vector<shared_sstable>& removed, const std::vector<shared_sstable>& added) {
    _compaction_strategy_impl->notify_completion(removed, added);
}

bool compaction_strategy::parallel_compaction() const {
    return _compaction_strategy_impl->parallel_compaction();
}

int64_t compaction_strategy::estimated_pending_compactions(column_family& cf) const {
    return _compaction_strategy_impl->estimated_pending_compactions(cf);
}

bool compaction_strategy::use_clustering_key_filter() const {
    return _compaction_strategy_impl->use_clustering_key_filter();
}

sstable_set
compaction_strategy::make_sstable_set(schema_ptr schema) const {
    return sstable_set(
            _compaction_strategy_impl->make_sstable_set(schema),
            schema,
            make_lw_shared<sstable_list>());
}

compaction_backlog_tracker& compaction_strategy::get_backlog_tracker() {
    return _compaction_strategy_impl->get_backlog_tracker();
}

uint64_t compaction_strategy::adjust_partition_estimate(const mutation_source_metadata& ms_meta, uint64_t partition_estimate) {
    return _compaction_strategy_impl->adjust_partition_estimate(ms_meta, partition_estimate);
}

reader_consumer compaction_strategy::make_interposer_consumer(const mutation_source_metadata& ms_meta, reader_consumer end_consumer) {
    return _compaction_strategy_impl->make_interposer_consumer(ms_meta, std::move(end_consumer));
}

bool compaction_strategy::use_interposer_consumer() const {
    return _compaction_strategy_impl->use_interposer_consumer();
}

compaction_strategy make_compaction_strategy(compaction_strategy_type strategy, const std::map<sstring, sstring>& options) {
    ::shared_ptr<compaction_strategy_impl> impl;

    switch(strategy) {
    case compaction_strategy_type::null:
        impl = make_shared<null_compaction_strategy>(null_compaction_strategy());
        break;
    case compaction_strategy_type::size_tiered:
        impl = make_shared<size_tiered_compaction_strategy>(size_tiered_compaction_strategy(options));
        break;
    case compaction_strategy_type::leveled:
        impl = make_shared<leveled_compaction_strategy>(leveled_compaction_strategy(options));
        break;
    case compaction_strategy_type::date_tiered:
        impl = make_shared<date_tiered_compaction_strategy>(date_tiered_compaction_strategy(options));
        break;
    case compaction_strategy_type::time_window:
        impl = make_shared<time_window_compaction_strategy>(time_window_compaction_strategy(options));
        break;
    default:
        throw std::runtime_error("strategy not supported");
    }

    return compaction_strategy(std::move(impl));
}

}
