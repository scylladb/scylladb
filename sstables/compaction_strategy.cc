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
#include <cmath>
#include <ctgmath>
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

logging::logger date_tiered_manifest::logger = logging::logger("DateTieredCompactionStrategy");
logging::logger leveled_manifest::logger("LeveledManifest");

namespace sstables {

extern logging::logger clogger;

class incremental_selector_impl {
public:
    virtual ~incremental_selector_impl() {}
    virtual std::tuple<dht::token_range, std::vector<shared_sstable>, dht::token> select(const dht::token& token) = 0;
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

sstable_set::sstable_set(std::unique_ptr<sstable_set_impl> impl, lw_shared_ptr<sstable_list> all)
        : _impl(std::move(impl))
        , _all(std::move(all)) {
}

sstable_set::sstable_set(const sstable_set& x)
        : _impl(x._impl->clone())
        , _all(make_lw_shared(sstable_list(*x._all))) {
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

void
sstable_set::insert(shared_sstable sst) {
    _impl->insert(sst);
    try {
        _all->insert(sst);
    } catch (...) {
        _impl->erase(sst);
        throw;
    }
}

void
sstable_set::erase(shared_sstable sst) {
    _impl->erase(sst);
    _all->erase(sst);
}

sstable_set::~sstable_set() = default;

sstable_set::incremental_selector::incremental_selector(std::unique_ptr<incremental_selector_impl> impl)
    : _impl(std::move(impl)) {
}

sstable_set::incremental_selector::~incremental_selector() = default;

sstable_set::incremental_selector::incremental_selector(sstable_set::incremental_selector&&) noexcept = default;

sstable_set::incremental_selector::selection
sstable_set::incremental_selector::select(const dht::token& t) const {
    if (!_current_token_range || !_current_token_range->contains(t, dht::token_comparator())) {
        std::tie(_current_token_range, _current_sstables, _current_next_token) = _impl->select(t);
    }
    return {_current_sstables, _current_next_token};
}

sstable_set::incremental_selector
sstable_set::make_incremental_selector() const {
    return incremental_selector(_impl->make_incremental_selector());
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
        _sstables.erase(boost::find(_sstables, sst));
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
    virtual std::tuple<dht::token_range, std::vector<shared_sstable>, dht::token> select(const dht::token& token) override {
        return std::make_tuple(dht::token_range::make_open_ended_both_sides(), _sstables, dht::maximum_token());
    }
};

std::unique_ptr<incremental_selector_impl> bag_sstable_set::make_incremental_selector() const {
    return std::make_unique<incremental_selector>(_sstables);
}

// specialized when sstables are partitioned in the token range space
// e.g. leveled compaction strategy
class partitioned_sstable_set : public sstable_set_impl {
    using value_set = std::unordered_set<shared_sstable>;
    using interval_map_type = boost::icl::interval_map<compatible_ring_position, value_set>;
    using interval_type = interval_map_type::interval_type;
    using map_iterator = interval_map_type::const_iterator;
private:
    schema_ptr _schema;
    std::vector<shared_sstable> _unleveled_sstables;
    interval_map_type _leveled_sstables;
private:
    static interval_type make_interval(const schema& s, const dht::partition_range& range) {
        return interval_type::closed(
                compatible_ring_position(s, range.start()->value()),
                compatible_ring_position(s, range.end()->value()));
    }
    interval_type make_interval(const dht::partition_range& range) const {
        return make_interval(*_schema, range);
    }
    interval_type singular(const dht::ring_position& rp) const {
        auto crp = compatible_ring_position(*_schema, rp);
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
public:
    explicit partitioned_sstable_set(schema_ptr schema)
            : _schema(std::move(schema)) {
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
        if (sst->get_sstable_level() == 0) {
            _unleveled_sstables.push_back(std::move(sst));
        } else {
            auto first = sst->get_first_decorated_key().token();
            auto last = sst->get_last_decorated_key().token();
            using bound = dht::partition_range::bound;
            _leveled_sstables.add({
                    make_interval(
                            dht::partition_range(
                                    bound(dht::ring_position::starting_at(first)),
                                    bound(dht::ring_position::ending_at(last)))),
                    value_set({sst})});
        }
    }
    virtual void erase(shared_sstable sst) override {
        if (sst->get_sstable_level() == 0) {
            _unleveled_sstables.erase(std::remove(_unleveled_sstables.begin(), _unleveled_sstables.end(), sst), _unleveled_sstables.end());
        } else {
            auto first = sst->get_first_decorated_key().token();
            auto last = sst->get_last_decorated_key().token();
            using bound = dht::partition_range::bound;
            _leveled_sstables.subtract({
                    make_interval(
                            dht::partition_range(
                                    bound(dht::ring_position::starting_at(first)),
                                    bound(dht::ring_position::ending_at(last)))),
                    value_set({sst})});
        }
    }
    virtual std::unique_ptr<incremental_selector_impl> make_incremental_selector() const override;
    class incremental_selector;
};

class partitioned_sstable_set::incremental_selector : public incremental_selector_impl {
    schema_ptr _schema;
    const std::vector<shared_sstable>& _unleveled_sstables;
    map_iterator _it;
    const map_iterator _end;
private:
    static dht::token_range to_token_range(const interval_type& i) {
        return dht::token_range::make({i.lower().token(), boost::icl::is_left_closed(i.bounds())},
            {i.upper().token(), boost::icl::is_right_closed(i.bounds())});
    }
public:
    incremental_selector(schema_ptr schema, const std::vector<shared_sstable>& unleveled_sstables, const interval_map_type& leveled_sstables)
        : _schema(std::move(schema))
        , _unleveled_sstables(unleveled_sstables)
        , _it(leveled_sstables.begin())
        , _end(leveled_sstables.end()) {
    }
    virtual std::tuple<dht::token_range, std::vector<shared_sstable>, dht::token> select(const dht::token& token) override {
        auto pr = dht::partition_range::make(dht::ring_position::starting_at(token), dht::ring_position::ending_at(token));
        auto interval = make_interval(*_schema, std::move(pr));
        auto ssts = _unleveled_sstables;

        const auto next_token =  [&] {
            const auto next = std::next(_it);
            return next == _end ? dht::maximum_token() : next->first.lower().token();
        };

        const auto current_token =  [&] {
            return _it == _end ? dht::maximum_token() : _it->first.lower().token();
        };

        while (_it != _end) {
            if (boost::icl::contains(_it->first, interval)) {
                ssts.insert(ssts.end(), _it->second.begin(), _it->second.end());
                return std::make_tuple(to_token_range(_it->first), std::move(ssts), next_token());
            }
            // we don't want to skip current interval if token lies before it.
            if (boost::icl::lower_less(interval, _it->first)) {
                return std::make_tuple(dht::token_range::make({token, true}, {_it->first.lower().token(), false}),
                    std::move(ssts),
                    current_token());
            }
            _it++;
        }
        return std::make_tuple(dht::token_range::make_open_ended_both_sides(), std::move(ssts), dht::maximum_token());
    }
};

std::unique_ptr<incremental_selector_impl> partitioned_sstable_set::make_incremental_selector() const {
    return std::make_unique<incremental_selector>(_schema, _unleveled_sstables, _leveled_sstables);
}

std::unique_ptr<sstable_set_impl> compaction_strategy_impl::make_sstable_set(schema_ptr schema) const {
    return std::make_unique<bag_sstable_set>();
}

std::unique_ptr<sstable_set_impl> leveled_compaction_strategy::make_sstable_set(schema_ptr schema) const {
    return std::make_unique<partitioned_sstable_set>(std::move(schema));
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

// Backlog for one SSTable under STCS:
//
//   (1) Bi = Ei * log4 (T / Si),
//
// where Ei is the effective size of the SStable, Si is the Size of this
// SSTable, and T is the total size of the Table.
//
// To calculate the backlog, we can use the logarithm in any base, but we choose
// 4 as that is the historical minimum for the number of SSTables being compacted
// together. Although now that minimum could be lifted, this is still a good number
// of SSTables to aim for in a compaction execution.
//
// T, the total table size, is defined as
//
//   (2) T = Sum(i = 0...N) { Si }.
//
// Ei, the effective size, is defined as
//
//   (3) Ei = Si - Ci,
//
// where Ci is the total amount of bytes already compacted for this table.
// For tables that are not under compaction, Ci = 0 and Si = Ei.
//
// Using the fact that log(a / b) = log(a) - log(b), we rewrite (1) as:
//
//   Bi = Ei log4(T) - Ei log4(Si)
//
// For the entire Table, the Aggregate Backlog (A) is
//
//   A = Sum(i = 0...N) { Ei * log4(T) - Ei * log4(Si) },
//
// which can be expressed as a sum of a table component and a SSTable component:
//
//   A = Sum(i = 0...N) { Ei } * log4(T) - Sum(i = 0...N) { Ei * log4(Si) },
//
// and if we define C = Sum(i = 0...N) { Ci }, then we can write
//
//   A = (T - C) * log4(T) - Sum(i = 0...N) { (Si - Ci)* log4(Si) }.
//
// Because the SSTable number can be quite big, we'd like to keep iterations to a minimum.
// We can do that if we rewrite the expression above one more time, yielding:
//
//   (4) A = T * log4(T) - C * log4(T) - (Sum(i = 0...N) { Si * log4(Si) } - Sum(i = 0...N) { Ci * log4(Si) }
//
// When SSTables are added or removed, we update the static parts of the equation, and
// every time we need to compute the backlog we use the most up-to-date estimate of Ci to
// calculate the compacted parts, having to iterate only over the SSTables that are compacting,
// instead of all of them.
//
// For SSTables that are being currently written, we assume that they are a full SSTable in a
// certain point in time, whose size is the amount of bytes currently written. So all we need
// to do is keep track of them too, and add the current estimate to the static part of (4).
class size_tiered_backlog_tracker final : public compaction_backlog_tracker::impl {
    uint64_t _total_bytes = 0;
    uint64_t _sstables_compacted_total_bytes = 0;
    double _sstables_backlog_contribution = 0.0f;

    struct inflight_component {
        uint64_t total_bytes = 0;
        double contribution = 0;
    };

    inflight_component partial_backlog(compaction_backlog_tracker::ongoing_writes& ongoing_writes) {
        inflight_component in;
        for (auto& swp :  ongoing_writes) {
            auto written = swp.second->written();
            if (written > 0) {
                in.total_bytes += written;
                in.contribution += written * log4(written);
            }
        }
        return in;
    }

    inflight_component compacted_backlog(compaction_backlog_tracker::ongoing_compactions& ongoing_compactions) {
        inflight_component in;
        for (auto& crp : ongoing_compactions) {
            auto compacted = crp.second->compacted();
            in.total_bytes += compacted;
            in.contribution += compacted * log4((crp.first->data_size()));
        }
        return in;
    }
    double log4(double x) const {
        static constexpr double inv_log_4 = 1.0f / std::log(4);
        return log(x) * inv_log_4;
    }
public:
    virtual double backlog(compaction_backlog_tracker::ongoing_writes& ow, compaction_backlog_tracker::ongoing_compactions& oc) {
        inflight_component partial = partial_backlog(ow);
        inflight_component compacted = compacted_backlog(oc);

        auto total_bytes = _total_bytes + partial.total_bytes - compacted.total_bytes;
        if ((total_bytes == 0)) {
            return 0;
        }
        auto sstables_contribution = _sstables_backlog_contribution + partial.contribution - compacted.contribution;
        return (total_bytes * log4(total_bytes)) - sstables_contribution;
    }

    virtual void add_sstable(sstables::shared_sstable sst)  override {
        if (sst->data_size() > 0) {
            _total_bytes += sst->data_size();
            _sstables_backlog_contribution += sst->data_size() * log4(sst->data_size());
        }
    }

    // Removing could be the result of a failure of an in progress write, successful finish of a
    // compaction, or some one-off operation, like drop
    virtual void remove_sstable(sstables::shared_sstable sst)  override {
        if (sst->data_size() > 0) {
            _total_bytes -= sst->data_size();
            _sstables_backlog_contribution -= sst->data_size() * log4(sst->data_size());
        }
    }
};

struct unimplemented_backlog_tracker final : public compaction_backlog_tracker::impl {
    virtual double backlog(compaction_backlog_tracker::ongoing_writes& ow, compaction_backlog_tracker::ongoing_compactions& oc) override {
        return std::numeric_limits<double>::infinity();
    }
    virtual void add_sstable(sstables::shared_sstable sst) override { }
    virtual void remove_sstable(sstables::shared_sstable sst) override { }
};

struct null_backlog_tracker final : public compaction_backlog_tracker::impl {
    virtual double backlog(compaction_backlog_tracker::ongoing_writes& ow, compaction_backlog_tracker::ongoing_compactions& oc) override {
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

//
// Major compaction strategy is about compacting all available sstables into one.
//
class major_compaction_strategy : public compaction_strategy_impl {
    static constexpr size_t min_compact_threshold = 2;
public:
    virtual compaction_descriptor get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) override {
        // At least, two sstables must be available for compaction to take place.
        if (cfs.sstables_count() < min_compact_threshold) {
            return sstables::compaction_descriptor();
        }
        return sstables::compaction_descriptor(std::move(candidates));
    }

    virtual int64_t estimated_pending_compactions(column_family& cf) const override {
        return (cf.sstables_count() < min_compact_threshold) ? 0 : 1;
    }

    virtual compaction_strategy_type type() const {
        return compaction_strategy_type::major;
    }

    virtual compaction_backlog_tracker& get_backlog_tracker() override {
        return get_null_backlog_tracker();
    }
};

leveled_compaction_strategy::leveled_compaction_strategy(const std::map<sstring, sstring>& options)
        : compaction_strategy_impl(options)
        , _stcs_options(options)
        , _backlog_tracker(std::make_unique<unimplemented_backlog_tracker>())
{
    using namespace cql3::statements;

    auto tmp_value = compaction_strategy_impl::get_value(options, SSTABLE_SIZE_OPTION);
    _max_sstable_size_in_mb = property_definitions::to_int(SSTABLE_SIZE_OPTION, tmp_value, DEFAULT_MAX_SSTABLE_SIZE_IN_MB);
    if (_max_sstable_size_in_mb >= 1000) {
        leveled_manifest::logger.warn("Max sstable size of {}MB is configured; having a unit of compaction this large is probably a bad idea",
            _max_sstable_size_in_mb);
    } else if (_max_sstable_size_in_mb < 50) {
        leveled_manifest::logger.warn("Max sstable size of {}MB is configured. Testing done for CASSANDRA-5727 indicates that performance" \
            "improves up to 160MB", _max_sstable_size_in_mb);
    }
    _compaction_counter.resize(leveled_manifest::MAX_LEVELS);
}

time_window_compaction_strategy::time_window_compaction_strategy(const std::map<sstring, sstring>& options)
    : compaction_strategy_impl(options)
    , _options(options)
    , _stcs_options(options)
    , _backlog_tracker(std::make_unique<unimplemented_backlog_tracker>())
{
    if (!options.count(TOMBSTONE_COMPACTION_INTERVAL_OPTION) && !options.count(TOMBSTONE_THRESHOLD_OPTION)) {
        _disable_tombstone_compaction = true;
        clogger.debug("Disabling tombstone compactions for TWCS");
    } else {
        clogger.debug("Enabling tombstone compactions for TWCS");
    }
    _use_clustering_key_filter = true;
}

date_tiered_compaction_strategy::date_tiered_compaction_strategy(const std::map<sstring, sstring>& options)
    : compaction_strategy_impl(options)
    , _manifest(options)
    , _backlog_tracker(std::make_unique<unimplemented_backlog_tracker>())
{
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
            _compaction_strategy_impl->make_sstable_set(std::move(schema)),
            make_lw_shared<sstable_list>());
}

compaction_backlog_tracker& compaction_strategy::get_backlog_tracker() {
    return _compaction_strategy_impl->get_backlog_tracker();
}

compaction_strategy make_compaction_strategy(compaction_strategy_type strategy, const std::map<sstring, sstring>& options) {
    ::shared_ptr<compaction_strategy_impl> impl;

    switch(strategy) {
    case compaction_strategy_type::null:
        impl = make_shared<null_compaction_strategy>(null_compaction_strategy());
        break;
    case compaction_strategy_type::major:
        impl = make_shared<major_compaction_strategy>(major_compaction_strategy());
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
