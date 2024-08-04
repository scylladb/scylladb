/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include <seastar/util/defer.hh>

#include <boost/icl/interval_map.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/remove_if.hpp>
#include <boost/range/algorithm/sort.hpp>

#include "sstables.hh"

#include "dht/ring_position.hh"
#include "compaction/compaction_strategy_impl.hh"
#include "compaction/leveled_compaction_strategy.hh"
#include "compaction/time_window_compaction_strategy.hh"

#include "sstable_set_impl.hh"

#include "replica/database.hh"
#include "readers/from_mutations_v2.hh"
#include "readers/empty_v2.hh"
#include "readers/combined.hh"

namespace sstables {

extern logging::logger sstlog;

bool
sstable_first_key_less_comparator::operator()(const shared_sstable& s1, const shared_sstable& s2) const {
    auto r = s1->compare_by_first_key(*s2);
    if (r == 0) {
        position_in_partition::less_compare less_cmp(*s1->get_schema());
        return less_cmp(s1->first_partition_first_position(), s2->first_partition_first_position());
    }
    return r < 0;
}

bool sstable_run::will_introduce_overlapping(const shared_sstable& sst) const {
    // checks if s1 is *all* before s2, meaning their bounds don't overlap.
    auto completely_ordered_before = [] (const shared_sstable& s1, const shared_sstable& s2) {
        auto pkey_tri_cmp = [s = s1->get_schema()] (const dht::decorated_key& k1, const dht::decorated_key& k2) {
            return k1.tri_compare(*s, k2);
        };
        auto r = pkey_tri_cmp(s1->get_last_decorated_key(), s2->get_first_decorated_key());
        if (r == 0) {
            position_in_partition::tri_compare ckey_tri_cmp(*s1->get_schema());
            const auto& s1_last_position = s1->last_partition_last_position();
            const auto& s2_first_position = s2->first_partition_first_position();
            auto r2 = ckey_tri_cmp(s1_last_position, s2_first_position);
            // Forgive overlapping if s1's last position and s2's first position are both after key.
            // That still produces correct results because the writer translates after_all_prefixed
            // for s1's end bound into bound_kind::incl_end, and s2's start bound into bound_kind::excl_start,
            // meaning they don't actually overlap.
            if (r2 == 0 && s1_last_position.get_bound_weight() == bound_weight::after_all_prefixed) {
                return true;
            }
            return r2 < 0;
        }
        return r < 0;
    };
    // lower bound will be the 1st element which is not *all* before the candidate sstable.
    // upper bound will be the 1st element which the candidate sstable is *all* before.
    // if there's overlapping, lower bound will be 1st element which overlaps, whereas upper bound the 1st one which doesn't (or end iterator)
    // if there's not overlapping, lower and upper bound will both point to 1st element which the candidate sstable is *all* before (or end iterator).
    auto p = std::equal_range(_all.begin(), _all.end(), sst, completely_ordered_before);
    return p.first != p.second;
};

sstable_run::sstable_run(shared_sstable sst)
    : _all({std::move(sst)}) {
}

bool sstable_run::insert(shared_sstable sst) {
    if (will_introduce_overlapping(sst)) {
        return false;
    }
    _all.insert(std::move(sst));
    return true;
}

void sstable_run::erase(shared_sstable sst) {
    _all.erase(sst);
}

uint64_t sstable_run::data_size() const {
    return boost::accumulate(_all | boost::adaptors::transformed(std::mem_fn(&sstable::data_size)), uint64_t(0));
}

double sstable_run::estimate_droppable_tombstone_ratio(const gc_clock::time_point& compaction_time, const tombstone_gc_state& gc_state, const schema_ptr& s) const {
    auto estimate_sum = boost::accumulate(_all | boost::adaptors::transformed(std::bind(&sstable::estimate_droppable_tombstone_ratio, std::placeholders::_1, compaction_time, gc_state, s)), double(0));
    return _all.size() ? estimate_sum / _all.size() : double(0);
}

sstables::run_id sstable_run::run_identifier() const {
    return (_all.empty()) ? run_id() : (*_all.begin())->run_identifier();
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

sstable_set::sstable_set(std::unique_ptr<sstable_set_impl> impl)
        : _impl(std::move(impl))
{}

sstable_set::sstable_set(const sstable_set& x)
        : _impl(x._impl->clone())
{}

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

std::vector<frozen_sstable_run>
sstable_set::all_sstable_runs() const {
    return _impl->all_sstable_runs();
}

std::vector<frozen_sstable_run>
partitioned_sstable_set::all_sstable_runs() const {
    return boost::copy_range<std::vector<frozen_sstable_run>>(_all_runs | boost::adaptors::map_values);
}

lw_shared_ptr<const sstable_list>
sstable_set::all() const {
    return _impl->all();
}

void sstable_set::for_each_sstable(std::function<void(const shared_sstable&)> func) const {
    _impl->for_each_sstable_until([func = std::move(func)] (const shared_sstable& sst) {
        func(sst);
        return stop_iteration::no;
    });
}

stop_iteration sstable_set::for_each_sstable_until(std::function<stop_iteration(const shared_sstable&)> func) const {
    return _impl->for_each_sstable_until(std::move(func));
}

bool
sstable_set::insert(shared_sstable sst) {
    return _impl->insert(sst);
}

bool
sstable_set::erase(shared_sstable sst) {
    return _impl->erase(sst);
}

size_t
sstable_set::size() const noexcept {
    return _impl->size();
}

uint64_t
sstable_set::bytes_on_disk() const noexcept {
    return _impl->bytes_on_disk();
}

sstable_set::~sstable_set() = default;

sstable_set::incremental_selector::incremental_selector(std::unique_ptr<incremental_selector_impl> impl, const schema& s)
    : _impl(std::move(impl))
    , _cmp(s) {
}

sstable_set::incremental_selector::~incremental_selector() = default;

sstable_set::incremental_selector::incremental_selector(sstable_set::incremental_selector&&) noexcept = default;

sstable_set::incremental_selector::selection
sstable_set::incremental_selector::select(selector_pos s) const {
    if (!_current_range_view || !_current_range_view->contains(s.pos, _cmp)) {
        std::tie(_current_range, _current_sstables, _current_next_position) = _impl->select(s);
        _current_range_view = _current_range->transform([] (const dht::ring_position& rp) { return dht::ring_position_view(rp); });
    }
    return {_current_sstables, _current_next_position};
}

sstable_set::incremental_selector
sstable_set::make_incremental_selector() const {
    auto selector = _impl->make_incremental_selector();
    return incremental_selector(std::get<0>(std::move(selector)), std::get<1>(selector));
}

partitioned_sstable_set::interval_type partitioned_sstable_set::make_interval(const schema& s, const dht::partition_range& range) {
    return interval_type::closed(
            dht::compatible_ring_position_or_view(s, dht::ring_position_view(range.start()->value())),
            dht::compatible_ring_position_or_view(s, dht::ring_position_view(range.end()->value())));
}

partitioned_sstable_set::interval_type partitioned_sstable_set::make_interval(const dht::partition_range& range) const {
    return make_interval(*_schema, range);
}

partitioned_sstable_set::interval_type partitioned_sstable_set::make_interval(const schema_ptr& s, const sstable& sst) {
    return interval_type::closed(
            dht::compatible_ring_position_or_view(s, dht::ring_position(sst.get_first_decorated_key())),
            dht::compatible_ring_position_or_view(s, dht::ring_position(sst.get_last_decorated_key())));
}

partitioned_sstable_set::interval_type partitioned_sstable_set::make_interval(const sstable& sst) {
    return make_interval(_schema, sst);
}

partitioned_sstable_set::interval_type partitioned_sstable_set::singular(const dht::ring_position& rp) const {
    // We should use the view here, since this is used for queries.
    auto rpv = dht::ring_position_view(rp);
    auto crp = dht::compatible_ring_position_or_view(*_schema, std::move(rpv));
    return interval_type::closed(crp, crp);
}

std::pair<partitioned_sstable_set::map_iterator, partitioned_sstable_set::map_iterator>
partitioned_sstable_set::query(const dht::partition_range& range) const {
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

bool partitioned_sstable_set::store_as_unleveled(const shared_sstable& sst) const {
    return _use_level_metadata && sst->get_sstable_level() == 0;
}

dht::ring_position partitioned_sstable_set::to_ring_position(const dht::compatible_ring_position_or_view& crp) {
    // Ring position views, representing bounds of sstable intervals are
    // guaranteed to have key() != nullptr;
    const auto& pos = crp.position();
    return dht::ring_position(pos.token(), *pos.key());
}

dht::partition_range partitioned_sstable_set::to_partition_range(const interval_type& i) {
    return dht::partition_range::make(
            {to_ring_position(i.lower()), boost::icl::is_left_closed(i.bounds())},
            {to_ring_position(i.upper()), boost::icl::is_right_closed(i.bounds())});
}

dht::partition_range partitioned_sstable_set::to_partition_range(const dht::ring_position_view& pos, const interval_type& i) {
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

partitioned_sstable_set::partitioned_sstable_set(schema_ptr schema, bool use_level_metadata)
        : _schema(std::move(schema))
        , _all(make_lw_shared<sstable_list>())
        , _use_level_metadata(use_level_metadata) {
}

static std::unordered_map<run_id, shared_sstable_run> clone_runs(const std::unordered_map<run_id, shared_sstable_run>& runs) {
    return boost::copy_range<std::unordered_map<run_id, shared_sstable_run>>(runs | boost::adaptors::transformed([] (auto& p) {
        return std::make_pair(p.first, make_lw_shared<sstable_run>(*p.second));
    }));
}

partitioned_sstable_set::partitioned_sstable_set(schema_ptr schema, const std::vector<shared_sstable>& unleveled_sstables, const interval_map_type& leveled_sstables,
        const lw_shared_ptr<sstable_list>& all, const std::unordered_map<run_id, shared_sstable_run>& all_runs, bool use_level_metadata, uint64_t bytes_on_disk)
        : sstable_set_impl(bytes_on_disk)
        , _schema(schema)
        , _unleveled_sstables(unleveled_sstables)
        , _leveled_sstables(leveled_sstables)
        , _all(make_lw_shared<sstable_list>(*all))
        , _all_runs(clone_runs(all_runs))
        , _use_level_metadata(use_level_metadata) {
}

std::unique_ptr<sstable_set_impl> partitioned_sstable_set::clone() const {
    return std::make_unique<partitioned_sstable_set>(_schema, _unleveled_sstables, _leveled_sstables, _all, _all_runs, _use_level_metadata, _bytes_on_disk);
}

std::vector<shared_sstable> partitioned_sstable_set::select(const dht::partition_range& range) const {
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

lw_shared_ptr<const sstable_list> partitioned_sstable_set::all() const {
    return _all;
}

stop_iteration partitioned_sstable_set::for_each_sstable_until(std::function<stop_iteration(const shared_sstable&)> func) const {
    for (auto& sst : *_all) {
        if (func(sst)) {
            return stop_iteration::yes;
        }
    }
    return stop_iteration::no;
}

future<stop_iteration> partitioned_sstable_set::for_each_sstable_gently_until(std::function<future<stop_iteration>(const shared_sstable&)> func) const {
    for (auto& sst : *_all) {
        auto stop = co_await func(sst);
        if (stop) {
            co_return stop_iteration::yes;
        }
    }
    co_return stop_iteration::no;
}

bool partitioned_sstable_set::insert(shared_sstable sst) {
    auto [_, inserted] = _all->insert(sst);
    if (!inserted) {
        // sst is already in the set, no further handling is required
        return false;
    }
    auto n = sst->bytes_on_disk();
    add_bytes_on_disk(n);
    auto undo_all_insert = defer([&] () {
        _all->erase(sst);
        sub_bytes_on_disk(n);
    });

    auto maybe_insert_run_fragment = [this] (const shared_sstable& sst) mutable {
        auto it = _all_runs.find(sst->run_identifier());
        if (it == _all_runs.end()) {
            auto new_run = make_lw_shared<sstable_run>(sst);
            return _all_runs.emplace(sst->run_identifier(), std::move(new_run)).second;
        }
        return it->second->insert(sst);
    };

    // If sstable doesn't satisfy disjoint invariant, then place it in a new sstable run.
    while (!maybe_insert_run_fragment(sst)) {
        sstlog.warn("Generating a new run identifier for SSTable {} as overlapping was detected when inserting it into SSTable run {}",
                    sst->get_filename(), sst->run_identifier());
        sst->generate_new_run_identifier();
    }
    auto undo_all_runs_insert = defer([&] () { _all_runs[sst->run_identifier()]->erase(sst); });

    if (store_as_unleveled(sst)) {
        _unleveled_sstables.push_back(sst);
    } else {
        _leveled_sstables_change_cnt++;
        _leveled_sstables.add({make_interval(*sst), value_set({sst})});
    }
    undo_all_insert.cancel();
    undo_all_runs_insert.cancel();
    return true;
}

bool partitioned_sstable_set::erase(shared_sstable sst) {
    if (auto it = _all_runs.find(sst->run_identifier()); it != _all_runs.end()) {
        it->second->erase(sst);
        if (it->second->empty()) {
            _all_runs.erase(it);
        }
    }
    auto ret = _all->erase(sst) != 0;
    if (ret) {
        sub_bytes_on_disk(sst->bytes_on_disk());
    }
    if (store_as_unleveled(sst)) {
        _unleveled_sstables.erase(std::remove(_unleveled_sstables.begin(), _unleveled_sstables.end(), sst), _unleveled_sstables.end());
    } else {
        _leveled_sstables_change_cnt++;
        _leveled_sstables.subtract({make_interval(*sst), value_set({sst})});
    }
    return ret;
}

size_t
partitioned_sstable_set::size() const noexcept {
    return _all->size();
}

class partitioned_sstable_set::incremental_selector : public incremental_selector_impl {
    schema_ptr _schema;
    const std::vector<shared_sstable>& _unleveled_sstables;
    const interval_map_type& _leveled_sstables;
    const uint64_t& _leveled_sstables_change_cnt;
    uint64_t _last_known_leveled_sstables_change_cnt;
    map_iterator _it;
private:
    dht::ring_position_ext next_position(map_iterator it) {
        if (it == _leveled_sstables.end()) {
            return dht::ring_position_view::max();
        } else {
            auto&& next_position = partitioned_sstable_set::to_ring_position(it->first.lower());
            return dht::ring_position_ext(next_position, dht::ring_position_ext::after_key(!boost::icl::is_left_closed(it->first.bounds())));
        }
    }
    static bool is_before_interval(const dht::compatible_ring_position_or_view& crp, const interval_type& interval) {
        if (boost::icl::is_left_closed(interval.bounds())) {
            return crp < interval.lower();
        } else {
            return crp <= interval.lower();
        }
    }
    void maybe_invalidate_iterator(const dht::compatible_ring_position_or_view& crp) {
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
        , _it(leveled_sstables.begin()) {
    }
    virtual std::tuple<dht::partition_range, std::vector<shared_sstable>, dht::ring_position_ext> select(const selector_pos& s) override {
        const dht::ring_position_view& pos = s.pos;
        auto crp = dht::compatible_ring_position_or_view(*_schema, pos);
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

time_series_sstable_set::time_series_sstable_set(schema_ptr schema, bool enable_optimized_twcs_queries)
    : _schema(std::move(schema))
    , _reversed_schema(_schema->make_reversed())
    , _enable_optimized_twcs_queries(enable_optimized_twcs_queries)
    , _sstables(make_lw_shared<container_t>(position_in_partition::less_compare(*_schema)))
    , _sstables_reversed(make_lw_shared<container_t>(position_in_partition::less_compare(*_reversed_schema)))
{}

time_series_sstable_set::time_series_sstable_set(const time_series_sstable_set& s)
    : sstable_set_impl(s)
    , _schema(s._schema)
    , _reversed_schema(s._reversed_schema)
    , _enable_optimized_twcs_queries(s._enable_optimized_twcs_queries)
    , _sstables(make_lw_shared(*s._sstables))
    , _sstables_reversed(make_lw_shared(*s._sstables_reversed))
{}

std::unique_ptr<sstable_set_impl> time_series_sstable_set::clone() const {
    return std::make_unique<time_series_sstable_set>(*this);
}

std::vector<shared_sstable> time_series_sstable_set::select(const dht::partition_range& range) const {
    return boost::copy_range<std::vector<shared_sstable>>(*_sstables | boost::adaptors::map_values);
}

lw_shared_ptr<const sstable_list> time_series_sstable_set::all() const {
    return make_lw_shared<const sstable_list>(boost::copy_range<const sstable_list>(*_sstables | boost::adaptors::map_values));
}

size_t
time_series_sstable_set::size() const noexcept {
    return _sstables->size();
}

stop_iteration time_series_sstable_set::for_each_sstable_until(std::function<stop_iteration(const shared_sstable&)> func) const {
    for (auto& entry : *_sstables) {
        if (func(entry.second)) {
            return stop_iteration::yes;
        }
    }
    return stop_iteration::no;
}

future<stop_iteration> time_series_sstable_set::for_each_sstable_gently_until(std::function<future<stop_iteration>(const shared_sstable&)> func) const {
    for (const auto& [pos, sst] : *_sstables) {
        auto stop = co_await func(sst);
        if (stop) {
            co_return stop_iteration::yes;
        }
    }
    co_return stop_iteration::no;
}

// O(log n)
bool time_series_sstable_set::insert(shared_sstable sst) {
  try {
    auto min_pos = sst->min_position();
    auto max_pos_reversed = sst->max_position().reversed();
    _sstables->emplace(std::move(min_pos), sst);
    add_bytes_on_disk(sst->bytes_on_disk());
    _sstables_reversed->emplace(std::move(max_pos_reversed), std::move(sst));
  } catch (...) {
    erase(sst);
    throw;
  }
  return true;
}

// O(n) worst case, but should be close to O(log n) most of the time
bool time_series_sstable_set::erase(shared_sstable sst) {
    bool found;
    {
        auto [first, last] = _sstables->equal_range(sst->min_position());
        auto it = std::find_if(first, last,
                [&sst] (const std::pair<position_in_partition, shared_sstable>& p) { return sst == p.second; });
        found = it != last;
        if (found) {
            _sstables->erase(it);
            sub_bytes_on_disk(sst->bytes_on_disk());
        }
    }

    auto [first, last] = _sstables_reversed->equal_range(sst->max_position().reversed());
    auto it = std::find_if(first, last,
            [&sst] (const std::pair<position_in_partition, shared_sstable>& p) { return sst == p.second; });
    if (it != last) {
        _sstables_reversed->erase(it);
    }
    return found;
}

sstable_set_impl::selector_and_schema_t time_series_sstable_set::make_incremental_selector() const {
    struct selector : public incremental_selector_impl {
        const time_series_sstable_set& _set;

        selector(const time_series_sstable_set& set) : _set(set) {}

        virtual std::tuple<dht::partition_range, std::vector<shared_sstable>, dht::ring_position_ext>
        select(const selector_pos&) override {
            return std::make_tuple(dht::partition_range::make_open_ended_both_sides(), _set.select(), dht::ring_position_view::max());
        }
    };

    return std::make_tuple(std::make_unique<selector>(*this), std::cref(*_schema));
}

// Queue of readers of sstables in a time_series_sstable_set,
// returning readers in order of the sstables' clustering key lower bounds.
//
// For sstable `s` we take `s.min_position()` as the lower bound for non-reversed reads,
// and `s.max_position().reversed()` for reversed reads (in reversed reads comparisons
// are performed using a reversed schema). Let `lower_bound(s)` denote this lower bound
// in the comments below.
//
// Skips sstables that don't pass the supplied filter.
// Guarantees that the filter will be called at most once for each sstable;
// exactly once after all sstables are iterated over.
//
// The readers are created lazily on-demand using the supplied factory function.
//
// Additionally to the sstable readers, the queue always returns one ``dummy reader''
// that contains only the partition_start/end markers. This dummy reader is always
// returned as the first on the first `pop(b)` call for any `b`. Its upper bound
// is `before_all_clustered_rows`.
class sstable_position_reader_queue : public position_reader_queue {
    using container_t = time_series_sstable_set::container_t;
    using value_t = container_t::value_type;

    schema_ptr _query_schema;
    lw_shared_ptr<const container_t> _sstables;

    // Iterates over sstables in order of their lower bounds.
    // Invariant: _it == _end or filter(it->second) == true
    container_t::const_iterator _it;
    const container_t::const_iterator _end;

    position_in_partition::tri_compare _cmp;

    std::function<mutation_reader(sstable&)> _create_reader;
    std::function<bool(const sstable&)> _filter;

    // After construction contains a reader which returns only the partition
    // start (and end, if not in forwarding mode) markers. This is the first
    // returned reader.
    std::optional<mutation_reader> _dummy_reader;

    bool _reversed;

    mutation_reader create_reader(sstable& sst) {
        return _create_reader(sst);
    }

    bool filter(const sstable& sst) const {
        return _filter(sst);
    }

public:
    // Assumes that `create_reader` returns readers that emit only fragments from partition `pk`.
    //
    // For reversed reads `query_schema` must be reversed (see docs/dev/reverse-reads.md).
    sstable_position_reader_queue(const time_series_sstable_set& set,
            schema_ptr query_schema,
            std::function<mutation_reader(sstable&)> create_reader,
            std::function<bool(const sstable&)> filter,
            partition_key pk,
            reader_permit permit,
            streamed_mutation::forwarding fwd_sm,
            bool reversed)
        : _query_schema(std::move(query_schema))
        , _sstables(reversed ? set._sstables_reversed : set._sstables)
        , _it(_sstables->begin())
        , _end(_sstables->end())
        , _cmp(*_query_schema)
        , _create_reader(std::move(create_reader))
        , _filter(std::move(filter))
        , _dummy_reader(make_mutation_reader_from_mutations_v2(_query_schema,
                std::move(permit), mutation(_query_schema, std::move(pk)), _query_schema->full_slice(), fwd_sm))
        , _reversed(reversed)
    {
        while (_it != _end && !this->filter(*_it->second)) {
            ++_it;
        }
    }

    virtual ~sstable_position_reader_queue() override = default;

    // If the dummy reader was not yet returned, return the dummy reader.
    // Otherwise, open sstable readers to all sstables with smallest lower_bound() from the set
    // {S: filter(S) and prev_min_pos < lower_bound(S) <= bound}, where `prev_min_pos` is the lower_bound()
    // of the sstables returned from last non-empty pop() or -infinity if no sstables were previously returned,
    // and `filter` is the filtering function provided when creating the queue.
    //
    // Note that there may be multiple returned sstables (all with the same position) or none.
    //
    // Note that lower_bound(S) is global for sstable S; if the readers are used to inspect specific partitions,
    // the minimal positions in these partitions might actually all be greater than lower_bound(S).
    virtual std::vector<reader_and_upper_bound> pop(position_in_partition_view bound) override {
        if (empty(bound)) {
            return {};
        }

        if (_dummy_reader) {
            std::vector<reader_and_upper_bound> ret;
            ret.emplace_back(*std::exchange(_dummy_reader, std::nullopt), position_in_partition::before_all_clustered_rows());
            return ret;
        }

        // by !empty(bound) and `_it` invariant:
        //      _it != _end, _it->first <= bound, and filter(*_it->second) == true
        SCYLLA_ASSERT(_cmp(_it->first, bound) <= 0);
        // we don't SCYLLA_ASSERT(filter(*_it->second)) due to the requirement that `filter` is called at most once for each sstable

        // Find all sstables with the same position as `_it` (they form a contiguous range in the container).
        auto next = std::find_if(std::next(_it), _end, [this] (const value_t& v) { return _cmp(v.first, _it->first) != 0; });

        // We'll return all sstables in the range [_it, next) which pass the filter
        std::vector<reader_and_upper_bound> ret;
        do {
            // loop invariant: filter(*_it->second) == true
            auto upper_bound = _reversed ? _it->second->min_position().reversed() : _it->second->max_position();
            ret.emplace_back(create_reader(*_it->second), std::move(upper_bound));
            // restore loop invariant
            do {
                ++_it;
            } while (_it != next && !filter(*_it->second));
        } while (_it != next);

        // filter(*_it->second) wasn't called yet since the inner `do..while` above checks _it != next first
        // restore the `_it` invariant before returning
        while (_it != _end && !filter(*_it->second)) {
            ++_it;
        }

        return ret;
    }

    // If the dummy reader was not returned yet, returns false.
    // Otherwise checks if the set of sstables {S: filter(S) and prev_min_pos < lower_bound(S) <= bound}
    // is empty (see pop() for definition of `prev_min_pos`).
    virtual bool empty(position_in_partition_view bound) const override {
        return !_dummy_reader && (_it == _end || _cmp(_it->first, bound) > 0);
    }

    virtual future<> close() noexcept override {
        _it = _end;
        return make_ready_future<>();
    }
};

std::unique_ptr<position_reader_queue> time_series_sstable_set::make_position_reader_queue(
        std::function<mutation_reader(sstable&)> create_reader,
        std::function<bool(const sstable&)> filter,
        partition_key pk, schema_ptr query_schema, reader_permit permit,
        streamed_mutation::forwarding fwd_sm, bool reversed) const {
    return std::make_unique<sstable_position_reader_queue>(*this,
            std::move(query_schema), std::move(create_reader), std::move(filter),
            std::move(pk), std::move(permit), fwd_sm, reversed);
}

sstable_set_impl::selector_and_schema_t partitioned_sstable_set::make_incremental_selector() const {
    return std::make_tuple(std::make_unique<incremental_selector>(_schema, _unleveled_sstables, _leveled_sstables, _leveled_sstables_change_cnt), std::cref(*_schema));
}

std::unique_ptr<sstable_set_impl> compaction_strategy_impl::make_sstable_set(schema_ptr schema) const {
    // with use_level_metadata enabled, L0 sstables will not go to interval map, which suits well STCS.
    return std::make_unique<partitioned_sstable_set>(schema, true);
}

std::unique_ptr<sstable_set_impl> leveled_compaction_strategy::make_sstable_set(schema_ptr schema) const {
    return std::make_unique<partitioned_sstable_set>(std::move(schema));
}

std::unique_ptr<sstable_set_impl> time_window_compaction_strategy::make_sstable_set(schema_ptr schema) const {
    return std::make_unique<time_series_sstable_set>(std::move(schema), _options.enable_optimized_twcs_queries);
}

sstable_set make_partitioned_sstable_set(schema_ptr schema, bool use_level_metadata) {
    return sstable_set(std::make_unique<partitioned_sstable_set>(schema, use_level_metadata));
}

sstable_set
compaction_strategy::make_sstable_set(schema_ptr schema) const {
    return sstable_set(
            _compaction_strategy_impl->make_sstable_set(schema));
}

using sstable_reader_factory_type = std::function<mutation_reader(shared_sstable&, const dht::partition_range& pr)>;

static logging::logger irclogger("incremental_reader_selector");

// Incremental selector implementation for combined_mutation_reader that
// selects readers on-demand as the read progresses through the token
// range.
class incremental_reader_selector : public reader_selector {
    const dht::partition_range* _pr;
    lw_shared_ptr<const sstable_set> _sstables;
    tracing::trace_state_ptr _trace_state;
    std::optional<sstable_set::incremental_selector> _selector;
    std::unordered_set<generation_type> _read_sstable_gens;
    sstable_reader_factory_type _fn;

    mutation_reader create_reader(shared_sstable sst) {
        tracing::trace(_trace_state, "Reading partition range {} from sstable {}", *_pr, seastar::value_of([&sst] { return sst->get_filename(); }));
        return _fn(sst, *_pr);
    }
public:
    explicit incremental_reader_selector(schema_ptr s,
            lw_shared_ptr<const sstable_set> sstables,
            const dht::partition_range& pr,
            tracing::trace_state_ptr trace_state,
            sstable_reader_factory_type fn)
        : reader_selector(s, pr.start() ? pr.start()->value() : dht::ring_position_view::min())
        , _pr(&pr)
        , _sstables(std::move(sstables))
        , _trace_state(std::move(trace_state))
        , _selector(_sstables->make_incremental_selector())
        , _fn(std::move(fn)) {

        irclogger.trace("{}: created for range: {} with {} sstables",
                fmt::ptr(this),
                *_pr,
                _sstables->size());
    }

    incremental_reader_selector(const incremental_reader_selector&) = delete;
    incremental_reader_selector& operator=(const incremental_reader_selector&) = delete;

    incremental_reader_selector(incremental_reader_selector&&) = delete;
    incremental_reader_selector& operator=(incremental_reader_selector&&) = delete;

    virtual std::vector<mutation_reader> create_new_readers(const std::optional<dht::ring_position_view>& pos) override {
        irclogger.trace("{}: {}({})", fmt::ptr(this), __FUNCTION__, seastar::lazy_deref(pos));

        auto readers = std::vector<mutation_reader>();

        do {
            auto selection = _selector->select({_selector_position, _pr});
            _selector_position = selection.next_position;

            irclogger.trace("{}: {} sstables to consider, advancing selector to {}", fmt::ptr(this), selection.sstables.size(),
                    _selector_position);

            readers = boost::copy_range<std::vector<mutation_reader>>(selection.sstables
                    | boost::adaptors::filtered([this] (auto& sst) { return _read_sstable_gens.emplace(sst->generation()).second; })
                    | boost::adaptors::transformed([this] (auto& sst) { return this->create_reader(sst); }));
        } while (!_selector_position.is_max() && readers.empty() && (!pos || dht::ring_position_tri_compare(*_s, *pos, _selector_position) >= 0));

        irclogger.trace("{}: created {} new readers", fmt::ptr(this), readers.size());

        // prevents sstable_set::incremental_selector::_current_sstables from holding reference to
        // sstables when done selecting.
        if (_selector_position.is_max()) {
            _selector.reset();
        }

        return readers;
    }

    virtual std::vector<mutation_reader> fast_forward_to(const dht::partition_range& pr) override {
        _pr = &pr;

        auto pos = dht::ring_position_view::for_range_start(*_pr);
        if (dht::ring_position_tri_compare(*_s, pos, _selector_position) >= 0) {
            return create_new_readers(pos);
        }

        return {};
    }
};

// The returned function uses the bloom filter to check whether the given sstable
// may have a partition given by the ring position `pos`.
//
// Returning `false` means the sstable doesn't have such a partition.
// Returning `true` means it may, i.e. we don't know whether or not it does.
//
// Assumes the given `pos` and `schema` are alive during the function's lifetime.
static std::predicate<const sstable&> auto
make_pk_filter(const dht::ring_position& pos, const schema& schema) {
    return [&pos, key = utils::make_hashed_key(static_cast<bytes_view>(key::from_partition_key(schema, *pos.key()))), cmp = dht::ring_position_comparator(schema)] (const sstable& sst) {
        return cmp(pos, sst.get_first_decorated_key()) >= 0 &&
               cmp(pos, sst.get_last_decorated_key()) <= 0 &&
               sst.filter_has_key(key);
    };
}

const sstable_predicate& default_sstable_predicate() {
    static const sstable_predicate predicate = [] (const sstable&) { return true; };
    return predicate;
}

static std::predicate<const sstable&> auto
make_sstable_filter(const dht::ring_position& pos, const schema& schema, const sstable_predicate& predicate) {
    return [pk_filter = make_pk_filter(pos, schema), &predicate] (const sstable& sst) {
        return predicate(sst) && pk_filter(sst);
    };
}

// Filter out sstables for reader using bloom filter and supplied predicate
static std::vector<shared_sstable>
filter_sstable_for_reader(std::vector<shared_sstable>&& sstables, const schema& schema, const dht::ring_position& pos, const sstable_predicate& predicate) {
    auto filter = [_filter = make_sstable_filter(pos, schema, predicate)] (const shared_sstable& sst) { return !_filter(*sst); };
    sstables.erase(boost::remove_if(sstables, filter), sstables.end());
    return std::move(sstables);
}

// Filter out sstables for reader using sstable metadata that keeps track
// of a range for each clustering component.
static std::vector<shared_sstable>
filter_sstable_for_reader_by_ck(std::vector<shared_sstable>&& sstables, replica::column_family& cf, const schema_ptr& schema,
        const query::partition_slice& slice) {
    // no clustering filtering is applied if schema defines no clustering key or
    // compaction strategy thinks it will not benefit from such an optimization,
    // or the partition_slice includes static columns.
    if (!schema->clustering_key_size() || !cf.get_compaction_strategy().use_clustering_key_filter() || slice.static_columns.size()) {
        return std::move(sstables);
    }

    replica::cf_stats* stats = cf.cf_stats();
    stats->clustering_filter_count++;
    stats->sstables_checked_by_clustering_filter += sstables.size();

    auto ck_filtering_all_ranges = slice.get_all_ranges();
    // fast path to include all sstables if only one full range was specified.
    // For example, this happens if query only specifies a partition key.
    if (ck_filtering_all_ranges.size() == 1 && ck_filtering_all_ranges[0].is_full()) {
        stats->clustering_filter_fast_path_count++;
        stats->surviving_sstables_after_clustering_filter += sstables.size();
        return std::move(sstables);
    }

    auto skipped = std::partition(sstables.begin(), sstables.end(), [&ranges = ck_filtering_all_ranges] (const shared_sstable& sst) {
        return sst->may_contain_rows(ranges);
    });
    sstables.erase(skipped, sstables.end());
    stats->surviving_sstables_after_clustering_filter += sstables.size();

    return std::move(sstables);
}

std::vector<frozen_sstable_run>
sstable_set_impl::all_sstable_runs() const {
    throw_with_backtrace<std::bad_function_call>();
}

mutation_reader
sstable_set_impl::create_single_key_sstable_reader(
        replica::column_family* cf,
        schema_ptr schema,
        reader_permit permit,
        utils::estimated_histogram& sstable_histogram,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        const sstable_predicate& predicate) const
{
    const auto& pos = pr.start()->value();
    auto selected_sstables = filter_sstable_for_reader(select(pr), *schema, pos, predicate);
    auto num_sstables = selected_sstables.size();
    if (!num_sstables) {
        return make_empty_flat_reader_v2(schema, permit);
    }
    auto readers = boost::copy_range<std::vector<mutation_reader>>(
        filter_sstable_for_reader_by_ck(std::move(selected_sstables), *cf, schema, slice)
        | boost::adaptors::transformed([&] (const shared_sstable& sstable) {
            tracing::trace(trace_state, "Reading key {} from sstable {}", pos, seastar::value_of([&sstable] { return sstable->get_filename(); }));
            return sstable->make_reader(schema, permit, pr, slice, trace_state, fwd);
        })
    );

    // If filter_sstable_for_reader_by_ck filtered any sstable that contains the partition
    // we want to emit partition_start/end if no rows were found,
    // to prevent https://github.com/scylladb/scylla/issues/3552.
    //
    // Use `make_mutation_reader_from_mutations` with an empty mutation to emit
    // the partition_start/end pair and append it to the list of readers passed
    // to make_combined_reader to ensure partition_start/end are emitted even if
    // all sstables actually containing the partition were filtered.
    auto num_readers = readers.size();
    if (num_readers != num_sstables) {
        readers.push_back(make_mutation_reader_from_mutations_v2(schema, permit, mutation(schema, *pos.key()), slice, fwd));
    }
    sstable_histogram.add(num_readers);
    return make_combined_reader(schema, std::move(permit), std::move(readers), fwd, fwd_mr);
}

mutation_reader
time_series_sstable_set::create_single_key_sstable_reader(
        replica::column_family* cf,
        schema_ptr schema,
        reader_permit permit,
        utils::estimated_histogram& sstable_histogram,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd_sm,
        mutation_reader::forwarding fwd_mr,
        const sstable_predicate& predicate) const {
    const auto& pos = pr.start()->value();
    // First check if the optimized algorithm for TWCS single partition queries can be applied.
    // Multiple conditions must be satisfied:
    // 1. The sstables must be sufficiently modern so they contain the min/max column metadata.
    // 2. The schema cannot have static columns, since we're going to be opening new readers
    //    into new sstables in the middle of the partition query. TWCS sstables will usually pass
    //    this condition.
    // 3. The sstables cannot have partition tombstones for the same reason as above.
    //    TWCS sstables will usually pass this condition.
    // 4. The optimized query path must be enabled.
    using sst_entry = std::pair<position_in_partition, shared_sstable>;
    if (!_enable_optimized_twcs_queries
            || schema->has_static_columns()
            || std::any_of(_sstables->begin(), _sstables->end(),
                [] (const sst_entry& e) {
                    return e.second->get_version() < sstable_version_types::md
                        || e.second->may_have_partition_tombstones();
    })) {
        // Some of the conditions were not satisfied so we use the standard query path.
        return sstable_set_impl::create_single_key_sstable_reader(
                cf, std::move(schema), std::move(permit), sstable_histogram,
                pr, slice, std::move(trace_state), fwd_sm, fwd_mr, predicate);
    }

    auto sst_filter = make_sstable_filter(pos, *schema, predicate);
    auto it = std::find_if(_sstables->begin(), _sstables->end(), [&] (const sst_entry& e) { return sst_filter(*e.second); });
    if (it == _sstables->end()) {
        // No sstables contain data for the queried partition.
        return make_empty_flat_reader_v2(std::move(schema), std::move(permit));
    }

    auto& stats = *cf->cf_stats();
    stats.clustering_filter_count++;

    auto create_reader = [schema, permit, &pr, &slice, trace_state, fwd_sm] (sstable& sst) {
        return sst.make_reader(schema, permit, pr, slice, trace_state, fwd_sm);
    };

    auto pk_filter = make_pk_filter(pos, *schema);
    auto ck_filter = [ranges = slice.get_all_ranges()] (const sstable& sst) { return sst.may_contain_rows(ranges); };

    // We're going to pass this filter into sstable_position_reader_queue. The queue guarantees that
    // the filter is going to be called at most once for each sstable and exactly once after
    // the queue is exhausted. We use that fact to gather statistics.
    auto filter = [pk_filter = std::move(pk_filter), ck_filter = std::move(ck_filter), &stats]
        (const sstable& sst) {
            if (!pk_filter(sst)) {
                return false;
            }

            ++stats.sstables_checked_by_clustering_filter;
            if (ck_filter(sst)) {
                ++stats.surviving_sstables_after_clustering_filter;
                return true;
            }

            return false;
    };

    auto reversed = slice.is_reversed();
    // Note that `sstable_position_reader_queue` always includes a reader which emits a `partition_start` fragment,
    // guaranteeing that the reader we return emits it as well; this helps us avoid the problem from #3552.
    return make_clustering_combined_reader(
            schema, permit, fwd_sm,
            make_position_reader_queue(
                std::move(create_reader), std::move(filter), *pos.key(), schema, permit, fwd_sm, reversed));
}

compound_sstable_set::compound_sstable_set(schema_ptr schema, std::vector<lw_shared_ptr<sstable_set>> sets)
    : _schema(std::move(schema))
    , _sets(std::move(sets)) {
}

std::unique_ptr<sstable_set_impl> compound_sstable_set::clone() const {
    std::vector<lw_shared_ptr<sstable_set>> cloned_sets;
    cloned_sets.reserve(_sets.size());
    for (const auto& set : _sets) {
        // implicit clone by using sstable_set's copy ctor.
        auto cloned_set = make_lw_shared(*set);
        cloned_sets.push_back(std::move(cloned_set));
    }
    return std::make_unique<compound_sstable_set>(_schema, std::move(cloned_sets));
}

std::vector<shared_sstable> compound_sstable_set::select(const dht::partition_range& range) const {
    std::vector<shared_sstable> ret;
    for (auto& set : _sets) {
        auto ssts = set->select(range);
        if (ret.empty()) {
            ret = std::move(ssts);
        } else {
            ret.reserve(ret.size() + ssts.size());
            std::move(ssts.begin(), ssts.end(), std::back_inserter(ret));
        }
    }
    return ret;
}

std::vector<frozen_sstable_run> compound_sstable_set::all_sstable_runs() const {
    std::vector<frozen_sstable_run> ret;
    for (auto& set : _sets) {
        auto runs = set->all_sstable_runs();
        if (ret.empty()) {
            ret = std::move(runs);
        } else {
            ret.reserve(ret.size() + runs.size());
            std::move(runs.begin(), runs.end(), std::back_inserter(ret));
        }
    }
    return ret;
}

lw_shared_ptr<const sstable_list> compound_sstable_set::all() const {
    auto sets = _sets;
    auto it = std::partition(sets.begin(), sets.end(), [] (const auto& set) { return set->size() > 0; });
    auto non_empty_set_count = std::distance(sets.begin(), it);

    if (!non_empty_set_count) {
        return make_lw_shared<sstable_list>();
    }
    // optimize for common case where primary set contains sstables, but secondary one is empty for most of the time.
    if (non_empty_set_count == 1) {
        const auto& non_empty_set = *std::begin(sets);
        return non_empty_set->all();
    }

    auto ret = make_lw_shared<sstable_list>();
    for (auto& set : boost::make_iterator_range(sets.begin(), it)) {
        auto ssts = set->all();
        ret->reserve(ret->size() + ssts->size());
        ret->insert(ssts->begin(), ssts->end());
    }
    return ret;
}

stop_iteration compound_sstable_set::for_each_sstable_until(std::function<stop_iteration(const shared_sstable&)> func) const {
    for (auto& set : _sets) {
        if (set->for_each_sstable_until([&func] (const shared_sstable& sst) { return func(sst); })) {
            return stop_iteration::yes;
        }
    }
    return stop_iteration::no;
}

future<stop_iteration> compound_sstable_set::for_each_sstable_gently_until(std::function<future<stop_iteration>(const shared_sstable&)> func) const {
    for (auto& set : _sets) {
        auto stop = co_await set->for_each_sstable_gently_until([&func] (const shared_sstable& sst) { return func(sst); });
        if (stop) {
            co_return stop_iteration::yes;
        }
    }
    co_return stop_iteration::no;
}

bool compound_sstable_set::insert(shared_sstable sst) {
    throw_with_backtrace<std::bad_function_call>();
}
bool compound_sstable_set::erase(shared_sstable sst) {
    throw_with_backtrace<std::bad_function_call>();
}

size_t
compound_sstable_set::size() const noexcept {
    return boost::accumulate(_sets | boost::adaptors::transformed(std::mem_fn(&sstable_set::size)), size_t(0));
}

uint64_t
compound_sstable_set::bytes_on_disk() const noexcept {
    return boost::accumulate(_sets | boost::adaptors::transformed(std::mem_fn(&sstable_set::bytes_on_disk)), uint64_t(0));
}

class compound_sstable_set::incremental_selector : public incremental_selector_impl {
    const schema& _schema;
    const std::vector<lw_shared_ptr<sstable_set>>& _sets;
    std::vector<sstable_set::incremental_selector> _selectors;
private:
    std::vector<sstable_set::incremental_selector> make_selectors(const std::vector<lw_shared_ptr<sstable_set>>& sets) {
        return boost::copy_range<std::vector<sstable_set::incremental_selector>>(_sets | boost::adaptors::transformed([] (const auto& set) {
            return set->make_incremental_selector();
        }));
    }
public:
    incremental_selector(const schema& schema, const std::vector<lw_shared_ptr<sstable_set>>& sets)
            : _schema(schema)
            , _sets(sets)
            , _selectors(make_selectors(sets)) {
    }

    virtual std::tuple<dht::partition_range, std::vector<shared_sstable>, dht::ring_position_ext> select(const selector_pos& pos) override {
        // Return all sstables selected on the requested position from all selectors.
        std::vector<shared_sstable> sstables;
        // Return the lowest next position from all selectors, such that this function will be called again to select the
        // lowest next position from the selector which previously returned it.
        dht::ring_position_view lowest_next_position = dht::ring_position_view::max();
        // Always return minimum singular range, such that incremental_selector::select() will always call this function,
        // which in turn will call the selectors to decide on whether or not any select should be actually performed.
        const dht::partition_range current_range = dht::partition_range::make_singular(dht::ring_position::min());
        auto cmp = dht::ring_position_comparator(_schema);

        for (auto& selector : _selectors) {
            auto ret = selector.select(pos);
            sstables.reserve(sstables.size() + ret.sstables.size());
            std::copy(ret.sstables.begin(), ret.sstables.end(), std::back_inserter(sstables));
            if (cmp(ret.next_position, lowest_next_position) < 0) {
                lowest_next_position = ret.next_position;
            }
        }

        return std::make_tuple(std::move(current_range), std::move(sstables), dht::ring_position_ext(lowest_next_position));
    }
};

sstable_set_impl::selector_and_schema_t compound_sstable_set::make_incremental_selector() const {
    if (_sets.empty()) {
        // compound_sstable_set must manage one sstable set at least.
        abort();
    }
    auto sets = _sets;
    auto it = std::partition(sets.begin(), sets.end(), [] (const lw_shared_ptr<sstable_set>& set) { return set->size() > 0; });
    auto non_empty_set_count = std::distance(sets.begin(), it);

    // optimize for common case where only primary set contains sstables, so its selector can be built without an interposer.
    // optimization also applies when no set contains sstable, so any set can be picked as selection will be a no-op anyway.
    if (non_empty_set_count <= 1) {
        const auto& set = sets.front();
        return set->_impl->make_incremental_selector();
    }
    return std::make_tuple(std::make_unique<incremental_selector>(*_schema, _sets), std::cref(*_schema));
}

sstable_set make_compound_sstable_set(schema_ptr schema, std::vector<lw_shared_ptr<sstable_set>> sets) {
    return sstable_set(std::make_unique<compound_sstable_set>(schema, std::move(sets)));
}

mutation_reader
compound_sstable_set::create_single_key_sstable_reader(
        replica::column_family* cf,
        schema_ptr schema,
        reader_permit permit,
        utils::estimated_histogram& sstable_histogram,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        const sstable_predicate& predicate) const {
    auto sets = _sets;
    auto it = std::partition(sets.begin(), sets.end(), [] (const auto& set) { return set->size() > 0; });
    auto non_empty_set_count = std::distance(sets.begin(), it);

    if (!non_empty_set_count) {
        return make_empty_flat_reader_v2(schema, permit);
    }
    // optimize for common case where only 1 set is populated, avoiding the expensive combined reader
    if (non_empty_set_count == 1) {
        const auto& non_empty_set = *std::begin(sets);
        return non_empty_set->create_single_key_sstable_reader(cf, std::move(schema), std::move(permit), sstable_histogram, pr, slice, trace_state, fwd, fwd_mr, predicate);
    }

    auto readers = boost::copy_range<std::vector<mutation_reader>>(
        boost::make_iterator_range(sets.begin(), it)
        | boost::adaptors::transformed([&] (const lw_shared_ptr<sstable_set>& non_empty_set) {
            return non_empty_set->create_single_key_sstable_reader(cf, schema, permit, sstable_histogram, pr, slice, trace_state, fwd, fwd_mr, predicate);
        })
    );
    return make_combined_reader(std::move(schema), std::move(permit), std::move(readers), fwd, fwd_mr);
}

mutation_reader
sstable_set::create_single_key_sstable_reader(
        replica::column_family* cf,
        schema_ptr schema,
        reader_permit permit,
        utils::estimated_histogram& sstable_histogram,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        const sstable_predicate& predicate) const {
    SCYLLA_ASSERT(pr.is_singular() && pr.start()->value().has_key());
    return _impl->create_single_key_sstable_reader(cf, std::move(schema),
            std::move(permit), sstable_histogram, pr, slice, std::move(trace_state), fwd, fwd_mr, predicate);
}

class auto_closed_sstable_reader final : public mutation_reader::impl {
    shared_sstable _sst;
    mutation_reader_opt _reader;
private:
    future<> maybe_auto_close_sstable_reader(const dht::partition_range& pr) {
        if (!_sst) {
            co_return;
        }

        auto pos = dht::ring_position_view::for_range_start(pr);
        auto last_pos_in_reader = dht::ring_position_view(_sst->get_last_decorated_key());

        // If we're fast forwarding past the underlying reader, let's close it
        // and replace it by an empty reader.
        if (dht::ring_position_tri_compare(*_schema, pos, last_pos_in_reader) > 0) {
            co_await _reader->close();
            _reader = make_empty_flat_reader_v2(_schema, _permit);
            _sst = nullptr;
        }
    }
public:
    auto_closed_sstable_reader(shared_sstable sst,
                               mutation_reader sst_reader,
                               reader_permit permit)
            : impl(sst_reader.schema(), std::move(permit))
            , _sst(std::move(sst))
            , _reader(std::move(sst_reader)) {
    }
    virtual future<> fill_buffer() override {
        return _reader->fill_buffer().then([this] {
            _reader->move_buffer_content_to(*this);
            _end_of_stream = _reader->is_end_of_stream();
        });
    }
    future<> fast_forward_to(const dht::partition_range& pr) override {
        clear_buffer();

        co_await maybe_auto_close_sstable_reader(pr);

        _end_of_stream = false;
        co_await _reader->fast_forward_to(pr);
    }
    virtual future<> fast_forward_to(position_range pr) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (is_buffer_empty() && !is_end_of_stream()) {
            return _reader->next_partition();
        }
        return make_ready_future<>();
    }
    virtual future<> close() noexcept override {
        return _reader->close();
    }
};

mutation_reader make_auto_closed_sstable_reader(shared_sstable sst, mutation_reader sst_reader, reader_permit permit) {
    return make_mutation_reader<auto_closed_sstable_reader>(std::move(sst), std::move(sst_reader), std::move(permit));
}

mutation_reader
sstable_set::make_range_sstable_reader(
        schema_ptr s,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor_generator& monitor_generator) const
{
    auto reader_factory_fn = [s, permit, &slice, trace_state, fwd, fwd_mr, &monitor_generator]
            (shared_sstable& sst, const dht::partition_range& pr) mutable {
        return sst->make_reader(s, permit, pr, slice, trace_state, fwd, fwd_mr, monitor_generator(sst));
    };
    return make_combined_reader(s, std::move(permit), std::make_unique<incremental_reader_selector>(s,
                    shared_from_this(),
                    pr,
                    std::move(trace_state),
                    std::move(reader_factory_fn)),
            fwd,
            fwd_mr);
}

mutation_reader
sstable_set::make_local_shard_sstable_reader(
        schema_ptr s,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor_generator& monitor_generator,
        const sstable_predicate& predicate) const
{
    auto reader_factory_fn = [s, permit, &slice, trace_state, fwd, fwd_mr, &monitor_generator, &predicate]
            (shared_sstable& sst, const dht::partition_range& pr) mutable {
        SCYLLA_ASSERT(!sst->is_shared());
        if (!predicate(*sst)) {
            return make_empty_flat_reader_v2(s, permit);
        }
        auto reader = sst->make_reader(s, permit, pr, slice, trace_state, fwd, fwd_mr, monitor_generator(sst));
        // Auto-closed sstable reader is only enabled in the context of fast-forward to partition ranges
        if (!fwd && fwd_mr) {
            return make_auto_closed_sstable_reader(sst, std::move(reader), permit);
        }
        return reader;
    };
    if (_impl->size() == 1) [[unlikely]] {
        auto sstables = _impl->all();
        auto sst = *sstables->begin();
        return reader_factory_fn(sst, pr);
    }
    return make_combined_reader(s, std::move(permit), std::make_unique<incremental_reader_selector>(s,
                    shared_from_this(),
                    pr,
                    std::move(trace_state),
                    std::move(reader_factory_fn)),
            fwd,
            fwd_mr);
}

mutation_reader sstable_set::make_crawling_reader(
        schema_ptr schema,
        reader_permit permit,
        tracing::trace_state_ptr trace_ptr,
        read_monitor_generator& monitor_generator) const {
    std::vector<mutation_reader> readers;
    for_each_sstable([&] (const shared_sstable& sst) mutable {
        readers.emplace_back(sst->make_crawling_reader(schema, permit, trace_ptr, monitor_generator(sst)));
    });
    return make_combined_reader(schema, std::move(permit), std::move(readers), streamed_mutation::forwarding::no, mutation_reader::forwarding::no);
}

unsigned sstable_set_overlapping_count(const schema_ptr& schema, const std::vector<shared_sstable>& sstables) {
    unsigned overlapping_sstables = 0;
    auto prev_last = dht::ring_position::min();
    for (auto& sst : sstables) {
        if (dht::ring_position(sst->get_first_decorated_key()).tri_compare(*schema, prev_last) <= 0) {
            overlapping_sstables++;
        }
        prev_last = dht::ring_position(sst->get_last_decorated_key());
    }
    return overlapping_sstables;
}

} // namespace sstables
