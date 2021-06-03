/*
 * Copyright (C) 2020-present ScyllaDB
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

#include <boost/icl/interval_map.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/remove_if.hpp>
#include <boost/range/algorithm/sort.hpp>

#include "compatible_ring_position.hh"
#include "compaction_strategy_impl.hh"
#include "leveled_compaction_strategy.hh"
#include "time_window_compaction_strategy.hh"

#include "sstable_set_impl.hh"

#include "database.hh"

namespace sstables {

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

sstable_set::sstable_set(std::unique_ptr<sstable_set_impl> impl, schema_ptr s)
        : _impl(std::move(impl))
        , _schema(std::move(s)) {
}

sstable_set::sstable_set(const sstable_set& x)
        : _impl(x._impl->clone())
        , _schema(x._schema) {
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
sstable_set::select_sstable_runs(const std::vector<shared_sstable>& sstables) const {
    return _impl->select_sstable_runs(sstables);
}

std::vector<sstable_run>
partitioned_sstable_set::select_sstable_runs(const std::vector<shared_sstable>& sstables) const {
    auto has_run = [this] (const shared_sstable& sst) { return _all_runs.contains(sst->run_identifier()); };
    auto run_ids = boost::copy_range<std::unordered_set<utils::UUID>>(sstables | boost::adaptors::filtered(has_run) | boost::adaptors::transformed(std::mem_fn(&sstable::run_identifier)));
    return boost::copy_range<std::vector<sstable_run>>(run_ids | boost::adaptors::transformed([this] (utils::UUID run_id) {
        return _all_runs.at(run_id);
    }));
}

lw_shared_ptr<sstable_list>
sstable_set::all() const {
    return _impl->all();
}

void sstable_set::for_each_sstable(std::function<void(const shared_sstable&)> func) const {
    return _impl->for_each_sstable(std::move(func));
}

void
sstable_set::insert(shared_sstable sst) {
    _impl->insert(sst);
}

void
sstable_set::erase(shared_sstable sst) {
    _impl->erase(sst);
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

partitioned_sstable_set::interval_type partitioned_sstable_set::make_interval(const schema& s, const dht::partition_range& range) {
    return interval_type::closed(
            compatible_ring_position_or_view(s, dht::ring_position_view(range.start()->value())),
            compatible_ring_position_or_view(s, dht::ring_position_view(range.end()->value())));
}

partitioned_sstable_set::interval_type partitioned_sstable_set::make_interval(const dht::partition_range& range) const {
    return make_interval(*_schema, range);
}

partitioned_sstable_set::interval_type partitioned_sstable_set::make_interval(const schema_ptr& s, const sstable& sst) {
    return interval_type::closed(
            compatible_ring_position_or_view(s, dht::ring_position(sst.get_first_decorated_key())),
            compatible_ring_position_or_view(s, dht::ring_position(sst.get_last_decorated_key())));
}

partitioned_sstable_set::interval_type partitioned_sstable_set::make_interval(const sstable& sst) {
    return make_interval(_schema, sst);
}

partitioned_sstable_set::interval_type partitioned_sstable_set::singular(const dht::ring_position& rp) const {
    // We should use the view here, since this is used for queries.
    auto rpv = dht::ring_position_view(rp);
    auto crp = compatible_ring_position_or_view(*_schema, std::move(rpv));
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

dht::ring_position partitioned_sstable_set::to_ring_position(const compatible_ring_position_or_view& crp) {
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

partitioned_sstable_set::partitioned_sstable_set(schema_ptr schema, lw_shared_ptr<sstable_list> all, bool use_level_metadata)
        : _schema(std::move(schema))
        , _all(std::move(all))
        , _use_level_metadata(use_level_metadata) {
}

partitioned_sstable_set::partitioned_sstable_set(schema_ptr schema, const std::vector<shared_sstable>& unleveled_sstables, const interval_map_type& leveled_sstables,
        const lw_shared_ptr<sstable_list>& all, const std::unordered_map<utils::UUID, sstable_run>& all_runs, bool use_level_metadata)
        : _schema(schema)
        , _unleveled_sstables(unleveled_sstables)
        , _leveled_sstables(leveled_sstables)
        , _all(make_lw_shared<sstable_list>(*all))
        , _all_runs(all_runs)
        , _use_level_metadata(use_level_metadata) {
}

std::unique_ptr<sstable_set_impl> partitioned_sstable_set::clone() const {
    return std::make_unique<partitioned_sstable_set>(_schema, _unleveled_sstables, _leveled_sstables, _all, _all_runs, _use_level_metadata);
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

lw_shared_ptr<sstable_list> partitioned_sstable_set::all() const {
    return _all;
}

void partitioned_sstable_set::for_each_sstable(std::function<void(const shared_sstable&)> func) const {
    for (auto& sst : *_all) {
        func(sst);
    }
}

void partitioned_sstable_set::insert(shared_sstable sst) {
    _all->insert(sst);
    auto undo_all_insert = defer([&] () { _all->erase(sst); });

    _all_runs[sst->run_identifier()].insert(sst);
    auto undo_all_runs_insert = defer([&] () { _all_runs[sst->run_identifier()].erase(sst); });

    if (store_as_unleveled(sst)) {
        _unleveled_sstables.push_back(sst);
    } else {
        _leveled_sstables_change_cnt++;
        _leveled_sstables.add({make_interval(*sst), value_set({sst})});
    }
    undo_all_insert.cancel();
    undo_all_runs_insert.cancel();
}

void partitioned_sstable_set::erase(shared_sstable sst) {
    _all_runs[sst->run_identifier()].erase(sst);
    _all->erase(sst);
    if (store_as_unleveled(sst)) {
        _unleveled_sstables.erase(std::remove(_unleveled_sstables.begin(), _unleveled_sstables.end(), sst), _unleveled_sstables.end());
    } else {
        _leveled_sstables_change_cnt++;
        _leveled_sstables.subtract({make_interval(*sst), value_set({sst})});
    }
}

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

time_series_sstable_set::time_series_sstable_set(schema_ptr schema)
    : _schema(std::move(schema))
    , _sstables(make_lw_shared<container_t>(position_in_partition::less_compare(*_schema))) {}

time_series_sstable_set::time_series_sstable_set(const time_series_sstable_set& s)
    : _schema(s._schema)
    , _sstables(make_lw_shared(*s._sstables)) {}

std::unique_ptr<sstable_set_impl> time_series_sstable_set::clone() const {
    return std::make_unique<time_series_sstable_set>(*this);
}

std::vector<shared_sstable> time_series_sstable_set::select(const dht::partition_range& range) const {
    return boost::copy_range<std::vector<shared_sstable>>(*_sstables | boost::adaptors::map_values);
}

lw_shared_ptr<sstable_list> time_series_sstable_set::all() const {
    return make_lw_shared<sstable_list>(boost::copy_range<sstable_list>(*_sstables | boost::adaptors::map_values));
}

void time_series_sstable_set::for_each_sstable(std::function<void(const shared_sstable&)> func) const {
    for (auto& entry : *_sstables) {
        func(entry.second);
    }
}

// O(log n)
void time_series_sstable_set::insert(shared_sstable sst) {
    auto pos = sst->min_position();
    _sstables->emplace(pos, std::move(sst));
}

// O(n) worst case, but should be close to O(log n) most of the time
void time_series_sstable_set::erase(shared_sstable sst) {
    auto [first, last] = _sstables->equal_range(sst->min_position());
    auto it = std::find_if(first, last,
            [&sst] (const std::pair<position_in_partition, shared_sstable>& p) { return sst == p.second; });
    if (it != last) {
        _sstables->erase(it);
    }
}

std::unique_ptr<incremental_selector_impl> time_series_sstable_set::make_incremental_selector() const {
    struct selector : public incremental_selector_impl {
        const time_series_sstable_set& _set;

        selector(const time_series_sstable_set& set) : _set(set) {}

        virtual std::tuple<dht::partition_range, std::vector<shared_sstable>, dht::ring_position_view>
        select(const dht::ring_position_view&) override {
            return std::make_tuple(dht::partition_range::make_open_ended_both_sides(), _set.select(), dht::ring_position_view::max());
        }
    };

    return std::make_unique<selector>(*this);
}

// Queue of readers of sstables in a time_series_sstable_set,
// returning readers in order of the sstables' min_position()s.
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
class min_position_reader_queue : public position_reader_queue {
    using container_t = time_series_sstable_set::container_t;
    using value_t = container_t::value_type;

    schema_ptr _schema;
    lw_shared_ptr<const container_t> _sstables;

    // Iterates over sstables in order of min_position().
    // Invariant: _it == _end or filter(it->second) == true
    container_t::const_iterator _it;
    const container_t::const_iterator _end;

    position_in_partition::tri_compare _cmp;

    std::function<flat_mutation_reader(sstable&)> _create_reader;
    std::function<bool(const sstable&)> _filter;

    // After construction contains a reader which returns only the partition
    // start (and end, if not in forwarding mode) markers. This is the first
    // returned reader.
    std::optional<flat_mutation_reader> _dummy_reader;

    flat_mutation_reader create_reader(sstable& sst) {
        return _create_reader(sst);
    }

    bool filter(const sstable& sst) const {
        return _filter(sst);
    }

public:
    // Assumes that `create_reader` returns readers that emit only fragments from partition `pk`.
    min_position_reader_queue(schema_ptr schema,
            lw_shared_ptr<const time_series_sstable_set::container_t> sstables,
            std::function<flat_mutation_reader(sstable&)> create_reader,
            std::function<bool(const sstable&)> filter,
            partition_key pk,
            reader_permit permit,
            streamed_mutation::forwarding fwd_sm)
        : _schema(std::move(schema))
        , _sstables(std::move(sstables))
        , _it(_sstables->begin())
        , _end(_sstables->end())
        , _cmp(*_schema)
        , _create_reader(std::move(create_reader))
        , _filter(std::move(filter))
        , _dummy_reader(flat_mutation_reader_from_mutations(
                std::move(permit), {mutation(_schema, std::move(pk))}, _schema->full_slice(), fwd_sm))
    {
        while (_it != _end && !this->filter(*_it->second)) {
            ++_it;
        }
    }

    virtual ~min_position_reader_queue() override = default;

    // If the dummy reader was not yet returned, return the dummy reader.
    // Otherwise, open sstable readers to all sstables with smallest min_position() from the set
    // {S: filter(S) and prev_min_pos < S.min_position() <= bound}, where `prev_min_pos` is the min_position()
    // of the sstables returned from last non-empty pop() or -infinity if no sstables were previously returned,
    // and `filter` is the filtering function provided when creating the queue.
    //
    // Note that there may be multiple returned sstables (all with the same position) or none.
    //
    // Note that S.min_position() is global for sstable S; if the readers are used to inspect specific partitions,
    // the minimal positions in these partitions might actually all be greater than S.min_position().
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
        assert(_cmp(_it->first, bound) <= 0);
        // we don't assert(filter(*_it->second)) due to the requirement that `filter` is called at most once for each sstable

        // Find all sstables with the same position as `_it` (they form a contiguous range in the container).
        auto next = std::find_if(std::next(_it), _end, [this] (const value_t& v) { return _cmp(v.first, _it->first) != 0; });

        // We'll return all sstables in the range [_it, next) which pass the filter
        std::vector<reader_and_upper_bound> ret;
        do {
            // loop invariant: filter(*_it->second) == true
            ret.emplace_back(create_reader(*_it->second), _it->second->max_position());
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
    // Otherwise checks if the set of sstables {S: filter(S) and prev_min_pos < S.min_position() <= bound}
    // is empty (see pop() for definition of `prev_min_pos`).
    virtual bool empty(position_in_partition_view bound) const override {
        return !_dummy_reader && (_it == _end || _cmp(_it->first, bound) > 0);
    }

    virtual future<> close() noexcept override {
        _it = _end;
        return make_ready_future<>();
    }
};

std::unique_ptr<position_reader_queue> time_series_sstable_set::make_min_position_reader_queue(
        std::function<flat_mutation_reader(sstable&)> create_reader,
        std::function<bool(const sstable&)> filter,
        partition_key pk, schema_ptr schema, reader_permit permit,
        streamed_mutation::forwarding fwd_sm) const {
    return std::make_unique<min_position_reader_queue>(
            std::move(schema), _sstables, std::move(create_reader), std::move(filter),
            std::move(pk), std::move(permit), fwd_sm);
}

std::unique_ptr<incremental_selector_impl> partitioned_sstable_set::make_incremental_selector() const {
    return std::make_unique<incremental_selector>(_schema, _unleveled_sstables, _leveled_sstables, _leveled_sstables_change_cnt);
}

std::unique_ptr<sstable_set_impl> compaction_strategy_impl::make_sstable_set(schema_ptr schema) const {
    // with use_level_metadata enabled, L0 sstables will not go to interval map, which suits well STCS.
    return std::make_unique<partitioned_sstable_set>(schema, make_lw_shared<sstable_list>(), true);
}

std::unique_ptr<sstable_set_impl> leveled_compaction_strategy::make_sstable_set(schema_ptr schema) const {
    return std::make_unique<partitioned_sstable_set>(std::move(schema), make_lw_shared<sstable_list>());
}

std::unique_ptr<sstable_set_impl> time_window_compaction_strategy::make_sstable_set(schema_ptr schema) const {
    return std::make_unique<time_series_sstable_set>(std::move(schema));
}

sstable_set make_partitioned_sstable_set(schema_ptr schema, lw_shared_ptr<sstable_list> all, bool use_level_metadata) {
    return sstable_set(std::make_unique<partitioned_sstable_set>(schema, std::move(all), use_level_metadata), schema);
}

sstable_set
compaction_strategy::make_sstable_set(schema_ptr schema) const {
    return sstable_set(
            _compaction_strategy_impl->make_sstable_set(schema),
            schema);
}

using sstable_reader_factory_type = std::function<flat_mutation_reader(shared_sstable&, const dht::partition_range& pr)>;

static logging::logger irclogger("incremental_reader_selector");

// Incremental selector implementation for combined_mutation_reader that
// selects readers on-demand as the read progresses through the token
// range.
class incremental_reader_selector : public reader_selector {
    const dht::partition_range* _pr;
    lw_shared_ptr<const sstable_set> _sstables;
    tracing::trace_state_ptr _trace_state;
    std::optional<sstable_set::incremental_selector> _selector;
    std::unordered_set<int64_t> _read_sstable_gens;
    sstable_reader_factory_type _fn;

    flat_mutation_reader create_reader(shared_sstable sst) {
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
                _sstables->all()->size());
    }

    incremental_reader_selector(const incremental_reader_selector&) = delete;
    incremental_reader_selector& operator=(const incremental_reader_selector&) = delete;

    incremental_reader_selector(incremental_reader_selector&&) = delete;
    incremental_reader_selector& operator=(incremental_reader_selector&&) = delete;

    virtual std::vector<flat_mutation_reader> create_new_readers(const std::optional<dht::ring_position_view>& pos) override {
        irclogger.trace("{}: {}({})", fmt::ptr(this), __FUNCTION__, seastar::lazy_deref(pos));

        auto readers = std::vector<flat_mutation_reader>();

        do {
            auto selection = _selector->select(_selector_position);
            _selector_position = selection.next_position;

            irclogger.trace("{}: {} sstables to consider, advancing selector to {}", fmt::ptr(this), selection.sstables.size(),
                    _selector_position);

            readers = boost::copy_range<std::vector<flat_mutation_reader>>(selection.sstables
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

    virtual std::vector<flat_mutation_reader> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
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
    return [&pos, key = key::from_partition_key(schema, *pos.key()), cmp = dht::ring_position_comparator(schema)] (const sstable& sst) {
        return cmp(pos, sst.get_first_decorated_key()) >= 0 &&
               cmp(pos, sst.get_last_decorated_key()) <= 0 &&
               sst.filter_has_key(key);
    };
}

// Filter out sstables for reader using bloom filter
static std::vector<shared_sstable>
filter_sstable_for_reader_by_pk(std::vector<shared_sstable>&& sstables, const schema& schema, const dht::ring_position& pos) {
    auto filter = [_filter = make_pk_filter(pos, schema)] (const shared_sstable& sst) { return !_filter(*sst); };
    sstables.erase(boost::remove_if(sstables, filter), sstables.end());
    return std::move(sstables);
}

// Filter out sstables for reader using sstable metadata that keeps track
// of a range for each clustering component.
static std::vector<shared_sstable>
filter_sstable_for_reader_by_ck(std::vector<shared_sstable>&& sstables, column_family& cf, const schema_ptr& schema,
        const query::partition_slice& slice) {
    // no clustering filtering is applied if schema defines no clustering key or
    // compaction strategy thinks it will not benefit from such an optimization,
    // or the partition_slice includes static columns.
    if (!schema->clustering_key_size() || !cf.get_compaction_strategy().use_clustering_key_filter() || slice.static_columns.size()) {
        return std::move(sstables);
    }

    ::cf_stats* stats = cf.cf_stats();
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

std::vector<sstable_run>
sstable_set_impl::select_sstable_runs(const std::vector<shared_sstable>& sstables) const {
    throw_with_backtrace<std::bad_function_call>();
}

flat_mutation_reader
sstable_set_impl::create_single_key_sstable_reader(
        column_family* cf,
        schema_ptr schema,
        reader_permit permit,
        utils::estimated_histogram& sstable_histogram,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr) const
{
    const auto& pos = pr.start()->value();
    auto selected_sstables = filter_sstable_for_reader_by_pk(select(pr), *schema, pos);
    auto num_sstables = selected_sstables.size();
    if (!num_sstables) {
        return make_empty_flat_reader(schema, permit);
    }
    auto readers = boost::copy_range<std::vector<flat_mutation_reader>>(
        filter_sstable_for_reader_by_ck(std::move(selected_sstables), *cf, schema, slice)
        | boost::adaptors::transformed([&] (const shared_sstable& sstable) {
            tracing::trace(trace_state, "Reading key {} from sstable {}", pos, seastar::value_of([&sstable] { return sstable->get_filename(); }));
            return sstable->make_reader(schema, permit, pr, slice, pc, trace_state, fwd);
        })
    );

    // If filter_sstable_for_reader_by_ck filtered any sstable that contains the partition
    // we want to emit partition_start/end if no rows were found,
    // to prevent https://github.com/scylladb/scylla/issues/3552.
    //
    // Use `flat_mutation_reader_from_mutations` with an empty mutation to emit
    // the partition_start/end pair and append it to the list of readers passed
    // to make_combined_reader to ensure partition_start/end are emitted even if
    // all sstables actually containing the partition were filtered.
    auto num_readers = readers.size();
    if (num_readers != num_sstables) {
        readers.push_back(flat_mutation_reader_from_mutations(permit, {mutation(schema, *pos.key())}, slice, fwd));
    }
    sstable_histogram.add(num_readers);
    return make_combined_reader(schema, std::move(permit), std::move(readers), fwd, fwd_mr);
}

flat_mutation_reader
time_series_sstable_set::create_single_key_sstable_reader(
        column_family* cf,
        schema_ptr schema,
        reader_permit permit,
        utils::estimated_histogram& sstable_histogram,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd_sm,
        mutation_reader::forwarding fwd_mr) const {
    const auto& pos = pr.start()->value();
    // First check if the optimized algorithm for TWCS single partition queries can be applied.
    // Multiple conditions must be satisfied:
    // 1. The sstables must be sufficiently modern so they contain the min/max column metadata.
    // 2. The schema cannot have static columns, since we're going to be opening new readers
    //    into new sstables in the middle of the partition query. TWCS sstables will usually pass
    //    this condition.
    // 3. The sstables cannot have partition tombstones for the same reason as above.
    //    TWCS sstables will usually pass this condition.
    using sst_entry = std::pair<position_in_partition, shared_sstable>;
    if (schema->has_static_columns()
            || std::any_of(_sstables->begin(), _sstables->end(),
                [] (const sst_entry& e) {
                    return e.second->get_version() < sstable_version_types::md
                        || e.second->may_have_partition_tombstones();
    })) {
        // Some of the conditions were not satisfied so we use the standard query path.
        return sstable_set_impl::create_single_key_sstable_reader(
                cf, std::move(schema), std::move(permit), sstable_histogram,
                pr, slice, pc, std::move(trace_state), fwd_sm, fwd_mr);
    }

    auto pk_filter = make_pk_filter(pos, *schema);
    auto it = std::find_if(_sstables->begin(), _sstables->end(), [&] (const sst_entry& e) { return pk_filter(*e.second); });
    if (it == _sstables->end()) {
        // No sstables contain data for the queried partition.
        return make_empty_flat_reader(std::move(schema), std::move(permit));
    }

    auto& stats = *cf->cf_stats();
    stats.clustering_filter_count++;

    auto create_reader = [schema, permit, &pr, &slice, &pc, trace_state, fwd_sm] (sstable& sst) {
        return sst.make_reader(schema, permit, pr, slice, pc, trace_state, fwd_sm);
    };

    auto ck_filter = [ranges = slice.get_all_ranges()] (const sstable& sst) { return sst.may_contain_rows(ranges); };

    // We're going to pass this filter into min_position_reader_queue. The queue guarantees that
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

    // Note that `min_position_reader_queue` always includes a reader which emits a `partition_start` fragment,
    // guaranteeing that the reader we return emits it as well; this helps us avoid the problem from #3552.
    return make_clustering_combined_reader(
            schema, permit, fwd_sm,
            make_min_position_reader_queue(
                std::move(create_reader), std::move(filter), *pos.key(), schema, permit, fwd_sm));
}

compound_sstable_set::compound_sstable_set(schema_ptr schema, std::vector<lw_shared_ptr<sstable_set>> sets)
    : _schema(std::move(schema))
    , _sets(std::move(sets)) {
}

std::unique_ptr<sstable_set_impl> compound_sstable_set::clone() const {
    std::vector<lw_shared_ptr<sstable_set>> cloned_sets;
    cloned_sets.reserve(_sets.size());
    for (auto& set : _sets) {
        // implicit clone by using sstable_set's copy ctor.
        cloned_sets.push_back(make_lw_shared(std::move(*set)));
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

std::vector<sstable_run> compound_sstable_set::select_sstable_runs(const std::vector<shared_sstable>& sstables) const {
    std::vector<sstable_run> ret;
    for (auto& set : _sets) {
        auto runs = set->select_sstable_runs(sstables);
        if (ret.empty()) {
            ret = std::move(runs);
        } else {
            ret.reserve(ret.size() + runs.size());
            std::move(runs.begin(), runs.end(), std::back_inserter(ret));
        }
    }
    return ret;
}

lw_shared_ptr<sstable_list> compound_sstable_set::all() const {
    auto sets = _sets;
    auto it = std::partition(sets.begin(), sets.end(), [] (const auto& set) { return !set->all()->empty(); });
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

void compound_sstable_set::for_each_sstable(std::function<void(const shared_sstable&)> func) const {
    for (auto& set : _sets) {
        set->for_each_sstable([&func] (const shared_sstable& sst) {
            func(sst);
        });
    }
}

void compound_sstable_set::insert(shared_sstable sst) {
    throw_with_backtrace<std::bad_function_call>();
}
void compound_sstable_set::erase(shared_sstable sst) {
    throw_with_backtrace<std::bad_function_call>();
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

    virtual std::tuple<dht::partition_range, std::vector<shared_sstable>, dht::ring_position_view> select(const dht::ring_position_view& pos) override {
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

        return std::make_tuple(std::move(current_range), std::move(sstables), lowest_next_position);
    }
};

std::unique_ptr<incremental_selector_impl> compound_sstable_set::make_incremental_selector() const {
    return std::make_unique<incremental_selector>(*_schema, _sets);
}

sstable_set make_compound_sstable_set(schema_ptr schema, std::vector<lw_shared_ptr<sstable_set>> sets) {
    return sstable_set(std::make_unique<compound_sstable_set>(schema, std::move(sets)), schema);
}

flat_mutation_reader
compound_sstable_set::create_single_key_sstable_reader(
        column_family* cf,
        schema_ptr schema,
        reader_permit permit,
        utils::estimated_histogram& sstable_histogram,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr) const {
    auto sets = _sets;
    auto it = std::partition(sets.begin(), sets.end(), [] (const auto& set) { return !set->all()->empty(); });
    auto non_empty_set_count = std::distance(sets.begin(), it);

    if (!non_empty_set_count) {
        return make_empty_flat_reader(schema, permit);
    }
    // optimize for common case where only 1 set is populated, avoiding the expensive combined reader
    if (non_empty_set_count == 1) {
        const auto& non_empty_set = *std::begin(sets);
        return non_empty_set->create_single_key_sstable_reader(cf, std::move(schema), std::move(permit), sstable_histogram, pr, slice, pc, trace_state, fwd, fwd_mr);
    }

    auto readers = boost::copy_range<std::vector<flat_mutation_reader>>(
        boost::make_iterator_range(sets.begin(), it)
        | boost::adaptors::transformed([&] (const lw_shared_ptr<sstable_set>& non_empty_set) {
            return non_empty_set->create_single_key_sstable_reader(cf, schema, permit, sstable_histogram, pr, slice, pc, trace_state, fwd, fwd_mr);
        })
    );
    return make_combined_reader(std::move(schema), std::move(permit), std::move(readers), fwd, fwd_mr);
}

flat_mutation_reader
sstable_set::create_single_key_sstable_reader(
        column_family* cf,
        schema_ptr schema,
        reader_permit permit,
        utils::estimated_histogram& sstable_histogram,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr) const {
    assert(pr.is_singular() && pr.start()->value().has_key());
    return _impl->create_single_key_sstable_reader(cf, std::move(schema),
            std::move(permit), sstable_histogram, pr, slice, pc, std::move(trace_state), fwd, fwd_mr);
}

flat_mutation_reader
sstable_set::make_range_sstable_reader(
        schema_ptr s,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor_generator& monitor_generator) const
{
    auto reader_factory_fn = [s, permit, &slice, &pc, trace_state, fwd, fwd_mr, &monitor_generator]
            (shared_sstable& sst, const dht::partition_range& pr) mutable {
        return sst->make_reader(s, permit, pr, slice, pc, trace_state, fwd, fwd_mr, monitor_generator(sst));
    };
    return make_combined_reader(s, std::move(permit), std::make_unique<incremental_reader_selector>(s,
                    shared_from_this(),
                    pr,
                    std::move(trace_state),
                    std::move(reader_factory_fn)),
            fwd,
            fwd_mr);
}

flat_mutation_reader
sstable_set::make_local_shard_sstable_reader(
        schema_ptr s,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor_generator& monitor_generator) const
{
    auto reader_factory_fn = [s, permit, &slice, &pc, trace_state, fwd, fwd_mr, &monitor_generator]
            (shared_sstable& sst, const dht::partition_range& pr) mutable {
        flat_mutation_reader reader = sst->make_reader(s, permit, pr, slice, pc,
                trace_state, fwd, fwd_mr, monitor_generator(sst));
        if (sst->is_shared()) {
            auto filter = [&s = *s](const dht::decorated_key& dk) -> bool {
                return dht::shard_of(s, dk.token()) == this_shard_id();
            };
            reader = make_filtering_reader(std::move(reader), std::move(filter));
        }
        return reader;
    };
    return make_combined_reader(s, std::move(permit), std::make_unique<incremental_reader_selector>(s,
                    shared_from_this(),
                    pr,
                    std::move(trace_state),
                    std::move(reader_factory_fn)),
            fwd,
            fwd_mr);
}

flat_mutation_reader make_restricted_range_sstable_reader(
        lw_shared_ptr<sstable_set> sstables,
        schema_ptr s,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor_generator& monitor_generator)
{
    auto ms = mutation_source([sstables=std::move(sstables), &monitor_generator] (
            schema_ptr s,
            reader_permit permit,
            const dht::partition_range& pr,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding fwd_mr) {
        return sstables->make_range_sstable_reader(std::move(s), std::move(permit), pr, slice, pc,
                std::move(trace_state), fwd, fwd_mr, monitor_generator);
    });
    return make_restricted_flat_reader(std::move(ms), std::move(s), std::move(permit), pr, slice, pc, std::move(trace_state), fwd, fwd_mr);
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
