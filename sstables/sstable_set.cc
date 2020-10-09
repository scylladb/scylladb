/*
 * Copyright (C) 2020 ScyllaDB
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

#include "compatible_ring_position.hh"
#include "compaction_strategy_impl.hh"
#include "leveled_compaction_strategy.hh"

#include "sstable_set_impl.hh"

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

sstable_set::sstable_set(std::unique_ptr<sstable_set_impl> impl, schema_ptr s, lw_shared_ptr<sstable_list> all)
        : _impl(std::move(impl))
        , _schema(std::move(s))
        , _all(std::move(all)) {
}

sstable_set::sstable_set(const sstable_set& x)
        : _impl(x._impl->clone())
        , _schema(x._schema)
        , _all(make_lw_shared<sstable_list>(*x._all))
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
sstable_set::select_sstable_runs(const std::vector<shared_sstable>& sstables) const {
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

std::unique_ptr<sstable_set_impl> bag_sstable_set::clone() const {
    return std::make_unique<bag_sstable_set>(*this);
}

std::vector<shared_sstable> bag_sstable_set::select(const dht::partition_range& range) const {
    return _sstables;
}

void bag_sstable_set::insert(shared_sstable sst) {
    _sstables.push_back(std::move(sst));
}

void bag_sstable_set::erase(shared_sstable sst) {
    auto it = boost::range::find(_sstables, sst);
    if (it != _sstables.end()){
        _sstables.erase(it);
    }
}

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

partitioned_sstable_set::partitioned_sstable_set(schema_ptr schema, bool use_level_metadata)
        : _schema(std::move(schema))
        , _use_level_metadata(use_level_metadata) {
}

std::unique_ptr<sstable_set_impl> partitioned_sstable_set::clone() const {
    return std::make_unique<partitioned_sstable_set>(*this);
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

void partitioned_sstable_set::insert(shared_sstable sst) {
    if (store_as_unleveled(sst)) {
        _unleveled_sstables.push_back(std::move(sst));
    } else {
        _leveled_sstables_change_cnt++;
        _leveled_sstables.add({make_interval(*sst), value_set({sst})});
    }
}

void partitioned_sstable_set::erase(shared_sstable sst) {
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

std::unique_ptr<incremental_selector_impl> partitioned_sstable_set::make_incremental_selector() const {
    return std::make_unique<incremental_selector>(_schema, _unleveled_sstables, _leveled_sstables, _leveled_sstables_change_cnt);
}

std::unique_ptr<sstable_set_impl> compaction_strategy_impl::make_sstable_set(schema_ptr schema) const {
    return std::make_unique<bag_sstable_set>();
}

std::unique_ptr<sstable_set_impl> leveled_compaction_strategy::make_sstable_set(schema_ptr schema) const {
    return std::make_unique<partitioned_sstable_set>(std::move(schema));
}

sstable_set make_partitioned_sstable_set(schema_ptr schema, lw_shared_ptr<sstable_list> all, bool use_level_metadata) {
    return sstable_set(std::make_unique<partitioned_sstable_set>(schema, use_level_metadata), schema, std::move(all));
}

sstable_set
compaction_strategy::make_sstable_set(schema_ptr schema) const {
    return sstable_set(
            _compaction_strategy_impl->make_sstable_set(schema),
            schema,
            make_lw_shared<sstable_list>());
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
    return sstables;
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
        return sstables;
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
        return sstables;
    }

    auto skipped = std::partition(sstables.begin(), sstables.end(), [&ranges = ck_filtering_all_ranges] (const shared_sstable& sst) {
        return sst->may_contain_rows(ranges);
    });
    sstables.erase(skipped, sstables.end());
    stats->surviving_sstables_after_clustering_filter += sstables.size();

    return sstables;
}

flat_mutation_reader
sstable_set_impl::create_single_key_sstable_reader(
        column_family* cf,
        schema_ptr schema,
        reader_permit permit,
        utils::estimated_histogram& sstable_histogram,
        const dht::ring_position& pos,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr) const
{
    auto selected_sstables = filter_sstable_for_reader_by_pk(select({pos}), *schema, pos);
    auto num_sstables = selected_sstables.size();
    if (!num_sstables) {
        return make_empty_flat_reader(schema, permit);
    }
    auto readers = boost::copy_range<std::vector<flat_mutation_reader>>(
        filter_sstable_for_reader_by_ck(std::move(selected_sstables), *cf, schema, slice)
        | boost::adaptors::transformed([&] (const shared_sstable& sstable) {
            tracing::trace(trace_state, "Reading key {} from sstable {}", pos, seastar::value_of([&sstable] { return sstable->get_filename(); }));
            return sstable->read_row_flat(schema, permit, pos, slice, pc, trace_state, fwd);
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
sstable_set::create_single_key_sstable_reader(
        column_family* cf,
        schema_ptr schema,
        reader_permit permit,
        utils::estimated_histogram& sstable_histogram,
        const dht::ring_position& pos,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr) const {
    return _impl->create_single_key_sstable_reader(cf, std::move(schema),
            std::move(permit), sstable_histogram, pos, slice, pc, std::move(trace_state), fwd, fwd_mr);
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
        return sst->read_range_rows_flat(s, permit, pr, slice, pc, trace_state, fwd, fwd_mr, monitor_generator(sst));
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
        flat_mutation_reader reader = sst->read_range_rows_flat(s, permit, pr, slice, pc,
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

} // namespace sstables
