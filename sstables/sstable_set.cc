/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <algorithm>

#include "utils/assert.hh"
#include "utils/on_internal_error.hh"
#include <seastar/util/defer.hh>

#include "sstables.hh"

#include "dht/ring_position.hh"

#include "sstable_set_impl.hh"

#include "replica/database.hh"
#include "readers/from_mutations.hh"
#include "readers/empty.hh"
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
    return std::ranges::fold_left(_all | std::views::transform(std::mem_fn(&sstable::data_size)), uint64_t(0), std::plus{});
}

double sstable_run::estimate_droppable_tombstone_ratio(const gc_clock::time_point& compaction_time, const tombstone_gc_state& gc_state, const schema_ptr& s) const {
    auto estimate_sum = std::ranges::fold_left(_all | std::views::transform(std::bind(&sstable::estimate_droppable_tombstone_ratio, std::placeholders::_1, compaction_time, gc_state, s)), double(0), std::plus{});
    return _all.size() ? estimate_sum / _all.size() : double(0);
}

sstables::run_id sstable_run::run_identifier() const {
    return (_all.empty()) ? run_id() : (*_all.begin())->run_identifier();
}

db_clock::time_point sstable_run::data_file_write_time() const {
    if (_all.empty()) {
        return db_clock::time_point();
    }
    return std::ranges::max(_all | std::views::transform([](const shared_sstable& s) { return s->data_file_write_time(); }));
}

std::ostream& operator<<(std::ostream& os, const sstables::sstable_run& run) {
    os << "Run = {\n";
    if (run.all().empty()) {
        os << "  Identifier: not found\n";
    } else {
        os << format("  Identifier: {}\n", (*run.all().begin())->run_identifier());
    }

    auto frags = run.all() | std::ranges::to<std::vector<shared_sstable>>();
    std::ranges::sort(frags, std::ranges::less(), [] (const shared_sstable& x) {
        return x->get_first_decorated_key().token();
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
        : enable_lw_shared_from_this<sstable_set>()
        , _impl(x._impl->clone())
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
    return _all_runs | std::views::values | std::ranges::to<std::vector<frozen_sstable_run>>();
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

file_size_stats
sstable_set::get_file_size_stats() const noexcept {
    return _impl->get_file_size_stats();
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

dht::token_range partitioned_sstable_set::to_token_range(const dht::partition_range& range) {
    // Exclusive bounds are widened to inclusive ones: a position excluded from
    // the range can share its token with an included one, so the token itself
    // has to stay in range.
    auto start = range.start()
            ? dht::token_range::bound(range.start()->value().token(), true)
            : dht::token_range::bound(dht::minimum_token(), true);
    auto end = range.end()
            ? dht::token_range::bound(range.end()->value().token(), true)
            : dht::token_range::bound(dht::maximum_token(), true);
    return dht::token_range(std::move(start), std::move(end));
}

uint8_t partitioned_sstable_set::tier_of(const sstable& sst) {
    // raw() is the ordered representation of a token, and it places the minimum
    // and the maximum at the ends of the int64 range rather than collapsing both
    // to zero the way unbias() does. An sstable with an unknown bound therefore
    // needs no special case here: it simply comes out spanning (almost) the whole
    // space and lands in the widest tier, where the window covers everything.
    const auto first = sst.get_first_token().raw();
    const auto last = sst.get_last_token().raw();
    // An sstable whose stored keys are misordered is rejected as malformed by
    // set_first_and_last_keys(), which compares the whole decorated key and so is
    // stricter than this. Anything reaching here has already passed that, so an
    // inverted range at this point means the in-memory invariant was broken after
    // the fact -- our bug rather than the file's, hence on_internal_error() and
    // not throw_malformed_sstable_exception().
    if (last < first) [[unlikely]] {
        on_internal_error(sstlog, format("SSTable {} spans an inverted token range: first={}, last={}",
                sst.get_filename(), first, last));
    }
    // Subtract in uint64: the span between two tokens can exceed what int64 holds,
    // and for last >= first two's complement makes the unsigned difference exact.
    return uint8_t(std::bit_width(uint64_t(last) - uint64_t(first)));
}

dht::token partitioned_sstable_set::tier_window_start(uint8_t exponent, const dht::token& start) {
    // A member of tier `exponent` spans fewer than 2^exponent tokens, so one
    // ending at or after `start` begins after start - 2^exponent. The widest tier
    // spans everything and cannot be bounded from below at all.
    if (exponent >= 64 || start.is_minimum() || start.is_maximum()) {
        return dht::minimum_token();
    }
    const uint64_t width_bound = uint64_t(1) << exponent;
    const uint64_t s = start.unbias();
    return s >= width_bound ? dht::bias(s - width_bound) : dht::minimum_token();
}

partitioned_sstable_set::partitioned_sstable_set(schema_ptr schema, dht::token_range token_range)
        : _schema(std::move(schema))
        , _all(make_lw_shared<sstable_list>()) {
}

static std::unordered_map<run_id, shared_sstable_run> clone_runs(const std::unordered_map<run_id, shared_sstable_run>& runs) {
    return runs | std::views::transform([] (auto& p) {
        return std::make_pair(p.first, make_lw_shared<sstable_run>(*p.second));
    }) | std::ranges::to<std::unordered_map<run_id, shared_sstable_run>>();
}

partitioned_sstable_set::partitioned_sstable_set(schema_ptr schema, const tier_map& tiers,
        const lw_shared_ptr<sstable_list>& all, const std::unordered_map<run_id, shared_sstable_run>& all_runs, file_size_stats bytes_on_disk)
        : sstable_set_impl(bytes_on_disk)
        , _schema(schema)
        , _tiers(tiers)
        , _all(make_lw_shared<sstable_list>(*all))
        , _all_runs(clone_runs(all_runs)) {
}

std::unique_ptr<sstable_set_impl> partitioned_sstable_set::clone() const {
    return std::make_unique<partitioned_sstable_set>(_schema, _tiers, _all, _all_runs, _file_size_stats);
}

std::vector<shared_sstable> partitioned_sstable_set::select(const dht::partition_range& range) const {
    auto tr = to_token_range(range);
    const auto& start = tr.start()->value();
    const auto& end = tr.end()->value();

    // How many sstables match is not known before scanning, and counting the
    // candidates would cost as much as the scan, so reserve for the common case: a
    // set holding few sstables, all of which may match. A set larger than the cap
    // grows from there, which is cheaper than doubling up from nothing but does not
    // try to size for a match that large in advance.
    static constexpr size_t initial_reservation = 32;
    std::vector<shared_sstable> ret;
    ret.reserve(std::min(size(), initial_reservation));

    // Within a tier both ends of the walk are bounded: no member reaches back
    // further than the tier's width bound, and none can start beyond the end of
    // the query range. What is left is the other half of the overlap test, which
    // is checked on each candidate.
    for (const auto& [exponent, sstables] : _tiers) {
        const auto window_end = sstables.upper_bound(end);
        for (auto it = sstables.lower_bound(tier_window_start(exponent, start)); it != window_end; ++it) {
            if (it->second->get_last_token() >= start) {
                ret.push_back(it->second);
            }
        }
    }
    return ret;
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
    auto size_stats = sst->get_file_size_stats();
    add_file_size_stats(size_stats);
    auto undo_all_insert = defer([&] noexcept {
        _all->erase(sst);
        sub_file_size_stats(size_stats);
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
    auto undo_all_runs_insert = defer([&] noexcept { _all_runs[sst->run_identifier()]->erase(sst); });

    _change_cnt++;
    _tiers[tier_of(*sst)].emplace(sst->get_first_token(), sst);
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
        sub_file_size_stats(sst->get_file_size_stats());
    }
    _change_cnt++;
    if (auto tier = _tiers.find(tier_of(*sst)); tier != _tiers.end()) {
        auto& sstables = tier->second;
        auto [begin, end] = sstables.equal_range(sst->get_first_token());
        for (auto it = begin; it != end; ++it) {
            if (it->second == sst) {
                sstables.erase(it);
                break;
            }
        }
        // Keep only non-empty tiers, so that a query and a sweep visit no more
        // tiers than the set actually spreads over.
        if (sstables.empty()) {
            _tiers.erase(tier);
        }
    }
    return ret;
}

size_t
partitioned_sstable_set::size() const noexcept {
    return _all->size();
}

// Sweeps the set in ring order, keeping the sstables that contain the cursor in an
// "active" map keyed by their last token, so that the one retiring soonest is at
// the front. Advancing the cursor activates the sstables whose first token it has
// reached and retires those whose last token it has passed, visiting each sstable
// at most once per sweep.
//
// The tiers are walked with one cursor each. A sweep does not benefit from the
// tiers' width bounds -- it starts at the beginning and advances monotonically, so
// nothing is ever skipped from below -- it just has to advance every cursor rather
// than one.
class partitioned_sstable_set::incremental_selector : public incremental_selector_impl {
    const tier_map& _tiers;
    const uint64_t& _change_cnt;
    uint64_t _last_known_change_cnt;
    // sstables containing the cursor, keyed by their last token. It does not
    // need the tier an entry came from: retiring uses the key and the caller
    // wants the sstables.
    token_map _active;
    // Next sstable to activate in each tier, in first-token order.
    std::vector<token_map::const_iterator> _next_to_activate;
    std::optional<dht::token> _cursor;
private:
    void advance_to(dht::token t) {
        while (!_active.empty() && _active.begin()->first < t) {
            _active.erase(_active.begin());
        }
        size_t i = 0;
        for (const auto& [exponent, sstables] : _tiers) {
            auto& next = _next_to_activate[i++];
            for (; next != sstables.end() && next->first <= t; ++next) {
                const auto& sst = next->second;
                auto last = sst->get_last_token();
                // An sstable whose range ends before the cursor was never active
                // at it; this happens when the cursor jumps over a whole sstable.
                if (last >= t) {
                    _active.emplace(last, sst);
                }
            }
        }
    }
    void seek_to(dht::token t) {
        _active.clear();
        _next_to_activate.clear();
        _next_to_activate.reserve(_tiers.size());
        for (const auto& [exponent, sstables] : _tiers) {
            _next_to_activate.push_back(sstables.begin());
        }
        advance_to(t);
        _last_known_change_cnt = _change_cnt;
    }
    // The token at which the active set changes next: the earliest first token
    // among the sstables still to be activated in any tier, or the token just
    // after the first one to end.
    std::optional<dht::token> next_change_token() const {
        std::optional<dht::token> change;
        size_t i = 0;
        for (const auto& [exponent, sstables] : _tiers) {
            const auto& next = _next_to_activate[i++];
            if (next != sstables.end() && (!change || next->first < *change)) {
                change = next->first;
            }
        }
        if (!_active.empty()) {
            auto last = _active.begin()->first;
            // A range that does not end before the end of the ring is never left
            // behind. That covers both an sstable ending at the last token and one
            // whose last key is unknown, which get_last_token() reports as the
            // maximum token -- next() is only defined for a key token, and on the
            // maximum it would yield a token behind the cursor rather than ahead
            // of it, which would walk the sweep backwards.
            if (!last.is_last() && !last.is_maximum()) {
                auto after = last.next();
                change = change ? std::min(*change, after) : after;
            }
        }
        return change;
    }
public:
    incremental_selector(const tier_map& tiers, const uint64_t& change_cnt)
        : _tiers(tiers)
        , _change_cnt(change_cnt)
        , _last_known_change_cnt(change_cnt) {
        for (const auto& [exponent, sstables] : _tiers) {
            _next_to_activate.push_back(sstables.begin());
        }
    }
    virtual std::tuple<dht::partition_range, std::vector<shared_sstable>, dht::ring_position_ext> select(const selector_pos& s) override {
        auto t = s.pos.token();

        // Callers are required to pass weakly monotonic positions, which is what
        // makes the sweep incremental. Rebuild the state from scratch if that
        // does not hold, or if the set changed under us.
        // The cursors are held positionally against _tiers, which is sound because
        // every change to _tiers bumps the counter and so forces a reseek; the size
        // check makes that a checked invariant rather than an assumed one.
        if (_last_known_change_cnt != _change_cnt || !_cursor || t < *_cursor
                || _next_to_activate.size() != _tiers.size()) {
            seek_to(t);
        } else {
            advance_to(t);
        }
        _cursor = t;

        std::vector<shared_sstable> ssts;
        ssts.reserve(_active.size());
        for (const auto& [_, sst] : _active) {
            ssts.push_back(sst);
        }

        // The active set holds for every position whose token lies between the
        // cursor and the next change, so report that as the range over which the
        // caller may reuse this result.
        auto change = next_change_token();
        if (!change) {
            return std::make_tuple(dht::partition_range::make_open_ended_both_sides(), std::move(ssts), dht::ring_position_view::max());
        }
        auto range = dht::partition_range::make(
                {dht::ring_position::starting_at(t), true},
                {dht::ring_position::starting_at(*change), false});
        return std::make_tuple(std::move(range), std::move(ssts), dht::ring_position_ext::starting_at(*change));
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
    return *_sstables | std::views::values | std::ranges::to<std::vector>();
}

lw_shared_ptr<const sstable_list> time_series_sstable_set::all() const {
    return make_lw_shared<const sstable_list>(*_sstables | std::views::values | std::ranges::to<sstable_list>());
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
    add_file_size_stats(sst->get_file_size_stats());
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
            sub_file_size_stats(sst->get_file_size_stats());
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
        , _dummy_reader(make_mutation_reader_from_mutations(_query_schema,
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
    return std::make_tuple(std::make_unique<incremental_selector>(_tiers, _change_cnt), std::cref(*_schema));
}

sstable_set make_partitioned_sstable_set(schema_ptr schema, dht::token_range token_range) {
    return sstable_set(std::make_unique<partitioned_sstable_set>(schema, std::move(token_range)));
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

    dht::ring_position_view pr_end() const {
        return dht::ring_position_view::for_range_end(*_pr);
    }

    bool end_of_stream() const {
        return _selector_position.is_max() || dht::ring_position_tri_compare(*_s, _selector_position, pr_end()) > 0;
    }
public:
    explicit incremental_reader_selector(schema_ptr s,
            lw_shared_ptr<const sstable_set> sstables,
            const dht::partition_range& pr,
            tracing::trace_state_ptr trace_state,
            sstable_reader_factory_type fn)
        : reader_selector(s, pr.start() ? pr.start()->value() : dht::ring_position_view::min(), sstables->size())
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

            irclogger.trace("{}: {} sstables to consider, advancing selector to {}, eos={}", fmt::ptr(this), selection.sstables.size(),
                    _selector_position, end_of_stream());

            readers.clear();
            for (auto& sst : selection.sstables) {
                if (_read_sstable_gens.emplace(sst->generation()).second) {
                    readers.push_back(create_reader(sst));
                }
            }
        } while (!end_of_stream() && readers.empty() && (!pos || dht::ring_position_tri_compare(*_s, *pos, _selector_position) >= 0));

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
        // If selector position Y is contained in new range [X, Z], then we should try selecting new
        // sstables since it might have sstables that overlap with that range.
        if (!_selector_position.is_max() && dht::ring_position_tri_compare(*_s, _selector_position, pr_end()) <= 0) {
            return create_new_readers(std::nullopt);
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
make_pk_filter(const dht::ring_position& pos, const utils::hashed_key& hash, const schema& schema) {
    return [&pos, hash, cmp = dht::ring_position_comparator(schema)] (const sstable& sst) {
        return cmp(pos, sst.get_first_decorated_key()) >= 0 &&
               cmp(pos, sst.get_last_decorated_key()) <= 0 &&
               sst.filter_has_key(hash);
    };
}

const sstable_predicate& default_sstable_predicate() {
    static const sstable_predicate predicate = [] (const sstable&) { return true; };
    return predicate;
}

static std::predicate<const sstable&> auto
make_sstable_filter(const dht::ring_position& pos, const utils::hashed_key& hash, const schema& schema, const sstable_predicate& predicate) {
    return [pk_filter = make_pk_filter(pos, hash, schema), &predicate] (const sstable& sst) {
        return predicate(sst) && pk_filter(sst);
    };
}

// Filter out sstables for reader using bloom filter and supplied predicate
static std::vector<shared_sstable>
filter_sstable_for_reader(std::vector<shared_sstable>&& sstables, const schema& schema, const dht::ring_position& pos, const utils::hashed_key& hash, const sstable_predicate& predicate) {
    auto filter = [_filter = make_sstable_filter(pos, hash, schema, predicate)] (const shared_sstable& sst) { return !_filter(*sst); };
    std::erase_if(sstables, filter);
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

    // `sstable::may_contain_rows()` interprets the ranges using the table (non-reversed)
    // schema, so a native-reversed slice must have its range bounds put back in table order.
    auto ck_filtering_all_ranges = slice.get_all_ranges();
    if (slice.is_reversed()) {
        for (auto& r : ck_filtering_all_ranges) {
            r = query::reverse(r);
        }
    }
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
    auto all_sstables = all();
    std::unordered_map<sstables::run_id, sstable_run> runs_m;
    std::vector<frozen_sstable_run> all_runs;

    for (auto&& sst : *all_sstables) {
        // When a run cannot accept sstable due to overlapping, treat the rejected sstable
        // as a single-fragment run.
        if (!runs_m[sst->run_identifier()].insert(sst)) {
            all_runs.push_back(make_lw_shared<const sstable_run>(sst));
        }
    }
    for (auto&& r : runs_m | std::views::values) {
        all_runs.push_back(make_lw_shared<const sstable_run>(std::move(r)));
    }
    return all_runs;
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
        const sstable_predicate& predicate,
        sstables::integrity_check integrity) const
{
    const auto& pos = pr.start()->value();
    auto hash = utils::make_hashed_key(static_cast<bytes_view>(key::from_partition_key(*schema, *pos.key())));
    auto selected_sstables = filter_sstable_for_reader(select(pr), *schema, pos, hash, predicate);
    auto num_sstables = selected_sstables.size();
    if (!num_sstables) {
        return make_empty_mutation_reader(schema, permit);
    }
    auto readers = filter_sstable_for_reader_by_ck(std::move(selected_sstables), *cf, schema, slice)
        | std::views::transform([&] (const shared_sstable& sstable) {
            tracing::trace(trace_state, "Reading key {} from sstable {}", pos, seastar::value_of([&sstable] { return sstable->get_filename(); }));
            return sstable->make_reader(schema, permit, pr, slice, trace_state, fwd, mutation_reader::forwarding::yes,
                default_read_monitor(), integrity, &hash);
          })
        | std::ranges::to<std::vector<mutation_reader>>();

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
        readers.push_back(make_mutation_reader_from_mutations(schema, permit, mutation(schema, *pos.key()), slice, fwd));
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
        const sstable_predicate& predicate,
        sstables::integrity_check integrity) const {
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
                pr, slice, std::move(trace_state), fwd_sm, fwd_mr, predicate, integrity);
    }

    auto hash = utils::make_hashed_key(static_cast<bytes_view>(key::from_partition_key(*schema, *pos.key())));
    auto sst_filter = make_sstable_filter(pos, hash, *schema, predicate);
    auto it = std::find_if(_sstables->begin(), _sstables->end(), [&] (const sst_entry& e) { return sst_filter(*e.second); });
    if (it == _sstables->end()) {
        // No sstables contain data for the queried partition.
        return make_empty_mutation_reader(std::move(schema), std::move(permit));
    }

    auto& stats = *cf->cf_stats();
    stats.clustering_filter_count++;

    auto create_reader = [schema, permit, &pr, &slice, trace_state, fwd_sm, hash] (sstable& sst) {
        return sst.make_reader(schema, permit, pr, slice, trace_state, fwd_sm, mutation_reader::forwarding::yes,
                default_read_monitor(), integrity_check::no, &hash);
    };

    auto pk_filter = make_pk_filter(pos, hash, *schema);
    // See the comment in `filter_sstable_for_reader_by_ck()`.
    auto ck_ranges = slice.get_all_ranges();
    if (slice.is_reversed()) {
        for (auto& r : ck_ranges) {
            r = query::reverse(r);
        }
    }
    auto ck_filter = [ranges = std::move(ck_ranges)] (const sstable& sst) { return sst.may_contain_rows(ranges); };

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
    for (auto& set : std::ranges::subrange(sets.begin(), it)) {
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
    return std::ranges::fold_left(_sets | std::views::transform(std::mem_fn(&sstable_set::size)), size_t(0), std::plus{});
}

file_size_stats
compound_sstable_set::get_file_size_stats() const noexcept {
    return std::ranges::fold_left(_sets | std::views::transform(std::mem_fn(&sstable_set::get_file_size_stats)), file_size_stats{}, std::plus{});
}

class compound_sstable_set::incremental_selector : public incremental_selector_impl {
    const schema& _schema;
    const std::vector<lw_shared_ptr<sstable_set>>& _sets;
    std::vector<sstable_set::incremental_selector> _selectors;
private:
    std::vector<sstable_set::incremental_selector> make_selectors(const std::vector<lw_shared_ptr<sstable_set>>& sets) {
        return _sets | std::views::transform([] (const auto& set) {
            return set->make_incremental_selector();
        }) | std::ranges::to<std::vector<sstable_set::incremental_selector>>();
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
        const sstable_predicate& predicate,
        sstables::integrity_check integrity) const {
    auto sets = _sets;
    auto it = std::partition(sets.begin(), sets.end(), [] (const auto& set) { return set->size() > 0; });
    auto non_empty_set_count = std::distance(sets.begin(), it);

    if (!non_empty_set_count) {
        return make_empty_mutation_reader(schema, permit);
    }
    // optimize for common case where only 1 set is populated, avoiding the expensive combined reader
    if (non_empty_set_count == 1) {
        const auto& non_empty_set = *std::begin(sets);
        return non_empty_set->create_single_key_sstable_reader(cf, std::move(schema), std::move(permit), sstable_histogram, pr, slice, trace_state, fwd, fwd_mr, predicate, integrity);
    }

    auto readers = std::ranges::subrange(sets.begin(), it)
        | std::views::transform([&] (const lw_shared_ptr<sstable_set>& non_empty_set) {
            return non_empty_set->create_single_key_sstable_reader(cf, schema, permit, sstable_histogram, pr, slice, trace_state, fwd, fwd_mr, predicate, integrity);
          })
        | std::ranges::to<std::vector<mutation_reader>>();
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
        const sstable_predicate& predicate,
        sstables::integrity_check integrity) const {
    SCYLLA_ASSERT(pr.is_singular() && pr.start()->value().has_key());
    return _impl->create_single_key_sstable_reader(cf, std::move(schema),
            std::move(permit), sstable_histogram, pr, slice, std::move(trace_state), fwd, fwd_mr, predicate, integrity);
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
            _reader = make_empty_mutation_reader(_schema, _permit);
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
        read_monitor_generator& monitor_generator,
        integrity_check integrity) const
{
    auto reader_factory_fn = [s, permit, &slice, trace_state, fwd, fwd_mr, &monitor_generator, integrity]
            (shared_sstable& sst, const dht::partition_range& pr) mutable {
        return sst->make_reader(s, permit, pr, slice, trace_state, fwd, fwd_mr, monitor_generator(sst), integrity);
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
        const sstable_predicate& predicate,
        combined_reader_statistics* statistics,
        integrity_check integrity) const
{
    auto reader_factory_fn = [s, permit, &slice, trace_state, fwd, fwd_mr, &monitor_generator, &predicate, integrity]
            (shared_sstable& sst, const dht::partition_range& pr) mutable {
        SCYLLA_ASSERT(!sst->is_shared());
        if (!predicate(*sst)) {
            return make_empty_mutation_reader(s, permit);
        }
        auto reader = sst->make_reader(s, permit, pr, slice, trace_state, fwd, fwd_mr, monitor_generator(sst), integrity);
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
            fwd_mr,
            statistics);
}

mutation_reader sstable_set::make_full_scan_reader(
        schema_ptr schema,
        reader_permit permit,
        tracing::trace_state_ptr trace_ptr,
        read_monitor_generator& monitor_generator,
        integrity_check integrity) const {
    std::vector<mutation_reader> readers;
    readers.reserve(size());
    for_each_sstable([&] (const shared_sstable& sst) mutable {
        readers.emplace_back(sst->make_full_scan_reader(schema, permit, trace_ptr, monitor_generator(sst), integrity));
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
