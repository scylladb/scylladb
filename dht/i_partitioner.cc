/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "i_partitioner.hh"
#include "sharder.hh"
#include <seastar/core/seastar.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include "dht/ring_position.hh"
#include "dht/token-sharding.hh"
#include "utils/assert.hh"
#include "utils/class_registrator.hh"
#include <boost/range/adaptor/map.hpp>
#include <boost/range/irange.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include "sstables/key.hh"
#include <seastar/core/thread.hh>
#include <seastar/core/on_internal_error.hh>
#include "log.hh"

namespace dht {

static logging::logger logger("i_partitioner");

sharder::sharder(unsigned shard_count, unsigned sharding_ignore_msb_bits)
    : _shard_count(shard_count)
    // if one shard, ignore sharding_ignore_msb_bits as they will just cause needless
    // range breaks
    , _sharding_ignore_msb_bits(shard_count > 1 ? sharding_ignore_msb_bits : 0)
{}

static_sharder::static_sharder(unsigned shard_count, unsigned sharding_ignore_msb_bits)
    : sharder(shard_count, sharding_ignore_msb_bits)
    , _shard_start(init_zero_based_shard_start(_shard_count, _sharding_ignore_msb_bits))
{}

unsigned
static_sharder::shard_of(const token& t) const {
    return dht::shard_of(_shard_count, _sharding_ignore_msb_bits, t);
}

unsigned
static_sharder::shard_for_reads(const token& t) const {
    return shard_of(t);
}

shard_replica_set
static_sharder::shard_for_writes(const token& t, std::optional<write_replica_set_selector> sel) const {
    return {shard_of(t)};
}

token
static_sharder::token_for_next_shard(const token& t, shard_id shard, unsigned spans) const {
    return dht::token_for_next_shard(_shard_start, _shard_count, _sharding_ignore_msb_bits, t, shard, spans);
}

token
static_sharder::token_for_next_shard_for_reads(const token& t, shard_id shard, unsigned spans) const {
    return token_for_next_shard(t, shard, spans);
}

std::optional<shard_and_token>
static_sharder::next_shard(const token& t) const {
    auto shard = shard_for_reads(t);
    auto next_shard = shard + 1 == _shard_count ? 0 : shard + 1;
    auto next_token = token_for_next_shard_for_reads(t, next_shard);
    if (next_token.is_maximum()) {
        return std::nullopt;
    }
    return shard_and_token{next_shard, next_token};
}

std::optional<shard_and_token>
static_sharder::next_shard_for_reads(const token& t) const {
    return next_shard(t);
}

std::unique_ptr<dht::i_partitioner> make_partitioner(sstring partitioner_name) {
    try {
        return create_object<i_partitioner>(partitioner_name);
    } catch (std::exception& e) {
        auto supported_partitioners = fmt::join(
            class_registry<i_partitioner>::classes() |
            boost::adaptors::map_keys,
            ", ");
        throw std::runtime_error(format("Partitioner {} is not supported, supported partitioners = {{ {} }} : {}",
                partitioner_name, supported_partitioners, e.what()));
    }
}

bool
decorated_key::equal(const schema& s, const decorated_key& other) const {
    if (_token == other._token) {
        return _key.legacy_equal(s, other._key);
    }
    return false;
}

std::strong_ordering
decorated_key::tri_compare(const schema& s, const decorated_key& other) const {
    auto r = _token <=> other._token;
    if (r != 0) {
        return r;
    } else {
        return _key.legacy_tri_compare(s, other._key);
    }
}

std::strong_ordering
decorated_key::tri_compare(const schema& s, const ring_position& other) const {
    auto r = _token <=> other.token();
    if (r != 0) {
        return r;
    } else if (other.has_key()) {
        return _key.legacy_tri_compare(s, *other.key());
    }
    return 0 <=> other.relation_to_keys();
}

bool
decorated_key::less_compare(const schema& s, const ring_position& other) const {
    return tri_compare(s, other) < 0;
}

bool
decorated_key::less_compare(const schema& s, const decorated_key& other) const {
    return tri_compare(s, other) < 0;
}

decorated_key::less_comparator::less_comparator(schema_ptr s)
    : s(std::move(s))
{ }

bool
decorated_key::less_comparator::operator()(const decorated_key& lhs, const decorated_key& rhs) const {
    return lhs.less_compare(*s, rhs);
}

bool
decorated_key::less_comparator::operator()(const ring_position& lhs, const decorated_key& rhs) const {
    return rhs.tri_compare(*s, lhs) > 0;
}

bool
decorated_key::less_comparator::operator()(const decorated_key& lhs, const ring_position& rhs) const {
    return lhs.tri_compare(*s, rhs) < 0;
}

unsigned static_shard_of(const schema& s, const token& t) {
    return s.get_sharder().shard_for_reads(t);
}

std::optional<dht::token_range>
selective_token_range_sharder::next() {
    if (_done) {
        return {};
    }
    while (_range.overlaps(dht::token_range(_start_boundary, {}), dht::token_comparator())
            && !(_start_boundary && _start_boundary->value() == maximum_token())) {
        auto end_token = _sharder.token_for_next_shard_for_reads(_start_token, _next_shard);
        auto candidate = dht::token_range(std::move(_start_boundary), interval_bound<dht::token>(end_token, false));
        auto intersection = _range.intersection(std::move(candidate), dht::token_comparator());
        _start_token = _sharder.token_for_next_shard_for_reads(end_token, _shard);
        _start_boundary = interval_bound<dht::token>(_start_token);
        if (intersection) {
            return *intersection;
        }
    }

    _done = true;
    return {};
}

std::optional<ring_position_range_and_shard>
ring_position_range_sharder::next(const schema& s) {
    if (_done) {
        return {};
    }
    auto token = _range.start() ? _range.start()->value().token() : dht::minimum_token();
    auto shard = _sharder.shard_for_reads(token);
    auto next_shard_and_token = _sharder.next_shard_for_reads(token);
    if (!next_shard_and_token) {
        _done = true;
        return ring_position_range_and_shard{std::move(_range), shard};
    }
    auto shard_boundary_token = next_shard_and_token->token;
    auto shard_boundary = ring_position::starting_at(shard_boundary_token);
    if ((!_range.end() || shard_boundary.less_compare(s, _range.end()->value()))
            && !shard_boundary_token.is_maximum()) {
        // split the range at end_of_shard
        auto start = _range.start();
        auto end = interval_bound<ring_position>(shard_boundary, false);
        _range = dht::partition_range(
                interval_bound<ring_position>(std::move(shard_boundary), true),
                std::move(_range.end()));
        return ring_position_range_and_shard{dht::partition_range(std::move(start), std::move(end)), shard};
    }
    _done = true;
    return ring_position_range_and_shard{std::move(_range), shard};
}

ring_position_range_vector_sharder::ring_position_range_vector_sharder(const sharder& sharder, dht::partition_range_vector ranges)
        : _ranges(std::move(ranges))
        , _sharder(sharder)
        , _current_range(_ranges.begin()) {
    next_range();
}

std::optional<ring_position_range_and_shard_and_element>
ring_position_range_vector_sharder::next(const schema& s) {
    if (!_current_sharder) {
        return std::nullopt;
    }
    auto range_and_shard = _current_sharder->next(s);
    while (!range_and_shard && _current_range != _ranges.end()) {
        next_range();
        range_and_shard = _current_sharder->next(s);
    }
    auto ret = std::optional<ring_position_range_and_shard_and_element>();
    if (range_and_shard) {
        ret.emplace(std::move(*range_and_shard), _current_range - _ranges.begin() - 1);
    }
    return ret;
}

future<utils::chunked_vector<partition_range>>
split_range_to_single_shard(const schema& s, const static_sharder& sharder, const partition_range& pr, shard_id shard) {
    auto start_token = pr.start() ? pr.start()->value().token() : minimum_token();
    auto start_shard = sharder.shard_of(start_token);
    auto start_boundary = start_shard == shard ? pr.start() : interval_bound<ring_position>(ring_position::starting_at(sharder.token_for_next_shard(start_token, shard)));
    start_token = start_shard == shard ? start_token : sharder.token_for_next_shard(start_token, shard);
    return repeat_until_value([&sharder,
            &pr,
            cmp = ring_position_comparator(s),
            ret = utils::chunked_vector<partition_range>(),
            start_token,
            start_boundary,
            shard] () mutable {
        if (pr.overlaps(partition_range(start_boundary, {}), cmp)
                && !(start_boundary && start_boundary->value().token().is_maximum())) {
            dht::token end_token = maximum_token();
            auto s_a_t = sharder.next_shard(start_token);
            if (s_a_t) {
                end_token = s_a_t->token;
            }
            auto candidate = partition_range(std::move(start_boundary), interval_bound<ring_position>(ring_position::starting_at(end_token), false));
            auto intersection = pr.intersection(std::move(candidate), cmp);
            if (intersection) {
                ret.push_back(std::move(*intersection));
            }
            if (!s_a_t) {
                return make_ready_future<std::optional<utils::chunked_vector<partition_range>>>(std::move(ret));
            }
            if (s_a_t->shard == shard) {
                start_token = end_token;
            } else {
                start_token = sharder.token_for_next_shard(end_token, shard);
            }
            start_boundary = interval_bound<ring_position>(ring_position::starting_at(start_token));
            return make_ready_future<std::optional<utils::chunked_vector<partition_range>>>();
        }
        return make_ready_future<std::optional<utils::chunked_vector<partition_range>>>(std::move(ret));
    });
}

std::strong_ordering ring_position::tri_compare(const schema& s, const ring_position& o) const {
    return ring_position_comparator(s)(*this, o);
}

bool ring_position::equal(const schema& s, const ring_position& other) const {
    return tri_compare(s, other) == 0;
}

bool ring_position::less_compare(const schema& s, const ring_position& other) const {
    return tri_compare(s, other) < 0;
}

std::strong_ordering ring_position_tri_compare(const schema& s, ring_position_view lh, ring_position_view rh) {
    auto token_cmp = *lh._token <=> *rh._token;
    if (token_cmp != 0) {
        return token_cmp;
    }
    if (lh._key && rh._key) {
        auto c = lh._key->legacy_tri_compare(s, *rh._key);
        if (c != 0) {
            return c;
        }
        return (lh._weight - rh._weight) <=> 0;
    }
    if (!lh._key && !rh._key) {
        return lh._weight - rh._weight <=> 0;
    } else if (!lh._key) {
        return lh._weight > 0 ? std::strong_ordering::greater : std::strong_ordering::less;
    } else {
        return rh._weight > 0 ? std::strong_ordering::less : std::strong_ordering::greater;
    }
}

std::strong_ordering ring_position_comparator_for_sstables::operator()(ring_position_view lh, sstables::decorated_key_view rh) const {
    auto token_cmp = *lh._token <=> rh.token();
    if (token_cmp != 0) {
        return token_cmp;
    }
    if (lh._key) {
        auto rel = rh.key().tri_compare(s, *lh._key);
        if (rel != std::strong_ordering::equal) {
            return 0 <=> rel;
        }
    }
    return lh._weight <=> 0;
}

std::strong_ordering ring_position_comparator_for_sstables::operator()(sstables::decorated_key_view a, ring_position_view b) const {
    return 0 <=> (*this)(b, a);
}

dht::partition_range
to_partition_range(dht::token_range r) {
    using bound = dht::partition_range::bound;
    using bound_opt = std::optional<dht::partition_range::bound>;
    auto start = r.start()
                 ? bound_opt(bound(dht::ring_position(r.start()->value(),
                                                r.start()->is_inclusive()
                                                ? dht::ring_position::token_bound::start
                                                : dht::ring_position::token_bound::end),
                         r.start()->is_inclusive()))
                 : bound_opt();

    auto end = r.end()
               ? bound_opt(bound(dht::ring_position(r.end()->value(),
                                              r.end()->is_inclusive()
                                              ? dht::ring_position::token_bound::end
                                              : dht::ring_position::token_bound::start),
                         r.end()->is_inclusive()))
               : bound_opt();

    return { std::move(start), std::move(end) };
}

dht::partition_range_vector to_partition_ranges(const dht::token_range_vector& ranges, utils::can_yield can_yield) {
    dht::partition_range_vector prs;
    prs.reserve(ranges.size());
    for (auto& range : ranges) {
        prs.push_back(dht::to_partition_range(range));
        utils::maybe_yield(can_yield);
    }
    return prs;
}

std::map<unsigned, dht::partition_range_vector>
split_range_to_shards(dht::partition_range pr, const schema& s, const sharder& raw_sharder) {
    std::map<unsigned, dht::partition_range_vector> ret;
    auto sharder = dht::ring_position_range_sharder(raw_sharder, std::move(pr));
    auto rprs = sharder.next(s);
    while (rprs) {
        ret[rprs->shard].emplace_back(rprs->ring_range);
        rprs = sharder.next(s);
    }
    return ret;
}

future<dht::partition_range_vector> subtract_ranges(const schema& schema, const dht::partition_range_vector& source_ranges, dht::partition_range_vector ranges_to_subtract) {
    auto cmp = dht::ring_position_comparator(schema);
    // optimize set of potentially overlapping ranges by deoverlapping them.
    auto ranges = dht::partition_range::deoverlap(source_ranges, cmp);
    dht::partition_range_vector res;
    res.reserve(ranges.size() * 2);

    auto range = ranges.begin();
    auto range_end = ranges.end();
    auto range_to_subtract = ranges_to_subtract.begin();
    auto range_to_subtract_end = ranges_to_subtract.end();
    while (range != range_end) {
        if (range_to_subtract == range_to_subtract_end) {
            // We're done with range_to_subtracts
            res.emplace_back(std::move(*range));
            ++range;
            continue;
        }

        auto diff = range->subtract(*range_to_subtract, cmp);
        auto size = diff.size();
        switch (size) {
        case 0:
            // current range is fully covered by range_to_subtract, done with it
            // range_to_subtrace.start <= range.start &&
            //   range_to_subtrace.end >= range.end
            ++range;
            break;
        case 1:
            // Possible cases:
            // a. range and range_to_subtract are disjoint (so diff == range)
            //    a.i range_to_subtract.end < range.start
            //    a.ii range_to_subtract.start > range.end
            // b. range_to_subtrace.start > range.start, so it removes the range suffix
            // c. range_to_subtrace.start < range.start, so it removes the range prefix

            // Does range_to_subtract sort after range?
            if (range_to_subtract->start() && (!range->start() || cmp(range_to_subtract->start()->value(), range->start()->value()) > 0)) {
                // save range prefix in the result
                // (note that diff[0] == range in the disjoint case)
                res.emplace_back(std::move(diff[0]));
                // done with current range
                ++range;
            } else {
                // set the current range to the remaining suffix
                *range = std::move(diff[0]);
                // done with current range_to_subtract
                ++range_to_subtract;
            }
            break;
        case 2:
            // range contains range_to_subtract

            // save range prefix in the result
            res.emplace_back(std::move(diff[0]));
            // set the current range to the remaining suffix
            *range = std::move(diff[1]);
            // done with current range_to_subtract
            ++range_to_subtract;
            break;
        default:
            SCYLLA_ASSERT(size <= 2);
        }
        co_await coroutine::maybe_yield();
    }

    co_return res;
}

dht::token_range_vector split_token_range_msb(unsigned most_significant_bits) {
    dht::token_range_vector ret;
    // Avoid shift left 64
    if (!most_significant_bits) {
        auto&& start_bound = dht::token_range::bound(dht::minimum_token(), true);
        auto&& end_bound = dht::token_range::bound(dht::maximum_token(), true);
        ret.emplace_back(std::move(start_bound), std::move(end_bound));
        return ret;
    }
    uint64_t number_of_ranges = 1 << most_significant_bits;
    ret.reserve(number_of_ranges);
    SCYLLA_ASSERT(most_significant_bits < 64);
    dht::token prev_last_token;
    for (uint64_t i = 0; i < number_of_ranges; i++) {
        std::optional<dht::token_range::bound> start_bound;
        std::optional<dht::token_range::bound> end_bound;
        if (i == 0) {
            start_bound = dht::token_range::bound(dht::minimum_token(), true);
        } else {
            auto token = dht::next_token(prev_last_token);
            if (compaction_group_of(most_significant_bits, token) != i) {
                on_fatal_internal_error(logger, format("split_token_range_msb: inconsistent end_bound compaction group: index={} msbits={} token={} compaction_group_of={}",
                                                       i, most_significant_bits, token, compaction_group_of(most_significant_bits, token)));
            }
            start_bound = dht::token_range::bound(prev_last_token, false);
        }
        prev_last_token = dht::last_token_of_compaction_group(most_significant_bits, i);
        end_bound = dht::token_range::bound(prev_last_token, true);
        ret.emplace_back(std::move(start_bound), std::move(end_bound));
    }
    return ret;
}

dht::token first_token(const dht::partition_range& pr) {
    auto start = dht::ring_position_view::for_range_start(pr);
    auto token = start.token();
    // Check if the range excludes "token".
    if (!start.key()
        && start.get_token_bound() == dht::ring_position::token_bound::end
        && token._kind == dht::token::kind::key
        && !token.is_last()) {
        token = dht::next_token(token);
    }
    return token;
}

std::optional<shard_id> is_single_shard(const dht::sharder& sharder, const schema& s, const dht::partition_range& pr) {
    auto token = first_token(pr);
    auto shard = sharder.shard_for_reads(token);
    if (pr.is_singular()) {
        return shard;
    }
    if (auto s_a_t = sharder.next_shard_for_reads(token)) {
        dht::ring_position_comparator cmp(s);
        auto end = dht::ring_position_view::for_range_end(pr);
        if (cmp(end, dht::ring_position_view::starting_at(s_a_t->token)) > 0) {
            return std::nullopt;
        }
    }
    return shard;
}

}

auto fmt::formatter<dht::ring_position_view>::format(const dht::ring_position_view& pos, fmt::format_context& ctx) const
    -> decltype(ctx.out()) {
    auto out = ctx.out();
    out = fmt::format_to(out, "{{{}", *pos._token);
    if (pos._key) {
        out = fmt::format_to(out, ", {}", *pos._key);
    }
    return fmt::format_to(out, ", w={}}}", static_cast<int>(pos._weight));
}

auto fmt::formatter<dht::ring_position>::format(const dht::ring_position& pos, fmt::format_context& ctx) const
    -> decltype(ctx.out()) {
    auto out = ctx.out();
    out = fmt::format_to(out, "{{{}", pos.token());
    if (pos.has_key()) {
        out = fmt::format_to(out, ", {}", *pos.key());
    } else {
        out = fmt::format_to(out, ", {}", (pos.relation_to_keys() < 0) ? "start" : "end");
    }
    return fmt::format_to(out, "}}");
}

auto fmt::formatter<dht::partition_ranges_view>::format(const dht::partition_ranges_view& v, fmt::format_context& ctx) const
    -> decltype(ctx.out()) {
    auto out = fmt::format_to(ctx.out(), "{{");
    for (auto& range : v) {
        out = fmt::format_to(out, "{}", range);
    }
    return fmt::format_to(out, "}}");
}
