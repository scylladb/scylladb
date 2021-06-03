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

#include "i_partitioner.hh"
#include "sharder.hh"
#include <seastar/core/seastar.hh>
#include "dht/token-sharding.hh"
#include "utils/class_registrator.hh"
#include "types.hh"
#include "utils/murmur_hash.hh"
#include "utils/div_ceil.hh"
#include <deque>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/irange.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include "sstables/key.hh"
#include <seastar/core/thread.hh>

namespace dht {

sharder::sharder(unsigned shard_count, unsigned sharding_ignore_msb_bits)
    : _shard_count(shard_count)
    // if one shard, ignore sharding_ignore_msb_bits as they will just cause needless
    // range breaks
    , _sharding_ignore_msb_bits(shard_count > 1 ? sharding_ignore_msb_bits : 0)
    , _shard_start(init_zero_based_shard_start(_shard_count, _sharding_ignore_msb_bits))
{}

unsigned
sharder::shard_of(const token& t) const {
    return dht::shard_of(_shard_count, _sharding_ignore_msb_bits, t);
}

token
sharder::token_for_next_shard(const token& t, shard_id shard, unsigned spans) const {
    return dht::token_for_next_shard(_shard_start, _shard_count, _sharding_ignore_msb_bits, t, shard, spans);
}

std::ostream& operator<<(std::ostream& out, const decorated_key& dk) {
    return out << "{key: " << dk._key << ", token:" << dk._token << "}";
}

std::ostream& operator<<(std::ostream& out, partition_ranges_view v) {
    out << "{";

    if (v.empty()) {
        out << " }";
        return out;
    }

    auto it = v.begin();
    out << *it;
    ++it;

    for (;it != v.end(); ++it) {
        out << ", " << *it;
    }

    out << "}";
    return out;
}

std::unique_ptr<dht::i_partitioner> make_partitioner(sstring partitioner_name) {
    try {
        return create_object<i_partitioner>(partitioner_name);
    } catch (std::exception& e) {
        auto supported_partitioners = ::join(", ", class_registry<i_partitioner>::classes() |
                boost::adaptors::map_keys);
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
    auto r = dht::tri_compare(_token, other._token);
    if (r != 0) {
        return r;
    } else {
        return _key.legacy_tri_compare(s, other._key) <=> 0;
    }
}

std::strong_ordering
decorated_key::tri_compare(const schema& s, const ring_position& other) const {
    auto r = dht::tri_compare(_token, other.token());
    if (r != 0) {
        return r;
    } else if (other.has_key()) {
        return _key.legacy_tri_compare(s, *other.key()) <=> 0;
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

std::ostream& operator<<(std::ostream& out, const ring_position_ext& pos) {
    return out << (ring_position_view)pos;
}

std::ostream& operator<<(std::ostream& out, const ring_position& pos) {
    out << "{" << pos.token();
    if (pos.has_key()) {
        out << ", " << *pos.key();
    } else {
        out << ", " << ((pos.relation_to_keys() < 0) ? "start" : "end");
    }
    return out << "}";
}

std::ostream& operator<<(std::ostream& out, ring_position_view pos) {
    out << "{" << *pos._token;
    if (pos._key) {
        out << ", " << *pos._key;
    }
    out << ", w=" << static_cast<int>(pos._weight);
    return out << "}";
}

std::ostream& operator<<(std::ostream& out, const i_partitioner& p) {
    out << "{partitioner name = " << p.name();
    return out << "}";
}

unsigned shard_of(const schema& s, const token& t) {
    return s.get_sharder().shard_of(t);
}

std::optional<dht::token_range>
selective_token_range_sharder::next() {
    if (_done) {
        return {};
    }
    while (_range.overlaps(dht::token_range(_start_boundary, {}), dht::token_comparator())
            && !(_start_boundary && _start_boundary->value() == maximum_token())) {
        auto end_token = _sharder.token_for_next_shard(_start_token, _next_shard);
        auto candidate = dht::token_range(std::move(_start_boundary), range_bound<dht::token>(end_token, false));
        auto intersection = _range.intersection(std::move(candidate), dht::token_comparator());
        _start_token = _sharder.token_for_next_shard(end_token, _shard);
        _start_boundary = range_bound<dht::token>(_start_token);
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
    auto shard = _range.start() ? _sharder.shard_of(_range.start()->value().token()) : token::shard_of_minimum_token();
    auto next_shard = shard + 1 < _sharder.shard_count() ? shard + 1 : 0;
    auto shard_boundary_token = _sharder.token_for_next_shard(_range.start() ? _range.start()->value().token() : minimum_token(), next_shard);
    auto shard_boundary = ring_position::starting_at(shard_boundary_token);
    if ((!_range.end() || shard_boundary.less_compare(s, _range.end()->value()))
            && shard_boundary_token != maximum_token()) {
        // split the range at end_of_shard
        auto start = _range.start();
        auto end = range_bound<ring_position>(shard_boundary, false);
        _range = dht::partition_range(
                range_bound<ring_position>(std::move(shard_boundary), true),
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
split_range_to_single_shard(const schema& s, const partition_range& pr, shard_id shard) {
    const sharder& sharder = s.get_sharder();
    auto next_shard = shard + 1 == sharder.shard_count() ? 0 : shard + 1;
    auto start_token = pr.start() ? pr.start()->value().token() : minimum_token();
    auto start_shard = sharder.shard_of(start_token);
    auto start_boundary = start_shard == shard ? pr.start() : range_bound<ring_position>(ring_position::starting_at(sharder.token_for_next_shard(start_token, shard)));
    return repeat_until_value([&sharder,
            &pr,
            cmp = ring_position_comparator(s),
            ret = utils::chunked_vector<partition_range>(),
            start_token,
            start_boundary,
            shard,
            next_shard] () mutable {
        if (pr.overlaps(partition_range(start_boundary, {}), cmp)
                && !(start_boundary && start_boundary->value().token() == maximum_token())) {
            auto end_token = sharder.token_for_next_shard(start_token, next_shard);
            auto candidate = partition_range(std::move(start_boundary), range_bound<ring_position>(ring_position::starting_at(end_token), false));
            auto intersection = pr.intersection(std::move(candidate), cmp);
            if (intersection) {
                ret.push_back(std::move(*intersection));
            }
            start_token = sharder.token_for_next_shard(end_token, shard);
            start_boundary = range_bound<ring_position>(ring_position::starting_at(start_token));
            return make_ready_future<std::optional<utils::chunked_vector<partition_range>>>();
        }
        return make_ready_future<std::optional<utils::chunked_vector<partition_range>>>(std::move(ret));
    });
}

std::strong_ordering ring_position::tri_compare(const schema& s, const ring_position& o) const {
    return ring_position_comparator(s)(*this, o);
}

std::strong_ordering token_comparator::operator()(const token& t1, const token& t2) const {
    return tri_compare(t1, t2);
}

bool ring_position::equal(const schema& s, const ring_position& other) const {
    return tri_compare(s, other) == 0;
}

bool ring_position::less_compare(const schema& s, const ring_position& other) const {
    return tri_compare(s, other) < 0;
}

std::strong_ordering ring_position_tri_compare(const schema& s, ring_position_view lh, ring_position_view rh) {
    auto token_cmp = tri_compare(*lh._token, *rh._token);
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
    auto token_cmp = tri_compare(*lh._token, rh.token());
    if (token_cmp != 0) {
        return token_cmp;
    }
    if (lh._key) {
        auto rel = rh.key().tri_compare(s, *lh._key);
        if (rel) {
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
    using bound_opt = std::optional<dht::partition_range::bound>;
    auto start = r.start()
                 ? bound_opt(dht::ring_position(r.start()->value(),
                                                r.start()->is_inclusive()
                                                ? dht::ring_position::token_bound::start
                                                : dht::ring_position::token_bound::end))
                 : bound_opt();

    auto end = r.end()
               ? bound_opt(dht::ring_position(r.end()->value(),
                                              r.end()->is_inclusive()
                                              ? dht::ring_position::token_bound::end
                                              : dht::ring_position::token_bound::start))
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
split_range_to_shards(dht::partition_range pr, const schema& s) {
    std::map<unsigned, dht::partition_range_vector> ret;
    auto sharder = dht::ring_position_range_sharder(s.get_sharder(), std::move(pr));
    auto rprs = sharder.next(s);
    while (rprs) {
        ret[rprs->shard].emplace_back(rprs->ring_range);
        rprs = sharder.next(s);
    }
    return ret;
}

}
