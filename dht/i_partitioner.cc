/*
 * Copyright (C) 2015 ScyllaDB
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
#include "core/reactor.hh"
#include "murmur3_partitioner.hh"
#include "utils/class_registrator.hh"
#include "types.hh"
#include "utils/murmur_hash.hh"
#include "utils/div_ceil.hh"
#include <deque>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/irange.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include "sstables/key.hh"

namespace dht {

static const token min_token{ token::kind::before_all_keys, {} };
static const token max_token{ token::kind::after_all_keys, {} };

const token&
minimum_token() {
    return min_token;
}

const token&
maximum_token() {
    return max_token;
}

// result + overflow bit
std::pair<bytes, bool>
add_bytes(bytes_view b1, bytes_view b2, bool carry = false) {
    auto sz = std::max(b1.size(), b2.size());
    auto expand = [sz] (bytes_view b) {
        bytes ret(bytes::initialized_later(), sz);
        auto bsz = b.size();
        auto p = std::copy(b.begin(), b.end(), ret.begin());
        std::fill_n(p, sz - bsz, 0);
        return ret;
    };
    auto eb1 = expand(b1);
    auto eb2 = expand(b2);
    auto p1 = eb1.begin();
    auto p2 = eb2.begin();
    unsigned tmp = carry;
    for (size_t idx = 0; idx < sz; ++idx) {
        tmp += uint8_t(p1[sz - idx - 1]);
        tmp += uint8_t(p2[sz - idx - 1]);
        p1[sz - idx - 1] = tmp;
        tmp >>= std::numeric_limits<uint8_t>::digits;
    }
    return { std::move(eb1), bool(tmp) };
}

bytes
shift_right(bool carry, bytes b) {
    unsigned tmp = carry;
    auto sz = b.size();
    auto p = b.begin();
    for (size_t i = 0; i < sz; ++i) {
        auto lsb = p[i] & 1;
        p[i] = (tmp << std::numeric_limits<uint8_t>::digits) | uint8_t(p[i]) >> 1;
        tmp = lsb;
    }
    return b;
}

token
midpoint_unsigned_tokens(const token& t1, const token& t2) {
    // calculate the average of the two tokens.
    // before_all_keys is implicit 0, after_all_keys is implicit 1.
    bool c1 = t1._kind == token::kind::after_all_keys;
    bool c2 = t1._kind == token::kind::after_all_keys;
    if (c1 && c2) {
        // both end-of-range tokens?
        return t1;
    }
    // we can ignore beginning-of-range, since their representation is 0.0
    auto sum_carry = add_bytes(t1._data, t2._data);
    auto& sum = sum_carry.first;
    // if either was end-of-range, we added 0.0, so pretend we added 1.0 and
    // and got a carry:
    bool carry = sum_carry.second || c1 || c2;
    auto avg = shift_right(carry, std::move(sum));
    if (t1 > t2) {
        // wrap around the ring.  We really want (t1 + (t2 + 1.0)) / 2, so add 0.5.
        // example: midpoint(0.9, 0.2) == midpoint(0.9, 1.2) == 1.05 == 0.05
        //                             == (0.9 + 0.2) / 2 + 0.5 (mod 1)
        if (avg.size() > 0) {
            avg[0] ^= 0x80;
        }
    }
    return token{token::kind::key, std::move(avg)};
}

int tri_compare(token_view t1, token_view t2) {
    if (t1._kind == t2._kind) {
        return global_partitioner().tri_compare(t1, t2);
    } else if (t1._kind < t2._kind) {
        return -1;
    }
    return 1;
}

bool operator==(token_view t1, token_view t2) {
    if (t1._kind != t2._kind) {
        return false;
    } else if (t1._kind == token::kind::key) {
        return global_partitioner().is_equal(t1, t2);
    }
    return true;
}

bool operator<(token_view t1, token_view t2) {
    if (t1._kind < t2._kind) {
        return true;
    } else if (t1._kind == token::kind::key && t2._kind == token::kind::key) {
        return global_partitioner().is_less(t1, t2);
    }
    return false;
}

std::ostream& operator<<(std::ostream& out, const token& t) {
    if (t._kind == token::kind::after_all_keys) {
        out << "maximum token";
    } else if (t._kind == token::kind::before_all_keys) {
        out << "minimum token";
    } else {
        out << global_partitioner().to_sstring(t);
    }
    return out;
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

// FIXME: make it per-keyspace
std::unique_ptr<i_partitioner> default_partitioner;

void set_global_partitioner(const sstring& class_name, unsigned ignore_msb)
{
    try {
        default_partitioner = create_object<i_partitioner, const unsigned&, const unsigned&>(class_name, smp::count, ignore_msb);
    } catch (std::exception& e) {
        auto supported_partitioners = ::join(", ", class_registry<i_partitioner>::classes() |
                boost::adaptors::map_keys);
        throw std::runtime_error(sprint("Partitioner %s is not supported, supported partitioners = { %s } : %s",
                class_name, supported_partitioners, e.what()));
    }
}

i_partitioner&
global_partitioner() {
    if (!default_partitioner) {
        default_partitioner = std::make_unique<murmur3_partitioner>(smp::count, 12);
    }
    return *default_partitioner;
}

bool
decorated_key::equal(const schema& s, const decorated_key& other) const {
    if (_token == other._token) {
        return _key.legacy_equal(s, other._key);
    }
    return false;
}

int
decorated_key::tri_compare(const schema& s, const decorated_key& other) const {
    auto r = dht::tri_compare(_token, other._token);
    if (r != 0) {
        return r;
    } else {
        return _key.legacy_tri_compare(s, other._key);
    }
}

int
decorated_key::tri_compare(const schema& s, const ring_position& other) const {
    auto r = dht::tri_compare(_token, other.token());
    if (r != 0) {
        return r;
    } else if (other.has_key()) {
        return _key.legacy_tri_compare(s, *other.key());
    }
    return -other.relation_to_keys();
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

unsigned shard_of(const token& t) {
    return global_partitioner().shard_of(t);
}

stdx::optional<dht::token_range>
selective_token_range_sharder::next() {
    if (_done) {
        return {};
    }
    while (_range.overlaps(dht::token_range(_start_boundary, {}), dht::token_comparator())
            && !(_start_boundary && _start_boundary->value() == maximum_token())) {
        auto end_token = _partitioner.token_for_next_shard(_start_token, _next_shard);
        auto candidate = dht::token_range(std::move(_start_boundary), range_bound<dht::token>(end_token, false));
        auto intersection = _range.intersection(std::move(candidate), dht::token_comparator());
        _start_token = _partitioner.token_for_next_shard(end_token, _shard);
        _start_boundary = range_bound<dht::token>(_start_token);
        if (intersection) {
            return *intersection;
        }
    }

    _done = true;
    return {};
}

stdx::optional<ring_position_range_and_shard>
ring_position_range_sharder::next(const schema& s) {
    if (_done) {
        return {};
    }
    auto shard = _range.start() ? _partitioner.shard_of(_range.start()->value().token()) : _partitioner.shard_of_minimum_token();
    auto next_shard = shard + 1 < _partitioner.shard_count() ? shard + 1 : 0;
    auto shard_boundary_token = _partitioner.token_for_next_shard(_range.start() ? _range.start()->value().token() : minimum_token(), next_shard);
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


ring_position_exponential_sharder::ring_position_exponential_sharder(const i_partitioner& partitioner, partition_range pr)
        : _partitioner(partitioner)
        , _range(std::move(pr))
        , _last_ends(_partitioner.shard_count()) {
    if (_range.start()) {
        _first_shard = _next_shard = _partitioner.shard_of(_range.start()->value().token());
    }
}

ring_position_exponential_sharder::ring_position_exponential_sharder(partition_range pr)
        : ring_position_exponential_sharder(global_partitioner(), std::move(pr)) {
}

stdx::optional<ring_position_exponential_sharder_result>
ring_position_exponential_sharder::next(const schema& s) {
    auto ret = ring_position_exponential_sharder_result{};
    ret.per_shard_ranges.reserve(std::min(_spans_per_iteration, _partitioner.shard_count()));
    ret.inorder = _spans_per_iteration <= _partitioner.shard_count();
    unsigned spans_to_go = _spans_per_iteration;
    auto cmp = ring_position_comparator(s);
    auto spans_per_shard = _spans_per_iteration / _partitioner.shard_count();
    auto shards_with_extra_span = _spans_per_iteration % _partitioner.shard_count();
    auto first_shard = _next_shard;
    _next_shard = (_next_shard + _spans_per_iteration) % _partitioner.shard_count();
    for (auto i : boost::irange(0u, std::min(_partitioner.shard_count(), _spans_per_iteration))) {
        auto shard = (first_shard + i) % _partitioner.shard_count();
        if (_last_ends[shard] && *_last_ends[shard] == maximum_token()) {
            continue;
        }
        range_bound<ring_position> this_shard_start = [&] {
            if (_last_ends[shard]) {
                return range_bound<ring_position>(ring_position::starting_at(*_last_ends[shard]));
            } else {
                return _range.start().value_or(range_bound<ring_position>(ring_position::starting_at(minimum_token())));
            }
        }();
        // token_for_next_span() may give us the wrong boundary on the first pass, so add an extra span:
        auto extra_span = !_last_ends[shard] && shard != _first_shard;
        auto spans = spans_per_shard + unsigned(i < shards_with_extra_span);
        auto boundary = _partitioner.token_for_next_shard(this_shard_start.value().token(), shard, spans + extra_span);
        auto proposed_range = partition_range(this_shard_start, range_bound<ring_position>(ring_position::starting_at(boundary), false));
        auto intersection = _range.intersection(proposed_range, cmp);
        if (!intersection) {
            continue;
        }
        spans_to_go -= spans;
        auto this_shard_result = ring_position_range_and_shard{std::move(*intersection), shard};
        _last_ends[shard] = boundary;
        ret.per_shard_ranges.push_back(std::move(this_shard_result));
    }
    if (ret.per_shard_ranges.empty()) {
        return stdx::nullopt;
    }
    _spans_per_iteration *= 2;
    return stdx::make_optional(std::move(ret));
}


ring_position_exponential_vector_sharder::ring_position_exponential_vector_sharder(const std::vector<nonwrapping_range<ring_position>>&& ranges) {
    std::move(ranges.begin(), ranges.end(), std::back_inserter(_ranges));
    if (!_ranges.empty()) {
        _current_sharder.emplace(_ranges.front());
        _ranges.pop_front();
        ++_element;
    }
}

stdx::optional<ring_position_exponential_vector_sharder_result>
ring_position_exponential_vector_sharder::next(const schema& s) {
    if (!_current_sharder) {
        return stdx::nullopt;
    }
    while (true) {  // yuch
        auto ret = _current_sharder->next(s);
        if (ret) {
            auto augmented = ring_position_exponential_vector_sharder_result{std::move(*ret), _element};
            return stdx::make_optional(std::move(augmented));
        }
        if (_ranges.empty()) {
            _current_sharder = stdx::nullopt;
            return stdx::nullopt;
        }
        _current_sharder.emplace(_ranges.front());
        _ranges.pop_front();
        ++_element;
    }
}


ring_position_range_vector_sharder::ring_position_range_vector_sharder(dht::partition_range_vector ranges)
        : _ranges(std::move(ranges))
        , _current_range(_ranges.begin()) {
    next_range();
}

stdx::optional<ring_position_range_and_shard_and_element>
ring_position_range_vector_sharder::next(const schema& s) {
    if (!_current_sharder) {
        return stdx::nullopt;
    }
    auto range_and_shard = _current_sharder->next(s);
    while (!range_and_shard && _current_range != _ranges.end()) {
        next_range();
        range_and_shard = _current_sharder->next(s);
    }
    auto ret = stdx::optional<ring_position_range_and_shard_and_element>();
    if (range_and_shard) {
        ret.emplace(std::move(*range_and_shard), _current_range - _ranges.begin() - 1);
    }
    return ret;
}


std::deque<partition_range>
split_range_to_single_shard(const i_partitioner& partitioner, const schema& s, const partition_range& pr, shard_id shard) {
    auto cmp = ring_position_comparator(s);
    auto ret = std::deque<partition_range>();
    auto next_shard = shard + 1 == partitioner.shard_count() ? 0 : shard + 1;
    auto start_token = pr.start() ? pr.start()->value().token() : minimum_token();
    auto start_shard = partitioner.shard_of(start_token);
    auto start_boundary = start_shard == shard ? pr.start() : range_bound<ring_position>(ring_position::starting_at(partitioner.token_for_next_shard(start_token, shard)));
    while (pr.overlaps(partition_range(start_boundary, {}), cmp)
            && !(start_boundary && start_boundary->value().token() == maximum_token())) {
        auto end_token = partitioner.token_for_next_shard(start_token, next_shard);
        auto candidate = partition_range(std::move(start_boundary), range_bound<ring_position>(ring_position::starting_at(end_token), false));
        auto intersection = pr.intersection(std::move(candidate), cmp);
        if (intersection) {
            ret.push_back(std::move(*intersection));
        }
        start_token = partitioner.token_for_next_shard(end_token, shard);
        start_boundary = range_bound<ring_position>(ring_position::starting_at(start_token));
    }
    return ret;
}

std::deque<partition_range>
split_range_to_single_shard(const schema& s, const partition_range& pr, shard_id shard) {
    return split_range_to_single_shard(global_partitioner(), s, pr, shard);
}


int ring_position::tri_compare(const schema& s, const ring_position& o) const {
    return ring_position_comparator(s)(*this, o);
}

int token_comparator::operator()(const token& t1, const token& t2) const {
    return tri_compare(t1, t2);
}

bool ring_position::equal(const schema& s, const ring_position& other) const {
    return tri_compare(s, other) == 0;
}

bool ring_position::less_compare(const schema& s, const ring_position& other) const {
    return tri_compare(s, other) < 0;
}

int ring_position_tri_compare(const schema& s, ring_position_view lh, ring_position_view rh) {
    auto token_cmp = tri_compare(*lh._token, *rh._token);
    if (token_cmp) {
        return token_cmp;
    }
    if (lh._key && rh._key) {
        auto c = lh._key->legacy_tri_compare(s, *rh._key);
        if (c) {
            return c;
        }
        return lh._weight - rh._weight;
    }
    if (!lh._key && !rh._key) {
        return lh._weight - rh._weight;
    } else if (!lh._key) {
        return lh._weight > 0 ? 1 : -1;
    } else {
        return rh._weight > 0 ? -1 : 1;
    }
}

int ring_position_comparator::operator()(ring_position_view lh, ring_position_view rh) const {
    return ring_position_tri_compare(s, lh, rh);
}

int ring_position_comparator::operator()(ring_position_view lh, sstables::decorated_key_view rh) const {
    auto token_cmp = tri_compare(*lh._token, rh.token());
    if (token_cmp) {
        return token_cmp;
    }
    if (lh._key) {
        auto rel = rh.key().tri_compare(s, *lh._key);
        if (rel) {
            return -rel;
        }
    }
    return lh._weight;
}

int ring_position_comparator::operator()(sstables::decorated_key_view a, ring_position_view b) const {
    return -(*this)(b, a);
}

dht::partition_range
to_partition_range(dht::token_range r) {
    using bound_opt = std::experimental::optional<dht::partition_range::bound>;
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

std::map<unsigned, dht::partition_range_vector>
split_range_to_shards(dht::partition_range pr, const schema& s) {
    std::map<unsigned, dht::partition_range_vector> ret;
    auto sharder = dht::ring_position_range_sharder(std::move(pr));
    auto rprs = sharder.next(s);
    while (rprs) {
        ret[rprs->shard].emplace_back(rprs->ring_range);
        rprs = sharder.next(s);
    }
    return ret;
}

std::map<unsigned, dht::partition_range_vector>
split_ranges_to_shards(const dht::token_range_vector& ranges, const schema& s) {
    std::map<unsigned, dht::partition_range_vector> ret;
    for (const auto& range : ranges) {
        auto pr = dht::to_partition_range(range);
        auto sharder = dht::ring_position_range_sharder(std::move(pr));
        auto rprs = sharder.next(s);
        while (rprs) {
            ret[rprs->shard].emplace_back(rprs->ring_range);
            rprs = sharder.next(s);
        }
    }
    return ret;
}

}

namespace std {

size_t
hash<dht::token>::hash_large_token(const managed_bytes& b) const {
    auto read_bytes = boost::irange<size_t>(0, b.size())
            | boost::adaptors::transformed([&b] (size_t idx) { return b[idx]; });
    std::array<uint64_t, 2> result;
    utils::murmur_hash::hash3_x64_128(read_bytes.begin(), b.size(), 0, result);
    return result[0];
}

}
