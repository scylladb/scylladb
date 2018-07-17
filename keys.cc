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

#include <iostream>

#include "keys.hh"
#include "dht/i_partitioner.hh"
#include "clustering_bounds_comparator.hh"
#include <boost/algorithm/string.hpp>
#include <boost/any.hpp>

std::ostream& operator<<(std::ostream& out, const partition_key& pk) {
    return out << "pk{" << to_hex(pk) << "}";
}

std::ostream& operator<<(std::ostream& out, const partition_key::with_schema_wrapper& pk) {
    auto type_iterator = pk.pkey.get_compound_type(pk.s)->types().begin();
    bool first = true;
    for (auto&& e : pk.pkey.components(pk.s)) {
        if (!first) {
            out << ":";
        }
        first = false;
        out << (*type_iterator)->to_string(to_bytes(e));
        ++type_iterator;
    }
    return out;
}

std::ostream& operator<<(std::ostream& out, const partition_key_view& pk) {
    return out << "pk{" << to_hex(pk.representation()) << "}";
}

std::ostream& operator<<(std::ostream& out, const clustering_key_prefix& ckp) {
    return out << "ckp{" << to_hex(ckp) << "}";
}

const legacy_compound_view<partition_key_view::c_type>
partition_key_view::legacy_form(const schema& s) const {
    return { *get_compound_type(s), _bytes };
}

int
partition_key_view::legacy_tri_compare(const schema& s, partition_key_view o) const {
    auto cmp = legacy_compound_view<c_type>::tri_comparator(*get_compound_type(s));
    return cmp(this->representation(), o.representation());
}

int
partition_key_view::ring_order_tri_compare(const schema& s, partition_key_view k2) const {
    auto t1 = dht::global_partitioner().get_token(s, *this);
    auto t2 = dht::global_partitioner().get_token(s, k2);
    if (t1 != t2) {
        return t1 < t2 ? -1 : 1;
    }
    return legacy_tri_compare(s, k2);
}

partition_key partition_key::from_nodetool_style_string(const schema_ptr s, const sstring& key) {
    std::vector<sstring> vec;
    boost::split(vec, key, boost::is_any_of(":"));

    auto it = std::begin(vec);
    if (vec.size() != s->partition_key_type()->types().size()) {
        throw std::invalid_argument("partition key '" + key + "' has mismatch number of components");
    }
    std::vector<bytes> r;
    r.reserve(vec.size());
    for (auto t : s->partition_key_type()->types()) {
        r.emplace_back(t->from_string(*it++));
    }
    return partition_key::from_range(std::move(r));
}

std::ostream& operator<<(std::ostream& out, const bound_kind k) {
    switch(k) {
    case bound_kind::excl_end:
        return out << "excl end";
    case bound_kind::incl_start:
        return out << "incl start";
    case bound_kind::incl_end:
        return out << "incl end";
    case bound_kind::excl_start:
        return out << "excl start";
    }
    abort();
}

bound_kind invert_kind(bound_kind k) {
    switch(k) {
    case bound_kind::excl_start: return bound_kind::incl_end;
    case bound_kind::incl_start: return bound_kind::excl_end;
    case bound_kind::excl_end:   return bound_kind::incl_start;
    case bound_kind::incl_end:   return bound_kind::excl_start;
    }
    abort();
}

int32_t weight(bound_kind k) {
    switch(k) {
    case bound_kind::excl_end:
        return -2;
    case bound_kind::incl_start:
        return -1;
    case bound_kind::incl_end:
        return 1;
    case bound_kind::excl_start:
        return 2;
    }
    abort();
}

const thread_local clustering_key_prefix bound_view::_empty_prefix = clustering_key::make_empty();
