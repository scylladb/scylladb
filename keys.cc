/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <iostream>

#include "keys.hh"
#include "dht/i_partitioner.hh"
#include "clustering_bounds_comparator.hh"
#include <boost/algorithm/string.hpp>

logging::logger klog("keys");

const legacy_compound_view<partition_key_view::c_type>
partition_key_view::legacy_form(const schema& s) const {
    return { *get_compound_type(s), _bytes };
}

std::strong_ordering
partition_key_view::legacy_tri_compare(const schema& s, partition_key_view o) const {
    auto cmp = legacy_compound_view<c_type>::tri_comparator(*get_compound_type(s));
    return cmp(this->representation(), o.representation());
}

std::strong_ordering
partition_key_view::ring_order_tri_compare(const schema& s, partition_key_view k2) const {
    auto t1 = dht::get_token(s, *this);
    auto t2 = dht::get_token(s, k2);
    if (t1 != t2) {
        return t1 < t2 ? std::strong_ordering::less : std::strong_ordering::greater;
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

auto fmt::formatter<bound_kind>::format(bound_kind k, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    std::string_view name;
    switch (k) {
    case bound_kind::excl_end:
        name = "excl end";
        break;
    case bound_kind::incl_start:
        name = "incl start";
        break;
    case bound_kind::incl_end:
        name = "incl end";
        break;
    case bound_kind::excl_start:
        name = "excl start";
        break;
    }
    return fmt::format_to(ctx.out(), "{}", name);
}

bound_kind invert_kind(bound_kind k) {
    switch (k) {
    case bound_kind::excl_start: return bound_kind::incl_end;
    case bound_kind::incl_start: return bound_kind::excl_end;
    case bound_kind::excl_end:   return bound_kind::incl_start;
    case bound_kind::incl_end:   return bound_kind::excl_start;
    }
    abort();
}

bound_kind reverse_kind(bound_kind k) {
    switch (k) {
    case bound_kind::excl_start: return bound_kind::excl_end;
    case bound_kind::incl_start: return bound_kind::incl_end;
    case bound_kind::excl_end:   return bound_kind::excl_start;
    case bound_kind::incl_end:   return bound_kind::incl_start;
    }
    on_internal_error(klog, format("reverse_kind(): invalid value for `bound_kind`: {}", static_cast<std::underlying_type_t<bound_kind>>(k)));
}

int32_t weight(bound_kind k) {
    switch (k) {
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

std::ostream&
operator<<(std::ostream& os, const exploded_clustering_prefix& ecp) {
    // Can't pass to_hex() to transformed(), since it is overloaded, so wrap:
    auto enhex = [] (auto&& x) { return fmt_hex(x); };
    fmt::print(os, "prefix{{{}}}", fmt::join(ecp._v | boost::adaptors::transformed(enhex), ":"));
    return os;
}
