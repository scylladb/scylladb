/*
 * Copyright 2015 ScyllaDB
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

#include <seastar/core/bitops.hh>

#include "utils/dynamic_bitset.hh"

namespace utils {

template<typename Transform>
size_t dynamic_bitset::do_find_first(Transform&& transform) const
{
    size_t pos = 0;
    for (auto v : _bits) {
        v = transform(v);
        if (v) {
            pos += count_trailing_zeros(v);
            break;
        }
        pos += bits_per_int;
    }
    return pos < _bits_count ? pos : npos;
}

template<typename Transform>
size_t dynamic_bitset::do_find_next(size_t n, Transform&& transform) const
{
    size_t pos = align_down(++n, bits_per_int);
    auto it = _bits.begin() + n / bits_per_int;
    auto v = transform(*it);
    v &= ~mask_lower_bits(n % bits_per_int);
    if (v) {
        pos += count_trailing_zeros(v);
        return pos < _bits_count ? pos : npos;
    }
    pos += bits_per_int;
    for (++it; it != _bits.end(); ++it) {
        auto v = transform(*it);
        if (v) {
            pos += count_trailing_zeros(v);
            return pos < _bits_count ? pos : npos;
        }
        pos += bits_per_int;
    }
    return npos;
}

template<typename Transform>
size_t dynamic_bitset::do_find_last(Transform&& transform) const
{
    auto it = _bits.rbegin();
    auto v = transform(*it);
    auto d = align_up(_bits_count, bits_per_int) - _bits_count;
    v &= ~mask_higher_bits(d);
    auto pos = (_bits.size() - 1) * bits_per_int;
    if (v) {
        return pos + bits_per_int - count_leading_zeros(v) - 1;
    }
    for (++it; it != _bits.rend(); ++it) {
        pos -= bits_per_int;
        v = transform(*it);
        if (v) {
            return pos + bits_per_int - count_leading_zeros(v) - 1;
        }
    }
    return npos;
}

template<typename Transform>
size_t dynamic_bitset::do_find_previous(size_t n, Transform&& transform) const
{
    if (!n) {
        return npos;
    }
    auto it = _bits.begin() + n / bits_per_int;
    auto v = transform(*it);
    v &= mask_lower_bits(n % bits_per_int);
    auto pos = (std::distance(_bits.begin(), it)) * bits_per_int;
    if (v) {
        return pos + bits_per_int - count_leading_zeros(v) - 1;
    }
    while (it != _bits.begin()) {
        --it;
        pos -= bits_per_int;
        v = transform(*it);
        if (v) {
            return pos + bits_per_int - count_leading_zeros(v) - 1;
        }
    }
    return npos;
}

size_t dynamic_bitset::find_first_set() const
{
    return do_find_first([] (auto x) { return x; });
}

size_t dynamic_bitset::find_first_clear() const
{
    return do_find_first([] (auto x) { return ~x; });
}

size_t dynamic_bitset::find_next_set(size_t n) const
{
    return do_find_next(n, [] (auto x) { return x; });
}

size_t dynamic_bitset::find_next_clear(size_t n) const
{
    return do_find_next(n, [] (auto x) { return ~x; });
}

size_t dynamic_bitset::find_last_set() const
{
    return do_find_last([] (auto x) { return x; });
}

size_t dynamic_bitset::find_previous_set(size_t n) const
{
    return do_find_previous(n, [] (auto x) { return x; });
}

size_t dynamic_bitset::find_last_clear() const
{
    return do_find_last([] (auto x) { return ~x; });
}

size_t dynamic_bitset::find_previous_clear(size_t n) const
{
    return do_find_previous(n, [] (auto x) { return ~x; });
}

void dynamic_bitset::resize(size_t n, bool set)
{
    if (_bits_count && _bits_count < n) {
        auto d = align_up(_bits_count, bits_per_int) - _bits_count;
        if (set) {
            _bits.back() |= mask_higher_bits(d);
        } else {
            _bits.back() &= ~mask_higher_bits(d);
        }
    }
    _bits.resize(align_up(n, bits_per_int) / bits_per_int, set ? all_set : 0);
    _bits_count = n;
}

}