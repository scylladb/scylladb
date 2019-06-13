/*
 * Copyright (C) 2018 ScyllaDB
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

#pragma once

#include <algorithm>
#include <random>

#include <boost/range/algorithm/generate.hpp>
#include <iostream>

#include <seastar/testing/test_runner.hh>

namespace tests::random {

inline std::default_random_engine& gen() {
    return seastar::testing::local_random_engine;
}

template<typename T>
T get_int() {
    std::uniform_int_distribution<T> dist;
    return dist(gen());
}

template<typename T>
T get_int(T max) {
    std::uniform_int_distribution<T> dist(0, max);
    return dist(gen());
}

template<typename T>
T get_int(T min, T max) {
    std::uniform_int_distribution<T> dist(min, max);
    return dist(gen());
}

inline bool get_bool() {
    static std::bernoulli_distribution dist;
    return dist(gen());
}

inline bytes get_bytes(size_t n) {
    bytes b(bytes::initialized_later(), n);
    boost::generate(b, [] { return get_int<bytes::value_type>(); });
    return b;
}

inline bytes get_bytes() {
    return get_bytes(get_int<unsigned>(128 * 1024));
}

inline sstring get_sstring(size_t n) {
    sstring str(sstring::initialized_later(), n);
    boost::generate(str, [] { return get_int<sstring::value_type>('a', 'z'); });
    return str;
}

inline sstring get_sstring() {
    return get_sstring(get_int<unsigned>(1024));
}

}
