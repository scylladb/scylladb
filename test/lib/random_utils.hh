/*
 * Copyright (C) 2018-present ScyllaDB
 */
/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <algorithm>
#include <random>
#include <ranges>
#include <iostream>

#include <seastar/testing/random.hh>
#include <seastar/testing/test_runner.hh>

#include "bytes.hh"
#include "utils/assert.hh"
#include "utils/preempt.hh"

namespace tests::random {

inline std::default_random_engine& gen() {
    return seastar::testing::local_random_engine;
}

/// Produces random integers from a set of steps.
///
/// Each step has a weight and a uniform distribution that determines the range
/// of values for that step. The probability of the generated number to be from
/// any given step is Ws/Wt, where Ws is the weight of the step and Wt is the
/// sum of the weight of all steps.
template <typename Integer>
class stepped_int_distribution {
public:
    struct step {
        double weight;
        std::pair<Integer, Integer> range;
    };

private:
    std::discrete_distribution<Integer> _step_index_dist;
    std::vector<std::uniform_int_distribution<Integer>> _step_ranges;

public:
    explicit stepped_int_distribution(std::initializer_list<step> steps) {
        std::vector<double> step_weights;
        for (auto& s : steps) {
            step_weights.push_back(s.weight);
            _step_ranges.emplace_back(s.range.first, s.range.second);
        }
        _step_index_dist = std::discrete_distribution<Integer>{step_weights.begin(), step_weights.end()};
    }
    template <typename RandomEngine>
    Integer operator()(RandomEngine& engine) {
        return _step_ranges[_step_index_dist(engine)](engine);
    }
};

template<typename T, typename RandomEngine>
T get_int(T min, T max, RandomEngine& engine) {
    std::uniform_int_distribution<T> dist(min, max);
    return dist(engine);
}

template<typename T, typename RandomEngine>
T get_int(T max, RandomEngine& engine) {
    return get_int(T{0}, max, engine);
}

template<typename T, typename RandomEngine>
T get_int(RandomEngine& engine) {
    return get_int(T{0}, std::numeric_limits<T>::max(), engine);
}

template<typename T>
T get_int() {
    return get_int(T{0}, std::numeric_limits<T>::max(), gen());
}

template<typename T>
T get_int(T max) {
    return get_int(T{0}, max, gen());
}

template<typename T>
T get_int(T min, T max) {
    return get_int(min, max, gen());
}

template <typename Real, typename RandomEngine>
Real get_real(Real min, Real max, RandomEngine& engine) {
    auto dist = std::uniform_real_distribution<Real>(min, max);
    return dist(engine);
}

template <typename Real, typename RandomEngine>
Real get_real(Real max, RandomEngine& engine) {
    return get_real<Real>(Real{0}, max, engine);
}

/// Returns true with probability p.
/// p = 1.0 means 100%.
inline
bool with_probability(double p) {
    return get_real<double>(1, gen()) < p;
}

template <typename Real, typename RandomEngine>
Real get_real(RandomEngine& engine) {
    return get_real<Real>(Real{0}, std::numeric_limits<Real>::max(), engine);
}

template <typename Real>
Real get_real(Real min, Real max) {
    return get_real<Real>(min, max, gen());
}

template <typename Real>
Real get_real(Real max) {
    return get_real<Real>(Real{0}, max, gen());
}

template <typename Real>
Real get_real() {
    return get_real<Real>(Real{0}, std::numeric_limits<Real>::max(), gen());
}

template <typename RandomEngine>
inline bool get_bool(RandomEngine& engine) {
    static std::bernoulli_distribution dist;
    return dist(engine);
}

inline bool get_bool() {
    return get_bool(gen());
}

inline bytes get_bytes(size_t n) {
    bytes b(bytes::initialized_later(), n);
    std::ranges::generate(b, [] { return get_int<bytes::value_type>(); });
    return b;
}

inline bytes get_bytes() {
    return get_bytes(get_int<unsigned>(128 * 1024));
}

template <typename RandomEngine>
inline sstring get_sstring(size_t n, RandomEngine& engine) {
    sstring str = uninitialized_string(n);
    std::ranges::generate(str, [&engine] { return get_int<sstring::value_type>('a', 'z', engine); });
    return str;
}

inline sstring get_sstring(size_t n) {
    return get_sstring(n, gen());
}

inline sstring get_sstring() {
    return get_sstring(get_int<unsigned>(1024));
}

// Picks a random subset of size `m` from the given vector.
template <typename T>
std::vector<T> random_subset(std::vector<T> v, unsigned m, std::mt19937& engine) {
    SCYLLA_ASSERT(m <= v.size());
    std::shuffle(v.begin(), v.end(), engine);
    return {v.begin(), v.begin() + m};
}

// Picks a random subset of size `m` from the set {0, ..., `n` - 1}.
template<typename T>
std::vector<T> random_subset(unsigned n, unsigned m, std::mt19937& engine) {
    SCYLLA_ASSERT(m <= n);

    std::vector<T> the_set(n);
    std::iota(the_set.begin(), the_set.end(), T{});
    return random_subset(std::move(the_set), m, engine);
}

inline
preemption_check random_preempt() {
    return [] () noexcept {
        return get_bool();
    };
}

}
