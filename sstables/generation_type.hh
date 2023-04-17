/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <fmt/core.h>
#include <cstdint>
#include <compare>
#include <limits>
#include <iostream>
#include <type_traits>
#include <boost/range/adaptors.hpp>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>

namespace sstables {

class generation_type {
public:
    using int_t = int64_t;

private:
    int_t _value;

public:
    generation_type() = delete;

    explicit constexpr generation_type(int_t value) noexcept: _value(value) {}
    constexpr int_t value() const noexcept { return _value; }

    constexpr bool operator==(const generation_type& other) const noexcept { return _value == other._value; }
    constexpr std::strong_ordering operator<=>(const generation_type& other) const noexcept { return _value <=> other._value; }
};

constexpr generation_type generation_from_value(generation_type::int_t value) {
    return generation_type{value};
}
constexpr generation_type::int_t generation_value(generation_type generation) {
    return generation.value();
}

template <std::ranges::range Range, typename Target = std::vector<sstables::generation_type>>
Target generations_from_values(const Range& values) {
    return boost::copy_range<Target>(values | boost::adaptors::transformed([] (auto value) {
        return generation_type(value);
    }));
}

template <typename Target = std::vector<sstables::generation_type>>
Target generations_from_values(std::initializer_list<generation_type::int_t> values) {
    return boost::copy_range<Target>(values | boost::adaptors::transformed([] (auto value) {
        return generation_type(value);
    }));
}

class sstable_generation_generator {
    // We still want to do our best to keep the generation numbers shard-friendly.
    // Each destination shard will manage its own generation counter.
    //
    // operator() is called by multiple shards in parallel when performing reshard,
    // so we have to use atomic<> here.
    using int_t = sstables::generation_type::int_t;
    int_t _last_generation;
    static int_t base_generation(int_t highest_generation) {
        // get the base generation so we can increment it by smp::count without
        // conflicting with other shards
        return highest_generation - highest_generation % seastar::smp::count + seastar::this_shard_id();
    }
public:
    explicit sstable_generation_generator(int64_t last_generation)
        : _last_generation(base_generation(last_generation)) {}
    void update_known_generation(int64_t generation) {
        if (generation > _last_generation) {
            _last_generation = generation;
        }
    }
    sstables::generation_type operator()() {
        // each shard has its own "namespace" so we increment the generation id
        // by smp::count to avoid name confliction of sstables
        _last_generation += seastar::smp::count;
        return sstables::generation_from_value(_last_generation);
    }
};

} //namespace sstables

namespace std {
template <>
struct hash<sstables::generation_type> {
    size_t operator()(const sstables::generation_type& generation) const noexcept {
        return hash<sstables::generation_type::int_t>{}(generation.value());
    }
};

// for min_max_tracker
template <>
struct numeric_limits<sstables::generation_type> : public numeric_limits<sstables::generation_type::int_t> {
    static constexpr sstables::generation_type min() noexcept {
        return sstables::generation_type{numeric_limits<sstables::generation_type::int_t>::min()};
    }
    static constexpr sstables::generation_type max() noexcept {
        return sstables::generation_type{numeric_limits<sstables::generation_type::int_t>::max()};
    }
};
} //namespace std

template <>
struct fmt::formatter<sstables::generation_type> : fmt::formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const sstables::generation_type& generation, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", generation.value());
    }
};
