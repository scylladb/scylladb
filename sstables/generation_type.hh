/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <chrono>
#include <fmt/core.h>
#include <cstdint>
#include <compare>
#include <limits>
#include <iostream>
#include <type_traits>
#include <boost/range/adaptors.hpp>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include "types/types.hh"
#include "utils/UUID_gen.hh"

namespace sstables {

class generation_type {
public:
    using int_t = int64_t;

private:
    int_t _value;

public:
    generation_type() = delete;

    explicit constexpr generation_type(int_t value) noexcept: _value(value) {}
    constexpr int_t as_int() const noexcept {
	return _value;
    }
    // convert to data_value
    //
    // this function is used when performing queries to SSTABLES_REGISTRY in
    // the "system_keyspace", since its "generation" column cannot be a variant
    // of bigint and timeuuid, we need to use a single value to represent these
    // two types, and single value should allow us to tell the type of the
    // original value of generation identifier, so we can convert the value back
    // to the generation when necessary without losing its type information.
    // since the timeuuid always encodes the timestamp in its MSB, and the timestamp
    // should always be greater than zero, we use this fact to tell a regular
    // timeuuid from a timeuuid converted from a bigint -- we just use zero
    // for its timestamp of the latter.
    explicit operator data_value() const noexcept {
        // use zero as the timestamp to differentiate from the regular timeuuid,
        // and use the least_sig_bits to encode the value of generation identifier.
        return data_value(
            utils::UUID(
                utils::UUID_gen::create_time(std::chrono::milliseconds::zero()),
                _value));
    }
    static generation_type from_uuid(utils::UUID value) {
        // if the value of generation_type should be an int64_t, its timestamp
        // must be zero, and the least significant bits is used to encode the
        // value of the int64_t.
        assert(value.timestamp() == 0);
        return generation_type(value.get_least_significant_bits());
    }

    constexpr bool operator==(const generation_type& other) const noexcept { return _value == other._value; }
    constexpr std::strong_ordering operator<=>(const generation_type& other) const noexcept { return _value <=> other._value; }
};

constexpr generation_type generation_from_value(generation_type::int_t value) {
    return generation_type{value};
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
        return hash<sstables::generation_type::int_t>{}(generation.as_int());
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
        return fmt::format_to(ctx.out(), "{}", generation.as_int());
    }
};
