/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <bit>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "utils/enum_option.hh"

namespace netw {

// An enum wrapper, describing supported RPC compression algorithms.
// Always contains a valid value -- the constructors won't allow
// an invalid/unknown enum variant to be constructed.
struct compression_algorithm {
    using underlying = uint8_t;
    enum class type : underlying {
        RAW,
        LZ4,
        ZSTD,
        COUNT,
    } _value;

    // Construct from an integer.
    // Used to deserialize the algorithm from the first byte of the frame.
    constexpr compression_algorithm(underlying x) {
        if (x < std::to_underlying(type::RAW) || x >= std::to_underlying(type::COUNT)) {
            throw std::runtime_error(std::string("Invalid value ") + std::to_string(unsigned(x)) + " for enum compression_algorithm");
        }
        _value = static_cast<type>(x);
    }

    // Construct from `type`. Makes sure that `type` has a valid value.
    constexpr compression_algorithm(type x) : compression_algorithm(std::to_underlying(x)) {}

    // These names are used in multiple places:
    // RPC negotiation, in metric labels, and config.
    static constexpr std::string_view names[] = {
        "raw",
        "lz4",
        "zstd",
    };
    static_assert(std::size(names) == static_cast<int>(compression_algorithm::type::COUNT));

    // Implements enum_option.
    static auto map() {
        std::unordered_map<std::string, type> ret;
        for (size_t i = 0; i < std::size(names); ++i) {
            ret.insert(std::make_pair(std::string(names[i]), compression_algorithm(i).get()));
        }
        return ret;
    }

    constexpr std::string_view name() const noexcept { return names[idx()]; }
    constexpr underlying idx() const noexcept { return std::to_underlying(_value); }
    constexpr type get() const noexcept { return _value; }
    constexpr static size_t count() { return static_cast<size_t>(type::COUNT); }
    bool operator<=>(const compression_algorithm&) const = default;
};

// Represents a set of compression algorithms.
// Backed by a bitset.
// Used for convenience during algorithm negotiations.
class compression_algorithm_set {
    uint8_t _bitset;
    static_assert(std::numeric_limits<decltype(_bitset)>::digits > compression_algorithm::count());
    constexpr compression_algorithm_set(uint8_t v) noexcept : _bitset(v) {}
public:
    // Returns a set containing the given algorithm and all algorithms weaker (smaller in the enum order)
    // than it.
    constexpr static compression_algorithm_set this_or_lighter(compression_algorithm algo) noexcept {
        auto x = 1 << algo.idx();
        return {uint8_t(x + (x - 1))};
    }

    // Returns the strongest (greatest in the enum order) algorithm in the set.
    constexpr compression_algorithm heaviest() const {
        return {compression_algorithm::underlying(std::bit_width(_bitset) - 1)};
    }

    // The usual set operations.
    constexpr static compression_algorithm_set singleton(compression_algorithm algo) noexcept {
        return {uint8_t(1 << algo.idx())};
    }
    constexpr compression_algorithm_set intersection(compression_algorithm_set o) const noexcept {
        return {uint8_t(_bitset & o._bitset)};
    }
    constexpr compression_algorithm_set difference(compression_algorithm_set o) const noexcept {
        return {uint8_t(_bitset &~ o._bitset)};
    }
    constexpr compression_algorithm_set sum(compression_algorithm_set o) const noexcept {
        return {uint8_t(_bitset | o._bitset)};
    }
    constexpr bool contains(compression_algorithm algo) const noexcept {
        return _bitset & (1 << algo.idx());
    }
    constexpr bool operator==(const compression_algorithm_set&) const = default;

    // Returns the contained bitset. Used for serialization.
    constexpr uint8_t value() const noexcept {
        return _bitset;
    }

    // Reconstructs the set from the output of `value()`. Used for deserialization.
    constexpr static compression_algorithm_set from_value(uint8_t bitset) {
        compression_algorithm_set x = bitset;
        x.heaviest(); // Validation: throws on illegal/unknown bits.
        return x;
    }
};

using algo_config = std::vector<enum_option<compression_algorithm>>;

struct dict_training_when {
    enum class type {
        NEVER,
        WHEN_LEADER,
        ALWAYS,
        COUNT,
    };

    static constexpr std::string_view names[] = {
        "never",
        "when_leader",
        "always",
    };
    static_assert(std::size(names) == static_cast<size_t>(type::COUNT));

    // Implements enum_option.
    static std::unordered_map<std::string, type> map() {
        std::unordered_map<std::string, type> ret;
        for (size_t i = 0; i < std::size(names); ++i) {
            ret.insert({std::string(names[i]), type(i)});
        }
        return ret;
    }
};

} // namespace netw
