/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/UUID.hh"
#include <boost/algorithm/string/classification.hpp>
#include <fmt/core.h>
#include <cstdint>
#include <compare>
#include <limits>
#include <iostream>
#include <memory>
#include <boost/algorithm/string/split.hpp>
#include <seastar/core/sstring.hh>
#include <stdexcept>
#include <string>

namespace sstables {

class generation_type {
public:
    using value_type = std::variant<int64_t, utils::UUID>;
private:
    value_type _value;
public:
    generation_type() noexcept
        : _value(0)
    {}

    explicit constexpr generation_type(int64_t value) noexcept: _value(value) {}
    explicit constexpr generation_type(utils::UUID value) noexcept: _value(value) {}
    explicit constexpr generation_type(value_type value) noexcept: _value(value) {}
    constexpr value_type value() const noexcept { return _value; }
    constexpr bool is_uuid_based() const noexcept {
        return _value.index() == 1;
    }
    constexpr operator int64_t() const noexcept {
        assert(_value.index() == 0);
        return std::get<int64_t>(_value);
    }
    constexpr operator utils::UUID() const noexcept {
        assert(_value.index() == 1);
        return std::get<utils::UUID>(_value);
    }
    constexpr shard_id shard() const noexcept {
        if (is_uuid_based()) {
            return utils::UUID(*this).shard(smp::count);
        } else {
            return int64_t(*this) / smp::count;
        }
    }
    constexpr bool operator==(const generation_type& other) const noexcept { return _value == other._value; }
    constexpr std::strong_ordering operator<=>(const generation_type& other) const noexcept { return _value <=> other._value; }
    friend std::istream& operator>>(std::istream& in, generation_type& generation) {
        sstring token;
        in >> token;
        try {
            if (auto dash = token.find('-'); dash != token.npos) {
                generation = generation_type{utils::UUID{token}};
            } else {
                generation = generation_type{std::stol(token)};
            }
        }  catch (const std::invalid_argument&) {
            in.setstate(std::ios_base::failbit);
            throw;
        }
        return in;
    }
};

constexpr generation_type generation_from_value(generation_type::value_type value) {
    return generation_type{value};
}
constexpr generation_type::value_type generation_value(generation_type generation) {
    return generation.value();
}

class generation_generator {
public:
    virtual generation_type generate() = 0;
    virtual ~generation_generator() = default;
};

class int_generation_generator final : public generation_generator {
    std::atomic<int64_t> _last_generation;
public:
    int_generation_generator(int64_t last_generation)
        : _last_generation{last_generation} {}
    generation_type generate() final {
        return generation_type{_last_generation.fetch_add(smp::count, std::memory_order_relaxed)};
    }
};

class uuid_generation_generator : public generation_generator {
public:
    generation_type generate() final {
        return generation_type{utils::UUID_gen::get_time_UUID()};
    }
};

} //namespace sstables

namespace std {
template <>
struct hash<sstables::generation_type> {
    size_t operator()(const sstables::generation_type& generation) const noexcept {
        return hash<sstables::generation_type::value_type>{}(generation.value());
    }
};

// for min_max_tracker
template <>
struct numeric_limits<sstables::generation_type> : public numeric_limits<int64_t> {
    static constexpr sstables::generation_type min() noexcept {
        return sstables::generation_type{numeric_limits<int64_t>::min()};
    }
    static constexpr sstables::generation_type max() noexcept {
        return sstables::generation_type{numeric_limits<int64_t>::max()};
    }
};
} //namespace std

template <>
struct fmt::formatter<sstables::generation_type> : fmt::formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const sstables::generation_type& generation, FormatContext& ctx) const {
        return std::visit([&ctx] (auto&& v) {
            return fmt::format_to(ctx.out(), "{}", v);
        }, generation.value());
    }
};
