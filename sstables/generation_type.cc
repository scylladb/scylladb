/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "sstables/generation_type.hh"
#include "api/api.hh"
#include <cctype>
#include <fmt/core.h>
#include <cstdint>
#include <iterator>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
sstring to_base36(uint64_t i) {
    std::vector<char> buf(13, '\0');
    auto s = buf.begin();
    do {
        *s = "0123456789abcdefghijklmnopqrstuvwxyz"[i % 36];
        i /= 36;
        s++;
    } while (i);

    return sstring(std::make_reverse_iterator(s), buf.rend());
}

uint64_t from_base36(const sstring& buf) {
    uint64_t total = 0;
    for (auto i = buf.size() - 1; i >= 0; --i) {
        total *= 36;
        const char c = std::tolower(buf[i]);
        total += (c >= 'a' && c <= 'z') ? c - 'a' + 10 : c - '0';
    }
    return total;
}
} // anon. namespace

namespace seastar {
sstring to_sstring(const sstables::generation_type& gen) {
    return std::visit(make_visitor(
        [] (int64_t x) { return fmt::format("{}", x); },
        [] (utils::UUID u) { 
            auto ts = u.timestamp();
            const auto nano_part = ts % 10'000'000;
            ts /= 10'000'000;
            const auto seconds = ts % 86'400;
            ts /= 86'400;
            return fmt::format("{:0>4s}_{:0>4s}_{:0>5s}{:0>13s}",
                to_base36(ts),
                to_base36(seconds),
                to_base36(nano_part),
                to_base36(u.get_least_significant_bits()));
        }
    ), gen.value);
}
} // namespace seastar

namespace sstables {
int calc_shard_num(const generation_type& gen) {
    return std::visit(make_visitor(
        [] (int64_t x) { return int(x % smp::count); },
        [] (utils::UUID u) -> int { throw std::runtime_error("unimplemented"); }
    ), gen.value);
}

generation_type generation_from_string(const sstring& str) {
    if (str.size() == 28) {
        // UUID-based generation is 28 chars long
        const auto days = from_base36(str.substr(0, 4));
        const auto seconds = from_base36(str.substr(5, 4));
        const auto nano_part = from_base36(str.substr(10, 5));

        const auto lsb = from_base36(str.substr(15, 13));
        const auto msb = ((days * 86'400) + seconds) * 10'000'000 + nano_part;

        return generation_type{utils::UUID(msb, lsb)};
    } else {
        // int-based (legacy) generation
        return generation_type{std::stoll(str)};
    }
}

generation_type increment(const generation_type& gen) {
    throw std::runtime_error("unimplemented");
}
} // namespace sstables
