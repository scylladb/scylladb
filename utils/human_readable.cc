/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

/*
 * Copyright (C) 2020-present ScyllaDB
 */

#include "utils/human_readable.hh"

#include <array>
#include <ostream>

namespace utils {

static human_readable_value to_human_readable_value(uint64_t value, uint64_t step, uint64_t precision, const std::array<char, 5>& suffixes) {
    if (!value) {
        return {0, suffixes[0]};
    }

    uint64_t result = value;
    uint64_t remainder = 0;
    unsigned i = 0;
    // If there is no remainder we go below precision because we don't loose any.
    while (((!remainder && result >= step) || result >= precision)) {
        remainder = result % step;
        result /= step;
        if (i == suffixes.size()) {
            break;
        } else {
            ++i;
        }
    }
    return {uint16_t(remainder < (step / 2) ? result : result + 1), suffixes[i]};
}

human_readable_value to_hr_size(uint64_t size) {
    const std::array<char, 5> suffixes = {'B', 'K', 'M', 'G', 'T'};
    return to_human_readable_value(size, 1024, 8192, suffixes);
}

} // namespace utils

auto fmt::formatter<utils::human_readable_value>::format(const utils::human_readable_value& val, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    auto out = fmt::format_to(ctx.out(), "{}", val.value);
    if (val.suffix) {
        out = fmt::format_to(out, "{}", val.suffix);
    }
    return out;
}
