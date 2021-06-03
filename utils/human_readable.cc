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

/*
 * Copyright (C) 2020-present ScyllaDB
 */

#include "utils/human_readable.hh"

#include <array>
#include <ostream>

namespace utils {

std::ostream& operator<<(std::ostream& os, const human_readable_value& val) {
    os << val.value;
    if (val.suffix) {
        os << val.suffix;
    }
    return os;
}

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
