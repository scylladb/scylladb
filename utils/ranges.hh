/*
 * Copyright (C) 2020-present ScyllaDB
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

#include <ranges>

namespace ranges {

#ifndef __clang__
template <std::ranges::range Container, std::ranges::range Range>
#else
// 'Range' should be constrained to std::ranges::range, but due to
// problems between clang and <ranges>, it cannot be
template <std::ranges::range Container, typename Range>
#endif
Container to(const Range& range) {
    return Container(range.begin(), range.end());
}

}
