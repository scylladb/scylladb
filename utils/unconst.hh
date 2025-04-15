/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <ranges>

template <typename Container>
std::ranges::range auto
unconst(Container& c, std::ranges::range auto&& r) {
    return std::ranges::subrange(
            c.erase(r.begin(), r.begin()),
            c.erase(r.end(), r.end())
    );
}

template <typename Container>
typename Container::iterator
unconst(Container& c, typename Container::const_iterator i) {
    return c.erase(i, i);
}
