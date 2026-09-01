/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <concepts>
#include <ranges>

namespace utils {

// Models a range whose elements are (or convert to) T, without requiring a materialized container.
template <typename R, typename T>
concept range_of = std::ranges::range<R> && std::same_as<std::ranges::range_value_t<R>, T>;

}
