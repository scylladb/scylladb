/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "interval.hh"

// range.hh is deprecated and should be replaced with interval.hh


template <typename T>
using range_bound = interval_bound<T>;

template <typename T>
using nonwrapping_range = interval<T>;

template <typename T>
using wrapping_range = wrapping_interval<T>;

template <typename T>
using range = wrapping_interval<T>;

template <template<typename> typename T, typename U>
concept Range = Interval<T, U>;
