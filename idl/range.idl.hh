/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

template<typename T>
class interval_bound {
    T value();
    bool is_inclusive();
};

template<typename T>
class wrapping_interval {
    std::optional<interval_bound<T>> start();
    std::optional<interval_bound<T>> end();
    bool is_singular();
};

template<typename T>
class interval {
    std::optional<interval_bound<T>> start();
    std::optional<interval_bound<T>> end();
    bool is_singular();
};
