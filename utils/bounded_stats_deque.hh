/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <deque>

namespace utils {

/**
 * bounded threadsafe deque
 */
class bounded_stats_deque {
private:
    std::deque<long> _deque;
    long _sum = 0;
    int _max_size;
public:
    bounded_stats_deque(int size)
        : _max_size(size) {
    }

    int size() const {
        return _deque.size();
    }

    void add(long i) {
        if (size() >= _max_size) {
            auto removed = _deque.front();
            _deque.pop_front();
            _sum -= removed;
        }
        _deque.push_back(i);
        _sum += i;
    }

    long sum() const {
        return _sum;
    }

    double mean() const {
        return size() > 0 ? ((double) sum()) / size() : 0;
    }

    const std::deque<long>& deque() const {
        return _deque;
    }
};

}
