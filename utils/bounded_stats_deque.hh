/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
 */

#pragma once

namespace utils {

#include <deque>

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

    int size() {
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

    long sum() {
        return _sum;
    }

    double mean() {
        return size() > 0 ? ((double) sum()) / size() : 0;
    }

    const std::deque<long>& deque() const {
        return _deque;
    }
};

}
