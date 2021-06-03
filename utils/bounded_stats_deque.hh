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
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
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
