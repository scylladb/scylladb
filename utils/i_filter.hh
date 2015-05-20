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
 */

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */
#pragma once

#include "bytes.hh"
#include "bloom_calculations.hh"

namespace utils {

struct i_filter;
using filter_ptr = std::unique_ptr<i_filter>;

// FIXME: serialize() and serialized_size() not implemented. We should only be serializing to
// disk, not in the wire.
struct i_filter {
    virtual ~i_filter() {}

    virtual void add(const bytes_view& key) = 0;
    virtual bool is_present(const bytes_view& key) = 0;
    virtual void clear() = 0;
    virtual void close() = 0;

    /**
     * @return The smallest bloom_filter that can provide the given false
     *         positive probability rate for the given number of elements.
     *
     *         Asserts that the given probability can be satisfied using this
     *         filter.
     */
    static filter_ptr get_filter(long num_elements, double max_false_pos_prob);
    /**
     * @return A bloom_filter with the lowest practical false positive
     *         probability for the given number of elements.
     */
    static filter_ptr get_filter(long num_elements, int target_buckets_per_elem);
};
}
