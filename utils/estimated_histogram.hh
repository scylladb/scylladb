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
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include <cmath>
#include <algorithm>
#include <vector>
#include <chrono>
#include "core/metrics_types.hh"
#include <seastar/core/print.hh>

namespace utils {

struct estimated_histogram {
    using clock = std::chrono::steady_clock;
    using duration = clock::duration;
    /**
     * The series of values to which the counts in `buckets` correspond:
     * 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 17, 20, etc.
     * Thus, a `buckets` of [0, 0, 1, 10] would mean we had seen one value of 3 and 10 values of 4.
     *
     * The series starts at 1 and grows by 1.2 each time (rounding and removing duplicates). It goes from 1
     * to around 36M by default (creating 90+1 buckets), which will give us timing resolution from microseconds to
     * 36 seconds, with less precision as the numbers get larger.
     *
     * When using the histogram for latency, the values are in microseconds
     *
     * Each bucket represents values from (previous bucket offset, current offset].
     */
    std::vector<int64_t> bucket_offsets;

    // buckets is one element longer than bucketOffsets -- the last element is values greater than the last offset
    std::vector<int64_t> buckets;

    int64_t _count = 0;
    int64_t _sample_sum = 0;

    estimated_histogram(int bucket_count = 90) {

        new_offsets(bucket_count);
        buckets.resize(bucket_offsets.size() + 1, 0);
    }

    seastar::metrics::histogram get_histogram(size_t lower_bucket = 1, size_t max_buckets = 16) const {
        seastar::metrics::histogram res;
        res.buckets.resize(max_buckets);
        int64_t last_bound = lower_bucket;
        uint64_t cummulative_count = 0;
        size_t pos = 0;

        res.sample_count = _count;
        res.sample_sum = _sample_sum;
        for (size_t i = 0; i < res.buckets.size(); i++) {
            auto& v = res.buckets[i];
            v.upper_bound = last_bound;

            while (bucket_offsets[pos] <= last_bound) {
                cummulative_count += buckets[pos];
                pos++;
            }

            v.count = cummulative_count;

            last_bound <<= 1;
        }
        return res;
    }

    seastar::metrics::histogram get_histogram(duration minmal_latency, size_t max_buckets = 16) const {
        return get_histogram(std::chrono::duration_cast<std::chrono::microseconds>(minmal_latency).count(), max_buckets);
    }


    // FIXME: convert Java code below.
#if 0
    public EstimatedHistogram(long[] offsets, long[] bucketData)
    {
        assert bucketData.length == offsets.length +1;
        bucketOffsets = offsets;
        buckets = new AtomicLongArray(bucketData);
    }
#endif
private:
    void new_offsets(int size) {
        bucket_offsets.resize(size);
        if (size == 0) {
            return;
        }
        int64_t last = 1;
        bucket_offsets[0] = last;
        for (int i = 1; i < size; i++) {
            int64_t next = round(last * 1.2);
            if (next == last) {
                next++;
            }
            bucket_offsets[i] = next;
            last = next;
        }
    }
public:
    /**
     * @return the histogram values corresponding to each bucket index
     */
    const std::vector<int64_t>& get_bucket_offsets() const {
        return bucket_offsets;
    }

    /**
     * @return the histogram buckets
     */
    const std::vector<int64_t>& get_buckets() const {
        return buckets;
    }

    void clear() {
        std::fill(buckets.begin(), buckets.end(), 0);
        _count = 0;
        _sample_sum = 0;
    }
    /**
     * Increments the count of the bucket closest to n, rounding UP.
     * @param n
     */
    void add(int64_t n) {
        auto pos = bucket_offsets.size();
        auto low = std::lower_bound(bucket_offsets.begin(), bucket_offsets.end(), n);
        if (low != bucket_offsets.end()) {
            pos = std::distance(bucket_offsets.begin(), low);
        }
        buckets.at(pos)++;
        _count++;
        _sample_sum += n;
    }

    /**
     * Increments the count of the bucket closest to n, rounding UP.
     * when using sampling, the number of items in the bucket will
     * be increase so that the overall number of items will be equal
     * to the new count
     * @param n
     */
    void add_nano(int64_t n, int64_t new_count) {
        n /= 1000;
        if (new_count <= _count) {
            return;
        }
        auto pos = bucket_offsets.size();
        auto low = std::lower_bound(bucket_offsets.begin(), bucket_offsets.end(), n);
        if (low != bucket_offsets.end()) {
            pos = std::distance(bucket_offsets.begin(), low);
        }
        buckets.at(pos)+= new_count - _count;
        _sample_sum += n * (new_count - _count);
        _count = new_count;
    }

    void add(duration latency, int64_t new_count) {
        add_nano(std::chrono::duration_cast<std::chrono::nanoseconds>(latency).count(), new_count);
    }

    /**
     * @return the smallest value that could have been added to this histogram
     */
    int64_t min() const {
        size_t i = 0;
        for (auto b : buckets) {
            if (b > 0) {
                return i == 0 ? 0 : 1 + bucket_offsets[i - 1];
            }
            i++;
        }
        return 0;
    }

    /**
     * @return the largest value that could have been added to this histogram.  If the histogram
     * overflowed, returns INT64_MAX.
     */
    int64_t max() const {
        int lastBucket = buckets.size() - 1;
        if (buckets[lastBucket] > 0) {
            return INT64_MAX;
        }
        for (int i = lastBucket - 1; i >= 0; i--) {
            if (buckets[i] > 0) {
                return bucket_offsets[i];
            }
        }
        return 0;
    }

    /**
     * merge a histogram to the current one.
     */
    estimated_histogram& merge(const estimated_histogram& b) {
        if (bucket_offsets.size() < b.bucket_offsets.size()) {
            new_offsets(b.bucket_offsets.size());
            buckets.resize(b.bucket_offsets.size() + 1, 0);
        }
        size_t i = 0;
        for (auto p: b.buckets) {
            buckets[i++] += p;
        }
        _count += b._count;
        _sample_sum += b._sample_sum;
        return *this;
    }

    friend estimated_histogram merge(estimated_histogram a, const estimated_histogram& b);

    // FIXME: convert Java code below.
#if 0
    /**
     * @return the count in the given bucket
     */
    long get(int bucket)
    {
        return buckets.get(bucket);
    }

    /**
     * @param reset zero out buckets afterwards if true
     * @return a long[] containing the current histogram buckets
     */
    public long[] getBuckets(boolean reset)
    {
        final int len = buckets.length();
        long[] rv = new long[len];

        if (reset)
            for (int i = 0; i < len; i++)
                rv[i] = buckets.getAndSet(i, 0L);
        else
            for (int i = 0; i < len; i++)
                rv[i] = buckets.get(i);

        return rv;
    }

    /**
     * @return the smallest value that could have been added to this histogram
     */
    public long min()
    {
        for (int i = 0; i < buckets.length(); i++)
        {
            if (buckets.get(i) > 0)
                return i == 0 ? 0 : 1 + bucketOffsets[i - 1];
        }
        return 0;
    }

    /**
     * @return the largest value that could have been added to this histogram.  If the histogram
     * overflowed, returns Long.MAX_VALUE.
     */
    public long max()
    {
        int lastBucket = buckets.length() - 1;
        if (buckets.get(lastBucket) > 0)
            return Long.MAX_VALUE;

        for (int i = lastBucket - 1; i >= 0; i--)
        {
            if (buckets.get(i) > 0)
                return bucketOffsets[i];
        }
        return 0;
    }
#endif

    /**
     * @param percentile
     * @return estimated value at given percentile
     */
    int64_t percentile(double perc) const {
        assert(perc >= 0 && perc <= 1.0);
        auto last_bucket = buckets.size() - 1;

        auto c = count();

        if (!c) {
            return 0; // no data
        }

        auto pcount = int64_t(std::floor(c * perc));
        int64_t elements = 0;
        for (size_t i = 0; i < last_bucket; i++) {
            if (buckets[i]) {
                elements += buckets[i];
                if (elements >= pcount) {
                    return bucket_offsets[i];
                }
            }
        }
        return round(bucket_offsets.back() * 1.2); // overflowed value is in the requested percentile
    }

    /**
     * @return the mean histogram value (average of bucket offsets, weighted by count)
     */
    int64_t mean() const {
        auto lastBucket = buckets.size() - 1;
        int64_t elements = 0;
        int64_t sum = 0;
        for (size_t i = 0; i < lastBucket; i++) {
            long bCount = buckets[i];
            elements += bCount;
            sum += bCount * bucket_offsets[i];
        }

        return ((double) (sum + elements -1)/ elements);
    }

    /**
     * @return the total number of non-zero values
     */
    int64_t count() const {
        int64_t sum = 0L;
        for (size_t i = 0; i < buckets.size(); i++) {
            sum += buckets[i];
        }
        return sum;
    }

    estimated_histogram& operator*=(double v) {
        for (size_t i = 0; i < buckets.size(); i++) {
            buckets[i] *= v;
        }
        return *this;
    }
#if 0
    /**
     * @return true if this histogram has overflowed -- that is, a value larger than our largest bucket could bound was added
     */
    public boolean isOverflowed()
    {
        return buckets.get(buckets.length() - 1) > 0;
    }
#endif

    friend std::ostream& operator<<(std::ostream& out, const estimated_histogram& h) {
        // only print overflow if there is any
        size_t name_count;
        if (h.buckets[h.buckets.size() - 1] == 0) {
            name_count = h.buckets.size() - 1;
        } else {
            name_count = h.buckets.size();
        }
        std::vector<sstring> names;
        names.reserve(name_count);

        size_t max_name_len = 0;
        for (size_t i = 0; i < name_count; i++) {
            names.push_back(h.name_of_range(i));
            max_name_len = std::max(max_name_len, names.back().size());
        }

        sstring formatstr = sprint("%%%ds: %%d\n", max_name_len);
        for (size_t i = 0; i < name_count; i++) {
            int64_t count = h.buckets[i];
            // sort-of-hack to not print empty ranges at the start that are only used to demarcate the
            // first populated range. for code clarity we don't omit this record from the maxNameLength
            // calculation, and accept the unnecessary whitespace prefixes that will occasionally occur
            if (i == 0 && count == 0) {
                continue;
            }
            out << sprint(formatstr, names[i], count);
        }
        return out;
    }

    sstring name_of_range(size_t index) const {
        sstring s;
        s += "[";
        if (index == 0) {
            if (bucket_offsets[0] > 0) {
                // by original definition, this histogram is for values greater than zero only;
                // if values of 0 or less are required, an entry of lb-1 must be inserted at the start
                s += "1";
            } else {
                s += "-Inf";
            }
        } else {
            s += sprint("%d", bucket_offsets[index - 1] + 1);
        }
        s += "..";
        if (index == bucket_offsets.size()) {
            s += "Inf";
        } else {
            s += sprint("%d", bucket_offsets[index]);
        }
        s += "]";
        return s;
    }

#if 0
    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof EstimatedHistogram))
            return false;

        EstimatedHistogram that = (EstimatedHistogram) o;
        return Arrays.equals(getBucketOffsets(), that.getBucketOffsets()) &&
               Arrays.equals(getBuckets(false), that.getBuckets(false));
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(getBucketOffsets(), getBuckets(false));
    }

    public static class EstimatedHistogramSerializer implements ISerializer<EstimatedHistogram>
    {
        public void serialize(EstimatedHistogram eh, DataOutputPlus out) throws IOException
        {
            long[] offsets = eh.getBucketOffsets();
            long[] buckets = eh.getBuckets(false);
            out.writeInt(buckets.length);
            for (int i = 0; i < buckets.length; i++)
            {
                out.writeLong(offsets[i == 0 ? 0 : i - 1]);
                out.writeLong(buckets[i]);
            }
        }

        public EstimatedHistogram deserialize(DataInput in) throws IOException
        {
            int size = in.readInt();
            long[] offsets = new long[size - 1];
            long[] buckets = new long[size];

            for (int i = 0; i < size; i++) {
                offsets[i == 0 ? 0 : i - 1] = in.readLong();
                buckets[i] = in.readLong();
            }
            return new EstimatedHistogram(offsets, buckets);
        }

        public long serializedSize(EstimatedHistogram eh, TypeSizes typeSizes)
        {
            int size = 0;

            long[] offsets = eh.getBucketOffsets();
            long[] buckets = eh.getBuckets(false);
            size += typeSizes.sizeof(buckets.length);
            for (int i = 0; i < buckets.length; i++)
            {
                size += typeSizes.sizeof(offsets[i == 0 ? 0 : i - 1]);
                size += typeSizes.sizeof(buckets[i]);
            }
            return size;
        }
    }
#endif
};

inline estimated_histogram estimated_histogram_merge(estimated_histogram a, const estimated_histogram& b) {
    return a.merge(b);
}

}
