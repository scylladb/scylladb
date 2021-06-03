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
 * Copyright (C) 2015-present ScyllaDB
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
#include <seastar/core/metrics_types.hh>
#include <seastar/core/print.hh>
#include "seastarx.hh"
#include <seastar/core/bitops.hh>
#include <limits>
#include <array>

namespace utils {

/**
 * This is a pseudo-exponential implementation of an estimated histogram.
 *
 * An exponential-histogram with coefficient 'coef', is a histogram where for bucket 'i'
 * the lower limit is coef^i and the higher limit is coef^(i+1).
 *
 * A pseudo-exponential is similar but the bucket limits are an approximation.
 *
 * The approx_exponential_histogram is an efficient pseudo-exponential implementation.
 *
 * The histogram is defined by a Min and Max value limits, and a Precision (all should be power of 2
 * and will be explained).
 *
 * When adding a value to a histogram:
 * All values lower than Min will be included in the first bucket (the assumption is that it's
 * not suppose to happen but it is ok if it does).
 *
 * All values higher than Max will be included in the last bucket that serves as the
 * infinity bucket (the assumption is that it can happen but it is rare).
 *
 * Note the difference between the first and last buckets.
 * The first bucket is just like a regular bucket but has a second roll to collect unexpected low values.
 * The last bucket, also known as the infinity bucket, collect all values that passes the defined Max,
 * it only collect those values.
 *
 * Buckets Distribution (limits)
 * =============================
 * The buckets limits in the histogram are defined similar to a floating-point representation.
 *
 * Buckets limits have an exponent part and a linear part.
 *
 * The exponential part is a power of 2. Each power-of-2 range [2^n..2^n+1)
 * is split linearly to 'Precision' number of buckets.
 *
 * The total number of buckets is:
 *   NUM_BUCKETS = log2(Max/Min)*Precision +1
 *
 * For example, if the Min value is 128, the Max is 1024 and the Precision is 4, the number of buckets is 13.
 *
 * Anything below 160 will be in the bucket 0, anything above 1024 will be in bucket 13.
 * Note that the first bucket will include all values below Min.
 *
 * the range [128, 1024) will be split into log2(1024/128) = 3 ranges:
 * 128, 256, 512, 1024
 * Or more mathematically: [128, 256), [256, 512), [512,1024)
 *
 * Each range is split into 4 (The Precision).
 * 128            | 256            | 512            | 1024
 * 128 160 192 224| 256 320 384 448| 512 640 768 896|
 *
 * To get the exponential part of an index you divide by the Precision.
 * The linear part of the index is Modulus the precision.
 *
 * Calculating the bucket lower limit of bucket i:
 * The exponential part: exp_part = 2^floor(i/Precision)* Min
 *    with the above example 2^floor(i/4)*128
 * The linear part: (i%Precision) * (exp_part/Precision)
 *    With the example: (i%4) * (exp_part/4)
 *
 * So the lower limit of bucket 6:
 *    2^floor(6/4)* 128  = 256
 *    (6%4) * 256/4 = 128
 *    lower-limit   = 384
 *
 * How to find a bucket index for a value
 * =======================================
 * The bucket index consist of two parts:
 * higher bits (exponential part) are based on log2(value/min)
 *
 * lower bits (linear part) are based on the 'n' MSB (ignoring the leading 1) where n=log2(Precision).
 * Continuing with the example where the number of precision bits: PRECISION_BITS = log2(4) = 2
 *
 * for example: 330 (101001010)
 * The number of precision_bits: PRECISION_BITS = log2(4) = 2
 * higher bits: log2(330/128) = 1
 * MSB: 01 (the highest two bits following the leading 1)
 * So the index: 101 = 5
 *
 * About the Min, Max and Precision
 * ================================
 * For Min, Max and Precision, choose numbers that are a power of 2.
 *
 * Limitation: You must set the MIN value to be higher or equal to the Precision.
 *
 */
template<uint64_t Min, uint64_t Max, size_t Precision>
requires (Min >= Precision && Min < Max && log2floor(Max) == log2ceil(Max) && log2floor(Min) == log2ceil(Min) && log2floor(Precision) == log2ceil(Precision))
class approx_exponential_histogram {
public:

    static constexpr unsigned NUM_EXP_RANGES = log2floor(Max/Min);
    static constexpr size_t NUM_BUCKETS = NUM_EXP_RANGES * Precision + 1;
    static constexpr unsigned PRECISION_BITS = log2floor(Precision);
    static constexpr unsigned BASESHIFT = log2floor(Min);
    static constexpr uint64_t LOWER_BITS_MASK = Precision - 1;
private:
    std::array<uint64_t, NUM_BUCKETS> _buckets;
public:
    approx_exponential_histogram() {
        clear();
    }

    /*!
     * \brief Returns the bucket lower limit given the bucket id.
     * The first and last bucket will always return the MIN and MAX respectively.
     *
     */
    uint64_t get_bucket_lower_limit(uint16_t bucket_id) const {
        if (bucket_id == NUM_BUCKETS - 1) {
            return Max;
        }
        int16_t exp_rang = (bucket_id >> PRECISION_BITS);
        return (Min << exp_rang) +  ((bucket_id & LOWER_BITS_MASK) << (exp_rang + BASESHIFT - PRECISION_BITS));
    }

    /*!
     * \brief Returns the bucket upper limit given the bucket id.
     * The last bucket (Infinity bucket) will return UMAX_INT.
     *
     */
    uint64_t get_bucket_upper_limit(uint16_t bucket_id) const {
        if (bucket_id == NUM_BUCKETS - 1) {
            return std::numeric_limits<uint64_t>::max();
        }
        return get_bucket_lower_limit(bucket_id + 1);
    }

    /*!
     * \brief Find the bucket index for a given value
     * The position of a value that is lower or equal to Min will always be 0.
     * The position of a value that is higher or equal to MAX will always be NUM_BUCKETS - 1.
     */
    uint16_t find_bucket_index(uint64_t val) const {
        if (val >= Max) {
            return NUM_BUCKETS - 1;
        }
        if (val <= Min) {
            return 0;
        }
        uint16_t range = log2floor(val);
        val >>= range - PRECISION_BITS; // leave the top most N+1 bits where N is the resolution.
        return ((range - BASESHIFT) << PRECISION_BITS) + (val & LOWER_BITS_MASK);
    }

    /*!
     * \brief clear the current values.
     */
    void clear() {
        std::fill(_buckets.begin(), _buckets.end(), 0);
    }

    /*!
     * \brief Add an item to the histogram
     * Increments the count of the bucket holding that value
     */
    void add(uint64_t n) {
        _buckets.at(find_bucket_index(n))++;
    }

    /*!
     * \brief returns the smallest value that could have been added to this histogram
     * This method looks for the first non-empty bucket and returns its lower limit.
     * Note that for non-empty histogram the lowest potentail value is Min.
     *
     * It will return 0 if the histogram is empty.
     */
    uint64_t min() const {
        for (size_t i = 0; i < NUM_BUCKETS; i ++) {
            if (_buckets[i] > 0) {
                return get_bucket_lower_limit(i);
            }
        }
        return 0;
    }

    /*!
     * \brief returns the largest value that could have been added to this histogram.
     * This method looks for the first non empty bucket and return its upper limit.
     * If the histogram overflowed, it will returns UINT64_MAX.
     *
     * It will return 0 if the histogram is empty.
     */
    uint64_t max() const {
        for (int i = NUM_BUCKETS - 1; i >= 0; i--) {
            if (_buckets[i] > 0) {
                return get_bucket_upper_limit(i);
            }
        }
        return 0;
    }

    /*!
     * \brief merge a histogram to the current one.
     */
    approx_exponential_histogram& merge(const approx_exponential_histogram& b) {
        for (size_t i = 0; i < NUM_BUCKETS; i++) {
            _buckets[i] += b.get(i);
        }
        return *this;
    }

    template<uint64_t A, uint64_t B, size_t C>
    friend approx_exponential_histogram<A, B, C> merge(approx_exponential_histogram<A, B, C> a, const approx_exponential_histogram<A, B, C>& b);

    /*
     * \brief returns the count in the given bucket
     */
    uint64_t get(size_t bucket) const {
        return _buckets[bucket];
    }

    /*!
     * \brief get a histogram quantile
     *
     * This method will returns the estimated value at a given quantile.
     * If there are N values in the histogram.
     * It would look for the bucket that the total number of elements in the buckets
     * before it are less than N * quantile and return that bucket lower limit.
     *
     * For example, quantile(0.5) will find the bucket that that sum of all buckets values
     * below it is less than half and will return that bucket lower limit.
     * In this example, this is a median estimation.
     *
     * It will return 0 if the histogram is empty.
     *
     */
    uint64_t quantile(float quantile) const {
        if (quantile < 0 || quantile > 1.0) {
            throw std::runtime_error("Invalid quantile value " + std::to_string(quantile) + ". Value should be between 0 and 1");
        }
        auto c = count();

        if (!c) {
            return 0; // no data
        }

        auto pcount = uint64_t(std::floor(c * quantile));
        uint64_t elements = 0;
        for (size_t i = 0; i < NUM_BUCKETS - 2; i++) {
            if (_buckets[i]) {
                elements += _buckets[i];
                if (elements >= pcount) {
                    return get_bucket_lower_limit(i);
                }
            }
        }
        return Max; // overflowed value is in the requested quantile
    }

    /*!
     * \brief returns the mean histogram value (average of bucket offsets, weighted by count)
     * It will return 0 if the histogram is empty.
     */
    uint64_t mean() const {
        uint64_t elements = 0;
        double sum = 0;
        for (size_t i = 0; i < NUM_BUCKETS - 1; i++) {
            elements += _buckets[i];
            sum += _buckets[i] * get_bucket_lower_limit(i);
        }
        return (elements) ?  sum / elements : 0;
    }

    /*!
     * \brief returns the number of buckets;
     */
    size_t size() const {
        return NUM_BUCKETS;
    }

    /*!
     * \brief returns the total number of values inserted
     */
    uint64_t count() const {
        uint64_t sum = 0L;
        for (size_t i = 0; i < NUM_BUCKETS; i++) {
            sum += _buckets[i];
        }
        return sum;
    }

    /*!
     * \brief multiple all the buckets content in the histogram by a constant
     */
    approx_exponential_histogram& operator*=(double v) {
        for (size_t i = 0; i < NUM_BUCKETS; i++) {
            _buckets[i] *= v;
        }
        return *this;
    }
};

template<uint64_t Min, uint64_t Max, size_t NumBuckets>
inline approx_exponential_histogram<Min, Max, NumBuckets> base_estimated_histogram_merge(approx_exponential_histogram<Min, Max, NumBuckets> a, const approx_exponential_histogram<Min, Max, NumBuckets>& b) {
    return a.merge(b);
}

/*!
 * \brief estimated histogram for duration values
 * time_estimated_histogram is used for short task timing.
 * It covers the range of 0.5ms to 33s with a precision of 4.
 *
 * 512us, 640us, 768us, 896us, 1024us, 1280us, 1536us, 1792us...16s, 20s, 25s, 29s, 33s (33554432us)
 */
class time_estimated_histogram : public approx_exponential_histogram<512, 33554432, 4> {
public:
    using clock = std::chrono::steady_clock;
    using duration = clock::duration;
    time_estimated_histogram& merge(const time_estimated_histogram& b) {
        approx_exponential_histogram<512, 33554432, 4>::merge(b);
        return *this;
    }

    void add_micro(uint64_t n) {
        approx_exponential_histogram<512, 33554432, 4>::add(n);
    }

    void add(const duration& latency) {
        add_micro(std::chrono::duration_cast<std::chrono::microseconds>(latency).count());
    }
};

inline time_estimated_histogram time_estimated_histogram_merge(time_estimated_histogram a, const time_estimated_histogram& b) {
    return a.merge(b);
}

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

    /**
     * @return the count in the given bucket
     */
    int64_t get(int bucket) {
        return buckets[bucket];
    }

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

        sstring formatstr = format("{{:{:d}s}}: {{:d}}\n", max_name_len);
        for (size_t i = 0; i < name_count; i++) {
            int64_t count = h.buckets[i];
            // sort-of-hack to not print empty ranges at the start that are only used to demarcate the
            // first populated range. for code clarity we don't omit this record from the maxNameLength
            // calculation, and accept the unnecessary whitespace prefixes that will occasionally occur
            if (i == 0 && count == 0) {
                continue;
            }
            out << format(formatstr.c_str(), names[i], count);
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
            s += format("{:d}", bucket_offsets[index - 1] + 1);
        }
        s += "..";
        if (index == bucket_offsets.size()) {
            s += "Inf";
        } else {
            s += format("{:d}", bucket_offsets[index]);
        }
        s += "]";
        return s;
    }
};

inline estimated_histogram estimated_histogram_merge(estimated_histogram a, const estimated_histogram& b) {
    return a.merge(b);
}

}
