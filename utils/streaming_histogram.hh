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

#include <cstdint>
#include <map>

namespace utils {

/**
 * Histogram that can be constructed from streaming of data.
 *
 * The algorithm is taken from following paper:
 * Yael Ben-Haim and Elad Tom-Tov, "A Streaming Parallel Decision Tree Algorithm" (2010)
 * http://jmlr.csail.mit.edu/papers/volume11/ben-haim10a/ben-haim10a.pdf
 */
struct streaming_histogram {
    // TreeMap to hold bins of histogram.
    std::map<double, uint64_t> bin;

    // maximum bin size for this histogram
    uint32_t max_bin_size;

    /**
     * Creates a new histogram with max bin size of maxBinSize
     * @param maxBinSize maximum number of bins this histogram can have
     */
    streaming_histogram(int max_bin_size = 0)
        : max_bin_size(max_bin_size) {
    }

    streaming_histogram(int max_bin_size, std::map<double, uint64_t>&& bin)
        : bin(std::move(bin))
        , max_bin_size(max_bin_size) {
    }

    /**
     * Adds new point p to this histogram.
     * @param p
     */
    void update(double p) {
        update(p, 1);
    }

    /**
     * Adds new point p with value m to this histogram.
     * @param p
     * @param m
     */
    void update(double p, uint64_t m) {
        auto it = bin.find(p);
        if (it != bin.end()) {
            bin[p] = it->second + m;
        } else {
            bin[p] = m;
            // if bin size exceeds maximum bin size then trim down to max size
            while (bin.size() > max_bin_size) {
                // find points p1, p2 which have smallest difference
                auto it = bin.begin();
                double p1 = it->first;
                it++;
                double p2 = it->first;
                it++;
                double smallestDiff = p2 - p1;
                double q1 = p1, q2 = p2;
                while(it != bin.end()) {
                    p1 = p2;
                    p2 = it->first;
                    it++;
                    double diff = p2 - p1;
                    if (diff < smallestDiff)
                    {
                        smallestDiff = diff;
                        q1 = p1;
                        q2 = p2;
                    }
                }
                // merge those two
                uint64_t k1 = bin.at(q1);
                uint64_t k2 = bin.at(q2);
                bin.erase(q1);
                bin.erase(q2);
                bin.insert({(q1 * k1 + q2 * k2) / (k1 + k2), k1 + k2});
            }
        }
    }


    /**
     * Merges given histogram with this histogram.
     *
     * @param other histogram to merge
     */
    void merge(streaming_histogram& other) {
        if (!other.bin.size()) {
            return;
        }

        for (auto& it : other.bin) {
            update(it.first, it.second);
        }
    }

    /**
     * Calculates estimated number of points in interval [-inf,b].
     *
     * @param b upper bound of a interval to calculate sum
     * @return estimated number of points in a interval [-inf,b].
     */
    double sum(double b) const {
        double sum = 0;
        // find the points pi, pnext which satisfy pi <= b < pnext
        auto pnext = bin.upper_bound(b);
        if (pnext == bin.end()) {
            // if b is greater than any key in this histogram,
            // just count all appearance and return
            for (auto& e : bin) {
                sum += e.second;
            }
        } else {
            // return key-value mapping associated with the greatest key less than or equal to the given key
            auto pi = bin.lower_bound(b);
            if (pi == bin.end() || (pi == bin.begin() && b < pi->first)) {
                return 0;
            }
            if (pi->first != b) {
                --pi;
            }

            // calculate estimated count mb for point b
            double weight = (b - pi->first) / (pnext->first - pi->first);
            double mb = pi->second + (int64_t(pnext->second) - int64_t(pi->second)) * weight;
            sum += (pi->second + mb) * weight / 2;

            sum += pi->second / 2.0;
            // iterate through portion of map whose keys are less than pi->first
            auto it_end = bin.lower_bound(pi->first);
            for (auto it = bin.begin(); it != it_end; it++) {
                sum += it->second;
            }
        }
        return sum;
    }

    // FIXME: convert Java code below.
#if 0
    public Map<Double, Long> getAsMap()
    {
        return Collections.unmodifiableMap(bin);
    }

    public static class StreamingHistogramSerializer implements ISerializer<StreamingHistogram>
    {
        public void serialize(StreamingHistogram histogram, DataOutputPlus out) throws IOException
        {
            out.writeInt(histogram.maxBinSize);
            Map<Double, Long> entries = histogram.getAsMap();
            out.writeInt(entries.size());
            for (Map.Entry<Double, Long> entry : entries.entrySet())
            {
                out.writeDouble(entry.getKey());
                out.writeLong(entry.getValue());
            }
        }

        public StreamingHistogram deserialize(DataInput in) throws IOException
        {
            int maxBinSize = in.readInt();
            int size = in.readInt();
            Map<Double, Long> tmp = new HashMap<>(size);
            for (int i = 0; i < size; i++)
            {
                tmp.put(in.readDouble(), in.readLong());
            }

            return new StreamingHistogram(maxBinSize, tmp);
        }

        public long serializedSize(StreamingHistogram histogram, TypeSizes typeSizes)
        {
            long size = typeSizes.sizeof(histogram.maxBinSize);
            Map<Double, Long> entries = histogram.getAsMap();
            size += typeSizes.sizeof(entries.size());
            // size of entries = size * (8(double) + 8(long))
            size += entries.size() * (8 + 8);
            return size;
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof StreamingHistogram))
            return false;

        StreamingHistogram that = (StreamingHistogram) o;
        return maxBinSize == that.maxBinSize && bin.equals(that.bin);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(bin.hashCode(), maxBinSize);
    }
#endif
};

}
