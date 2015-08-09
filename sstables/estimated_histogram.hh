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

#include "disk_types.hh"
#include <cmath>
#include <algorithm>
#include <vector>

namespace sstables {

struct estimated_histogram {
    /**
     * The series of values to which the counts in `buckets` correspond:
     * 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 17, 20, etc.
     * Thus, a `buckets` of [0, 0, 1, 10] would mean we had seen one value of 3 and 10 values of 4.
     *
     * The series starts at 1 and grows by 1.2 each time (rounding and removing duplicates). It goes from 1
     * to around 36M by default (creating 90+1 buckets), which will give us timing resolution from microseconds to
     * 36 seconds, with less precision as the numbers get larger.
     *
     * Each bucket represents values from (previous bucket offset, current offset].
     */
    std::vector<int64_t> bucket_offsets;

    // buckets is one element longer than bucketOffsets -- the last element is values greater than the last offset
    std::vector<int64_t> buckets;

    estimated_histogram(int bucket_count = 90) {

        new_offsets(bucket_count);
        buckets.resize(bucket_offsets.size() + 1, 0);
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
        buckets.resize(buckets.size(), 0);
    }
    /**
     * Increments the count of the bucket closest to n, rounding UP.
     * @param n
     */
    void add(int64_t n) {
        auto low = std::lower_bound(bucket_offsets.begin(), bucket_offsets.end(), n);
        if (low == bucket_offsets.end()) {
            low--;
        }
        auto pos = low - bucket_offsets.begin();
        buckets.at(pos)++;
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
              if (buckets[i] > 0)
                  return bucket_offsets[i];
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

    /**
     * @param percentile
     * @return estimated value at given percentile
     */
    public long percentile(double percentile)
    {
        assert percentile >= 0 && percentile <= 1.0;
        int lastBucket = buckets.length() - 1;
        if (buckets.get(lastBucket) > 0)
            throw new IllegalStateException("Unable to compute when histogram overflowed");

        long pcount = (long) Math.floor(count() * percentile);
        if (pcount == 0)
            return 0;

        long elements = 0;
        for (int i = 0; i < lastBucket; i++)
        {
            elements += buckets.get(i);
            if (elements >= pcount)
                return bucketOffsets[i];
        }
        return 0;
    }

#endif

    /**
     * @return the mean histogram value (average of bucket offsets, weighted by count)
     */
    int64_t mean() {
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
#if 0
    /**
     * @return true if this histogram has overflowed -- that is, a value larger than our largest bucket could bound was added
     */
    public boolean isOverflowed()
    {
        return buckets.get(buckets.length() - 1) > 0;
    }

    /**
     * log.debug() every record in the histogram
     *
     * @param log
     */
    public void log(Logger log)
    {
        // only print overflow if there is any
        int nameCount;
        if (buckets.get(buckets.length() - 1) == 0)
            nameCount = buckets.length() - 1;
        else
            nameCount = buckets.length();
        String[] names = new String[nameCount];

        int maxNameLength = 0;
        for (int i = 0; i < nameCount; i++)
        {
            names[i] = nameOfRange(bucketOffsets, i);
            maxNameLength = Math.max(maxNameLength, names[i].length());
        }

        // emit log records
        String formatstr = "%" + maxNameLength + "s: %d";
        for (int i = 0; i < nameCount; i++)
        {
            long count = buckets.get(i);
            // sort-of-hack to not print empty ranges at the start that are only used to demarcate the
            // first populated range. for code clarity we don't omit this record from the maxNameLength
            // calculation, and accept the unnecessary whitespace prefixes that will occasionally occur
            if (i == 0 && count == 0)
                continue;
            log.debug(String.format(formatstr, names[i], count));
        }
    }

    private static String nameOfRange(long[] bucketOffsets, int index)
    {
        StringBuilder sb = new StringBuilder();
        appendRange(sb, bucketOffsets, index);
        return sb.toString();
    }

    private static void appendRange(StringBuilder sb, long[] bucketOffsets, int index)
    {
        sb.append("[");
        if (index == 0)
            if (bucketOffsets[0] > 0)
                // by original definition, this histogram is for values greater than zero only;
                // if values of 0 or less are required, an entry of lb-1 must be inserted at the start
                sb.append("1");
            else
                sb.append("-Inf");
        else
            sb.append(bucketOffsets[index - 1] + 1);
        sb.append("..");
        if (index == bucketOffsets.length)
            sb.append("Inf");
        else
            sb.append(bucketOffsets[index]);
        sb.append("]");
    }

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

inline estimated_histogram merge(estimated_histogram a, const estimated_histogram& b) {
    return a.merge(b);
}

}
