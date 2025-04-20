/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "utils/assert.hh"
#include <seastar/core/format.hh>
#include "exceptions/exceptions.hh"

namespace utils {

/**
 * The following calculations are taken from:
 * http://www.cs.wisc.edu/~cao/papers/summary-cache/node8.html
 * "Bloom Filters - the math"
 *
 * This class's static methods are meant to facilitate the use of the Bloom
 * Filter class by helping to choose correct values of 'bits per element' and
 * 'number of hash functions, k'.
 */
namespace bloom_calculations {

    /**
     * A wrapper class that holds two key parameters for a Bloom Filter: the
     * number of hash functions used, and the number of buckets per element used.
     */
    struct bloom_specification final {
        int K; // number of hash functions.
        int buckets_per_element;

        bloom_specification(int k, int buckets_per_element) : K(k), buckets_per_element(buckets_per_element) { }

        operator sstring() {
            return format("bloom_specification(K={:d}, buckets_per_element={:d})", K, buckets_per_element);
        }
    };

    int constexpr min_buckets = 2;
    int constexpr min_k = 1;
    int constexpr EXCESS = 20;

    extern const std::vector<std::vector<double>> probs;
    extern const std::vector<int> opt_k_per_buckets;

    /**
     * Given the number of buckets that can be used per element, return a
     * specification that minimizes the false positive rate.
     *
     * @param buckets_per_element The number of buckets per element for the filter.
     * @return A spec that minimizes the false positive rate.
     */
    inline bloom_specification compute_bloom_spec(int buckets_per_element) {
        SCYLLA_ASSERT(buckets_per_element >= 1);
        SCYLLA_ASSERT(buckets_per_element <= int(probs.size()) - 1);
        return bloom_specification(opt_k_per_buckets[buckets_per_element], buckets_per_element);
    }

    /**
     * Given a maximum tolerable false positive probability, compute a Bloom
     * specification which will give less than the specified false positive rate,
     * but minimize the number of buckets per element and the number of hash
     * functions used.  Because bandwidth (and therefore total bitvector size)
     * is considered more expensive than computing power, preference is given
     * to minimizing buckets per element rather than number of hash functions.
     *
     * @param max_buckets_per_element The maximum number of buckets available for the filter.
     * @param max_false_pos_prob The maximum tolerable false positive rate.
     * @return A Bloom Specification which would result in a false positive rate
     * less than specified by the function call
     * @throws unsupported_operation_exception if a filter satisfying the parameters cannot be met
     */
    inline bloom_specification compute_bloom_spec(int max_buckets_per_element, double max_false_pos_prob) {
        SCYLLA_ASSERT(max_buckets_per_element >= 1);
        SCYLLA_ASSERT(max_buckets_per_element <= int(probs.size()) - 1);

        auto max_k = int(probs[max_buckets_per_element].size()) - 1;

        // Handle the trivial cases
        if(max_false_pos_prob >= probs[min_buckets][min_k]) {
            return bloom_specification(2, opt_k_per_buckets[2]);
        }

        if (max_false_pos_prob < probs[max_buckets_per_element][max_k]) {
            throw exceptions::unsupported_operation_exception(format("Unable to satisfy {:f} with {:d} buckets per element", max_false_pos_prob, max_buckets_per_element));
        }

        // First find the minimal required number of buckets:
        int buckets_per_element = 2;
        int K = opt_k_per_buckets[2];

        while(probs[buckets_per_element][K] > max_false_pos_prob){
            buckets_per_element++;
            K = opt_k_per_buckets[buckets_per_element];
        }
        // Now that the number of buckets is sufficient, see if we can relax K
        // without losing too much precision.
        while(probs[buckets_per_element][K - 1] <= max_false_pos_prob){
            K--;
        }

        return bloom_specification(K, buckets_per_element);
    }

    /**
     * Calculates the maximum number of buckets per element that this implementation
     * can support.  Crucially, it will lower the bucket count if necessary to meet
     * BitSet's size restrictions.
     */
    inline int max_buckets_per_element(long num_elements) {
        num_elements = std::max(1l, num_elements);

        auto v = std::numeric_limits<long>::max() - EXCESS;
        v = v / num_elements;

        if (v < 1) {
            throw exceptions::unsupported_operation_exception(format("Cannot compute probabilities for {:d} elements.", num_elements));
        }
        return std::min(probs.size() - 1, size_t(v));
    }

    /**
     * Retrieves the minimum supported bloom_filter_fp_chance value
     * if compute_bloom_spec() above is attempted with bloom_filter_fp_chance
     * lower than this, it will throw an unsupported_operation_exception.
     */
    inline double min_supported_bloom_filter_fp_chance() {
        return probs.back().back();
    }

}

}

#if 0
package org.apache.cassandra.utils;

/**
 * The following calculations are taken from:
 * http://www.cs.wisc.edu/~cao/papers/summary-cache/node8.html
 * "Bloom Filters - the math"
 *
 * This class's static methods are meant to facilitate the use of the Bloom
 * Filter class by helping to choose correct values of 'bits per element' and
 * 'number of hash functions, k'.
 */
class BloomCalculations {

    private static final int minBuckets = 2;
    private static final int minK = 1;

    private static final int EXCESS = 20;

    /**
     * In the following keyspaceName, the row 'i' shows false positive rates if i buckets
     * per element are used.  Cell 'j' shows false positive rates if j hash
     * functions are used.  The first row is 'i=0', the first column is 'j=0'.
     * Each cell (i,j) the false positive rate determined by using i buckets per
     * element and j hash functions.
     */
    static final double[][] probs = new double[][]{
        {1.0}, // dummy row representing 0 buckets per element
        {1.0, 1.0}, // dummy row representing 1 buckets per element
        {1.0, 0.393,  0.400},
        {1.0, 0.283,  0.237,   0.253},
        {1.0, 0.221,  0.155,   0.147,   0.160},
        {1.0, 0.181,  0.109,   0.092,   0.092,   0.101}, // 5
        {1.0, 0.154,  0.0804,  0.0609,  0.0561,  0.0578,   0.0638},
        {1.0, 0.133,  0.0618,  0.0423,  0.0359,  0.0347,   0.0364},
        {1.0, 0.118,  0.0489,  0.0306,  0.024,   0.0217,   0.0216,   0.0229},
        {1.0, 0.105,  0.0397,  0.0228,  0.0166,  0.0141,   0.0133,   0.0135,   0.0145},
        {1.0, 0.0952, 0.0329,  0.0174,  0.0118,  0.00943,  0.00844,  0.00819,  0.00846}, // 10
        {1.0, 0.0869, 0.0276,  0.0136,  0.00864, 0.0065,   0.00552,  0.00513,  0.00509},
        {1.0, 0.08,   0.0236,  0.0108,  0.00646, 0.00459,  0.00371,  0.00329,  0.00314},
        {1.0, 0.074,  0.0203,  0.00875, 0.00492, 0.00332,  0.00255,  0.00217,  0.00199,  0.00194},
        {1.0, 0.0689, 0.0177,  0.00718, 0.00381, 0.00244,  0.00179,  0.00146,  0.00129,  0.00121,  0.0012},
        {1.0, 0.0645, 0.0156,  0.00596, 0.003,   0.00183,  0.00128,  0.001,    0.000852, 0.000775, 0.000744}, // 15
        {1.0, 0.0606, 0.0138,  0.005,   0.00239, 0.00139,  0.000935, 0.000702, 0.000574, 0.000505, 0.00047,  0.000459},
        {1.0, 0.0571, 0.0123,  0.00423, 0.00193, 0.00107,  0.000692, 0.000499, 0.000394, 0.000335, 0.000302, 0.000287, 0.000284},
        {1.0, 0.054,  0.0111,  0.00362, 0.00158, 0.000839, 0.000519, 0.00036,  0.000275, 0.000226, 0.000198, 0.000183, 0.000176},
        {1.0, 0.0513, 0.00998, 0.00312, 0.0013,  0.000663, 0.000394, 0.000264, 0.000194, 0.000155, 0.000132, 0.000118, 0.000111, 0.000109},
        {1.0, 0.0488, 0.00906, 0.0027,  0.00108, 0.00053,  0.000303, 0.000196, 0.00014,  0.000108, 8.89e-05, 7.77e-05, 7.12e-05, 6.79e-05, 6.71e-05} // 20
    };  // the first column is a dummy column representing K=0.

    /**
     * The optimal number of hashes for a given number of bits per element.
     * These values are automatically calculated from the data above.
     */
    private static final int[] optKPerBuckets = new int[probs.length];

    static
    {
        for (int i = 0; i < probs.length; i++)
        {
            double min = Double.MAX_VALUE;
            double[] prob = probs[i];
            for (int j = 0; j < prob.length; j++)
            {
                if (prob[j] < min)
                {
                    min = prob[j];
                    optKPerBuckets[i] = Math.max(minK, j);
                }
            }
        }
    }

    /**
     * Given the number of buckets that can be used per element, return a
     * specification that minimizes the false positive rate.
     *
     * @param bucketsPerElement The number of buckets per element for the filter.
     * @return A spec that minimizes the false positive rate.
     */
    public static BloomSpecification computeBloomSpec(int bucketsPerElement)
    {
        SCYLLA_ASSERT bucketsPerElement >= 1;
        SCYLLA_ASSERT bucketsPerElement <= probs.length - 1;
        return new BloomSpecification(optKPerBuckets[bucketsPerElement], bucketsPerElement);
    }

    /**
     * A wrapper class that holds two key parameters for a Bloom Filter: the
     * number of hash functions used, and the number of buckets per element used.
     */
    public static class BloomSpecification
    {
        final int K; // number of hash functions.
        final int bucketsPerElement;

        public BloomSpecification(int k, int bucketsPerElement)
        {
            K = k;
            this.bucketsPerElement = bucketsPerElement;
        }

        public String toString()
        {
            return String.format("BloomSpecification(K=%d, bucketsPerElement=%d)", K, bucketsPerElement);
        }
    }

    /**
     * Given a maximum tolerable false positive probability, compute a Bloom
     * specification which will give less than the specified false positive rate,
     * but minimize the number of buckets per element and the number of hash
     * functions used.  Because bandwidth (and therefore total bitvector size)
     * is considered more expensive than computing power, preference is given
     * to minimizing buckets per element rather than number of hash functions.
     *
     * @param maxBucketsPerElement The maximum number of buckets available for the filter.
     * @param maxFalsePosProb The maximum tolerable false positive rate.
     * @return A Bloom Specification which would result in a false positive rate
     * less than specified by the function call
     * @throws UnsupportedOperationException if a filter satisfying the parameters cannot be met
     */
    public static BloomSpecification computeBloomSpec(int maxBucketsPerElement, double maxFalsePosProb)
    {
        SCYLLA_ASSERT maxBucketsPerElement >= 1;
        SCYLLA_ASSERT maxBucketsPerElement <= probs.length - 1;
        int maxK = probs[maxBucketsPerElement].length - 1;

        // Handle the trivial cases
        if(maxFalsePosProb >= probs[minBuckets][minK]) {
            return new BloomSpecification(2, optKPerBuckets[2]);
        }
        if (maxFalsePosProb < probs[maxBucketsPerElement][maxK]) {
            throw new UnsupportedOperationException(String.format("Unable to satisfy %s with %s buckets per element",
                                                                  maxFalsePosProb, maxBucketsPerElement));
        }

        // First find the minimal required number of buckets:
        int bucketsPerElement = 2;
        int K = optKPerBuckets[2];
        while(probs[bucketsPerElement][K] > maxFalsePosProb){
            bucketsPerElement++;
            K = optKPerBuckets[bucketsPerElement];
        }
        // Now that the number of buckets is sufficient, see if we can relax K
        // without losing too much precision.
        while(probs[bucketsPerElement][K - 1] <= maxFalsePosProb){
            K--;
        }

        return new BloomSpecification(K, bucketsPerElement);
    }

    /**
     * Calculates the maximum number of buckets per element that this implementation
     * can support.  Crucially, it will lower the bucket count if necessary to meet
     * BitSet's size restrictions.
     */
    public static int maxBucketsPerElement(long numElements)
    {
        numElements = Math.max(1, numElements);
        double v = (Long.MAX_VALUE - EXCESS) / (double)numElements;
        if (v < 1.0)
        {
            throw new UnsupportedOperationException("Cannot compute probabilities for " + numElements + " elements.");
        }
        return Math.min(BloomCalculations.probs.length - 1, (int)v);
    }
}
#endif
