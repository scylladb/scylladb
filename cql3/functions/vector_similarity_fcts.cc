/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "types/types.hh"
#include "exceptions/exceptions.hh"

namespace cql3 {
namespace functions {
namespace {

// The computations of similarity scores match the exact formulas of Cassandra's (jVector's) implementation to ensure compatibility.
// There exist tests checking the compliance of the results.
// Reference:
// https://github.com/datastax/jvector/blob/f967f1c9249035b63b55a566fac7d4dc38380349/jvector-base/src/main/java/io/github/jbellis/jvector/vector/VectorSimilarityFunction.java#L36-L69

// You should only use this function if you need to preserve the original vectors and cannot normalize
// them in advance.
float compute_cosine_similarity(const std::vector<data_value>& v1, const std::vector<data_value>& v2) {
    double dot_product = 0.0;
    double squared_norm_a = 0.0;
    double squared_norm_b = 0.0;

    for (size_t i = 0; i < v1.size(); ++i) {
        double a = value_cast<float>(v1[i]);
        double b = value_cast<float>(v2[i]);

        dot_product += a * b;
        squared_norm_a += a * a;
        squared_norm_b += b * b;
    }

    if (squared_norm_a == 0 || squared_norm_b == 0) {
        throw exceptions::invalid_request_exception("Function system.similarity_cosine doesn't support all-zero vectors");
    }

    // The cosine similarity is in the range [-1, 1].
    // It is mapped to a similarity score in the range [0, 1] (-1 -> 0, 1 -> 1)
    // for consistency with other similarity functions.
    return (1 + (dot_product / (std::sqrt(squared_norm_a * squared_norm_b)))) / 2;
}

float compute_euclidean_similarity(const std::vector<data_value>& v1, const std::vector<data_value>& v2) {
    double sum = 0.0;

    for (size_t i = 0; i < v1.size(); ++i) {
        double a = value_cast<float>(v1[i]);
        double b = value_cast<float>(v2[i]);

        double diff = a - b;
        sum += diff * diff;
    }

    // The squared Euclidean (L2) distance is of range [0, inf).
    // It is mapped to a similarity score in the range (0, 1] (0 -> 1, inf -> 0)
    // for consistency with other similarity functions.
    return (1 / (1 + sum));
}

// Assumes that both vectors are L2-normalized.
// This similarity is intended as an optimized way to perform cosine similarity calculation.
float compute_dot_product_similarity(const std::vector<data_value>& v1, const std::vector<data_value>& v2) {
    double dot_product = 0.0;

    for (size_t i = 0; i < v1.size(); ++i) {
        double a = value_cast<float>(v1[i]);
        double b = value_cast<float>(v2[i]);
        dot_product += a * b;
    }

    // The dot product is in the range [-1, 1] for L2-normalized vectors.
    // It is mapped to a similarity score in the range [0, 1] (-1 -> 0, 1 -> 1)
    // for consistency with other similarity functions.
    return ((1 + dot_product) / 2);
}

} // namespace
} // namespace functions
} // namespace cql3
