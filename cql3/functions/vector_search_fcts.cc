/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "vector_search_fcts.hh"
#include "cql3/query_options.hh"

namespace cql3 {
namespace functions {
namespace {

static size_t find_matching_key_index(const expr::evaluation_inputs& inputs, const std::vector<cql3::statements::primary_key>& keys) {
    for (size_t i = 0; i < keys.size(); ++i) {
        auto primary_key = std::move(keys[i]);
        auto partition_key = primary_key.partition.key().explode();
        auto clustering_key = primary_key.clustering.explode();

        // Check if partition key matches
        if (partition_key.size() == inputs.partition_key.size() &&
            std::equal(partition_key.begin(), partition_key.end(), inputs.partition_key.begin())) {
            if (clustering_key.size() == inputs.clustering_key.size() &&
                std::equal(clustering_key.begin(), clustering_key.end(), inputs.clustering_key.begin())) {
                return i;
            }
        }
    }
    return keys.size(); // Not found
}

}

bytes_opt vector_similarity_fct::execute(std::span<const bytes_opt> parameters, const expr::evaluation_inputs& inputs) {
    SCYLLA_ASSERT(parameters.empty());

    auto options = inputs.options->get_specific_options();
    auto [keys, distances] = options.ann_result;

    // It is not possible that the result here is empty when using ANN queries.
    // If the ANN returns no result, we won't call this function at all as it's called once per every row selected.
    // The rows are selected according to the ANN result - the same we use here.
    // No ANN results = no rows selected = no execute() function calls.
    if (keys.empty()) {
        throw exceptions::invalid_request_exception("vector_similarity function can only be used with ANN queries");
    }

    size_t index = find_matching_key_index(inputs, keys);
    if (index == keys.size()) {
        throw std::runtime_error("No matching distance found for given primary key");
    }

    return float_type->decompose(distances[index]);
}

}
}
