/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "external_score_provider.hh"

#include "cql3/values.hh"
#include "keys/keys.hh"
#include "query/query-request.hh"
#include "schema/schema.hh"
#include "types/types.hh"
#include "utils/log.hh"

#include <cmath>

namespace cql3::statements {

external_score_provider::external_score_provider(const vector_search::vector_store_client::primary_keys& results, size_t external_value_index, const schema& schema)
    : _results(results)
    , _current_index(0)
    , _external_value_index(external_value_index)
    , _schema(schema) {
}

bool external_score_provider::try_fill(std::vector<cql3::raw_value>& external_values, std::span<const bytes> partition_key, std::span<const bytes> clustering_key,
        const query::result_row_view&, const query::result_row_view*) const {
    const auto row_pk = ::partition_key::from_range(partition_key);
    const auto row_ck = (_schema.clustering_key_size() > 0) ? ::clustering_key_prefix::from_range(clustering_key) : ::clustering_key_prefix{};

    // Base-table results are merged in Vector Store primary-key order by
    // external_index_select_statement. Consume the matching score in that order.
    while (_current_index < _results.size()) {
        const auto& vs_result = _results[_current_index];

        if (!vs_result.partition.key().equal(_schema, row_pk)) {
            ++_current_index;
            continue;
        }

        // Compare clustering key if present.
        if (_schema.clustering_key_size() > 0) {
            if (!vs_result.clustering.equal(_schema, row_ck)) {
                ++_current_index;
                continue;
            }
        }

        float similarity_score = vs_result.similarity;
        ++_current_index;

        // Vector store can't return Inf over JSON API.
        // It also shouldn't return NaN (null in JSON),
        // but if it does, we treat it as an error and skip the row.
        if (!std::isfinite(similarity_score)) {
            return false;
        }

        external_values[_external_value_index] = cql3::raw_value::make_value(float_type->decompose(similarity_score));
        return true;
    }

    return false;
}

} // namespace cql3::statements
