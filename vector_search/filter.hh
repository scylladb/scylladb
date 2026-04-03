/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "bytes_ostream.hh"
#include "cql3/expr/expression.hh"

namespace cql3 {

class query_options;

namespace restrictions {

class statement_restrictions;

}
} // namespace cql3

namespace vector_search {

/// Metadata for a bind marker in the cached JSON template.
/// Contains the type and expression needed to evaluate the bind marker at execution time.
struct bind_marker_metadata {
    data_type type;
    cql3::expr::expression expr;
};

/// A cache entry is either a literal JSON string or a bind marker to be substituted.
using filter_cache_entry = std::variant<sstring, bind_marker_metadata>;

class prepared_filter {
public:
    using cache = std::vector<filter_cache_entry>;

    explicit prepared_filter(cache cache)
        : _cache(std::move(cache)) {
    }

    /// Serializes the prepared filter to a JSON buffer compatible with the Vector Store service filtering API.
    /// Bind marker placeholders in the cached template are substituted with actual values from `query_options`.
    /// Returns a bytes_ostream that can be efficiently consumed without materialization.
    bytes_ostream to_json(const cql3::query_options& options) const;

private:
    cache _cache;
};

/// Prepares a filter from CQL statement restrictions for use in Vector Store service.
/// This function extracts primary key restrictions from the statement_restrictions
/// and prepares them for serialization to JSON compatible to Vector Store service filtering API.
prepared_filter prepare_filter(const cql3::restrictions::statement_restrictions& restrictions, bool allow_filtering);

} // namespace vector_search
