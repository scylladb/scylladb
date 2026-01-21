/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/rjson.hh"
#include "cql3/expr/expression.hh"

namespace cql3 {

class query_options;

namespace restrictions {

class statement_restrictions;

}
} // namespace cql3

namespace vector_search {

struct prepared_rhs {
    data_type type;
    cql3::expr::expression expr;
};

struct prepared_restriction {
    rjson::value type_json;
    rjson::value lhs_json;
    std::variant<rjson::value, prepared_rhs> rhs;

    rjson::value rhs_to_json(const cql3::query_options& options) const;
};

class prepared_filter {
    std::vector<prepared_restriction> _restrictions;
    bool _allow_filtering;
    // Cached JSON representation for filters without bind markers.
    std::optional<rjson::value> _cached_json;

public:
    prepared_filter(std::vector<prepared_restriction> restrictions, bool allow_filtering, std::optional<rjson::value> cached_json = std::nullopt)
        : _restrictions(std::move(restrictions))
        , _allow_filtering(allow_filtering)
        , _cached_json(std::move(cached_json)) {
    }

    /// Serializes the prepared filter to JSON compatible with the Vector Store service filtering API.
    rjson::value to_json(const cql3::query_options& options) const;
};

/// Prepares a filter from CQL statement restrictions for use in Vector Store service.
/// This function extracts primary key restrictions from the statement_restrictions
/// and prepares them for serialization to JSON compatible to Vector Store service filtering API.
prepared_filter prepare_filter(const cql3::restrictions::statement_restrictions& restrictions, bool allow_filtering);

} // namespace vector_search
