// Copyright (C) 2023-present ScyllaDB
// SPDX-License-Identifier: AGPL-3.0-or-later

#pragma once

#include "expression.hh"

#include "bytes.hh"

namespace cql3 {

class query_options;
class raw_value;

}

namespace cql3::expr {

// Input data needed to evaluate an expression. Individual members can be
// null if not applicable (e.g. evaluating outside a row context)
struct evaluation_inputs {
    std::span<const bytes> partition_key;
    std::span<const bytes> clustering_key;
    std::span<const managed_bytes_opt> static_and_regular_columns; // indexes match `selection` member
    const cql3::selection::selection* selection = nullptr;
    const query_options* options = nullptr;
    std::span<const api::timestamp_type> static_and_regular_timestamps;  // indexes match `selection` member
    std::span<const int32_t> static_and_regular_ttls;  // indexes match `selection` member
    std::span<const cql3::raw_value> temporaries; // indexes match temporary::index
};

// Takes a prepared expression and calculates its value.
// Evaluates bound values, calls functions and returns just the bytes and type.
cql3::raw_value evaluate(const expression& e, const evaluation_inputs&);

cql3::raw_value evaluate(const expression& e, const query_options&);


}
