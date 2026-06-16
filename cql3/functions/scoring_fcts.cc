/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "scoring_fcts.hh"
#include "native_scalar_function.hh"
#include "utils/log.hh"
#include <seastar/core/on_internal_error.hh>

namespace cql3 {
namespace functions {

extern logging::logger log;

shared_ptr<function> make_bm25_function() {
    // BM25 fulltext scoring function: bm25(column, query) -> float
    // Registered with utf8_type args; ascii is implicitly coerced to utf8 by the type system.
    return make_native_scalar_function<true>("bm25", float_type, {utf8_type, utf8_type},
        [] (std::span<const bytes_opt>) -> bytes_opt {
            // bm25() is intercepted and routed to the fulltext index before evaluation.
            // This body is never reached in a valid full-text search query.
            on_internal_error(log, "bm25() was called as a plain scalar function");
        });
}

} // namespace functions
} // namespace cql3
