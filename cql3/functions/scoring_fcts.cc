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

namespace {

/// A native scalar function whose value comes from an external search system rather than from
/// evaluating its arguments locally.
///
/// Being external implies the two invariants every such function needs, so they are stated here
/// once instead of once per function:
///  - non-pure, so that the expression evaluator does not constant-fold a call whose arguments
///    happen to all be literals before statement preparation gets to claim it;
///  - no evaluation of its own: preparation either lowers the call to a value injected per row or
///    rejects the clause it appears in, so reaching this body is a bug.
class external_scalar_function : public native_scalar_function {
public:
    external_scalar_function(sstring name, data_type return_type, std::vector<data_type> arg_types)
        : native_scalar_function(std::move(name), std::move(return_type), std::move(arg_types)) {
    }

    bool is_pure() const override {
        return false;
    }

    bool is_external() const override {
        return true;
    }

    bytes_opt execute(std::span<const bytes_opt>) override {
        on_internal_error(log, format("{}() reached scalar evaluation; prepare-time handling should have prevented this", name()));
    }
};

} // anonymous namespace

shared_ptr<function> make_bm25_function() {
    // BM25 fulltext scoring function: bm25(column, query) -> float
    // Registered with utf8_type args; ascii is implicitly coerced to utf8 by the type system.
    //
    // BM25 scores depend on document statistics, so the result is not determined by the visible arguments alone.
    return ::make_shared<external_scalar_function>(BM25_FUNCTION_NAME.name, float_type, std::vector<data_type>{utf8_type, utf8_type});
}

shared_ptr<function> make_bm25_highlight_function() {
    // Full-text search highlighting function: bm25_highlight(column, query) -> text
    //
    // Answers with a fragment of the row's own text, with the terms of the query marked. Only the
    // full-text index can pick the fragment - it needs the corpus statistics and the analyzer -
    // and it may find none, which is why the return type is nullable.
    return ::make_shared<external_scalar_function>(BM25_HIGHLIGHT_FUNCTION_NAME.name, utf8_type, std::vector<data_type>{utf8_type, utf8_type});
}

shared_ptr<function> make_ann_function(const std::vector<data_type>& arg_types) {
    // ANN vector ordering function: ann(column, query_vector) -> float
    return ::make_shared<external_scalar_function>(ANN_FUNCTION_NAME.name, float_type, arg_types);
}

} // namespace functions
} // namespace cql3
