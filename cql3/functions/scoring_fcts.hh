/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/expr/expression.hh"
#include "cql3/functions/function.hh"
#include "cql3/functions/function_name.hh"
#include "cql3/functions/native_scalar_function.hh"

#include <string_view>

namespace cql3 {
namespace functions {

// BM25_SCORE(c, t) returns the same score as BM25(c, t), and BM25_RANK(c, t) the position of the
// row in the search's result, counted from 1. ANN_SCORE(c, v) and ANN_RANK(c, v) do the same for
// ANN(c, v). All calls with the same arguments describe one search, so a query using several of
// them makes one request.
static const function_name BM25_FUNCTION_NAME = function_name::native_function("bm25");
static const function_name BM25_SCORE_FUNCTION_NAME = function_name::native_function("bm25_score");
static const function_name BM25_RANK_FUNCTION_NAME = function_name::native_function("bm25_rank");
static const function_name ANN_FUNCTION_NAME = function_name::native_function("ann");
static const function_name ANN_SCORE_FUNCTION_NAME = function_name::native_function("ann_score");
static const function_name ANN_RANK_FUNCTION_NAME = function_name::native_function("ann_rank");

/// Whether `name` is one of the ANN family, whose argument types are not fixed but inferred from
/// the call site.
bool is_ann_function_name(const function_name& name);

/// The family of external search functions a function belongs to: the vector search family (ANN) or
/// the full-text search family (BM25). The family decides which index answers the search and what the
/// second argument is: a query vector for ANN, a search term for BM25.
enum class search_family { ann, bm25 };

/// Which value a search function returns.
enum class search_value {
    /// The score the index gave the row.
    score,
    /// The position of the row in the index's result, counted from 1.
    rank,
};

/// A function whose value comes from an external search index rather than from evaluating its
/// arguments: ANN(), BM25() and their families. Preparing the statement replaces the call with a
/// read of the value the index returns, or rejects the statement. Non-pure, so that a call with
/// constant arguments is not constant-folded. The only class whose is_external() is true.
class external_search_function : public native_scalar_function {
    search_family _family;
    search_value _value;
    // name() upper-cased, as written in error messages: "BM25_RANK".
    sstring _display_name;

public:
    external_search_function(sstring name, data_type return_type, std::vector<data_type> arg_types, search_family family,
            search_value value);

    search_family family() const { return _family; }
    search_value value() const { return _value; }
    std::string_view display_name() const { return _display_name; }

    bool is_pure() const override { return false; }
    bool is_external() const override { return true; }
    bytes_opt execute(std::span<const bytes_opt>) override;
};

/// The function object behind a resolved call, or nullptr if the call is not to an external search
/// function, which is what is_external() says.
const external_search_function* as_external_search_function(const expr::function_call& fc);

shared_ptr<function> make_bm25_function();
shared_ptr<function> make_bm25_score_function();
shared_ptr<function> make_bm25_rank_function();

/// Creates the ANN-family function `name`. The argument types are not fixed: they are inferred
/// from the call site and must be float vectors of the same dimension.
shared_ptr<function> make_ann_function(const function_name& name, const std::vector<data_type>& arg_types);

} // namespace functions
} // namespace cql3
