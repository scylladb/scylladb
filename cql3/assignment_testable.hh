/*
 * Copyright (C) 2014-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "column_specification.hh"
#include "data_dictionary/data_dictionary.hh"

namespace cql3 {

class assignment_testable {
public:
    virtual ~assignment_testable() {}

    enum class test_result {
        EXACT_MATCH,
        WEAKLY_ASSIGNABLE,
        NOT_ASSIGNABLE,
    };

    static bool is_assignable(test_result tr) {
        return tr != test_result::NOT_ASSIGNABLE;
    }

    static bool is_exact_match(test_result tr) {
        return tr == test_result::EXACT_MATCH;
    }

    /**
     * @return whether this object can be assigned to the provided receiver. We distinguish
     * between 3 values: 
     *   - EXACT_MATCH if this object is exactly of the type expected by the receiver
     *   - WEAKLY_ASSIGNABLE if this object is not exactly the expected type but is assignable nonetheless
     *   - NOT_ASSIGNABLE if it's not assignable
     * Most caller should just call the isAssignable() method on the result, though functions have a use for
     * testing "strong" equality to decide the most precise overload to pick when multiple could match.
     */
    virtual test_result test_assignment(data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver) const = 0;

    virtual std::optional<data_type> assignment_testable_type_opt() const = 0;

    // for error reporting
    virtual sstring assignment_testable_source_context() const = 0;
};

inline bool is_assignable(assignment_testable::test_result tr) {
    return assignment_testable::is_assignable(tr);
}

inline bool is_exact_match(assignment_testable::test_result tr) {
    return assignment_testable::is_exact_match(tr);
}

}

template <>
struct fmt::formatter<cql3::assignment_testable> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const cql3::assignment_testable& at, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", at.assignment_testable_source_context());
    }
};
