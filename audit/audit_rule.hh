/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "seastarx.hh"
#include "enum_set.hh"
#include <seastar/core/sstring.hh>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <array>
#include <string_view>
#include <vector>

namespace audit {

enum class statement_category {
    QUERY, DML, DDL, DCL, AUTH, ADMIN
};

using category_set = enum_set<super_enum<statement_category, statement_category::QUERY,
                                                             statement_category::DML,
                                                             statement_category::DDL,
                                                             statement_category::DCL,
                                                             statement_category::AUTH,
                                                             statement_category::ADMIN>>;

sstring category_to_string(statement_category category);

/// Required field names for an audit rule (used by both JSON and YAML parsers).
inline constexpr std::array<const char*, 4> audit_rule_required_fields = {
    "sinks", "categories", "qualified_table_names", "roles"
};

struct audit_rule {
    std::vector<sstring> sinks;
    category_set categories;
    std::vector<sstring> qualified_table_names;
    std::vector<sstring> roles;

    bool operator==(const audit_rule& other) const {
        return sinks == other.sinks
            && categories.mask() == other.categories.mask()
            && qualified_table_names == other.qualified_table_names
            && roles == other.roles;
    }
};

std::vector<audit_rule> parse_audit_rules_from_json(const sstring& json_str);

sstring audit_rules_to_json_string(const std::vector<audit_rule>& rules);

category_set parse_categories(const std::vector<sstring>& categories);

void validate_audit_rule(const audit_rule& rule);

} // namespace audit

template<>
struct fmt::formatter<audit::audit_rule> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const audit::audit_rule& rule, fmt::format_context& ctx) const {
        auto out = fmt::format_to(ctx.out(), "audit_rule{{sinks=[{}], categories=[", fmt::join(rule.sinks, ","));
        bool first = true;
        for (auto cat : rule.categories) {
            if (!first) { out = fmt::format_to(out, ","); }
            out = fmt::format_to(out, "{}", audit::category_to_string(cat));
            first = false;
        }
        return fmt::format_to(out, "], qualified_table_names=[{}], roles=[{}]}}",
            fmt::join(rule.qualified_table_names, ","), fmt::join(rule.roles, ","));
    }
};
