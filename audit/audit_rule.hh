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
#include <algorithm>
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

/// Formats a "keyspace.table" qualified table name from separate components.
inline sstring qualified_table_name(std::string_view keyspace, std::string_view table) {
    sstring result(sstring::initialized_later(), keyspace.size() + 1 + table.size());
    auto it = result.begin();
    it = std::copy(keyspace.begin(), keyspace.end(), it);
    *it++ = '.';
    std::copy(table.begin(), table.end(), it);
    return result;
}

enum class audit_sink {
    table,
    syslog,
    stdout,
};

using audit_sink_set = enum_set<super_enum<audit_sink, audit_sink::table, audit_sink::syslog, audit_sink::stdout>>;

/// Returns true if the category is table-scoped (DML, DDL, QUERY).
/// Table-independent categories (AUTH, ADMIN, DCL) bypass the table filter
/// because they represent operations that have no meaningful keyspace/table.
inline bool is_table_scoped_category(statement_category category) {
    return category == statement_category::DML
        || category == statement_category::DDL
        || category == statement_category::QUERY;
}

/// Returns true if the given category matches any of the rule's categories (bitmask check).
/// Empty categories set matches nothing.
bool matches_category(const audit_rule& rule, statement_category category);

/// Returns true if the given keyspace.table matches any of the rule's qualified_table_names
/// patterns (uses fnmatch with FNM_EXTMATCH for extended glob support).
/// Empty qualified_table_names list matches nothing.
bool matches_table(const audit_rule& rule, std::string_view keyspace, std::string_view table);

/// Same as matches_table but takes a pre-formed "keyspace.table" string.
bool matches_qualified_table(const audit_rule& rule, std::string_view qualified_table_name);

/// Returns true if the given role name matches any of the rule's role patterns
/// (uses fnmatch with FNM_EXTMATCH for extended glob support: @(), !(), +(), *(), ?()).
/// Empty roles list matches nothing.
bool matches_role(const audit_rule& rule, std::string_view role);

/// Returns the set of sinks declared by this rule as a bitmap.
audit_sink_set rule_sinks(const audit_rule& rule);

/// Returns true if a statement with the given attributes matches this rule.
/// A rule matches when all three filters (categories, tables, roles) match.
/// For table-independent categories (AUTH, ADMIN, DCL), the table filter is
/// bypassed — these operations have no meaningful keyspace/table context.
/// Sink filtering is handled separately via rule_sinks().
bool matches_rule(const audit_rule& rule, statement_category category,
                  std::string_view keyspace, std::string_view table,
                  std::string_view role);

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
