/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "audit/audit_rule.hh"
#include "audit/audit.hh"
#include "utils/rjson.hh"

#include <fmt/ranges.h>
#include <fnmatch.h>
#include <seastar/core/on_internal_error.hh>

namespace audit {

sstring category_to_string(statement_category category) {
    switch (category) {
        case statement_category::QUERY: return "QUERY";
        case statement_category::DML: return "DML";
        case statement_category::DDL: return "DDL";
        case statement_category::DCL: return "DCL";
        case statement_category::AUTH: return "AUTH";
        case statement_category::ADMIN: return "ADMIN";
    }
    return "";
}

static statement_category string_to_category(std::string_view s) {
    if (s == "QUERY") return statement_category::QUERY;
    if (s == "DML") return statement_category::DML;
    if (s == "DDL") return statement_category::DDL;
    if (s == "DCL") return statement_category::DCL;
    if (s == "AUTH") return statement_category::AUTH;
    if (s == "ADMIN") return statement_category::ADMIN;
    throw audit_exception(fmt::format(
        "Bad configuration: invalid category '{}' in audit rule", s));
}

namespace {

rjson::value string_vec_to_json(const std::vector<sstring>& vec) {
    rjson::value arr = rjson::empty_array();
    for (const auto& s : vec) {
        rjson::push_back(arr, rjson::from_string(s));
    }
    return arr;
}

std::vector<sstring> json_array_to_string_vec(const rjson::value& arr, const sstring& field_name) {
    if (!arr.IsArray()) {
        throw audit_exception(fmt::format(
            "Bad configuration: '{}' must be a JSON array", field_name));
    }
    std::vector<sstring> result;
    for (const auto& elem : arr.GetArray()) {
        if (!elem.IsString()) {
            throw audit_exception(fmt::format(
                "Bad configuration: '{}' array elements must be strings", field_name));
        }
        result.emplace_back(rjson::to_string_view(elem));
    }
    return result;
}

} // anonymous namespace

category_set parse_categories(const std::vector<sstring>& categories) {
    category_set result;
    for (const auto& cat : categories) {
        result.set(string_to_category(cat));
    }
    return result;
}

void validate_audit_rule(const audit_rule& rule) {
    // Sinks: must be non-empty, each must be "table" or "syslog"
    if (rule.sinks.empty()) {
        throw audit_exception("Bad configuration: 'sinks' must be non-empty in audit rule");
    }
    for (const auto& sink : rule.sinks) {
        if (sink != "table" && sink != "syslog") {
            throw audit_exception(fmt::format(
                "Bad configuration: invalid sink '{}' in audit rule (must be 'table' or 'syslog')", sink));
        }
    }
}

std::vector<audit_rule> parse_audit_rules_from_json(const sstring& json_str) {
    if (json_str.empty()) {
        return {};
    }

    rjson::value parsed;
    try {
        parsed = rjson::parse(json_str);
    } catch (const rjson::error& e) {
        throw audit_exception(fmt::format(
            "Bad configuration: failed to parse audit_rules JSON: {}", e.what()));
    }

    if (!parsed.IsArray()) {
        throw audit_exception("Bad configuration: audit_rules must be a JSON array");
    }

    std::vector<audit_rule> rules;
    for (const auto& elem : parsed.GetArray()) {
        if (!elem.IsObject()) {
            throw audit_exception("Bad configuration: each audit rule must be a JSON object");
        }

        for (const auto& field : audit_rule_required_fields) {
            if (!rjson::find(elem, field)) {
                throw audit_exception(fmt::format(
                    "Bad configuration: audit rule missing required field '{}'", field));
            }
        }

        audit_rule rule;
        rule.sinks = json_array_to_string_vec(*rjson::find(elem, "sinks"), "sinks");
        rule.categories = parse_categories(json_array_to_string_vec(*rjson::find(elem, "categories"), "categories"));
        rule.qualified_table_names = json_array_to_string_vec(*rjson::find(elem, "qualified_table_names"), "qualified_table_names");
        rule.roles = json_array_to_string_vec(*rjson::find(elem, "roles"), "roles");

        validate_audit_rule(rule);
        rules.push_back(std::move(rule));
    }

    return rules;
}

sstring audit_rules_to_json_string(const std::vector<audit_rule>& rules) {
    rjson::value arr = rjson::empty_array();

    for (const auto& rule : rules) {
        rjson::value obj = rjson::empty_object();
        rjson::add_with_string_name(obj, "sinks", string_vec_to_json(rule.sinks));
        rjson::value cat_arr = rjson::empty_array();
        for (auto cat : rule.categories) {
            rjson::push_back(cat_arr, rjson::from_string(category_to_string(cat)));
        }
        rjson::add_with_string_name(obj, "categories", std::move(cat_arr));
        rjson::add_with_string_name(obj, "qualified_table_names", string_vec_to_json(rule.qualified_table_names));
        rjson::add_with_string_name(obj, "roles", string_vec_to_json(rule.roles));
        rjson::push_back(arr, std::move(obj));
    }

    return rjson::print(arr);
}

bool matches_category(const audit_rule& rule, statement_category category) {
    return rule.categories.contains(category);
}

bool matches_table(const audit_rule& rule, std::string_view keyspace, std::string_view table) {
    return matches_qualified_table(rule, qualified_table_name(keyspace, table));
}

static bool matches_any_pattern(const std::vector<sstring>& patterns, std::string_view value) {
    if (patterns.empty()) {
        return false;
    }
    sstring value_s(value);
    for (const auto& pattern : patterns) {
        int rc = fnmatch(pattern.c_str(), value_s.c_str(), FNM_EXTMATCH);
        if (rc == 0) {
            return true;
        }
        if (rc != FNM_NOMATCH) {
            logger.warn("Audit: fnmatch error for pattern '{}' against value '{}' (rc={})",
                        pattern, value, rc);
        }
    }
    return false;
}

bool matches_qualified_table(const audit_rule& rule, std::string_view qualified_table_name) {
    return matches_any_pattern(rule.qualified_table_names, qualified_table_name);
}

bool matches_role(const audit_rule& rule, std::string_view role) {
    return matches_any_pattern(rule.roles, role);
}

audit_sink_set rule_sinks(const audit_rule& rule) {
    audit_sink_set result;
    for (const auto& s : rule.sinks) {
        if (s == "table") {
            result.set(audit_sink::table);
        } else if (s == "syslog") {
            result.set(audit_sink::syslog);
        } else {
            // Should never happen — validate_audit_rule() rejects unknown sinks.
            on_internal_error(logger, fmt::format("Unknown audit sink '{}' in validated rule", s));
        }
    }
    return result;
}

bool matches_rule(const audit_rule& rule, statement_category category,
                  std::string_view keyspace, std::string_view table,
                  std::string_view role) {
    return matches_category(rule, category)
        && (!is_table_scoped_category(category) || keyspace.empty() || matches_table(rule, keyspace, table))
        && matches_role(rule, role);
}

} // namespace audit
