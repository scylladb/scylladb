/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>

#include <boost/program_options.hpp>

#include <seastar/core/sstring.hh>
#include <seastar/json/json_elements.hh>

#include "audit/audit.hh"
#include "audit/audit_rule.hh"
#include "db/config.hh"

using namespace seastar;

namespace bpo = boost::program_options;

namespace {

audit::audit_rule make_rule(std::vector<sstring> sinks,
                            std::vector<sstring> categories = {},
                            std::vector<sstring> tables = {},
                            std::vector<sstring> roles = {}) {
    return {.sinks = std::move(sinks), .categories = audit::parse_categories(categories),
            .qualified_table_names = std::move(tables), .roles = std::move(roles)};
}

} // anonymous namespace

BOOST_AUTO_TEST_CASE(test_parse_audit_rules_json) {
    auto rules = audit::parse_audit_rules_from_json(R"([
        {"sinks":["table"],"categories":["DML","DDL"],"qualified_table_names":["ks.t1"],"roles":["admin"]},
        {"sinks":["syslog"],"categories":["AUTH"],"qualified_table_names":[],"roles":["*"]}
    ])");

    BOOST_REQUIRE_EQUAL(rules.size(), 2u);
    BOOST_CHECK_EQUAL(rules[0].sinks[0], "table");
    BOOST_CHECK(rules[0].categories.contains(audit::statement_category::DML));
    BOOST_CHECK(rules[0].categories.contains(audit::statement_category::DDL));
    BOOST_CHECK_EQUAL(rules[0].qualified_table_names[0], "ks.t1");
    BOOST_CHECK_EQUAL(rules[0].roles[0], "admin");
    BOOST_CHECK_EQUAL(rules[1].sinks[0], "syslog");
    BOOST_CHECK(rules[1].qualified_table_names.empty());

    BOOST_CHECK(audit::parse_audit_rules_from_json("[]").empty());
    BOOST_CHECK(audit::parse_audit_rules_from_json("").empty());

    BOOST_CHECK_THROW(audit::parse_audit_rules_from_json("{not json"), audit::audit_exception);
    // Top-level object instead of array.
    BOOST_CHECK_THROW(audit::parse_audit_rules_from_json(R"({"sinks":["table"]})"), audit::audit_exception);
    // Array element is not an object.
    BOOST_CHECK_THROW(audit::parse_audit_rules_from_json(R"(["not an object"])"), audit::audit_exception);

    // Each required field missing in turn.
    BOOST_CHECK_THROW(audit::parse_audit_rules_from_json(R"([{"categories":["DML"],"qualified_table_names":[],"roles":[]}])"), audit::audit_exception);
    BOOST_CHECK_THROW(audit::parse_audit_rules_from_json(R"([{"sinks":["table"],"qualified_table_names":[],"roles":[]}])"), audit::audit_exception);
    BOOST_CHECK_THROW(audit::parse_audit_rules_from_json(R"([{"sinks":["table"],"categories":["DML"],"roles":[]}])"), audit::audit_exception);
    BOOST_CHECK_THROW(audit::parse_audit_rules_from_json(R"([{"sinks":["table"],"categories":["DML"],"qualified_table_names":[]}])"), audit::audit_exception);

    // Unknown fields are silently ignored.
    rules = audit::parse_audit_rules_from_json(
            R"([{"sinks":["table"],"categories":["DML"],"qualified_table_names":[],"roles":[],"description":"ignored"}])");
    BOOST_REQUIRE_EQUAL(rules.size(), 1u);
}

BOOST_AUTO_TEST_CASE(test_validate_audit_rule) {
    BOOST_CHECK_NO_THROW(audit::validate_audit_rule(
        make_rule({"table", "syslog"}, {"QUERY", "DML", "DDL", "DCL", "AUTH", "ADMIN"},
                  {"ks.table", "ks.*", "!(system).*", "ks.table.extra", "", "table_only"},
                  {"admin", "admin_*", "user?", "user[0-9]", "admin\\*", "role\\?", "@(reader|writer)", "admin\\"})));
    BOOST_CHECK_NO_THROW(audit::validate_audit_rule(make_rule({"table"})));
    // Duplicate sinks are accepted.
    BOOST_CHECK_NO_THROW(audit::validate_audit_rule(make_rule({"table", "table"})));

    // No sinks.
    BOOST_CHECK_THROW(audit::validate_audit_rule(make_rule({}, {"DML"})), audit::audit_exception);
    // Unsupported sink.
    BOOST_CHECK_THROW(audit::validate_audit_rule(make_rule({"kafka"}, {"DML"})), audit::audit_exception);
    // Invalid category values.
    BOOST_CHECK_THROW(audit::validate_audit_rule(make_rule({"table"}, {"INVALID"})), audit::audit_exception);
    BOOST_CHECK_THROW(audit::validate_audit_rule(make_rule({"table"}, {"dml"})), audit::audit_exception);
    BOOST_CHECK_THROW(audit::validate_audit_rule(make_rule({"table"}, {"D*"})), audit::audit_exception);
    BOOST_CHECK_THROW(audit::validate_audit_rule(make_rule({"table"}, {""})), audit::audit_exception);
    BOOST_CHECK_THROW(audit::validate_audit_rule(make_rule({"table"}, {"[DA]*"})), audit::audit_exception);
}

BOOST_AUTO_TEST_CASE(test_json_round_trip) {
    std::vector<audit::audit_rule> rules = {
        make_rule({"table", "syslog"}, {"DML", "DDL"}, {"ks.t1"}, {"admin_*"}),
        make_rule({"syslog"}, {"AUTH"}, {}, {"admin\\*", "domain\\\\user", "user[0-9]", "test?"}),
    };

    auto parsed = audit::parse_audit_rules_from_json(audit::audit_rules_to_json_string(rules));
    BOOST_REQUIRE_EQUAL(parsed.size(), rules.size());
    BOOST_CHECK(parsed[0] == rules[0]);
    BOOST_CHECK(parsed[1] == rules[1]);

    BOOST_CHECK_EQUAL(audit::audit_rules_to_json_string({}), "[]");
}

BOOST_AUTO_TEST_CASE(test_config_audit_rules_yaml) {
    db::config cfg;
    BOOST_CHECK(cfg.audit_rules().empty());

    cfg.read_from_yaml(
        "audit_rules:\n"
        "  - sinks: [table]\n"
        "    categories: [DML, DDL]\n"
        "    qualified_table_names: [ks.t1]\n"
        "    roles: ['admin\\*', 'domain\\\\user', 'user[0-9]', '!(guest)']\n");

    BOOST_CHECK(cfg.audit_rules.source() == utils::config_file::config_source::SettingsFile);
    BOOST_REQUIRE_EQUAL(cfg.audit_rules().size(), 1u);
    BOOST_CHECK(cfg.audit_rules()[0].categories.contains(audit::statement_category::DML));
    BOOST_CHECK(cfg.audit_rules()[0].categories.contains(audit::statement_category::DDL));
    BOOST_CHECK_EQUAL(cfg.audit_rules()[0].roles[0], "admin\\*");

    auto parsed = audit::parse_audit_rules_from_json(cfg.audit_rules.value_as_json()._res);
    BOOST_REQUIRE_EQUAL(parsed.size(), 1u);
    BOOST_CHECK(parsed[0] == cfg.audit_rules()[0]);
}

BOOST_AUTO_TEST_CASE(test_config_audit_rules_cql) {
    db::config cfg;

    std::vector<audit::audit_rule> observed;
    auto observer = cfg.audit_rules.observe([&observed] (const std::vector<audit::audit_rule>& rules) {
        observed = rules;
    });
    BOOST_CHECK(cfg.audit_rules.set_value(
        R"([{"sinks":["syslog"],"categories":["AUTH"],"qualified_table_names":[],"roles":["*"]}])",
        utils::config_file::config_source::CQL));
    BOOST_CHECK(cfg.audit_rules.source() == utils::config_file::config_source::CQL);
    BOOST_REQUIRE_EQUAL(observed.size(), 1u);
    BOOST_CHECK_EQUAL(observed[0].sinks[0], "syslog");
    BOOST_CHECK(observed[0].categories.contains(audit::statement_category::AUTH));
}

BOOST_AUTO_TEST_CASE(test_config_audit_rules_cli) {
    db::config cfg;

    auto desc = cfg.get_options_description();
    bpo::variables_map vm;
    const char* argv[] = {"test", "--audit-rules", R"([{"sinks":["table"],"categories":["DML"],"qualified_table_names":["ks.t1"],"roles":["*"]}])"};
    bpo::store(bpo::parse_command_line(3, argv, desc), vm);
    bpo::notify(vm);
    BOOST_REQUIRE_EQUAL(cfg.audit_rules().size(), 1u);
    BOOST_CHECK_EQUAL(cfg.audit_rules()[0].qualified_table_names[0], "ks.t1");
}

BOOST_AUTO_TEST_CASE(test_config_audit_rules_rejects_invalid_values) {
    db::config cfg;
    BOOST_CHECK_THROW(
        cfg.read_from_yaml(
            "audit_rules:\n"
            "  - sinks: [kafka]\n"
            "    categories: [DML]\n"
            "    qualified_table_names: []\n"
            "    roles: []\n"),
        std::exception);

    // Each required field missing in turn.
    BOOST_CHECK_THROW(cfg.read_from_yaml(
        "audit_rules:\n"
        "  - categories: [DML]\n"
        "    qualified_table_names: []\n"
        "    roles: []\n"), std::exception);
    BOOST_CHECK_THROW(cfg.read_from_yaml(
        "audit_rules:\n"
        "  - sinks: [table]\n"
        "    qualified_table_names: []\n"
        "    roles: []\n"), std::exception);
    BOOST_CHECK_THROW(cfg.read_from_yaml(
        "audit_rules:\n"
        "  - sinks: [table]\n"
        "    categories: [DML]\n"
        "    roles: []\n"), std::exception);
    BOOST_CHECK_THROW(cfg.read_from_yaml(
        "audit_rules:\n"
        "  - sinks: [table]\n"
        "    categories: [DML]\n"
        "    qualified_table_names: []\n"), std::exception);
    // Scalar value where array is expected.
    BOOST_CHECK_THROW(cfg.read_from_yaml(
        "audit_rules:\n"
        "  - sinks: table\n"
        "    categories: [DML]\n"
        "    qualified_table_names: []\n"
        "    roles: []\n"), std::exception);
    BOOST_CHECK_THROW(cfg.read_from_yaml(
        "audit_rules:\n"
        "  - sinks: [table]\n"
        "    categories: DML\n"
        "    qualified_table_names: []\n"
        "    roles: []\n"), std::exception);
    BOOST_CHECK_THROW(cfg.read_from_yaml(
        "audit_rules:\n"
        "  - sinks: [table]\n"
        "    categories: [DML]\n"
        "    qualified_table_names: ks.t1\n"
        "    roles: []\n"), std::exception);
    BOOST_CHECK_THROW(cfg.read_from_yaml(
        "audit_rules:\n"
        "  - sinks: [table]\n"
        "    categories: [DML]\n"
        "    qualified_table_names: []\n"
        "    roles: '*'\n"), std::exception);

    BOOST_CHECK_THROW(cfg.audit_rules.set_value(sstring("{not json}"), utils::config_file::config_source::CQL), std::exception);
}
