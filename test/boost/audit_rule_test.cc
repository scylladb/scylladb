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
#include <seastar/testing/thread_test_case.hh>

#include "audit/audit.hh"
#include "audit/audit_rule.hh"
#include "audit/preprocessed_audit_rules.hh"
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

audit::audit_rule role_rule(std::vector<sstring> patterns) {
    return make_rule({"table"}, {}, {}, std::move(patterns));
}

audit::audit_rule table_rule(std::vector<sstring> patterns) {
    return make_rule({"table"}, {}, std::move(patterns), {});
}

bool role_matches(const sstring& pattern, const sstring& name) {
    return audit::matches_role(role_rule({pattern}), name);
}

} // anonymous namespace

namespace audit {

void add_alternator_batch_sink_tables(std::vector<std::pair<audit_sink, audit_table_set>>& sink_tables,
        audit_sink sink, const std::pair<sstring, sstring>& table);

}

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

BOOST_AUTO_TEST_CASE(test_matching_primitives) {
    auto category_rule = make_rule({"table"}, {"DML", "AUTH"});
    BOOST_CHECK(audit::matches_category(category_rule, audit::statement_category::DML));
    BOOST_CHECK(audit::matches_category(category_rule, audit::statement_category::AUTH));
    BOOST_CHECK(!audit::matches_category(category_rule, audit::statement_category::DDL));
    // Empty categories list matches nothing.
    BOOST_CHECK(!audit::matches_category(make_rule({"table"}), audit::statement_category::DML));

    auto tables = table_rule({"ks.t1", "ks.*", "!(system).t"});
    BOOST_CHECK(audit::matches_table(tables, "ks", "t1"));
    BOOST_CHECK(audit::matches_table(tables, "ks", "anything"));
    BOOST_CHECK(audit::matches_table(tables, "user", "t"));
    BOOST_CHECK(!audit::matches_table(tables, "system", "t"));
    // Empty table list matches nothing.
    BOOST_CHECK(!audit::matches_table(table_rule({}), "ks", "t"));

    BOOST_CHECK(role_matches("admin", "admin"));
    BOOST_CHECK(role_matches("admin_*", "admin_read"));
    BOOST_CHECK(role_matches("user?", "user1"));
    BOOST_CHECK(role_matches("user[0-9]", "user9"));
    BOOST_CHECK(role_matches("admin\\*", "admin*"));
    BOOST_CHECK(role_matches("domain\\\\user", "domain\\user"));
    BOOST_CHECK(role_matches("@(admin|root)_*", "root_full"));
    BOOST_CHECK(!role_matches("!(guest)", "guest"));
    // Trailing backslash: incomplete escape.
    BOOST_CHECK(!role_matches("admin\\", "admin"));
}

BOOST_AUTO_TEST_CASE(test_rule_matching_and_sinks) {
    auto rule = make_rule({"table", "syslog"}, {"DML", "AUTH"}, {"ks.t1"}, {"admin_*"});
    BOOST_CHECK(audit::matches_rule(rule, audit::statement_category::DML, "ks", "t1", "admin_read"));
    BOOST_CHECK(!audit::matches_rule(rule, audit::statement_category::DML, "ks", "t2", "admin_read"));
    BOOST_CHECK(!audit::matches_rule(rule, audit::statement_category::DML, "ks", "t1", "viewer"));

    // DML with empty keyspace bypasses table matching.
    BOOST_CHECK(audit::matches_rule(rule, audit::statement_category::DML, "", "tbl1|tbl2", "admin_read"));
    BOOST_CHECK(!audit::matches_rule(rule, audit::statement_category::DML, "", "tbl1|tbl2", "viewer"));

    // AUTH ignores table fields.
    BOOST_CHECK(audit::matches_rule(rule, audit::statement_category::AUTH, "", "", "admin_read"));
    BOOST_CHECK(audit::matches_rule(rule, audit::statement_category::AUTH, "wrong", "tbl", "admin_read"));

    BOOST_CHECK(audit::matches_rule(make_rule({"table"}, {"ADMIN"}, {"ks.t1"}, {"ops"}), audit::statement_category::ADMIN, "any", "table", "ops"));
    BOOST_CHECK(audit::matches_rule(make_rule({"table"}, {"DCL"}, {"ks.t1"}, {"admin"}), audit::statement_category::DCL, "", "", "admin"));
    // No filters means the rule matches nothing.
    BOOST_CHECK(!audit::matches_rule(make_rule({"table"}), audit::statement_category::DML, "ks", "t", "admin"));

    auto sinks = audit::rule_sinks(rule);
    BOOST_CHECK(sinks.contains(audit::audit_sink::table));
    BOOST_CHECK(sinks.contains(audit::audit_sink::syslog));
}

BOOST_AUTO_TEST_CASE(test_hash_consistency_for_transparent_lookups) {
    sstring s = "test_keyspace";
    std::string_view sv = s;
    BOOST_REQUIRE_EQUAL(std::hash<sstring>{}(s), std::hash<std::string_view>{}(sv));

    auto pair_ss = std::make_pair(sstring("ks"), sstring("tbl"));
    auto pair_sv = std::make_pair(std::string_view("ks"), std::string_view("tbl"));
    utils::tuple_hash h;
    BOOST_REQUIRE_EQUAL(h(pair_ss), h(pair_sv));
}

SEASTAR_THREAD_TEST_CASE(test_preprocessed_rules_match_fast_and_slow_paths) {
    audit::preprocessed_audit_rules rules({
            make_rule({"table"}, {"DML", "AUTH"}, {"ks.*"}, {"admin_*"}),
            make_rule({"syslog"}, {"DDL"}, {"ks.t2"}, {"admin_*"}),
    });
    rules.replace_known_entities({"admin_read", "viewer"}, {{"ks", "t1"}, {"ks", "t2"}}).get();

    BOOST_CHECK(rules.matching_sinks(audit::statement_category::DML, "ks", "t1", "admin_read").contains(audit::audit_sink::table));
    BOOST_CHECK(!rules.matching_sinks(audit::statement_category::DML, "ks", "t1", "viewer"));
    BOOST_CHECK(rules.matching_sinks(audit::statement_category::DML, "ks", "unknown", "admin_new").contains(audit::audit_sink::table));
    BOOST_CHECK(rules.matching_sinks(audit::statement_category::AUTH, "wrong", "tbl", "admin_read").contains(audit::audit_sink::table));

    auto ddl_sinks = rules.matching_sinks(audit::statement_category::DDL, "ks", "t2", "admin_read");
    BOOST_CHECK(ddl_sinks.contains(audit::audit_sink::syslog));
    BOOST_CHECK(!ddl_sinks.contains(audit::audit_sink::table));

    // DML with empty keyspace bypasses table matching.
    // Known role (fast path):
    BOOST_CHECK(rules.matching_sinks(audit::statement_category::DML, "", "tbl1|tbl2", "admin_read").contains(audit::audit_sink::table));
    BOOST_CHECK(!rules.matching_sinks(audit::statement_category::DML, "", "tbl1|tbl2", "viewer"));
    // Unknown role (slow path):
    BOOST_CHECK(rules.matching_sinks(audit::statement_category::DML, "", "tbl1|tbl2", "admin_new").contains(audit::audit_sink::table));
    BOOST_CHECK(!rules.matching_sinks(audit::statement_category::DML, "", "tbl1|tbl2", "other_user"));
}

SEASTAR_THREAD_TEST_CASE(test_preprocessed_rules_update_known_entities) {
    audit::preprocessed_audit_rules rules({make_rule({"table"}, {"DML"}, {"ks.t1"}, {"admin"})});
    rules.replace_known_entities({"admin"}, {{"ks", "t1"}}).get();

    BOOST_CHECK(rules.matching_sinks(audit::statement_category::DML, "ks", "t1", "admin"));

    // Removed from cache; falls back to slow-path glob.
    rules.remove_known_table("ks", "t1");
    BOOST_CHECK(rules.matching_sinks(audit::statement_category::DML, "ks", "t1", "admin"));

    // Same for role removal.
    rules.remove_known_role("admin");
    BOOST_CHECK(rules.matching_sinks(audit::statement_category::DML, "ks", "t1", "admin"));

    rules.add_known_role("admin");
    rules.add_known_table("ks", "t1");
    BOOST_CHECK(rules.matching_sinks(audit::statement_category::DML, "ks", "t1", "admin"));

    // After rule replacement, old criteria no longer match.
    rules.refresh_rules({make_rule({"syslog"}, {"DDL"}, {"ks.t2"}, {"viewer"})}).get();
    BOOST_CHECK(!rules.matching_sinks(audit::statement_category::DML, "ks", "t1", "admin"));
    BOOST_CHECK(rules.matching_sinks(audit::statement_category::DDL, "ks", "t2", "viewer").contains(audit::audit_sink::syslog));
}

SEASTAR_THREAD_TEST_CASE(test_preprocessed_empty_rules_skip_cache) {
    audit::preprocessed_audit_rules rules;
    rules.replace_known_entities({"alice", "bob"}, {{"ks", "t1"}, {"ks", "t2"}}).get();
    BOOST_CHECK(!rules.matching_sinks(audit::statement_category::DML, "ks", "t1", "alice"));

    rules.add_known_role("charlie");
    rules.add_known_table("ks", "t3");
    BOOST_CHECK(!rules.matching_sinks(audit::statement_category::DML, "ks", "t3", "charlie"));

    rules.refresh_rules({make_rule({"table"}, {"DML"}, {"ks.*"}, {"*"})}).get();
    BOOST_CHECK(rules.matching_sinks(audit::statement_category::DML, "ks", "t1", "alice"));
    BOOST_CHECK(rules.matching_sinks(audit::statement_category::DML, "ks", "t3", "charlie"));
}

BOOST_AUTO_TEST_CASE(test_alternator_batch_sink_tables_aggregate_per_sink) {
    std::vector<std::pair<audit::audit_sink, audit::audit_table_set>> sink_tables;
    audit::add_alternator_batch_sink_tables(sink_tables, audit::audit_sink::table, {"ks", "table_only"});
    audit::add_alternator_batch_sink_tables(sink_tables, audit::audit_sink::table, {"ks", "both_sinks"});
    audit::add_alternator_batch_sink_tables(sink_tables, audit::audit_sink::syslog, {"ks", "both_sinks"});

    BOOST_REQUIRE_EQUAL(sink_tables.size(), 2u);
    BOOST_CHECK(sink_tables[0].first == audit::audit_sink::table);
    BOOST_CHECK_EQUAL(sink_tables[0].second.size(), 2u);
    BOOST_CHECK(sink_tables[0].second.contains({"ks", "table_only"}));
    BOOST_CHECK(sink_tables[0].second.contains({"ks", "both_sinks"}));
    BOOST_CHECK(sink_tables[1].first == audit::audit_sink::syslog);
    BOOST_CHECK_EQUAL(sink_tables[1].second.size(), 1u);
    BOOST_CHECK(sink_tables[1].second.contains({"ks", "both_sinks"}));
}
