/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "audit/preprocessed_audit_rules.hh"
#include "audit/audit_rule.hh"

#include <seastar/coroutine/maybe_yield.hh>

namespace audit {

preprocessed_audit_rules::preprocessed_audit_rules(std::vector<audit_rule> rules) noexcept
    : _rules(std::move(rules))
{ }

future<> preprocessed_audit_rules::refresh_rules(std::vector<audit_rule> rules) {
    _rules = std::move(rules);
    if (!wants_eager_known_tables(_known_tables.size())) {
        _known_tables.clear();
        _known_table_cache_enabled = false;
    } else {
        _known_table_cache_enabled = true;
    }
    _role_to_matching_rules.clear();
    _table_to_matching_rules.clear();
    ++_cache_generation;
    co_await rebuild_cache();
}

void preprocessed_audit_rules::add_known_role(const sstring& role) {
    auto [it, inserted] = _known_roles.insert(role);
    if (inserted) {
        ++_cache_generation;
        _role_to_matching_rules[role] = compute_role_bits(_rules, role);
    }
}

void preprocessed_audit_rules::remove_known_role(const sstring& role) {
    if (_known_roles.erase(role)) {
        ++_cache_generation;
        _role_to_matching_rules.erase(role);
    }
}

void preprocessed_audit_rules::add_known_table(const sstring& keyspace, const sstring& table) {
    if (!_known_table_cache_enabled || _rules.empty()) {
        return;
    }
    if (_known_tables.size() >= max_preprocessed_known_tables) {
        _known_tables.clear();
        _table_to_matching_rules.clear();
        _known_table_cache_enabled = false;
        ++_cache_generation;
        return;
    }
    auto [it, inserted] = _known_tables.emplace(keyspace, table);
    if (inserted) {
        ++_cache_generation;
        _table_to_matching_rules[known_table{keyspace, table}] = compute_table_bits(_rules, keyspace, table);
    }
}

void preprocessed_audit_rules::remove_known_table(const sstring& keyspace, const sstring& table) {
    if (!_known_table_cache_enabled) {
        return;
    }
    if (_known_tables.erase(known_table{keyspace, table})) {
        ++_cache_generation;
        _table_to_matching_rules.erase(known_table{keyspace, table});
    }
}

preprocessed_audit_rules::rule_bitset
preprocessed_audit_rules::compute_role_bits(const std::vector<audit_rule>& rules, const sstring& role) const {
    rule_bitset bits(rules.size());
    for (size_t i = 0; i < rules.size(); ++i) {
        if (matches_role(rules[i], role)) {
            bits.set(i);
        }
    }
    return bits;
}

preprocessed_audit_rules::rule_bitset
preprocessed_audit_rules::compute_table_bits(const std::vector<audit_rule>& rules, const sstring& keyspace, const sstring& table) const {
    rule_bitset bits(rules.size());
    sstring qt = qualified_table_name(keyspace, table);
    for (size_t i = 0; i < rules.size(); ++i) {
        if (matches_qualified_table(rules[i], qt)) {
            bits.set(i);
        }
    }
    return bits;
}

audit_sink_set preprocessed_audit_rules::collect_sinks(const rule_bitset& bits,
        statement_category category) const {
    audit_sink_set result;
    for (auto i = bits.find_first(); i != rule_bitset::npos; i = bits.find_next(i)) {
        const auto& rule = _rules[i];
        if (matches_category(rule, category)) {
            result.add(rule_sinks(rule));
        }
    }
    return result;
}

future<> preprocessed_audit_rules::rebuild_cache() {
    // Retry loop: if rules, roles, or tables change while we yield during
    // the compute phase, the generation counter will have advanced and we
    // discard the stale result and rebuild from the updated snapshot.
    while (true) {
        // Snapshot current state to detect concurrent modifications after yielding.
        // These copies are lightweight (just names, not schema objects) and bounded
        // by the number of audit rules, roles, and tables — expected to be modest.
        auto rules = _rules;
        auto known_roles = _known_roles;
        auto known_table_cache_enabled = _known_table_cache_enabled;
        auto known_tables = known_table_cache_enabled ? _known_tables : known_table_set{};
        auto generation = _cache_generation;

        std::unordered_map<sstring, rule_bitset, sstring_hash, sstring_eq>
            role_to_matching_rules;
        std::unordered_map<known_table, rule_bitset, utils::tuple_hash, std::equal_to<>>
            table_to_matching_rules;

        // Precompute per-entity rule bitsets. Each iteration invokes fnmatch
        // for every rule pattern, making this more expensive than the copies above.
        if (!rules.empty()) {
            for (const auto& role : known_roles) {
                role_to_matching_rules[role] = compute_role_bits(rules, role);
                co_await coroutine::maybe_yield();
            }
            if (known_table_cache_enabled) {
                for (const auto& [ks, tbl] : known_tables) {
                    table_to_matching_rules[known_table{ks, tbl}] = compute_table_bits(rules, ks, tbl);
                    co_await coroutine::maybe_yield();
                }
            }
        }

        if (generation == _cache_generation) {
            _role_to_matching_rules = std::move(role_to_matching_rules);
            _table_to_matching_rules = std::move(table_to_matching_rules);
            co_return;
        }
    }
}

future<> preprocessed_audit_rules::replace_known_entities(std::unordered_set<sstring> roles, known_table_set tables) {
    _known_roles = std::move(roles);
    _known_table_cache_enabled = wants_eager_known_tables(tables.size());
    if (_known_table_cache_enabled) {
        _known_tables = std::move(tables);
    } else {
        _known_tables.clear();
    }
    _role_to_matching_rules.clear();
    _table_to_matching_rules.clear();
    ++_cache_generation;
    co_await rebuild_cache();
}

bool preprocessed_audit_rules::wants_eager_known_tables(size_t table_count) const noexcept {
    return !_rules.empty() && table_count <= max_preprocessed_known_tables;
}

audit_sink_set preprocessed_audit_rules::matching_sinks(statement_category category,
                                                         std::string_view keyspace,
                                                         std::string_view table,
                                                         std::string_view role) const {
    bool table_scoped = is_table_scoped_category(category);

    // Look up role in the precomputed map.
    auto role_it = _role_to_matching_rules.find(role);
    if (role_it == _role_to_matching_rules.end()) {
        // Unknown role — slow path: evaluate all rules with fnmatch.
        audit_sink_set result;
        for (const auto& rule : _rules) {
            if (matches_rule(rule, category, keyspace, table, role)) {
                result.add(rule_sinks(rule));
            }
        }
        return result;
    }

    if (!table_scoped || keyspace.empty()) {
        // Table-independent categories (AUTH, ADMIN, DCL) or operations with
        // empty keyspace: only role matching matters.
        return collect_sinks(role_it->second, category);
    }

    // Table-scoped categories (DML, DDL, QUERY): intersect role and table bitsets.
    auto table_it = _table_to_matching_rules.find(
        std::pair<std::string_view, std::string_view>{keyspace, table});
    if (table_it == _table_to_matching_rules.end()) {
        // Unknown table — slow path: evaluate all rules with fnmatch.
        audit_sink_set result;
        for (const auto& rule : _rules) {
            if (matches_rule(rule, category, keyspace, table, role)) {
                result.add(rule_sinks(rule));
            }
        }
        return result;
    }

    // Fast path: intersect precomputed bitsets and check category.
    // Iterates inline instead of using operator& to avoid heap-allocating a temporary dynamic_bitset.
    audit_sink_set result;
    const auto& role_bits = role_it->second;
    const auto& table_bits = table_it->second;
    for (auto i = role_bits.find_first(); i != rule_bitset::npos; i = role_bits.find_next(i)) {
        if (table_bits.test(i) && matches_category(_rules[i], category)) {
            result.add(rule_sinks(_rules[i]));
        }
    }
    return result;
}

} // namespace audit
