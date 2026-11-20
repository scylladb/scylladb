/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <atomic>
#include <map>
#include <unordered_map>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "cql3/util.hh"
#include "schema/schema_builder.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"
#include "utils/UUID_gen.hh"
#include "utils/overloaded_functor.hh"

namespace tablet_test_utils {

inline
seastar::future<table_id> add_table(cql_test_env& e, seastar::sstring test_ks_name = "", std::map<seastar::sstring, seastar::sstring> tablet_options = {}) {
    auto id = table_id(utils::UUID_gen::get_time_UUID());
    co_await e.create_table([&] (std::string_view ks_name) {
        if (!test_ks_name.empty()) {
            ks_name = test_ks_name;
        }
        auto builder = schema_builder(seastar::this_smp_shard_count(), ks_name, id.to_sstring(), id)
                .with_column("p1", utf8_type, column_kind::partition_key)
                .with_column("r1", int32_type);
        if (!tablet_options.empty()) {
            builder.set_tablet_options(std::move(tablet_options));
        }

        return *builder.build();
    });
    co_return id;
}

// Run in a seastar thread
inline
seastar::sstring do_add_keyspace(cql_test_env& e, std::unordered_map<seastar::sstring, std::variant<int, std::vector<seastar::sstring>>> dc_rf, int initial_tablets = 0) {
    static std::atomic<int> ks_id = 0;
    auto ks_name = fmt::format("keyspace{}", ks_id.fetch_add(1));

    seastar::sstring rf_options;
    for (auto& [dc, rf] : dc_rf) {
        auto rf_fmt = std::visit(overloaded_functor(
            [] (int rf) { return fmt::format("{}", rf); },
            [] (const std::vector<seastar::sstring>& racks) {
                return fmt::format("[{}]", fmt::join(racks | std::views::transform(&cql3::util::single_quote), ", "));
            }), rf);
        rf_options += fmt::format(", '{}': {}", dc, rf_fmt);
    }

    e.execute_cql(fmt::format("create keyspace {} with replication = {{'class': 'NetworkTopologyStrategy'{}}}"
                              " and tablets = {{'enabled': true, 'initial': {}}}",
                              ks_name, rf_options, initial_tablets)).get();

    return ks_name;
}

// Run in a seastar thread
inline
seastar::sstring add_keyspace(cql_test_env& e, std::unordered_map<seastar::sstring, int> dc_rf, int initial_tablets = 0) {
    std::unordered_map<seastar::sstring, std::variant<int, std::vector<seastar::sstring>>> dc_rf_expanded;
    for (auto& [dc, rf] : dc_rf) {
        dc_rf_expanded[dc] = rf;
    }
    return do_add_keyspace(e, std::move(dc_rf_expanded), initial_tablets);
}

// Run in a seastar thread
inline
seastar::sstring add_keyspace_racks(cql_test_env& e, std::unordered_map<seastar::sstring, std::vector<seastar::sstring>> dc_rf, int initial_tablets = 0) {
    std::unordered_map<seastar::sstring, std::variant<int, std::vector<seastar::sstring>>> dc_rf_expanded;
    for (auto& [dc, rf] : dc_rf) {
        dc_rf_expanded[dc] = rf;
    }
    return do_add_keyspace(e, std::move(dc_rf_expanded), initial_tablets);
}

} // namespace tablet_test_utils
