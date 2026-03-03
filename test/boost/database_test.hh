
/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "db/snapshot-ctl.hh"
#include "db/config.hh"
#include "test/lib/cql_test_env.hh"
#include "db/view/view_builder.hh"


// Snapshot tests and their helpers
// \param func: function to be called back, in a seastar thread.
inline future<> do_with_some_data_in_thread(std::vector<sstring> cf_names, std::function<void (cql_test_env&)> func, bool create_mvs = false, shared_ptr<db::config> db_cfg_ptr = {}, size_t num_keys = 2) {
    return seastar::async([cf_names = std::move(cf_names), func = std::move(func), create_mvs,  db_cfg_ptr = std::move(db_cfg_ptr), num_keys] () mutable {
        lw_shared_ptr<tmpdir> tmpdir_for_data;
        if (!db_cfg_ptr) {
            tmpdir_for_data = make_lw_shared<tmpdir>();
            db_cfg_ptr = make_shared<db::config>();
            db_cfg_ptr->data_file_directories(std::vector<sstring>({ tmpdir_for_data->path().string() }));
        }
        do_with_cql_env_thread([cf_names = std::move(cf_names), func = std::move(func), create_mvs, num_keys] (cql_test_env& e) {
            for (const auto& cf_name : cf_names) {
                e.create_table([&cf_name] (std::string_view ks_name) {
                    return *schema_builder(ks_name, cf_name)
                            .with_column("p1", utf8_type, column_kind::partition_key)
                            .with_column("c1", int32_type, column_kind::clustering_key)
                            .with_column("c2", int32_type, column_kind::clustering_key)
                            .with_column("r1", int32_type)
                            .build();
                }).get();
                auto stmt = e.prepare(fmt::format("insert into {} (p1, c1, c2, r1) values (?, ?, ?, ?)", cf_name)).get();
                auto make_key = [] (int64_t k) {
                    std::string s = fmt::format("key{}", k);
                    return cql3::raw_value::make_value(utf8_type->decompose(s));
                };
                auto make_val = [] (int64_t x) {
                    return cql3::raw_value::make_value(int32_type->decompose(int32_t{x}));
                };
                for (size_t i = 0; i < num_keys; ++i) {
                    auto key = tests::random::get_int<int32_t>(1, 1000000);
                    e.execute_prepared(stmt, {make_key(key), make_val(key), make_val(key + 1), make_val(key + 2)}).get();
                    e.execute_prepared(stmt, {make_key(key), make_val(key + 1), make_val(key + 1), make_val(key + 2)}).get();
                    e.execute_prepared(stmt, {make_key(key), make_val(key + 2), make_val(key + 1), make_val(key + 2)}).get();
                }

                if (create_mvs) {
                    auto f1 = e.local_view_builder().wait_until_built("ks", seastar::format("view_{}", cf_name));
                    e.execute_cql(seastar::format("create materialized view view_{0} as select * from {0} where p1 is not null and c1 is not null and c2 is "
                                                  "not null primary key (p1, c1, c2)",
                                                  cf_name))
                        .get();
                    f1.get();

                    auto f2 = e.local_view_builder().wait_until_built("ks", "index_cf_index");
                    e.execute_cql(seastar::format("CREATE INDEX index_{0} ON {0} (r1);", cf_name)).get();
                    f2.get();
                }
            }

            func(e);
        }, db_cfg_ptr).get();
    });
}

inline future<> take_snapshot(cql_test_env& e, sstring ks_name = "ks", sstring cf_name = "cf", sstring snapshot_name = "test", db::snapshot_options opts = {}) {
    try {
        auto uuid = e.db().local().find_uuid(ks_name, cf_name);
        co_await replica::database::snapshot_table_on_all_shards(e.db(), uuid, snapshot_name, opts);
    } catch (...) {
        testlog.error("Could not take snapshot for {}.{} snapshot_name={} skip_flush={}: {}",
                ks_name, cf_name, snapshot_name, opts.skip_flush, std::current_exception());
        throw;
    }
}
