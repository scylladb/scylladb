/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include "schema/schema_fwd.hh"

namespace replica {
class database;
class table;

class global_table_ptr {
    std::vector<foreign_ptr<lw_shared_ptr<table>>> _p;
    std::vector<foreign_ptr<lw_shared_ptr<table>>> _base; // relevant if _p is view or index
    std::vector<foreign_ptr<std::unique_ptr<std::vector<lw_shared_ptr<table>>>>> _views;
public:
    global_table_ptr();
    global_table_ptr(global_table_ptr&&) noexcept = default;
    void assign(database& db, table_id uuid);
    table* operator->() const noexcept;
    table& operator*() const noexcept;
    std::vector<lw_shared_ptr<table>>& views() const noexcept;
    void clear_views() noexcept;
    table& base() const noexcept;
    auto as_sharded_parameter() {
        return sharded_parameter([this] { return std::ref(**this); });
    }
};

future<global_table_ptr> get_table_on_all_shards(sharded<database>& db, table_id uuid);
future<global_table_ptr> get_table_on_all_shards(sharded<database>& db, sstring ks_name, sstring cf_name);

} // replica namespace
