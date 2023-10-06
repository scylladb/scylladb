/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
public:
    global_table_ptr();
    global_table_ptr(global_table_ptr&&) noexcept;
    ~global_table_ptr();
    void assign(table& t);
    table* operator->() const noexcept;
    table& operator*() const noexcept;
    auto as_sharded_parameter() {
        return sharded_parameter([this] { return std::ref(**this); });
    }
};

future<global_table_ptr> get_table_on_all_shards(sharded<database>& db, table_id uuid);
future<global_table_ptr> get_table_on_all_shards(sharded<database>& db, sstring ks_name, sstring cf_name);

} // replica namespace
