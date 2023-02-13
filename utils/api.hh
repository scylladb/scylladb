/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sstring.hh>

#include "replica/database_fwd.hh"
#include "schema/schema_fwd.hh"

namespace api {

struct table_info {
    seastar::sstring name;
    table_id id;
};

future<> run_on_table(sstring op, replica::database& db, std::string_view keyspace, table_info ti, std::function<future<> (replica::table&)> func);
future<> run_on_existing_tables(sstring op, replica::database& db, std::string_view keyspace, const std::vector<table_info> local_tables, std::function<future<> (replica::table&)> func);

}

namespace std {

std::ostream& operator<<(std::ostream& os, const api::table_info& ti);

} // namespace std
