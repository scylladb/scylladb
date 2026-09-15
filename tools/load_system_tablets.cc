/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "tools/load_system_tablets.hh"

#include <seastar/core/thread.hh>
#include <seastar/util/closeable.hh>

#include "utils/log.hh"
#include "db/system_keyspace.hh"
#include "mutation/mutation.hh"
#include "readers/combined.hh"
#include "replica/tablets.hh"
#include "tools/read_mutation.hh"
#include "types/list.hh"
#include "types/tuple.hh"

namespace tools {

future<tablets_t> load_system_tablets(const db::config& dbcfg,
                                      std::filesystem::path scylla_data_path,
                                      table_id table,
                                      reader_permit permit,
                                      std::optional<std::filesystem::path> tablets_directory) {
    tablets_t tablets;
    co_await query_system_table_offline(dbcfg, scylla_data_path, db::system_keyspace::tablets(),
            {data_value(table.uuid())}, std::nullopt, permit,
            [&tablets] (const query::result_set_row& row) {
                auto last_token = row.get_nonnull<int64_t>("last_token");
                auto replica_set = row.get_data_value("replicas");
                if (!replica_set) {
                    return;
                }
                tablets.emplace(last_token, replica::tablet_replica_set_from_cell(*replica_set));
            },
            std::move(tablets_directory));
    if (tablets.empty()) {
        throw std::runtime_error(fmt::format("failed to find tablets for table {}", table));
    }
    co_return tablets;
}

} // namespace tools
