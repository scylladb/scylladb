/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "tools/load_system_tablets.hh"

#include <algorithm>
#include <seastar/core/thread.hh>
#include <seastar/util/closeable.hh>

#include "log.hh"
#include "partition_slice_builder.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include "db/cql_type_parser.hh"
#include "db/system_keyspace.hh"
#include "mutation/mutation.hh"
#include "readers/combined.hh"
#include "reader_concurrency_semaphore.hh"
#include "tools/read_mutation.hh"
#include "tools/sstable_manager_service.hh"
#include "types/list.hh"
#include "types/tuple.hh"

namespace {

logging::logger logger{"load_sys_tablets"};


future<std::filesystem::path> get_table_directory(std::filesystem::path scylla_data_path,
                                                  std::string_view keyspace_name,
                                                  std::string_view table_name) {
    logger.trace("get_table_directory: {}.{} in {}", keyspace_name, table_name, scylla_data_path);
    auto system_tables_path = scylla_data_path / keyspace_name;
    auto system_tables_dir = co_await open_directory(system_tables_path.native());
    sstring found;
    // locate a directory named like
    //   "$scylla_data_path/system/tablets-fd4f7a4696bd3e7391bf99eb77e82a5c"
    auto h = system_tables_dir.list_directory([&] (directory_entry de) -> future<> {
        if (!found.empty()) {
            return make_ready_future();
        }

        auto dash_pos = de.name.find_last_of('-');
        if (dash_pos == de.name.npos) {
            // unlikely. but this should not be fatal
            return make_ready_future();
        }
        if (de.name.substr(0, dash_pos) != table_name) {
            return make_ready_future();
        }
        if (!de.type) {
            throw std::runtime_error(fmt::format("failed to load system.tablets from {}/{}: unrecognized type", scylla_data_path, de.name));
        }
        if (*de.type != directory_entry_type::directory) {
            throw std::runtime_error(fmt::format("failed to load system.tablets from {}/{}: not a directory", scylla_data_path, de.name));
        }
        found = de.name;
        return make_ready_future();
    });
    co_await h.done();

    if (found.empty()) {
        throw std::runtime_error(fmt::format("failed to load system.tablets from {}: couldn't find table directory", scylla_data_path));
    }
    co_return system_tables_path / found.c_str();
}

future<utils::UUID> get_table_id(std::filesystem::path scylla_data_path,
                                 std::string_view keyspace_name,
                                 std::string_view table_name) {
    auto path = co_await get_table_directory(scylla_data_path,
                                             keyspace_name,
                                             table_name);
    // the part after "-" is the string representation of the table_id
    //   "$scylla_data_path/system/tablets-fd4f7a4696bd3e7391bf99eb77e82a5c"
    auto fn = path.filename().native();
    auto dash_pos = fn.find_last_of('-');
    assert(dash_pos != fn.npos);
    if (dash_pos == fn.size()) {
        throw std::runtime_error(fmt::format("failed parse system.tablets path {}: bad path", path));
    }
    co_return utils::UUID{fn.substr(dash_pos + 1).c_str()};
}

locator::tablet_replica_set
replica_set_from_row(const query::result_set_row& row, std::string_view name) {
    std::vector<data_value> column = row.get_nonnull<const list_type_impl::native_type&>(sstring{name});
    locator::tablet_replica_set replica_set;
    for (auto& v : column) {
        std::vector<data_value> replica = value_cast<tuple_type_impl::native_type>(v);
        auto host = value_cast<utils::UUID>(replica[0]);
        auto shard = value_cast<int>(replica[1]);
        replica_set.emplace_back(locator::host_id{host}, shard);
    }
    return replica_set;
}

tools::tablets_t do_load_system_tablets(const db::config& dbcfg,
                                        std::filesystem::path scylla_data_path,
                                        std::string_view keyspace_name,
                                        std::string_view table_name,
                                        reader_permit permit) {
    sharded<sstable_manager_service> sst_man;
    sst_man.start(std::ref(dbcfg)).get();
    auto stop_sst_man_service = deferred_stop(sst_man);

    auto schema = db::system_keyspace::tablets();
    auto tablets_table_directory = get_table_directory(scylla_data_path,
                                                       db::system_keyspace::NAME,
                                                       schema->cf_name()).get();
    auto table_id = get_table_id(scylla_data_path, keyspace_name, table_name).get();
    auto mut = read_mutation_from_table_offline(sst_man,
                                                permit,
                                                tablets_table_directory,
                                                db::system_keyspace::NAME,
                                                db::system_keyspace::tablets,
                                                data_value(table_id),
                                                {});
    if (!mut || mut->partition().row_count() == 0) {
        throw std::runtime_error(fmt::format("failed to find tablets for {}.{}", keyspace_name, table_name));
    }

    auto ks = make_lw_shared<data_dictionary::keyspace_metadata>(keyspace_name,
                                                                 "org.apache.cassandra.locator.LocalStrategy",
                                                                 std::map<sstring, sstring>{},
                                                                 std::nullopt, false);
    db::cql_type_parser::raw_builder ut_builder(*ks);

    tools::tablets_t tablets;
    query::result_set result_set{*mut};
    for (auto& row : result_set.rows()) {
        auto last_token = row.get_nonnull<int64_t>("last_token");
        auto replica_set = replica_set_from_row(row, "replicas");
        tablets.emplace(last_token, std::move(replica_set));
    }
    return tablets;
}

} // anonymous namespace

namespace tools {

future<tablets_t> load_system_tablets(const db::config &dbcfg,
                                      std::filesystem::path scylla_data_path,
                                      std::string_view keyspace_name,
                                      std::string_view table_name,
                                      reader_permit permit) {
    return async([=, &dbcfg] {
        return do_load_system_tablets(dbcfg, scylla_data_path, keyspace_name, table_name, permit);
    });
}

} // namespace tools
