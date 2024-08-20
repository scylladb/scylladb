/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "tools/load_system_tablets.hh"

#include <seastar/core/thread.hh>
#include <seastar/util/closeable.hh>

#include "log.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include "db/cql_type_parser.hh"
#include "db/system_keyspace.hh"
#include "mutation/mutation.hh"
#include "readers/combined.hh"
#include "replica/tablets.hh"
#include "tools/read_mutation.hh"
#include "types/list.hh"
#include "types/tuple.hh"

namespace {

logging::logger logger{"load_sys_tablets"};

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
        auto replica_set = row.get_data_value("replicas");
        if (replica_set) {
            tablets.emplace(last_token,
                            replica::tablet_replica_set_from_cell(*replica_set));
        }
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
