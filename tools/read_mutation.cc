/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "tools/read_mutation.hh"
#include "readers/combined.hh"
#include "replica/database.hh"
#include "partition_slice_builder.hh"

#include <algorithm>
#include <seastar/util/closeable.hh>

future<std::filesystem::path> get_table_directory(std::filesystem::path scylla_data_path,
                                                  std::string_view keyspace_name,
                                                  std::string_view table_name) {
    auto system_tables_path = scylla_data_path / keyspace_name;
    auto system_tables_dir = co_await open_directory(system_tables_path.native());
    sstring found;
    // locate a directory named like
    //   "$scylla_data_path/system/tablets-fd4f7a4696bd3e7391bf99eb77e82a5c"
    auto h = system_tables_dir.list_directory([&] (directory_entry de) -> future<> {
        if (!found.empty()) {
            return make_ready_future();
        }
        if (!de.type) {
            throw std::runtime_error(fmt::format("failed to find directory: {}/{}: unrecognized type",
                                                 scylla_data_path, de.name));
        }
        if (*de.type != directory_entry_type::directory) {
            throw std::runtime_error(fmt::format("failed to find directory: {}/{}: not a directory",
                                                 scylla_data_path, de.name));
        }
        auto [cf_name, uuid] = replica::parse_table_directory_name(de.name);
        if (cf_name  != table_name) {
            return make_ready_future();
        }
        found = de.name;
        return make_ready_future();
    });
    co_await h.done();

    if (found.empty()) {
        throw std::runtime_error(fmt::format("failed to find directory for {}.{} under {}",
                                             keyspace_name, table_name, scylla_data_path));
    }
    co_return system_tables_path / found.c_str();
}

mutation_opt read_mutation_from_table_offline(sharded<sstable_manager_service>& sst_man,
                                              reader_permit permit,
                                              std::filesystem::path table_path,
                                              std::string_view keyspace,
                                              std::function<schema_ptr()> table_schema,
                                              data_value primary_key,
                                              std::optional<data_value> clustering_key) {
    sharded<sstables::sstable_directory> sst_dirs;
    sst_dirs.start(
        sharded_parameter([&sst_man] { return std::ref(sst_man.local().sst_man); }),
        sharded_parameter([&] { return table_schema(); }),
        sharded_parameter([&] { return std::ref(table_schema()->get_sharder()); }),
        table_path.native(),
        sstables::sstable_state::normal,
        sharded_parameter([] { return default_io_error_handler_gen(); })).get();
    auto stop_sst_dirs = deferred_stop(sst_dirs);

    using open_infos_t = std::vector<sstables::foreign_sstable_open_info>;
    auto sstable_open_infos = sst_dirs.map_reduce0(
        [] (sstables::sstable_directory& sst_dir) -> future<std::vector<sstables::foreign_sstable_open_info>> {
            co_await sst_dir.process_sstable_dir(sstables::sstable_directory::process_flags{ .sort_sstables_according_to_owner = false });
            const auto& unsorted_ssts = sst_dir.get_unsorted_sstables();
            open_infos_t open_infos;
            open_infos.reserve(unsorted_ssts.size());
            for (auto& sst : unsorted_ssts) {
                open_infos.push_back(co_await sst->get_open_info());
            }
            co_return open_infos;
        },
        open_infos_t{},
        [] (open_infos_t new_infos, open_infos_t collected) {
            std::ranges::move(new_infos, std::back_inserter(collected));
            return collected;
        }).get();
    if (sstable_open_infos.empty()) {
        return {};
    }

    std::vector<sstables::shared_sstable> sstables;
    sstables.reserve(sstable_open_infos.size());
    for (auto& open_info : sstable_open_infos) {
        auto sst = sst_dirs.local().load_foreign_sstable(open_info).get();
        sstables.push_back(std::move(sst));
    }

    auto schema = table_schema();
    auto pk = partition_key::from_deeply_exploded(*schema, {std::move(primary_key)});
    auto dk = dht::decorate_key(*schema, pk);
    auto pr = dht::partition_range::make_singular(dk);
    auto pb = partition_slice_builder(*schema);
    if (clustering_key.has_value()) {
        auto ck = clustering_key::from_deeply_exploded(*schema, {clustering_key.value()});
        auto cr = query::clustering_range::make({ck, true}, {ck, true});
        pb.with_range(cr);
    }
    auto ps = pb.build();

    std::vector<mutation_reader> readers;
    readers.reserve(sstables.size());
    for (const auto& sst : sstables) {
        readers.emplace_back(sst->make_reader(schema, permit, pr, ps));
    }
    auto reader = make_combined_reader(schema, permit, std::move(readers));
    auto close_reader = deferred_close(reader);

    return read_mutation_from_mutation_reader(reader).get();
}
