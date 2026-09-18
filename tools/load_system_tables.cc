/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "tools/load_system_tables.hh"

#include <seastar/core/thread.hh>
#include <seastar/util/closeable.hh>

#include "utils/log.hh"
#include "db/schema_tables.hh"
#include "db/system_keyspace.hh"
#include "mutation/mutation.hh"
#include "readers/combined.hh"
#include "replica/tablets.hh"
#include "sstables/sstables.hh"
#include "tools/read_mutation.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/tuple.hh"

namespace {

logging::logger sys_tables_logger{"load_sys_tables"};

// Reads the rows of "system.sstables_registry" the lister asks for, the way
// system_keyspace does on a running node, so that the sstables of a table on
// object storage can be enumerated from the data dir of a node which is down.
class offline_sstables_registry final : public sstables::sstables_registry {
    const db::config& _dbcfg;
    std::filesystem::path _scylla_data_path;
    reader_permit _permit;

    static future<> read_only() {
        return make_exception_future<>(std::runtime_error(
                "the sstables registry of a node which is not running is read-only"));
    }

public:
    offline_sstables_registry(const db::config& dbcfg, std::filesystem::path scylla_data_path, reader_permit permit)
        : _dbcfg(dbcfg)
        , _scylla_data_path(std::move(scylla_data_path))
        , _permit(std::move(permit))
    { }

    future<> create_entry(table_id, locator::host_id, sstring, sstables::sstable_state, sstables::entry_descriptor) override {
        return read_only();
    }
    future<> update_entry_status(table_id, locator::host_id, sstables::generation_type, sstring) override {
        return read_only();
    }
    future<> update_entry_state(table_id, locator::host_id, sstables::generation_type, sstables::sstable_state) override {
        return read_only();
    }
    future<> batch_update_entry_status(table_id, locator::host_id, const std::vector<sstables::generation_type>&, sstring) override {
        return read_only();
    }
    future<> delete_entry(table_id, locator::host_id, sstables::generation_type) override {
        return read_only();
    }
    future<> sstables_registry_list(table_id table, locator::host_id node_owner, entry_consumer consumer) override {
        auto entries = co_await tools::load_system_sstables_registry(_dbcfg, _scylla_data_path, table, node_owner, _permit);
        for (auto& entry : entries) {
            co_await consumer(std::move(entry.status), entry.state, std::move(entry.desc));
        }
    }
};

} // anonymous namespace

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
                tablet tablet{.replicas = replica::tablet_replica_set_from_cell(*replica_set)};
                // absent until the tablet is repaired for the first time
                if (auto repair_time = row.get<db_clock::time_point>("repair_time")) {
                    tablet.repair_time = *repair_time;
                }
                tablets.emplace(last_token, std::move(tablet));
            },
            std::move(tablets_directory));
    if (tablets.empty()) {
        throw std::runtime_error(fmt::format("failed to find tablets for table {}", table));
    }
    co_return tablets;
}

future<repaired_ranges_t> load_system_repair_history(const db::config& dbcfg,
                                      std::filesystem::path scylla_data_path,
                                      table_id table,
                                      reader_permit permit) {
    repaired_ranges_t repaired_ranges;
    co_await query_system_table_offline(dbcfg, scylla_data_path, db::system_keyspace::repair_history(),
            {data_value(table.uuid())}, std::nullopt, permit,
            [&repaired_ranges] (const query::result_set_row& row) {
                auto repair_time = row.get<db_clock::time_point>("repair_time");
                auto range_start = row.get<int64_t>("range_start");
                auto range_end = row.get<int64_t>("range_end");
                if (!repair_time || !range_start || !range_end) {
                    return;
                }
                // the recorded range is (range_start, range_end], with the minimum
                // int64 standing for the end of the ring on either side
                auto start = *range_start == std::numeric_limits<int64_t>::min()
                        ? dht::minimum_token() : dht::token::from_int64(*range_start);
                auto end = *range_end == std::numeric_limits<int64_t>::min()
                        ? dht::maximum_token() : dht::token::from_int64(*range_end);
                repaired_ranges.emplace_back(
                        dht::token_range(dht::token_range::bound(start, false), dht::token_range::bound(end, true)),
                        to_gc_clock(*repair_time));
            });
    co_return repaired_ranges;
}

future<std::optional<data_dictionary::storage_options>> load_keyspace_storage_options(const db::config& dbcfg,
                                      std::filesystem::path scylla_data_path,
                                      std::string_view keyspace,
                                      reader_permit permit) {
    std::optional<data_dictionary::storage_options> options;
    co_await query_system_table_offline(dbcfg, scylla_data_path, db::schema_tables::scylla_keyspaces(),
            {data_value(sstring(keyspace))}, std::nullopt, permit,
            [&options] (const query::result_set_row& row) {
                auto storage_type = row.get<sstring>("storage_type");
                auto storage_options = row.get<map_type_impl::native_type>("storage_options");
                if (options || !storage_type || !storage_options) {
                    return;
                }
                std::map<sstring, sstring> values;
                for (const auto& [key, value] : *storage_options) {
                    values.emplace(value_cast<sstring>(key), value_cast<sstring>(value));
                }
                options.emplace();
                options->value = data_dictionary::storage_options::from_map(*storage_type, values);
            });
    co_return options;
}

future<std::vector<sstables_registry_entry>> load_system_sstables_registry(const db::config& dbcfg,
                                      std::filesystem::path scylla_data_path,
                                      table_id table,
                                      locator::host_id node_owner,
                                      reader_permit permit) {
    std::vector<sstables_registry_entry> entries;
    co_await query_system_table_offline(dbcfg, scylla_data_path, db::system_keyspace::sstables_registry(),
            {data_value(table.uuid()), data_value(node_owner.uuid())}, std::nullopt, permit,
            [&entries, table] (const query::result_set_row& row) {
                auto status = row.get<sstring>("status");
                auto state = row.get<sstring>("state");
                auto generation = row.get<utils::UUID>("generation");
                auto sstable_id = row.get<utils::UUID>("sstable_id");
                auto version = row.get<sstring>("version");
                auto format = row.get<sstring>("format");
                if (!status || !state || !generation || !sstable_id || !version || !format) {
                    sys_tables_logger.warn("skipping incomplete {}.{} entry of table {}", db::system_keyspace::NAME,
                            db::system_keyspace::SSTABLES_REGISTRY, table);
                    return;
                }
                entries.emplace_back(std::move(*status), sstables::state_from_dir(*state),
                        sstables::entry_descriptor(sstables::generation_type(*generation),
                                sstables::sstable_id(*sstable_id),
                                sstables::version_from_string(*version),
                                sstables::format_from_string(*format),
                                sstables::component_type::TOC));
            });
    co_return entries;
}

std::unique_ptr<sstables::sstables_registry> make_offline_sstables_registry(const db::config& dbcfg,
                                      std::filesystem::path scylla_data_path,
                                      reader_permit permit) {
    return std::make_unique<offline_sstables_registry>(dbcfg, std::move(scylla_data_path), std::move(permit));
}

future<std::optional<local_node_info>> load_local_node_info(const db::config& dbcfg,
                                      std::filesystem::path scylla_data_path,
                                      reader_permit permit) {
    std::optional<local_node_info> info;
    co_await query_system_table_offline(dbcfg, scylla_data_path, db::system_keyspace::local(),
            {data_value(sstring(db::system_keyspace::LOCAL))}, std::nullopt, permit,
            [&info] (const query::result_set_row& row) {
                if (auto host_id = row.get<utils::UUID>("host_id"); host_id && !info) {
                    info = local_node_info{.host_id = locator::host_id(*host_id)};
                }
            });
    if (!info) {
        co_return std::nullopt;
    }

    // The sharding parameters of the node live in "system.topology", in the row
    // of its host id. The "scylla_nr_shards" and "scylla_msb_ignore" columns of
    // "system.local" are not an alternative: they are dropped columns, so
    // nothing has written them for a long time.
    co_await query_system_table_offline(dbcfg, scylla_data_path, db::system_keyspace::topology(),
            {data_value(sstring(db::system_keyspace::TOPOLOGY))}, data_value(info->host_id.uuid()), permit,
            [&info] (const query::result_set_row& row) {
                if (auto shard_count = row.get<int32_t>("shard_count")) {
                    info->shard_count = unsigned(*shard_count);
                }
                if (auto ignore_msb_bits = row.get<int32_t>("ignore_msb")) {
                    info->ignore_msb_bits = unsigned(*ignore_msb_bits);
                }
            });
    co_return info;
}

} // namespace tools
