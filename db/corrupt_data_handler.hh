/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "db/system_keyspace.hh"
#include "utils/UUID.hh"
#include "utils/pluggable.hh"

class reader_concurrency_semaphore;
class reader_permit;

namespace db {

class corrupt_data_handler {
public:
    // An ID identifying the corrupt data entry.
    // To be interpreted in the context of the storage where it is recorded, see storage_name().
    using entry_id = utils::tagged_uuid<struct corrupt_data_entry_tag>;

    struct stats {
        // Counters for the number of corrupt data entries reported.
        uint64_t corrupt_data_reported = 0;
        // Counters for the number of corrupt data entries recorded.
        // Can be less than reported depending on the configuration or if entries failed to be recorded.
        uint64_t corrupt_data_recorded = 0;

        uint64_t corrupt_clustering_rows_reported = 0;
        uint64_t corrupt_clustering_rows_recorded = 0;
    };

private:
    stats _stats;

    seastar::metrics::metric_groups _metrics;

protected:
    virtual future<entry_id> do_record_corrupt_clustering_row(const schema& s, const partition_key& pk, clustering_row cr, sstring origin, std::optional<sstring> sstable_name) = 0;

public:
    using register_metrics = bool_class<struct corrupt_data_handler_register_metrics_tag>;
    explicit corrupt_data_handler(register_metrics);
    virtual ~corrupt_data_handler() = default;

    const stats& get_stats() const noexcept {
        return _stats;
    }

    // The name of the storage where corrupt data is recorded.
    // The storage-name and the entry-id together should allow the user to unambiguously locate the entry.
    virtual sstring storage_name() const noexcept = 0;

    // Record a corrupt clustering row.
    // If the returned id is null, the row was not recorded.
    future<entry_id> record_corrupt_clustering_row(const schema& s, const partition_key& pk, clustering_row cr, sstring origin, std::optional<sstring> sstable_name);
};

// Stores corrupt data entries in the system.corrupt_data table.
class system_table_corrupt_data_handler final : public corrupt_data_handler {
public:
    using pluggable_system_keyspace = utils::pluggable<db::system_keyspace>;

    struct config {
        gc_clock::duration entry_ttl;
    };

private:
    gc_clock::duration _entry_ttl;

    pluggable_system_keyspace _sys_ks;
    std::unique_ptr<reader_concurrency_semaphore> _fragment_semaphore;

private:
    reader_permit make_fragment_permit(const schema& s);

    future<entry_id> do_record_corrupt_mutation_fragment(pluggable_system_keyspace::permit sys_ks, const schema& user_table_schema, const partition_key& pk, const clustering_key& ck,
            mutation_fragment_v2::kind kind, frozen_mutation_fragment_v2 mf, sstring origin, std::optional<sstring> sstable_name);

    virtual future<entry_id> do_record_corrupt_clustering_row(const schema& s, const partition_key& pk, clustering_row cr, sstring origin, std::optional<sstring> sstable_name) override;

public:
    explicit system_table_corrupt_data_handler(config, register_metrics);
    ~system_table_corrupt_data_handler();

    virtual sstring storage_name() const noexcept override {
        return format("{}.{}", db::system_keyspace::NAME, db::system_keyspace::CORRUPT_DATA);
    }

    void plug_system_keyspace(db::system_keyspace& sys_ks) noexcept;
    future<> unplug_system_keyspace() noexcept;
};

// A no-op corrupt data handler that does not record any data.
class nop_corrupt_data_handler final : public corrupt_data_handler {
    virtual future<entry_id> do_record_corrupt_clustering_row(const schema& s, const partition_key& pk, clustering_row cr, sstring origin, std::optional<sstring> sstable_name) override;

public:
    explicit nop_corrupt_data_handler(register_metrics rm)
        : corrupt_data_handler(rm) {}
    virtual sstring storage_name() const noexcept override {
        return "/dev/null";
    }
};

} // namespace db
