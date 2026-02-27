/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "db/corrupt_data_handler.hh"
#include "reader_concurrency_semaphore.hh"
#include "replica/database.hh"
#include "utils/UUID_gen.hh"

static logging::logger corrupt_data_logger("corrupt_data");

namespace sm = seastar::metrics;

namespace db {

corrupt_data_handler::corrupt_data_handler(register_metrics rm) {
    if (rm) {
        _metrics.add_group("corrupt_data", {
                sm::make_counter("entries_reported", _stats.corrupt_data_reported,
                               sm::description("Counts the number of corrupt data instances reported to the corrupt data handler. "
                                               "A non-zero value indicates that the database suffered data corruption."))
                });
    }
}

future<corrupt_data_handler::entry_id> corrupt_data_handler::record_corrupt_clustering_row(const schema& s, const partition_key& pk,
        clustering_row cr, sstring origin, std::optional<sstring> sstable_name) {
    ++_stats.corrupt_data_reported;
    ++_stats.corrupt_clustering_rows_reported;
    return do_record_corrupt_clustering_row(s, pk, std::move(cr), std::move(origin), std::move(sstable_name)).then([this] (entry_id id) {
        if (id) {
            ++_stats.corrupt_data_recorded;
            ++_stats.corrupt_clustering_rows_recorded;
        }
        return id;
    });
}

system_table_corrupt_data_handler::system_table_corrupt_data_handler(config cfg, register_metrics rm)
    : corrupt_data_handler(rm)
    , _entry_ttl(cfg.entry_ttl)
    , _sys_ks("system_table_corrupt_data_handler::system_keyspace")
{
}

system_table_corrupt_data_handler::~system_table_corrupt_data_handler() {
}

reader_permit system_table_corrupt_data_handler::make_fragment_permit(const schema& s) {
    return _fragment_semaphore->make_tracking_only_permit(s.shared_from_this(), "system_table_corrupt_data_handler::make_fragment_permit", db::no_timeout, {});
}

future<corrupt_data_handler::entry_id> system_table_corrupt_data_handler::do_record_corrupt_mutation_fragment(
        pluggable_system_keyspace::permit sys_ks,
        const schema& user_table_schema,
        const partition_key& pk,
        const clustering_key& ck,
        mutation_fragment_v2::kind kind,
        frozen_mutation_fragment_v2 fmf,
        sstring origin,
        std::optional<sstring> sstable_name) {
    const corrupt_data_handler::entry_id id{utils::UUID_gen::get_time_UUID()};

    const auto corrupt_data_schema = sys_ks->local_db().find_column_family(system_keyspace::NAME, system_keyspace::CORRUPT_DATA).schema();

    // Using the lower-level mutation API to avoid large allocation warnings when linearizing the frozen mutation fragment.
    mutation entry_mutation(corrupt_data_schema, partition_key::from_exploded(*corrupt_data_schema, {serialized(user_table_schema.ks_name()), serialized(user_table_schema.cf_name())}));
    auto& entry_row = entry_mutation.partition().clustered_row(*corrupt_data_schema, clustering_key::from_single_value(*corrupt_data_schema, serialized(timeuuid_native_type{id.uuid()})));

    const auto timestamp = api::new_timestamp();

    auto set_cell_raw = [this, &entry_row, &corrupt_data_schema, timestamp] (const char* cell_name, managed_bytes cell_value) {
        auto cdef = corrupt_data_schema->get_column_definition(cell_name);
        SCYLLA_ASSERT(cdef);

        entry_row.cells().apply(*cdef, atomic_cell::make_live(*cdef->type, timestamp, cell_value, _entry_ttl));
    }; 

    auto set_cell = [this, &entry_row, &corrupt_data_schema, timestamp] (const char* cell_name, data_value cell_value) {
        auto cdef = corrupt_data_schema->get_column_definition(cell_name);
        SCYLLA_ASSERT(cdef);

        entry_row.cells().apply(*cdef, atomic_cell::make_live(*cdef->type, timestamp, cell_value.serialize_nonnull(), _entry_ttl));
    };

    entry_row.apply(row_marker(timestamp, _entry_ttl, gc_clock::now() + _entry_ttl));
    set_cell("partition_key", data_value(to_bytes(pk.representation())));
    set_cell("clustering_key", data_value(to_bytes(ck.representation())));
    set_cell("mutation_fragment_kind", fmt::to_string(kind));
    // FIXME: Exposing knowledge here that bytes are serialized by just storing the raw value.
    // Need to replace with a fragmented-buffer serialize API call, which we don't have yet.
    set_cell_raw("frozen_mutation_fragment", std::move(fmf).representation().to_managed_bytes());
    set_cell("origin", origin);
    set_cell("sstable_name", sstable_name);

    return sys_ks->apply_mutation(std::move(entry_mutation)).then([id] {
        return id;
    });
}

future<corrupt_data_handler::entry_id> system_table_corrupt_data_handler::do_record_corrupt_clustering_row(const schema& s, const partition_key& pk,
        clustering_row cr, sstring origin, std::optional<sstring> sstable_name) {
    auto sys_ks = _sys_ks.get_permit();
    if (!sys_ks) {
        co_return corrupt_data_handler::entry_id::create_null_id();
    }

    const auto ck = cr.key();
    auto fmf = freeze(s, mutation_fragment_v2(s, make_fragment_permit(s), std::move(cr)));

    co_return co_await do_record_corrupt_mutation_fragment(std::move(sys_ks), s, pk, ck, mutation_fragment_v2::kind::clustering_row, std::move(fmf),
            std::move(origin), std::move(sstable_name));
}

void system_table_corrupt_data_handler::plug_system_keyspace(db::system_keyspace& sys_ks) noexcept {
    _sys_ks.plug(sys_ks.shared_from_this());
    _fragment_semaphore = std::make_unique<reader_concurrency_semaphore>(reader_concurrency_semaphore::no_limits{}, "system_table_corrupt_data_handler", reader_concurrency_semaphore::register_metrics::no);
}

future<> system_table_corrupt_data_handler::unplug_system_keyspace() noexcept {
    co_await _sys_ks.unplug();
    co_await _fragment_semaphore->stop();
}

future<corrupt_data_handler::entry_id> nop_corrupt_data_handler::do_record_corrupt_clustering_row(const schema& s, const partition_key& pk,
        clustering_row cr, sstring origin, std::optional<sstring> sstable_name) {
    return make_ready_future<entry_id>(entry_id::create_null_id());
}

} // namespace db
