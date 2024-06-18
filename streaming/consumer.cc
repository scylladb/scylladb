/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>

#include "consumer.hh"
#include "replica/database.hh"
#include "mutation/mutation_source_metadata.hh"
#include "db/view/view_builder.hh"
#include "db/view/view_update_checks.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"

namespace streaming {

std::function<future<> (mutation_reader)> make_streaming_consumer(sstring origin,
        sharded<replica::database>& db,
        sharded<db::view::view_builder>& vb,
        uint64_t estimated_partitions,
        stream_reason reason,
        sstables::offstrategy offstrategy,
        service::frozen_topology_guard frozen_guard) {
    return [&db, &vb, estimated_partitions, reason, offstrategy, origin = std::move(origin), frozen_guard] (mutation_reader reader) -> future<> {
        std::exception_ptr ex;
        try {
            if (current_scheduling_group() != db.local().get_streaming_scheduling_group()) {
                on_internal_error(sstables::sstlog, format("The stream consumer is not running in streaming group current_scheduling_group={}",
                        current_scheduling_group().name()));
            }

            auto cf = db.local().find_column_family(reader.schema()).shared_from_this();
            auto guard = service::topology_guard(frozen_guard);
            auto use_view_update_path = co_await db::view::check_needs_view_update_path(vb.local(), db.local().get_token_metadata(), *cf, reason);
            //FIXME: for better estimations this should be transmitted from remote
            auto metadata = mutation_source_metadata{};
            auto& cs = cf->get_compaction_strategy();
            // Data segregation is postponed to happen during off-strategy if latter is enabled, which
            // means partition estimation shouldn't be adjusted.
            const auto adjusted_estimated_partitions = (offstrategy) ? estimated_partitions : cs.adjust_partition_estimate(metadata, estimated_partitions, cf->schema());
            reader_consumer_v2 consumer =
                    [cf = std::move(cf), adjusted_estimated_partitions, use_view_update_path, &vb, origin = std::move(origin), offstrategy] (mutation_reader reader) {
                sstables::shared_sstable sst;
                try {
                    sst = use_view_update_path ? cf->make_streaming_staging_sstable() : cf->make_streaming_sstable_for_write();
                } catch (...) {
                    return current_exception_as_future().finally([reader = std::move(reader)] () mutable {
                        return reader.close();
                    });
                }
                schema_ptr s = reader.schema();

                auto cfg = cf->get_sstables_manager().configure_writer(origin);
                return sst->write_components(std::move(reader), adjusted_estimated_partitions, s,
                                             cfg, encoding_stats{}).then([sst] {
                    return sst->open_data();
                }).then([cf, sst, offstrategy, origin] {
                    if (offstrategy && sstables::repair_origin == origin) {
                        sstables::sstlog.debug("Enabled automatic off-strategy trigger for table {}.{}",
                                cf->schema()->ks_name(), cf->schema()->cf_name());
                        cf->enable_off_strategy_trigger();
                    }
                    return cf->add_sstable_and_update_cache(sst, offstrategy);
                }).then([cf, s, sst, use_view_update_path, &vb]() mutable -> future<> {
                    if (!use_view_update_path) {
                        return make_ready_future<>();
                    }
                    return vb.local().register_staging_sstable(sst, std::move(cf));
                });
            };
            if (!offstrategy) {
                consumer = cs.make_interposer_consumer(metadata, std::move(consumer));
            }
            co_return co_await consumer(std::move(reader));
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            co_await reader.close();
            std::rethrow_exception(std::move(ex));
        }
    };
}

}
