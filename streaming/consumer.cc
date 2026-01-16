/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/coroutine.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/core/future.hh>

#include "consumer.hh"
#include "db/view/view_building_worker.hh"
#include "replica/database.hh"
#include "mutation/mutation_source_metadata.hh"
#include "db/view/view_builder.hh"
#include "db/view/view_update_checks.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"

namespace streaming {

mutation_reader_consumer make_streaming_consumer(sstring origin,
        sharded<replica::database>& db,
        db::view::view_builder& vb,
        sharded<db::view::view_building_worker>& vbw,
        uint64_t estimated_partitions,
        stream_reason reason,
        sstables::offstrategy offstrategy,
        service::frozen_topology_guard frozen_guard,
        std::optional<int64_t> repaired_at,
        lw_shared_ptr<sstables::sstable_list> sstable_list_to_mark_as_repaired) {
    return [&db, &vb = vb.container(), &vbw, estimated_partitions, reason, offstrategy, origin = std::move(origin), frozen_guard, repaired_at, sstable_list_to_mark_as_repaired] (mutation_reader reader) -> future<> {
        std::exception_ptr ex;
        try {
            if (current_scheduling_group() != db.local().get_streaming_scheduling_group()) {
                on_internal_error(sstables::sstlog, format("The stream consumer is not running in streaming group current_scheduling_group={}",
                        current_scheduling_group().name()));
            }

            auto cf = db.local().find_column_family(reader.schema()).shared_from_this();
            auto guard = service::topology_guard(frozen_guard);
            auto use_view_update_path = co_await with_scheduling_group(db.local().get_gossip_scheduling_group(), [&] {
                return db::view::check_needs_view_update_path(vb.local(), db.local().get_token_metadata_ptr(), *cf, reason);
            });
            //FIXME: for better estimations this should be transmitted from remote
            auto metadata = mutation_source_metadata{};
            auto& cs = cf->get_compaction_strategy();
            // Data segregation is postponed to happen during off-strategy if latter is enabled, which
            // means partition estimation shouldn't be adjusted.
            const auto adjusted_estimated_partitions = (offstrategy) ? estimated_partitions : cs.adjust_partition_estimate(metadata, estimated_partitions, cf->schema());
            mutation_reader_consumer consumer =
                    [cf = std::move(cf), adjusted_estimated_partitions, use_view_update_path, &vb, &vbw, origin = std::move(origin),
                offstrategy, repaired_at, sstable_list_to_mark_as_repaired, frozen_guard] (mutation_reader reader) {
                sstables::shared_sstable sst;
                try {
                    sst = use_view_update_path == db::view::sstable_destination_decision::normal_directory ? cf->make_streaming_sstable_for_write() : cf->make_streaming_staging_sstable();
                } catch (...) {
                    return current_exception_as_future().finally([reader = std::move(reader)] () mutable {
                        return reader.close();
                    });
                }
                schema_ptr s = reader.schema();

                // SSTable will be only sealed when added to the sstable set, so we make sure unsplit sstables aren't
                // left sealed on the table directory.
                auto cfg = cf->get_sstables_manager().configure_writer(origin);
                cfg.leave_unsealed = true;
                return sst->write_components(std::move(reader), adjusted_estimated_partitions, s,
                                             cfg, encoding_stats{}).then([sst] {
                    return sst->open_data();
                }).then([cf, sst, offstrategy, origin, repaired_at, sstable_list_to_mark_as_repaired, frozen_guard, cfg] -> future<std::vector<sstables::shared_sstable>> {
                    auto on_add = [sst, origin, repaired_at, sstable_list_to_mark_as_repaired, frozen_guard, cfg] (sstables::shared_sstable loading_sst) -> future<> {
                        if (repaired_at && sstables::repair_origin == origin) {
                            loading_sst->being_repaired = frozen_guard;
                            if (sstable_list_to_mark_as_repaired) {
                                sstable_list_to_mark_as_repaired->insert(loading_sst);
                            }
                        }
                        if (loading_sst == sst) {
                            co_await loading_sst->seal_sstable(cfg.backup);
                        }
                        co_return;
                    };
                    if (offstrategy && sstables::repair_origin == origin) {
                        sstables::sstlog.debug("Enabled automatic off-strategy trigger for table {}.{}",
                                cf->schema()->ks_name(), cf->schema()->cf_name());
                        cf->enable_off_strategy_trigger();
                    }
                    co_return co_await cf->add_new_sstable_and_update_cache(sst, on_add, offstrategy);
                }).then([cf, s, sst, use_view_update_path, &vb, &vbw] (std::vector<sstables::shared_sstable> new_sstables) mutable -> future<> {
                    auto& vb_ = vb;
                    auto new_sstables_ = std::move(new_sstables);
                    auto table = cf;

                    if (use_view_update_path == db::view::sstable_destination_decision::staging_managed_by_vbc) {
                        co_return co_await vbw.local().register_staging_sstable_tasks(new_sstables_, cf->schema()->id());
                    } else if (use_view_update_path == db::view::sstable_destination_decision::staging_directly_to_generator) {
                        co_await coroutine::parallel_for_each(new_sstables_, [&vb_, &table] (sstables::shared_sstable sst) -> future<> {
                            return vb_.local().register_staging_sstable(sst, table);
                        });
                    }
                    co_return;
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
