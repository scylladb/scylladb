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
#include "db/config.hh"
#include "db/view/view_update_generator.hh"
#include "db/view/view_update_checks.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"

namespace streaming {

sstables::offstrategy maybe_disable_offstrategy(const replica::database& db, const sstables::compaction_strategy& cs, const schema_ptr& s, sstables::offstrategy offstrategy) {
    // Optionally disable off-strategy compaction for TWCS tables.
    // The benefit is that space overhead for such a table is limited to 1 / N, where N is
    // the configured number of time windows. The disadvantage is increased write ampl,
    // but the option is there to help critical scenarios where the space overhead will
    // make node run out of space.
    //
    // We cannot enable segregation on TWCS tables and still allow off-strategy, due to:
    //  1) streaming would produce 1 sstable per range per RF per window.
    //  2) hundreds of sstables per window will make reads inefficient
    // Streaming is token range oriented, so any range potentially have data for any window,
    // meaning each window will receive at least 1 new sstable for each range processed,
    // until operation completes.
    //
    // Even with off-strategy disabled, the old optimization of temporarily increasing the
    // min threshold still applies -- for replace and bootstrap --, in order to reduce the
    // write amp. For ops like decommission, we'll rely on compaction manager temporarily
    // increasing the minimum threshold when the write rate is high.

    // The long term fix is about enabling incremental compaction in TWCS.
    //
    if (cs.type() == sstables::compaction_strategy_type::time_window && !db.get_config().compaction_twcs_offstrategy_enabled()) {
        dblog.info0("Disabled off-strategy for table {}.{} using time window compaction strategy.", s->ks_name(), s->cf_name());
        return sstables::offstrategy::no;
    }
    return offstrategy;
}

std::function<future<> (flat_mutation_reader_v2)> make_streaming_consumer(sstring origin,
        sharded<replica::database>& db,
        sharded<db::system_distributed_keyspace>& sys_dist_ks,
        sharded<db::view::view_update_generator>& vug,
        uint64_t estimated_partitions,
        stream_reason reason,
        sstables::offstrategy offstrategy,
        service::frozen_topology_guard frozen_guard) {
    return [&db, &sys_dist_ks, &vug, estimated_partitions, reason, offstrategy, origin = std::move(origin), frozen_guard] (flat_mutation_reader_v2 reader) mutable -> future<> {
        std::exception_ptr ex;
        try {
            auto cf = db.local().find_column_family(reader.schema()).shared_from_this();
            auto guard = service::topology_guard(*cf, frozen_guard);
            auto use_view_update_path = co_await db::view::check_needs_view_update_path(sys_dist_ks.local(), db.local().get_token_metadata(), *cf, reason);
            //FIXME: for better estimations this should be transmitted from remote
            auto metadata = mutation_source_metadata{};
            auto& cs = cf->get_compaction_strategy();

            offstrategy = maybe_disable_offstrategy(db.local(), cs, cf->schema(), offstrategy);

            // Data segregation is postponed to happen during off-strategy if latter is enabled, which
            // means partition estimation shouldn't be adjusted.
            const auto adjusted_estimated_partitions = (offstrategy) ? estimated_partitions : cs.adjust_partition_estimate(metadata, estimated_partitions, cf->schema());
            auto make_interposer_consumer = [&cs, offstrategy] (const mutation_source_metadata& ms_meta, reader_consumer_v2 end_consumer) mutable {
                if (offstrategy) {
                    return end_consumer;
                }
                return cs.make_interposer_consumer(ms_meta, std::move(end_consumer));
            };

            auto consumer = make_interposer_consumer(metadata,
                    [cf = std::move(cf), adjusted_estimated_partitions, use_view_update_path, &vug, origin = std::move(origin), offstrategy] (flat_mutation_reader_v2 reader) {
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
                cfg.erm = cf->get_effective_replication_map();
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
                }).then([cf, s, sst, use_view_update_path, &vug]() mutable -> future<> {
                    if (!use_view_update_path) {
                        return make_ready_future<>();
                    }
                    return vug.local().register_staging_sstable(sst, std::move(cf));
                });
            });
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
