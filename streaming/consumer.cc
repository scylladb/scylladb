/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "consumer.hh"
#include "mutation_source_metadata.hh"
#include "service/priority_manager.hh"
#include "db/view/view_update_generator.hh"
#include "db/view/view_update_checks.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"

namespace streaming {

std::function<future<> (flat_mutation_reader)> make_streaming_consumer(sstring origin,
        sharded<database>& db,
        sharded<db::system_distributed_keyspace>& sys_dist_ks,
        sharded<db::view::view_update_generator>& vug,
        uint64_t estimated_partitions,
        stream_reason reason,
        sstables::offstrategy offstrategy) {
    return [&db, &sys_dist_ks, &vug, estimated_partitions, reason, offstrategy, origin = std::move(origin)] (flat_mutation_reader reader) {
        auto& cf = db.local().find_column_family(reader.schema());
        return db::view::check_needs_view_update_path(sys_dist_ks.local(), cf, reason).then([cf = cf.shared_from_this(), &vug, estimated_partitions, offstrategy, reason, reader = std::move(reader), origin = std::move(origin)] (bool use_view_update_path) mutable {
            //FIXME: for better estimations this should be transmitted from remote
            auto metadata = mutation_source_metadata{};
            auto& cs = cf->get_compaction_strategy();
            const auto adjusted_estimated_partitions = cs.adjust_partition_estimate(metadata, estimated_partitions);
            auto consumer = cs.make_interposer_consumer(metadata,
                    [cf = std::move(cf), adjusted_estimated_partitions, use_view_update_path, &vug, origin = std::move(origin), offstrategy, reason] (flat_mutation_reader reader) {
                sstables::shared_sstable sst = use_view_update_path ? cf->make_streaming_staging_sstable() : cf->make_streaming_sstable_for_write();
                schema_ptr s = reader.schema();
                auto& pc = service::get_local_streaming_priority();

                return sst->write_components(std::move(reader), adjusted_estimated_partitions, s,
                                             cf->get_sstables_manager().configure_writer(origin),
                                             encoding_stats{}, pc).then([sst] {
                    return sst->open_data();
                }).then([cf, sst, offstrategy, reason] {
                    if (offstrategy && (reason == stream_reason::repair)) {
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
            return consumer(std::move(reader));
        });
    };
}

}
