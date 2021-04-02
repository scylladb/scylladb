/*
 * Copyright (C) 2018 ScyllaDB
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

#include "repair/writer.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/view/view_update_checks.hh"
#include "mutation_writer/multishard_writer.hh"
#include "mutation_source_metadata.hh"
#include "service/priority_manager.hh"
#include "db/view/view_update_generator.hh"
#include "sstables/sstables_manager.hh"

extern logging::logger rlogger;

extern distributed<db::system_distributed_keyspace>* _sys_dist_ks;
extern distributed<db::view::view_update_generator>* _view_update_generator;

repair_writer::repair_writer(
    schema_ptr schema,
    reader_permit permit,
    uint64_t estimated_partitions,
    size_t nr_peer_nodes,
    streaming::stream_reason reason)
    : _schema(std::move(schema))
    , _permit(std::move(permit))
    , _estimated_partitions(estimated_partitions)
    , _nr_peer_nodes(nr_peer_nodes)
    , _reason(reason) {
}

future<> repair_writer::write_start_and_mf(lw_shared_ptr<const decorated_key_with_hash> dk, mutation_fragment mf)  {
    _current_dk_written_to_sstable = dk;
    if (mf.is_partition_start()) {
        return _mq->push(std::move(mf)).then([this] {
            _partition_opened = true;
        });
    } else {
        auto start = mutation_fragment(*_schema, _permit, partition_start(dk->dk, tombstone()));
        return _mq->push(std::move(start)).then([this, mf = std::move(mf)] () mutable {
            _partition_opened = true;
            return _mq->push(std::move(mf));
        });
    }
};

sstables::offstrategy is_offstrategy_supported(streaming::stream_reason reason) {
    std::unordered_set<streaming::stream_reason> operations_supported = {
        streaming::stream_reason::bootstrap,
        streaming::stream_reason::replace,
    };
    return sstables::offstrategy(operations_supported.contains(reason));
}

void repair_writer::create_writer(sharded<database>& db) {
    if (_writer_done) {
        return;
    }
    table& t = db.local().find_column_family(_schema->id());
    auto [queue_reader, queue_handle] = make_queue_reader(_schema, _permit);
    _mq = std::move(queue_handle);
    auto writer = shared_from_this();
    _writer_done = mutation_writer::distribute_reader_and_consume_on_shards(_schema, std::move(queue_reader),
        [&db, reason = this->_reason, estimated_partitions = this->_estimated_partitions] (flat_mutation_reader reader) {
            auto& t = db.local().find_column_family(reader.schema());
            return db::view::check_needs_view_update_path(_sys_dist_ks->local(), t, reason).then([t = t.shared_from_this(), estimated_partitions, reader = std::move(reader), reason] (bool use_view_update_path) mutable {
                //FIXME: for better estimations this should be transmitted from remote
                auto metadata = mutation_source_metadata{};
                auto& cs = t->get_compaction_strategy();
                const auto adjusted_estimated_partitions = cs.adjust_partition_estimate(metadata, estimated_partitions);
                sstables::offstrategy offstrategy = is_offstrategy_supported(reason);
                auto consumer = cs.make_interposer_consumer(metadata,
                                 [t = std::move(t), use_view_update_path, adjusted_estimated_partitions, offstrategy] (flat_mutation_reader reader) {
                        sstables::shared_sstable sst = use_view_update_path ? t->make_streaming_staging_sstable() : t->make_streaming_sstable_for_write();
                        schema_ptr s = reader.schema();
                        auto& pc = service::get_local_streaming_priority();
                        return sst->write_components(std::move(reader), adjusted_estimated_partitions, s,
                                                     t->get_sstables_manager().configure_writer("repair"),
                                                     encoding_stats{}, pc).then([sst] {
                            return sst->open_data();
                        }).then([t, sst, offstrategy] {
                            return t->add_sstable_and_update_cache(sst, offstrategy);
                        }).then([t, s, sst, use_view_update_path]() mutable -> future<> {
                            if (!use_view_update_path) {
                                return make_ready_future<>();
                            }
                            return _view_update_generator->local().register_staging_sstable(sst, std::move(t));
                        });
                    });
                return consumer(std::move(reader));
            });
        },
        t.stream_in_progress()).then([writer] (uint64_t partitions) {
        rlogger.debug("repair_writer: keyspace={}, table={}, managed to write partitions={} to sstable",
                      writer->_schema->ks_name(), writer->_schema->cf_name(), partitions);
    }).handle_exception([writer] (std::exception_ptr ep) {
        rlogger.warn("repair_writer: keyspace={}, table={}, multishard_writer failed: {}",
                     writer->_schema->ks_name(), writer->_schema->cf_name(), ep);
        writer->_mq->abort(ep);
        return make_exception_future<>(std::move(ep));
    });
}

future<> repair_writer::write_partition_end() {
    if (_partition_opened) {
        return _mq->push(mutation_fragment(*_schema, _permit, partition_end())).then([this] {
            _partition_opened = false;
        });
    }
    return make_ready_future<>();
}

future<> repair_writer::do_write(lw_shared_ptr<const decorated_key_with_hash> dk, mutation_fragment mf) {
    if (_current_dk_written_to_sstable) {
        const auto cmp_res = _current_dk_written_to_sstable->dk.tri_compare(*_schema, dk->dk);
        if (cmp_res > 0) {
            on_internal_error(rlogger, format("repair_writer::do_write(): received out-of-order partition, current: {}, next: {}", _current_dk_written_to_sstable->dk, dk->dk));
        } else if (cmp_res == 0) {
            return _mq->push(std::move(mf));
        } else {
            return write_partition_end().then([this,
                                                  dk = std::move(dk), mf = std::move(mf)] () mutable {
                return write_start_and_mf(std::move(dk), std::move(mf));
            });
        }
    } else {
        return write_start_and_mf(std::move(dk), std::move(mf));
    }
}

future<> repair_writer::write_end_of_stream() {
    if (_mq) {
        return with_semaphore(_sem, 1, [this] {
            // Partition_end is never sent on wire, so we have to write one ourselves.
            return write_partition_end().then([this] () mutable {
                _mq->push_end_of_stream();
            }).handle_exception([this] (std::exception_ptr ep) {
                _mq->abort(ep);
                rlogger.warn("repair_writer: keyspace={}, table={}, write_end_of_stream failed: {}",
                             _schema->ks_name(), _schema->cf_name(), ep);
                return make_exception_future<>(std::move(ep));
            });
        });
    } else {
        return make_ready_future<>();
    }
}

future<> repair_writer::do_wait_for_writer_done() {
    if (_writer_done) {
        return std::move(*(_writer_done));
    } else {
        return make_ready_future<>();
    }
}

future<> repair_writer::wait_for_writer_done() {
    return when_all_succeed(write_end_of_stream(), do_wait_for_writer_done()).discard_result().handle_exception(
        [this] (std::exception_ptr ep) {
            rlogger.warn("repair_writer: keyspace={}, table={}, wait_for_writer_done failed: {}",
                         _schema->ks_name(), _schema->cf_name(), ep);
            return make_exception_future<>(std::move(ep));
        });
}
