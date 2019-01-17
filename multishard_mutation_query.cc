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

#include "schema_registry.hh"
#include "service/priority_manager.hh"
#include "multishard_mutation_query.hh"

#include <boost/range/adaptor/reversed.hpp>

#include <fmt/ostream.h>

logging::logger mmq_log("multishard_mutation_query");

template <typename T>
using foreign_unique_ptr = foreign_ptr<std::unique_ptr<T>>;

/// Context object for a multishard read.
///
/// Handles logic related to looking up, creating, saving and cleaning up remote
/// (shard) readers for the `multishard_mutation_reader`.
/// Has a state machine for each of the shard readers. See the state transition
/// diagram below, above the declaration of `reader state`.
/// The `read_context` is a short-lived object that is only kept around for the
/// duration of a single page. A new `read_context` is created on each page and
/// is discarded at the end of the page, after the readers are either saved
/// or the process of their safe disposal was started in the background.
/// Intended usage:
/// * Create the `read_context`.
/// * Call `read_context::lookup_readers()` to find any saved readers from the
///   previous page.
/// * Create the `multishard_mutation_reader`.
/// * Fill the page.
/// * Destroy the `multishard_mutation_reader` to trigger the disposal of the
///   shard readers.
/// * Call `read_context::save_readers()` if the read didn't finish yet, that is
///   more pages are expected.
/// * Call `read_context::stop()` to initiate the cleanup of any unsaved readers
///   and their dependencies.
/// * Destroy the `read_context`.
///
/// Note:
/// 1) Each step can only be started when the previous phase has finished.
/// 2) This usage is implemented in the `do_query_mutations()` function below.
/// 3) Both, `read_context::lookup_readers()` and `read_context::save_readers()`
///    knows to do nothing when the query is not stateful and just short
///    circuit.
class read_context : public reader_lifecycle_policy {
    struct reader_params {
        std::unique_ptr<const dht::partition_range> range;
        std::unique_ptr<const query::partition_slice> slice;

        reader_params(dht::partition_range range, query::partition_slice slice)
            : range(std::make_unique<const dht::partition_range>(std::move(range)))
            , slice(std::make_unique<const query::partition_slice>(std::move(slice))) {
        }
        reader_params(std::unique_ptr<const dht::partition_range> range, std::unique_ptr<const query::partition_slice> slice)
            : range(std::move(range))
            , slice(std::move(slice)) {
        }
    };

    struct bundled_remote_reader {
        foreign_unique_ptr<reader_params> params;
        foreign_unique_ptr<utils::phased_barrier::operation> read_operation;
        foreign_unique_ptr<flat_mutation_reader> reader;
        reader_concurrency_semaphore* semaphore;
    };
    struct paused_reader {
        shard_id shard;
        reader_concurrency_semaphore::inactive_read_handle handle;
        bool has_pending_next_partition;
    };
    struct inactive_read : public reader_concurrency_semaphore::inactive_read {
        foreign_unique_ptr<flat_mutation_reader> reader;
        explicit inactive_read(foreign_unique_ptr<flat_mutation_reader> reader)
            : reader(std::move(reader)) {
        }
        virtual void evict() override {
            reader.reset();
        }
    };

    using inexistent_state = std::monostate;
    struct successful_lookup_state {
        foreign_unique_ptr<reader_params> params;
        foreign_unique_ptr<utils::phased_barrier::operation> read_operation;
        foreign_unique_ptr<flat_mutation_reader> reader;
    };
    struct used_state {
        foreign_unique_ptr<reader_params> params;
        foreign_unique_ptr<utils::phased_barrier::operation> read_operation;
    };
    struct dismantling_state {
        foreign_unique_ptr<reader_params> params;
        foreign_unique_ptr<utils::phased_barrier::operation> read_operation;
        std::variant<foreign_unique_ptr<flat_mutation_reader>, paused_reader> reader;
        circular_buffer<mutation_fragment> buffer;
    };
    struct ready_to_save_state {
        foreign_unique_ptr<reader_params> params;
        foreign_unique_ptr<utils::phased_barrier::operation> read_operation;
        std::variant<foreign_unique_ptr<flat_mutation_reader>, paused_reader> reader;
        circular_buffer<mutation_fragment> buffer;
    };
    struct paused_state {
        foreign_unique_ptr<reader_params> params;
        foreign_unique_ptr<utils::phased_barrier::operation> read_operation;
        reader_concurrency_semaphore::inactive_read_handle handle;
    };
    struct evicted_state {
    };

    //              ( )    (O)
    //               |      ^
    //               |      |
    //         +--- inexistent ---+
    //         |                  |
    //     (1) |              (3) |    (3)
    //         |                  |  +------ evicted -> (O)
    //  successful_lookup         |  |          ^
    //     |         |            |  |  (7)     |
    //     |         |            |  +-------+  | (8)
    //     |         |    (4)     |  |       |  |
    //     |         +----------> used      paused
    //     |                      |  |  (6)  ^  |
    // (2) |                      |  +-------+  |
    //     |                  (5) |             | (5)
    //     |                      |             |
    //     |                      |             |
    //     |                 dismantling <------+
    //     |                      |
    //     |                  (2) |
    //     |                      |
    //     +---------------> ready_to_save
    //                            |
    //                           (O)
    //
    //  1) lookup_readers()
    //  2) save_readers()
    //  3) do_make_remote_reader()
    //  4) make_remote_reader()
    //  5) dismantle_reader()
    //  6) pause_reader()
    //  7) try_resume() - success
    //  8) try_resume() - failure
    using reader_state = std::variant<
        inexistent_state,
        successful_lookup_state,
        used_state,
        paused_state,
        evicted_state,
        dismantling_state,
        ready_to_save_state>;

    struct dismantle_buffer_stats {
        size_t partitions = 0;
        size_t fragments = 0;
        size_t bytes = 0;
        size_t discarded_partitions = 0;
        size_t discarded_fragments = 0;
        size_t discarded_bytes = 0;

        void add(const schema& s, const mutation_fragment& mf) {
            partitions += unsigned(mf.is_partition_start());
            ++fragments;
            bytes += mf.memory_usage(s);
        }
        void add(const schema& s, const range_tombstone& rt) {
            ++fragments;
            bytes += rt.memory_usage(s);
        }
        void add(const schema& s, const static_row& sr) {
            ++fragments;
            bytes += sr.memory_usage(s);
        }
        void add(const schema& s, const partition_start& ps) {
            ++partitions;
            ++fragments;
            bytes += ps.memory_usage(s);
        }
        void add_discarded(const schema& s, const mutation_fragment& mf) {
            discarded_partitions += unsigned(mf.is_partition_start());
            ++discarded_fragments;
            discarded_bytes += mf.memory_usage(s);
        }
        void add_discarded(const schema& s, const range_tombstone& rt) {
            ++discarded_fragments;
            discarded_bytes += rt.memory_usage(s);
        }
        void add_discarded(const schema& s, const static_row& sr) {
            ++discarded_fragments;
            discarded_bytes += sr.memory_usage(s);
        }
        void add_discarded(const schema& s, const partition_start& ps) {
            ++discarded_partitions;
            ++discarded_fragments;
            discarded_bytes += ps.memory_usage(s);
        }
        friend std::ostream& operator<<(std::ostream& os, const dismantle_buffer_stats& s) {
            os << format(
                    "kept {} partitions/{} fragments/{} bytes, discarded {} partitions/{} fragments/{} bytes",
                    s.partitions,
                    s.fragments,
                    s.bytes,
                    s.discarded_partitions,
                    s.discarded_fragments,
                    s.discarded_bytes);
            return os;
        }
    };

    distributed<database>& _db;
    schema_ptr _schema;
    const query::read_command& _cmd;
    const dht::partition_range_vector& _ranges;
    tracing::trace_state_ptr _trace_state;

    // One for each shard. Index is shard id.
    std::vector<reader_state> _readers;
    std::vector<reader_concurrency_semaphore*> _semaphores;

    gate _dismantling_gate;

    reader_concurrency_semaphore& semaphore() {
        return *_semaphores[engine().cpu_id()];
    }

    static std::string_view reader_state_to_string(const reader_state& rs);

    static future<bundled_remote_reader> do_make_remote_reader(
            distributed<database>& db,
            shard_id shard,
            schema_ptr schema,
            const dht::partition_range& pr,
            const query::partition_slice& ps,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state);

    future<foreign_unique_ptr<flat_mutation_reader>> make_remote_reader(
            shard_id shard,
            schema_ptr schema,
            const dht::partition_range& pr,
            const query::partition_slice& ps,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            mutation_reader::forwarding fwd_mr);

    void dismantle_reader(shard_id shard, future<paused_or_stopped_reader>&& reader_fut);

    dismantle_buffer_stats dismantle_combined_buffer(circular_buffer<mutation_fragment> combined_buffer, const dht::decorated_key& pkey);
    dismantle_buffer_stats dismantle_compaction_state(detached_compaction_state compaction_state);
    future<> save_reader(ready_to_save_state& current_state, const dht::decorated_key& last_pkey,
            const std::optional<clustering_key_prefix>& last_ckey);

public:
    read_context(distributed<database>& db, schema_ptr s, const query::read_command& cmd, const dht::partition_range_vector& ranges,
            tracing::trace_state_ptr trace_state)
            : _db(db)
            , _schema(std::move(s))
            , _cmd(cmd)
            , _ranges(ranges)
            , _trace_state(std::move(trace_state)) {
        _readers.resize(smp::count);
        _semaphores.resize(smp::count);
    }

    read_context(read_context&&) = delete;
    read_context(const read_context&) = delete;

    read_context& operator=(read_context&&) = delete;
    read_context& operator=(const read_context&) = delete;

    virtual future<foreign_unique_ptr<flat_mutation_reader>> create_reader(
            shard_id shard,
            schema_ptr schema,
            const dht::partition_range& pr,
            const query::partition_slice& ps,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            mutation_reader::forwarding fwd_mr) override {
        return make_remote_reader(shard, std::move(schema), pr, ps, pc, std::move(trace_state), fwd_mr);
    }

    virtual void destroy_reader(shard_id shard, future<paused_or_stopped_reader> reader_fut) noexcept override {
        dismantle_reader(shard, std::move(reader_fut));
    }

    virtual future<> pause(foreign_unique_ptr<flat_mutation_reader> reader) override;
    virtual future<foreign_unique_ptr<flat_mutation_reader>> try_resume(shard_id shard) override;

    future<> lookup_readers();

    future<> save_readers(circular_buffer<mutation_fragment> unconsumed_buffer, detached_compaction_state compaction_state,
            std::optional<clustering_key_prefix> last_ckey);

    future<> stop();
};

// Deliberatly not using the `reader_state` alias here, so that we can enlist
// the help of the compiler to keep this up-to-date.
std::string_view read_context::reader_state_to_string(const std::variant<
        inexistent_state,
        successful_lookup_state,
        used_state,
        paused_state,
        evicted_state,
        dismantling_state,
        ready_to_save_state>& rs) {
    static const std::array<const char*, 7> reader_state_names{
        "inexistent",
        "successful_lookup",
        "used",
        "paused",
        "evicted",
        "dismantling",
        "ready_to_save",
    };
    return reader_state_names.at(rs.index());
}

future<read_context::bundled_remote_reader> read_context::do_make_remote_reader(
        distributed<database>& db,
        shard_id shard,
        schema_ptr schema,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        const io_priority_class&,
        tracing::trace_state_ptr trace_state) {
    return db.invoke_on(shard, [gs = global_schema_ptr(schema), &pr, &ps, gts = tracing::global_trace_state_ptr(std::move(trace_state))] (
                database& db) {
        auto s = gs.get();
        auto& table = db.find_column_family(s);
        //TODO need a way to transport io_priority_calls across shards
        auto& pc = service::get_local_sstable_query_read_priority();
        auto params = reader_params(pr, ps);
        auto read_operation = table.read_in_progress();
        auto reader = table.as_mutation_source().make_reader(std::move(s), *params.range, *params.slice, pc, gts.get());

        return make_ready_future<bundled_remote_reader>(bundled_remote_reader{
                make_foreign(std::make_unique<reader_params>(std::move(params))),
                make_foreign(std::make_unique<utils::phased_barrier::operation>(std::move(read_operation))),
                make_foreign(std::make_unique<flat_mutation_reader>(std::move(reader))),
                &table.read_concurrency_semaphore()});
    });
}

future<foreign_unique_ptr<flat_mutation_reader>> read_context::make_remote_reader(
        shard_id shard,
        schema_ptr schema,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        mutation_reader::forwarding) {
    auto& rs = _readers[shard];

    if (!std::holds_alternative<successful_lookup_state>(rs) && !std::holds_alternative<inexistent_state>(rs)
            && !std::holds_alternative<evicted_state>(rs)) {
        auto msg = format("Unexpected request to create reader for shard {}."
                " The reader is expected to be in either `successful_lookup`, `inexistent` or `evicted` state,"
                " but is in `{}` state instead.", shard, reader_state_to_string(rs));
        mmq_log.warn(msg.c_str());
        throw std::logic_error(msg.c_str());
    }

    // The reader is either in inexistent, evicted or successful lookup state.
    if (auto current_state = std::get_if<successful_lookup_state>(&rs)) {
        auto reader = std::move(current_state->reader);
        rs = used_state{std::move(current_state->params), std::move(current_state->read_operation)};
        return make_ready_future<foreign_unique_ptr<flat_mutation_reader>>(std::move(reader));
    }

    return do_make_remote_reader(_db, shard, std::move(schema), pr, ps, pc, std::move(trace_state)).then(
            [this, &rs, shard] (bundled_remote_reader&& bundled_reader) mutable {
        _semaphores[shard] = bundled_reader.semaphore;
        rs = used_state{std::move(bundled_reader.params), std::move(bundled_reader.read_operation)};
        return make_ready_future<foreign_unique_ptr<flat_mutation_reader>>(std::move(bundled_reader.reader));
    });
}

void read_context::dismantle_reader(shard_id shard, future<paused_or_stopped_reader>&& reader_fut) {
    with_gate(_dismantling_gate, [this, shard, reader_fut = std::move(reader_fut)] () mutable {
        return reader_fut.then_wrapped([this, shard] (future<paused_or_stopped_reader>&& reader_fut) {
            auto& rs = _readers[shard];

            if (reader_fut.failed()) {
                mmq_log.debug("Failed to stop reader on shard {}: {}", shard, reader_fut.get_exception());
                ++_db.local().get_stats().multishard_query_failed_reader_stops;
                rs = inexistent_state{};
                return;
            }

            auto reader = reader_fut.get0();
            if (auto* maybe_used_state = std::get_if<used_state>(&rs)) {
                auto read_operation = std::move(maybe_used_state->read_operation);
                auto params = std::move(maybe_used_state->params);
                rs = dismantling_state{std::move(params), std::move(read_operation), std::move(reader.remote_reader),
                        std::move(reader.unconsumed_fragments)};
            } else if (auto* maybe_paused_state = std::get_if<paused_state>(&rs)) {
                auto read_operation = std::move(maybe_paused_state->read_operation);
                auto params = std::move(maybe_paused_state->params);
                auto handle = maybe_paused_state->handle;
                rs = dismantling_state{std::move(params), std::move(read_operation), paused_reader{shard, handle, reader.has_pending_next_partition},
                        std::move(reader.unconsumed_fragments)};
            // Do nothing for evicted readers.
            } else if (!std::holds_alternative<evicted_state>(rs)) {
                mmq_log.warn(
                        "Unexpected request to dismantle reader in state `{}` for shard {}."
                        " Reader was not created nor is in the process of being created.",
                        reader_state_to_string(rs),
                        shard);
            }
        });
    });
}

future<> read_context::stop() {
    auto pr = promise<>();
    auto fut = pr.get_future();
    auto gate_fut = _dismantling_gate.is_closed() ? make_ready_future<>() : _dismantling_gate.close();
    gate_fut.then([this] {
        for (shard_id shard = 0; shard != smp::count; ++shard) {
            if (auto* maybe_dismantling_state = std::get_if<dismantling_state>(&_readers[shard])) {
                _db.invoke_on(shard, [schema = global_schema_ptr(_schema), reader = std::move(maybe_dismantling_state->reader),
                        params = std::move(maybe_dismantling_state->params),
                        read_operation = std::move(maybe_dismantling_state->read_operation)] (database& db) mutable {
                    if (auto* maybe_stopped_reader = std::get_if<foreign_unique_ptr<flat_mutation_reader>>(&reader)) {
                        maybe_stopped_reader->release();
                    } else {
                        // We cannot use semaphore() here, as this can be already destroyed.
                        auto& table = db.find_column_family(schema);
                        table.read_concurrency_semaphore().unregister_inactive_read(std::get<paused_reader>(reader).handle);
                    }
                    params.release();
                    read_operation.release();
                });
            }
        }
    }).finally([pr = std::move(pr)] () mutable {
        pr.set_value();
    });
    return fut;
}

read_context::dismantle_buffer_stats read_context::dismantle_combined_buffer(circular_buffer<mutation_fragment> combined_buffer,
        const dht::decorated_key& pkey) {
    auto& partitioner = dht::global_partitioner();

    std::vector<mutation_fragment> tmp_buffer;
    dismantle_buffer_stats stats;

    auto rit = std::reverse_iterator(combined_buffer.end());
    const auto rend = std::reverse_iterator(combined_buffer.begin());
    for (;rit != rend; ++rit) {
        if (rit->is_partition_start()) {
            const auto shard = partitioner.shard_of(rit->as_partition_start().key().token());
            auto maybe_dismantling_state = std::get_if<dismantling_state>(&_readers[shard]);

            // It is possible that the reader this partition originates from
            // does not exist anymore. Either because we failed stopping it or
            // because it was evicted.
            if (!maybe_dismantling_state) {
                for (auto& smf : tmp_buffer) {
                    stats.add_discarded(*_schema, smf);
                }
                stats.add_discarded(*_schema, *rit);
                tmp_buffer.clear();
                continue;
            }

            auto& shard_buffer = maybe_dismantling_state->buffer;
            for (auto& smf : tmp_buffer) {
                stats.add(*_schema, smf);
                shard_buffer.emplace_front(std::move(smf));
            }
            stats.add(*_schema, *rit);
            shard_buffer.emplace_front(std::move(*rit));
            tmp_buffer.clear();
        } else {
            tmp_buffer.emplace_back(std::move(*rit));
        }
    }

    const auto shard = partitioner.shard_of(pkey.token());
    auto& shard_buffer = std::get<dismantling_state>(_readers[shard]).buffer;
    for (auto& smf : tmp_buffer) {
        stats.add(*_schema, smf);
        shard_buffer.emplace_front(std::move(smf));
    }

    return stats;
}

read_context::dismantle_buffer_stats read_context::dismantle_compaction_state(detached_compaction_state compaction_state) {
    auto stats = dismantle_buffer_stats();
    auto& partitioner = dht::global_partitioner();
    const auto shard = partitioner.shard_of(compaction_state.partition_start.key().token());
    auto maybe_dismantling_state = std::get_if<dismantling_state>(&_readers[shard]);

    // It is possible that the reader this partition originates from does not
    // exist anymore. Either because we failed stopping it or because it was
    // evicted.
    if (!maybe_dismantling_state) {
        for (auto& rt : compaction_state.range_tombstones) {
            stats.add_discarded(*_schema, rt);
        }
        if (compaction_state.static_row) {
            stats.add_discarded(*_schema, *compaction_state.static_row);
        }
        stats.add_discarded(*_schema, compaction_state.partition_start);
        return stats;
    }

    auto& shard_buffer = maybe_dismantling_state->buffer;

    for (auto& rt : compaction_state.range_tombstones | boost::adaptors::reversed) {
        stats.add(*_schema, rt);
        shard_buffer.emplace_front(std::move(rt));
    }

    if (compaction_state.static_row) {
        stats.add(*_schema, *compaction_state.static_row);
        shard_buffer.emplace_front(std::move(*compaction_state.static_row));
    }

    stats.add(*_schema, compaction_state.partition_start);
    shard_buffer.emplace_front(std::move(compaction_state.partition_start));

    return stats;
}

future<> read_context::save_reader(ready_to_save_state& current_state, const dht::decorated_key& last_pkey,
        const std::optional<clustering_key_prefix>& last_ckey) {
    auto* maybe_stopped_reader = std::get_if<foreign_unique_ptr<flat_mutation_reader>>(&current_state.reader);
    const auto shard = maybe_stopped_reader
        ? maybe_stopped_reader->get_owner_shard()
        : std::get<paused_reader>(current_state.reader).shard;

    return _db.invoke_on(shard, [this, shard, query_uuid = _cmd.query_uuid, query_ranges = _ranges, &current_state,
            &last_pkey, &last_ckey, gts = tracing::global_trace_state_ptr(_trace_state)] (database& db) mutable {
        try {
            auto params = current_state.params.release();
            auto read_operation = current_state.read_operation.release();

            flat_mutation_reader_opt reader;
            if (auto* maybe_paused_reader = std::get_if<paused_reader>(&current_state.reader)) {
                if (auto inactive_read_ptr = semaphore().unregister_inactive_read(maybe_paused_reader->handle)) {
                    reader = std::move(*static_cast<inactive_read&>(*inactive_read_ptr).reader);
                    if (maybe_paused_reader->has_pending_next_partition) {
                        reader->next_partition();
                    }
                }
            } else {
                reader = std::move(*std::get<foreign_unique_ptr<flat_mutation_reader>>(current_state.reader));
            }

            if (!reader) {
                return;
            }

            auto& buffer = current_state.buffer;
            const auto fragments = buffer.size();
            const auto size_before = reader->buffer_size();

            auto rit = std::reverse_iterator(buffer.cend());
            auto rend = std::reverse_iterator(buffer.cbegin());
            auto& schema = *reader->schema();
            for (;rit != rend; ++rit) {
                // Copy the fragment, the buffer is on another shard.
                reader->unpop_mutation_fragment(mutation_fragment(schema, *rit));
            }

            const auto size_after = reader->buffer_size();

            auto querier = query::shard_mutation_querier(
                    std::move(query_ranges),
                    std::move(params->range),
                    std::move(params->slice),
                    std::move(*reader),
                    last_pkey,
                    last_ckey);

            db.get_querier_cache().insert(query_uuid, std::move(querier), gts.get());

            db.get_stats().multishard_query_unpopped_fragments += fragments;
            db.get_stats().multishard_query_unpopped_bytes += (size_after - size_before);
        } catch (...) {
            // We don't want to fail a read just because of a failure to
            // save any of the readers.
            mmq_log.debug("Failed to save reader: {}", std::current_exception());
            ++db.get_stats().multishard_query_failed_reader_saves;
        }
    }).handle_exception([this, shard] (std::exception_ptr e) {
        // We don't want to fail a read just because of a failure to
        // save any of the readers.
        mmq_log.debug("Failed to save reader on shard {}: {}", shard, e);
        // This will account the failure on the local shard but we don't
        // know where exactly the failure happened anyway.
        ++_db.local().get_stats().multishard_query_failed_reader_saves;
    });
}

future<> read_context::pause(foreign_unique_ptr<flat_mutation_reader> reader) {
    const auto shard = reader.get_owner_shard();
    return _db.invoke_on(shard, [this, reader = std::move(reader)] (database& db) mutable {
        return semaphore().register_inactive_read(std::make_unique<inactive_read>(std::move(reader)));
    }).then([this, shard] (reader_concurrency_semaphore::inactive_read_handle handle) {
        auto& current_state = std::get<used_state>(_readers[shard]);
        _readers[shard] = paused_state{std::move(current_state.params), std::move(current_state.read_operation), handle};
    });
}

future<foreign_unique_ptr<flat_mutation_reader>> read_context::try_resume(shard_id shard) {
    return _db.invoke_on(shard, [this, handle = std::get<paused_state>(_readers[shard]).handle] (database& db) mutable {
        if (auto inactive_read_ptr = semaphore().unregister_inactive_read(handle)) {
            return std::move(static_cast<inactive_read&>(*inactive_read_ptr).reader);
        }
        return foreign_unique_ptr<flat_mutation_reader>();
    }).then([this, shard] (foreign_unique_ptr<flat_mutation_reader> reader) {
        if (reader) {
            auto& current_state = std::get<paused_state>(_readers[shard]);
            _readers[shard] = used_state{std::move(current_state.params), std::move(current_state.read_operation)};
        } else {
            _readers[shard] = evicted_state{};
        }
        return std::move(reader);
    });
}

future<> read_context::lookup_readers() {
    if (_cmd.query_uuid == utils::UUID{} || _cmd.is_first_page) {
        return make_ready_future<>();
    }

    return parallel_for_each(boost::irange(0u, smp::count), [this] (shard_id shard) {
        return _db.invoke_on(shard,
                [shard, cmd = &_cmd, ranges = &_ranges, gs = global_schema_ptr(_schema), gts = tracing::global_trace_state_ptr(_trace_state)] (
                        database& db) mutable -> future<reader_state, reader_concurrency_semaphore*> {
            auto schema = gs.get();
            auto querier_opt = db.get_querier_cache().lookup_shard_mutation_querier(cmd->query_uuid, *schema, *ranges, cmd->slice, gts.get());
            auto& table = db.find_column_family(schema);
            if (!querier_opt) {
                return make_ready_future<reader_state, reader_concurrency_semaphore*>(inexistent_state{}, &table.read_concurrency_semaphore());
            }

            auto& q = *querier_opt;
            auto params = make_foreign(std::make_unique<reader_params>(std::move(q).reader_range(), std::move(q).reader_slice()));
            auto read_operation = make_foreign(std::make_unique<utils::phased_barrier::operation>(table.read_in_progress()));
            auto reader = make_foreign(std::make_unique<flat_mutation_reader>(std::move(q).reader()));
            return make_ready_future<reader_state, reader_concurrency_semaphore*>(
                    successful_lookup_state{std::move(params), std::move(read_operation), std::move(reader)},
                    &table.read_concurrency_semaphore());
        }).then([this, shard] (reader_state&& state, reader_concurrency_semaphore* sem) {
            _readers[shard] = std::move(state);
            _semaphores[shard] = sem;
        });
    });
}

future<> read_context::save_readers(circular_buffer<mutation_fragment> unconsumed_buffer, detached_compaction_state compaction_state,
            std::optional<clustering_key_prefix> last_ckey) {
    if (_cmd.query_uuid == utils::UUID{}) {
        return make_ready_future<>();
    }

    return _dismantling_gate.close().then([this, unconsumed_buffer = std::move(unconsumed_buffer), compaction_state = std::move(compaction_state),
          last_ckey = std::move(last_ckey)] () mutable {
        auto last_pkey = compaction_state.partition_start.key();

        const auto cb_stats = dismantle_combined_buffer(std::move(unconsumed_buffer), last_pkey);
        tracing::trace(_trace_state, "Dismantled combined buffer: {}", cb_stats);

        const auto cs_stats = dismantle_compaction_state(std::move(compaction_state));
        tracing::trace(_trace_state, "Dismantled compaction state: {}", cs_stats);

        return do_with(std::move(last_pkey), std::move(last_ckey), [this] (const dht::decorated_key& last_pkey,
                const std::optional<clustering_key_prefix>& last_ckey) {
            return parallel_for_each(_readers, [this, &last_pkey, &last_ckey] (reader_state& rs) {
                if (auto* maybe_successful_lookup_state = std::get_if<successful_lookup_state>(&rs)) {
                    auto& current_state = *maybe_successful_lookup_state;
                    rs = ready_to_save_state{std::move(current_state.params), std::move(current_state.read_operation),
                            std::move(current_state.reader), circular_buffer<mutation_fragment>{}};
                    return save_reader(std::get<ready_to_save_state>(rs), last_pkey, last_ckey);
                }

                if (auto* maybe_dismantling_state = std::get_if<dismantling_state>(&rs)) {
                    auto& current_state = *maybe_dismantling_state;
                    rs = ready_to_save_state{std::move(current_state.params), std::move(current_state.read_operation),
                            std::move(current_state.reader), std::move(current_state.buffer)};
                    return save_reader(std::get<ready_to_save_state>(rs), last_pkey, last_ckey);
                }

                return make_ready_future<>();
            });
        });
    });
}

static future<reconcilable_result> do_query_mutations(
        distributed<database>& db,
        schema_ptr s,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout,
        query::result_memory_accounter&& accounter) {
    return do_with(seastar::make_shared<read_context>(db, s, cmd, ranges, trace_state), [s, &cmd, &ranges, trace_state, timeout,
            accounter = std::move(accounter)] (shared_ptr<read_context>& ctx) mutable {
        return ctx->lookup_readers().then([&ctx, s = std::move(s), &cmd, &ranges, trace_state, timeout,
                accounter = std::move(accounter)] () mutable {
            auto ms = mutation_source([&] (schema_ptr s,
                    const dht::partition_range& pr,
                    const query::partition_slice& ps,
                    const io_priority_class& pc,
                    tracing::trace_state_ptr trace_state,
                    streamed_mutation::forwarding,
                    mutation_reader::forwarding fwd_mr) {
                return make_multishard_combining_reader(ctx, dht::global_partitioner(), std::move(s), pr, ps, pc, std::move(trace_state), fwd_mr);
            });
            auto reader = make_flat_multi_range_reader(s, std::move(ms), ranges, cmd.slice, service::get_local_sstable_query_read_priority(),
                    trace_state, mutation_reader::forwarding::no);

            auto compaction_state = make_lw_shared<compact_for_mutation_query_state>(*s, cmd.timestamp, cmd.slice, cmd.row_limit,
                    cmd.partition_limit);

            return do_with(std::move(reader), std::move(compaction_state), [&, accounter = std::move(accounter), timeout] (
                        flat_mutation_reader& reader, lw_shared_ptr<compact_for_mutation_query_state>& compaction_state) mutable {
                auto rrb = reconcilable_result_builder(*reader.schema(), cmd.slice, std::move(accounter));
                return query::consume_page(reader,
                        compaction_state,
                        cmd.slice,
                        std::move(rrb),
                        cmd.row_limit,
                        cmd.partition_limit,
                        cmd.timestamp,
                        timeout).then([&] (std::optional<clustering_key_prefix>&& last_ckey, reconcilable_result&& result) mutable {
                    return make_ready_future<std::optional<clustering_key_prefix>,
                            reconcilable_result,
                            circular_buffer<mutation_fragment>,
                            lw_shared_ptr<compact_for_mutation_query_state>>(std::move(last_ckey), std::move(result), reader.detach_buffer(),
                                    std::move(compaction_state));
                });
            }).then_wrapped([&ctx] (future<std::optional<clustering_key_prefix>, reconcilable_result, circular_buffer<mutation_fragment>,
                    lw_shared_ptr<compact_for_mutation_query_state>>&& result_fut) {
                if (result_fut.failed()) {
                    return make_exception_future<reconcilable_result>(std::move(result_fut.get_exception()));
                }

                auto [last_ckey, result, unconsumed_buffer, compaction_state] = result_fut.get();
                if (!compaction_state->are_limits_reached() && !result.is_short_read()) {
                    return make_ready_future<reconcilable_result>(std::move(result));
                }

                return ctx->save_readers(std::move(unconsumed_buffer), std::move(*compaction_state).detach_state(),
                        std::move(last_ckey)).then_wrapped([result = std::move(result)] (future<>&&) mutable {
                    return make_ready_future<reconcilable_result>(std::move(result));
                });
            }).finally([&ctx] {
                return ctx->stop();
            });
        });
    });
}

future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature> query_mutations_on_all_shards(
        distributed<database>& db,
        schema_ptr s,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        uint64_t max_size,
        db::timeout_clock::time_point timeout) {
    if (cmd.row_limit == 0 || cmd.slice.partition_row_limit() == 0 || cmd.partition_limit == 0) {
        return make_ready_future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>(
                make_foreign(make_lw_shared<reconcilable_result>()),
                db.local().find_column_family(s).get_global_cache_hit_rate());
    }

    return db.local().get_result_memory_limiter().new_mutation_read(max_size).then([&, s = std::move(s), trace_state = std::move(trace_state),
            timeout] (query::result_memory_accounter accounter) mutable {
        return do_query_mutations(db, s, cmd, ranges, std::move(trace_state), timeout, std::move(accounter)).then_wrapped(
                    [&db, s = std::move(s)] (future<reconcilable_result>&& f) {
            auto& local_db = db.local();
            auto& stats = local_db.get_stats();
            if (f.failed()) {
                ++stats.total_reads_failed;
                return make_exception_future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>(f.get_exception());
            } else {
                ++stats.total_reads;
                auto result = f.get0();
                stats.short_mutation_queries += bool(result.is_short_read());
                auto hit_rate = local_db.find_column_family(s).get_global_cache_hit_rate();
                return make_ready_future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>(
                        make_foreign(make_lw_shared<reconcilable_result>(std::move(result))), hit_rate);
            }
        });
    });
}
