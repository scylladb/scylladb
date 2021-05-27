/*
 * Copyright (C) 2018-present ScyllaDB
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
#include "database.hh"
#include "db/config.hh"
#include "query-result-writer.hh"

#include <seastar/core/coroutine.hh>

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
/// 2) This usage is implemented in the `do_query()` function below.
/// 3) Both, `read_context::lookup_readers()` and `read_context::save_readers()`
///    knows to do nothing when the query is not stateful and just short
///    circuit.
class read_context : public reader_lifecycle_policy {

    //              ( )    (O)
    //               |      ^
    //               |      |
    //         +--- inexistent ---+
    //         |                  |
    //     (1) |              (3) |
    //         |                  |
    //  successful_lookup         |
    //     |         |            |
    //     |         |            |
    //     |         |    (3)     |
    //     |         +---------> used
    // (2) |                      |
    //     |                  (4) |
    //     |                      |
    //     +---------------> saving_state
    //                            |
    //                           (O)
    //
    //  1) lookup_readers()
    //  2) save_readers()
    //  3) create_reader()
    //  4) destroy_reader()
    enum class reader_state {
        inexistent,
        successful_lookup,
        used,
        saving,
    };

    struct reader_meta {
        struct remote_parts {
            reader_permit permit;
            std::unique_ptr<const dht::partition_range> range;
            std::unique_ptr<const query::partition_slice> slice;
            utils::phased_barrier::operation read_operation;

            remote_parts(
                    reader_permit permit,
                    std::unique_ptr<const dht::partition_range> range = nullptr,
                    std::unique_ptr<const query::partition_slice> slice = nullptr,
                    utils::phased_barrier::operation read_operation = {})
                : permit(std::move(permit))
                , range(std::move(range))
                , slice(std::move(slice))
                , read_operation(std::move(read_operation)) {
            }
        };

        reader_state state = reader_state::inexistent;
        foreign_unique_ptr<remote_parts> rparts;
        foreign_unique_ptr<reader_concurrency_semaphore::inactive_read_handle> handle;
        std::optional<flat_mutation_reader::tracked_buffer> buffer;

        reader_meta() = default;

        // Remote constructor.
        reader_meta(reader_state s, std::optional<remote_parts> rp = {}, reader_concurrency_semaphore::inactive_read_handle h = {})
            : state(s)
            , handle(make_foreign(std::make_unique<reader_concurrency_semaphore::inactive_read_handle>(std::move(h)))) {
            if (rp) {
                rparts = make_foreign(std::make_unique<remote_parts>(std::move(*rp)));
            }
        }
    };

    struct dismantle_buffer_stats {
        size_t partitions = 0;
        size_t fragments = 0;
        size_t bytes = 0;
        size_t discarded_partitions = 0;
        size_t discarded_fragments = 0;
        size_t discarded_bytes = 0;

        void add(const mutation_fragment& mf) {
            partitions += unsigned(mf.is_partition_start());
            ++fragments;
            bytes += mf.memory_usage();
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
        void add_discarded(const mutation_fragment& mf) {
            discarded_partitions += unsigned(mf.is_partition_start());
            ++discarded_fragments;
            discarded_bytes += mf.memory_usage();
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
    reader_permit _permit;
    const query::read_command& _cmd;
    const dht::partition_range_vector& _ranges;
    tracing::trace_state_ptr _trace_state;

    // One for each shard. Index is shard id.
    std::vector<reader_meta> _readers;
    std::vector<reader_concurrency_semaphore*> _semaphores;

    static std::string_view reader_state_to_string(reader_state rs);

    dismantle_buffer_stats dismantle_combined_buffer(flat_mutation_reader::tracked_buffer combined_buffer, const dht::decorated_key& pkey);
    dismantle_buffer_stats dismantle_compaction_state(detached_compaction_state compaction_state);
    future<> save_reader(shard_id shard, const dht::decorated_key& last_pkey, const std::optional<clustering_key_prefix>& last_ckey);

public:
    read_context(distributed<database>& db, schema_ptr s, const query::read_command& cmd, const dht::partition_range_vector& ranges,
            tracing::trace_state_ptr trace_state)
            : _db(db)
            , _schema(std::move(s))
            , _permit(_db.local().get_reader_concurrency_semaphore().make_permit(_schema.get(), "multishard-mutation-query"))
            , _cmd(cmd)
            , _ranges(ranges)
            , _trace_state(std::move(trace_state))
            , _semaphores(smp::count, nullptr) {
        _readers.resize(smp::count);
    }

    read_context(read_context&&) = delete;
    read_context(const read_context&) = delete;

    read_context& operator=(read_context&&) = delete;
    read_context& operator=(const read_context&) = delete;

    distributed<database>& db() {
        return _db;
    }

    reader_permit permit() const {
        return _permit;
    }

    virtual flat_mutation_reader create_reader(
            schema_ptr schema,
            reader_permit permit,
            const dht::partition_range& pr,
            const query::partition_slice& ps,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            mutation_reader::forwarding fwd_mr) override;

    virtual future<> destroy_reader(shard_id shard, stopped_reader reader) noexcept override;

    virtual reader_concurrency_semaphore& semaphore() override {
        const auto shard = this_shard_id();
        if (!_semaphores[shard]) {
            _semaphores[shard] = &_db.local().get_reader_concurrency_semaphore();
        }
        return *_semaphores[shard];
    }

    future<> lookup_readers();

    future<> save_readers(flat_mutation_reader::tracked_buffer unconsumed_buffer, detached_compaction_state compaction_state,
            std::optional<clustering_key_prefix> last_ckey);

    future<> stop();
};

std::string_view read_context::reader_state_to_string(reader_state rs) {
    switch (rs) {
        case reader_state::inexistent:
            return "inexistent";
        case reader_state::successful_lookup:
            return "successful_lookup";
        case reader_state::used:
            return "used";
        case reader_state::saving:
            return "saving";
    }
    // If we got here, we are logging an error anyway, so the above layers
    // (should) have detected the invalid state.
    return "invalid";
}

flat_mutation_reader read_context::create_reader(
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        mutation_reader::forwarding fwd_mr) {
    const auto shard = this_shard_id();
    auto& rm = _readers[shard];

    if (rm.state != reader_state::used && rm.state != reader_state::successful_lookup && rm.state != reader_state::inexistent) {
        auto msg = format("Unexpected request to create reader for shard {}."
                " The reader is expected to be in either `used`, `successful_lookup` or `inexistent` state,"
                " but is in `{}` state instead.", shard, reader_state_to_string(rm.state));
        mmq_log.warn(msg.c_str());
        throw std::logic_error(msg.c_str());
    }

    // The reader is either in inexistent or successful lookup state.
    if (rm.state == reader_state::successful_lookup) {
        if (auto reader_opt = try_resume(std::move(*rm.handle))) {
            rm.state = reader_state::used;
            return std::move(*reader_opt);
        }
    }

    auto& table = _db.local().find_column_family(schema);

    if (!rm.rparts) {
        rm.rparts = make_foreign(std::make_unique<reader_meta::remote_parts>(std::move(permit)));
    }

    rm.rparts->range = std::make_unique<const dht::partition_range>(pr);
    rm.rparts->slice = std::make_unique<const query::partition_slice>(ps);
    rm.rparts->read_operation = table.read_in_progress();
    rm.state = reader_state::used;

    return table.as_mutation_source().make_reader(std::move(schema), rm.rparts->permit, *rm.rparts->range, *rm.rparts->slice, pc,
            std::move(trace_state), streamed_mutation::forwarding::no, fwd_mr);
}

future<> read_context::destroy_reader(shard_id shard, stopped_reader reader) noexcept {
            auto& rm = _readers[shard];

            if (rm.state == reader_state::used) {
                rm.state = reader_state::saving;
                rm.handle = std::move(reader.handle);
                rm.buffer = std::move(reader.unconsumed_fragments);
            } else {
                mmq_log.warn(
                        "Unexpected request to dismantle reader in state `{}` for shard {}."
                        " Reader was not created nor is in the process of being created.",
                        reader_state_to_string(rm.state),
                        shard);
            }
    return make_ready_future<>();
}

future<> read_context::stop() {
        return parallel_for_each(smp::all_cpus(), [this] (unsigned shard) {
            if (_readers[shard].rparts) {
                return _db.invoke_on(shard, [rm = std::move(_readers[shard])] (database& db) mutable {
                    auto rparts = rm.rparts.release();
                    auto irh = rm.handle.release();
                    if (*irh) {
                        auto reader_opt = rparts->permit.semaphore().unregister_inactive_read(std::move(*rm.handle));
                        if (reader_opt) {
                            return reader_opt->close().then([rparts = std::move(rparts)] { });
                        }
                    }
                    return make_ready_future<>();
                });
            }
            return make_ready_future<>();
        });
}

read_context::dismantle_buffer_stats read_context::dismantle_combined_buffer(flat_mutation_reader::tracked_buffer combined_buffer,
        const dht::decorated_key& pkey) {
    auto& sharder = _schema->get_sharder();

    std::vector<mutation_fragment> tmp_buffer;
    dismantle_buffer_stats stats;

    auto rit = std::reverse_iterator(combined_buffer.end());
    const auto rend = std::reverse_iterator(combined_buffer.begin());
    for (;rit != rend; ++rit) {
        if (rit->is_partition_start()) {
            const auto shard = sharder.shard_of(rit->as_partition_start().key().token());

            // It is possible that the reader this partition originates from
            // does not exist anymore. Either because we failed stopping it or
            // because it was evicted.
            if (_readers[shard].state != reader_state::saving) {
                for (auto& smf : tmp_buffer) {
                    stats.add_discarded(smf);
                }
                stats.add_discarded(*rit);
                tmp_buffer.clear();
                continue;
            }

            auto& shard_buffer = *_readers[shard].buffer;
            for (auto& smf : tmp_buffer) {
                stats.add(smf);
                shard_buffer.emplace_front(std::move(smf));
            }
            stats.add(*rit);
            shard_buffer.emplace_front(std::move(*rit));
            tmp_buffer.clear();
        } else {
            tmp_buffer.emplace_back(std::move(*rit));
        }
    }

    const auto shard = sharder.shard_of(pkey.token());
    auto& shard_buffer = *_readers[shard].buffer;
    for (auto& smf : tmp_buffer) {
        stats.add(smf);
        shard_buffer.emplace_front(std::move(smf));
    }

    return stats;
}

read_context::dismantle_buffer_stats read_context::dismantle_compaction_state(detached_compaction_state compaction_state) {
    auto stats = dismantle_buffer_stats();
    auto& sharder = _schema->get_sharder();
    const auto shard = sharder.shard_of(compaction_state.partition_start.key().token());

    // It is possible that the reader this partition originates from does not
    // exist anymore. Either because we failed stopping it or because it was
    // evicted.
    if (_readers[shard].state != reader_state::saving) {
        for (auto& rt : compaction_state.range_tombstones) {
            stats.add_discarded(*_schema, rt);
        }
        if (compaction_state.static_row) {
            stats.add_discarded(*_schema, *compaction_state.static_row);
        }
        stats.add_discarded(*_schema, compaction_state.partition_start);
        return stats;
    }

    auto& shard_buffer = *_readers[shard].buffer;

    for (auto& rt : compaction_state.range_tombstones | boost::adaptors::reversed) {
        stats.add(*_schema, rt);
        shard_buffer.emplace_front(*_schema, _permit, std::move(rt));
    }

    if (compaction_state.static_row) {
        stats.add(*_schema, *compaction_state.static_row);
        shard_buffer.emplace_front(*_schema, _permit, std::move(*compaction_state.static_row));
    }

    stats.add(*_schema, compaction_state.partition_start);
    shard_buffer.emplace_front(*_schema, _permit, std::move(compaction_state.partition_start));

    return stats;
}

future<> read_context::save_reader(shard_id shard, const dht::decorated_key& last_pkey, const std::optional<clustering_key_prefix>& last_ckey) {
  return do_with(std::exchange(_readers[shard], {}), [this, shard, &last_pkey, &last_ckey] (reader_meta& rm) mutable {
    return _db.invoke_on(shard, [this, query_uuid = _cmd.query_uuid, query_ranges = _ranges, &rm,
            &last_pkey, &last_ckey, gts = tracing::global_trace_state_ptr(_trace_state)] (database& db) mutable {
        try {
            auto rparts = rm.rparts.release(); // avoid another round-trip when destroying rparts
            auto irh = rm.handle.release();
            flat_mutation_reader_opt reader = rparts->permit.semaphore().unregister_inactive_read(std::move(*irh));

            if (!reader) {
                return make_ready_future<>();
            }

            auto& buffer = *rm.buffer;
            const auto fragments = buffer.size();
            const auto size_before = reader->buffer_size();

            auto rit = std::reverse_iterator(buffer.cend());
            auto rend = std::reverse_iterator(buffer.cbegin());
            auto& schema = *reader->schema();
            for (;rit != rend; ++rit) {
                // Copy the fragment, the buffer is on another shard.
                reader->unpop_mutation_fragment(mutation_fragment(schema, rparts->permit, *rit));
            }

            const auto size_after = reader->buffer_size();

            auto querier = query::shard_mutation_querier(
                    std::move(query_ranges),
                    std::move(rparts->range),
                    std::move(rparts->slice),
                    std::move(*reader),
                    std::move(rparts->permit),
                    last_pkey,
                    last_ckey);

            db.get_querier_cache().insert(query_uuid, std::move(querier), gts.get());

            db.get_stats().multishard_query_unpopped_fragments += fragments;
            db.get_stats().multishard_query_unpopped_bytes += (size_after - size_before);
            return make_ready_future<>();
        } catch (...) {
            // We don't want to fail a read just because of a failure to
            // save any of the readers.
            mmq_log.debug("Failed to save reader: {}", std::current_exception());
            ++db.get_stats().multishard_query_failed_reader_saves;
            return make_ready_future<>();
        }
    }).handle_exception([this, shard] (std::exception_ptr e) {
        // We don't want to fail a read just because of a failure to
        // save any of the readers.
        mmq_log.debug("Failed to save reader on shard {}: {}", shard, e);
        // This will account the failure on the local shard but we don't
        // know where exactly the failure happened anyway.
        ++_db.local().get_stats().multishard_query_failed_reader_saves;
    });
  });
}

future<> read_context::lookup_readers() {
    if (_cmd.query_uuid == utils::UUID{} || _cmd.is_first_page) {
        return make_ready_future<>();
    }

    return parallel_for_each(boost::irange(0u, smp::count), [this] (shard_id shard) {
        return _db.invoke_on(shard, [this, shard, cmd = &_cmd, ranges = &_ranges, gs = global_schema_ptr(_schema),
                gts = tracing::global_trace_state_ptr(_trace_state)] (database& db) mutable {
            auto schema = gs.get();
            auto querier_opt = db.get_querier_cache().lookup_shard_mutation_querier(cmd->query_uuid, *schema, *ranges, cmd->slice, gts.get());
            auto& table = db.find_column_family(schema);
            auto& semaphore = this->semaphore();

            if (!querier_opt) {
                return reader_meta(reader_state::inexistent);
            }

            auto& q = *querier_opt;

            if (&q.permit().semaphore() != &semaphore) {
                on_internal_error(mmq_log, format("looked-up reader belongs to different semaphore than the one appropriate for this query class: "
                        "looked-up reader belongs to {} (0x{:x}) the query class appropriate is {} (0x{:x})",
                        q.permit().semaphore().name(),
                        reinterpret_cast<uintptr_t>(&q.permit().semaphore()),
                        semaphore.name(),
                        reinterpret_cast<uintptr_t>(&semaphore)));
            }

            auto handle = pause(semaphore, std::move(q).reader());
            return reader_meta(
                    reader_state::successful_lookup,
                    reader_meta::remote_parts(q.permit(), std::move(q).reader_range(), std::move(q).reader_slice(), table.read_in_progress()),
                    std::move(handle));
        }).then([this, shard] (reader_meta rm) {
            _readers[shard] = std::move(rm);
        });
    });
}

future<> read_context::save_readers(flat_mutation_reader::tracked_buffer unconsumed_buffer, detached_compaction_state compaction_state,
            std::optional<clustering_key_prefix> last_ckey) {
    if (_cmd.query_uuid == utils::UUID{}) {
        return make_ready_future<>();
    }

        auto last_pkey = compaction_state.partition_start.key();

        // Ensure all readers have engaged reader_meta::buffer member.
        for (auto& rm : _readers) {
            if (!rm.buffer) {
                rm.buffer.emplace(_permit);
            }
        }

        const auto cb_stats = dismantle_combined_buffer(std::move(unconsumed_buffer), last_pkey);
        tracing::trace(_trace_state, "Dismantled combined buffer: {}", cb_stats);

        const auto cs_stats = dismantle_compaction_state(std::move(compaction_state));
        tracing::trace(_trace_state, "Dismantled compaction state: {}", cs_stats);

        return do_with(std::move(last_pkey), std::move(last_ckey), [this] (const dht::decorated_key& last_pkey,
                const std::optional<clustering_key_prefix>& last_ckey) {
            return parallel_for_each(boost::irange(0u, smp::count), [this, &last_pkey, &last_ckey] (shard_id shard) {
                auto& rm = _readers[shard];
                if (rm.state == reader_state::successful_lookup || rm.state == reader_state::saving) {
                    return save_reader(shard, last_pkey, last_ckey);
                }

                return make_ready_future<>();
            });
        });
}

namespace {

template <typename ResultType>
using compact_for_result_state = compact_for_query_state<ResultType::only_live>;

template <typename ResultBuilder>
struct page_consume_result {
    std::optional<clustering_key_prefix> last_ckey;
    typename ResultBuilder::result_type result;
    flat_mutation_reader::tracked_buffer unconsumed_fragments;
    lw_shared_ptr<compact_for_result_state<ResultBuilder>> compaction_state;

    page_consume_result(std::optional<clustering_key_prefix>&& ckey, typename ResultBuilder::result_type&& result, flat_mutation_reader::tracked_buffer&& unconsumed_fragments,
            lw_shared_ptr<compact_for_result_state<ResultBuilder>>&& compaction_state) noexcept
        : last_ckey(std::move(ckey))
        , result(std::move(result))
        , unconsumed_fragments(std::move(unconsumed_fragments))
        , compaction_state(std::move(compaction_state)) {
        static_assert(std::is_nothrow_move_constructible_v<typename ResultBuilder::result_type>);
    }
};

} // anonymous namespace

template <typename ResultBuilder>
future<page_consume_result<ResultBuilder>> read_page(
        shared_ptr<read_context> ctx,
        schema_ptr s,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout,
        ResultBuilder&& result_builder) {
    auto ms = mutation_source([&] (schema_ptr s,
            reader_permit permit,
            const dht::partition_range& pr,
            const query::partition_slice& ps,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding,
            mutation_reader::forwarding fwd_mr) {
        return make_multishard_combining_reader(ctx, std::move(s), std::move(permit), pr, ps, pc, std::move(trace_state), fwd_mr);
    });
    auto compaction_state = make_lw_shared<compact_for_result_state<ResultBuilder>>(*s, cmd.timestamp, cmd.slice, cmd.get_row_limit(),
            cmd.partition_limit);

    auto reader = make_flat_multi_range_reader(s, ctx->permit(), std::move(ms), ranges,
            cmd.slice, service::get_local_sstable_query_read_priority(), trace_state, mutation_reader::forwarding::no);

    std::exception_ptr ex;
    try {
        auto [ckey, result] = co_await query::consume_page(reader, compaction_state, cmd.slice, std::move(result_builder), cmd.get_row_limit(),
                cmd.partition_limit, cmd.timestamp, timeout, *cmd.max_result_size);
        auto buffer = reader.detach_buffer();
        co_await reader.close();
        // page_consume_result cannot fail so there's no risk of double-closing reader.
        co_return page_consume_result<ResultBuilder>(std::move(ckey), std::move(result), std::move(buffer), std::move(compaction_state));
    } catch (...) {
        ex = std::current_exception();
    }
    co_await reader.close();
    std::rethrow_exception(std::move(ex));
}

template <typename ResultBuilder>
future<typename ResultBuilder::result_type> do_query(
        distributed<database>& db,
        schema_ptr s,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout,
        ResultBuilder&& result_builder) {
    auto ctx = seastar::make_shared<read_context>(db, s, cmd, ranges, trace_state);

    co_await ctx->lookup_readers();

    std::exception_ptr ex;

    try {
        auto [last_ckey, result, unconsumed_buffer, compaction_state] = co_await read_page<ResultBuilder>(ctx, s, cmd, ranges, trace_state, timeout,
                std::move(result_builder));

        if (compaction_state->are_limits_reached() || result.is_short_read()) {
            co_await ctx->save_readers(std::move(unconsumed_buffer), std::move(*compaction_state).detach_state(), std::move(last_ckey));
        }

        co_await ctx->stop();
        co_return std::move(result);
    } catch (...) {
        ex = std::current_exception();
    }

    co_await ctx->stop();

    std::rethrow_exception(std::move(ex));
}

template <typename ResultBuilder>
static future<std::tuple<foreign_ptr<lw_shared_ptr<typename ResultBuilder::result_type>>, cache_temperature>> do_query_on_all_shards(
        distributed<database>& db,
        schema_ptr s,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout,
        std::function<ResultBuilder(query::result_memory_accounter&&)> result_builder_factory) {
    if (cmd.get_row_limit() == 0 || cmd.slice.partition_row_limit() == 0 || cmd.partition_limit == 0) {
        co_return std::tuple(
                make_foreign(make_lw_shared<typename ResultBuilder::result_type>()),
                db.local().find_column_family(s).get_global_cache_hit_rate());
    }

    auto& local_db = db.local();
    auto& stats = local_db.get_stats();
    const auto short_read_allowed = query::short_read(cmd.slice.options.contains<query::partition_slice::option::allow_short_read>());

    try {
        auto accounter = co_await local_db.get_result_memory_limiter().new_mutation_read(*cmd.max_result_size, short_read_allowed);

        auto result_builder = result_builder_factory(std::move(accounter));

        auto result = co_await do_query<ResultBuilder>(db, s, cmd, ranges, std::move(trace_state), timeout, std::move(result_builder));

        ++stats.total_reads;
        stats.short_mutation_queries += bool(result.is_short_read());
        auto hit_rate = local_db.find_column_family(s).get_global_cache_hit_rate();
        co_return std::tuple(make_foreign(make_lw_shared<typename ResultBuilder::result_type>(std::move(result))), hit_rate);
    } catch (...) {
        ++stats.total_reads_failed;
        throw;
    }
}

namespace {

class mutation_query_result_builder {
public:
    using result_type = reconcilable_result;
    static constexpr emit_only_live_rows only_live = emit_only_live_rows::no;

private:
    reconcilable_result_builder _builder;

public:
    mutation_query_result_builder(const schema& s, const query::partition_slice& slice, query::result_memory_accounter&& accounter)
        : _builder(s, slice, std::move(accounter)) { }

    void consume_new_partition(const dht::decorated_key& dk) { _builder.consume_new_partition(dk); }
    void consume(tombstone t) { _builder.consume(t); }
    stop_iteration consume(static_row&& sr, tombstone t, bool is_alive) { return _builder.consume(std::move(sr), t, is_alive); }
    stop_iteration consume(clustering_row&& cr, row_tombstone t, bool is_alive) { return _builder.consume(std::move(cr), t, is_alive); }
    stop_iteration consume(range_tombstone&& rt) { return _builder.consume(std::move(rt)); }
    stop_iteration consume_end_of_partition()  { return _builder.consume_end_of_partition(); }
    result_type consume_end_of_stream() { return _builder.consume_end_of_stream(); }
};

class data_query_result_builder {
public:
    using result_type = query::result;
    static constexpr emit_only_live_rows only_live = emit_only_live_rows::yes;

private:
    std::unique_ptr<query::result::builder> _res_builder;
    query_result_builder _builder;

public:
    data_query_result_builder(const schema& s, const query::partition_slice& slice, query::result_options opts, query::result_memory_accounter&& accounter)
        : _res_builder(std::make_unique<query::result::builder>(slice, opts, std::move(accounter)))
        , _builder(s, *_res_builder) { }

    void consume_new_partition(const dht::decorated_key& dk) { _builder.consume_new_partition(dk); }
    void consume(tombstone t) { _builder.consume(t); }
    stop_iteration consume(static_row&& sr, tombstone t, bool is_alive) { return _builder.consume(std::move(sr), t, is_alive); }
    stop_iteration consume(clustering_row&& cr, row_tombstone t, bool is_alive) { return _builder.consume(std::move(cr), t, is_alive); }
    stop_iteration consume(range_tombstone&& rt) { return _builder.consume(std::move(rt)); }
    stop_iteration consume_end_of_partition()  { return _builder.consume_end_of_partition(); }
    result_type consume_end_of_stream() {
        _builder.consume_end_of_stream();
        return _res_builder->build();
    }
};

} // anonymous namespace

future<std::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>> query_mutations_on_all_shards(
        distributed<database>& db,
        schema_ptr s,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout) {
    return do_query_on_all_shards<mutation_query_result_builder>(db, s, cmd, ranges, std::move(trace_state), timeout,
            [s, &cmd] (query::result_memory_accounter&& accounter) {
        return mutation_query_result_builder(*s, cmd.slice, std::move(accounter));
    });
}

namespace {

future<std::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>> query_data_on_all_shards_in_reverse(
        distributed<database>& db,
        schema_ptr s,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        query::result_options opts,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout) {
    auto [res, ct] = co_await query_mutations_on_all_shards(db, s, cmd, ranges, std::move(trace_state), timeout);
    co_return std::tuple(
            make_foreign(make_lw_shared<query::result>(to_data_query_result(*res, s, cmd.slice, cmd.get_row_limit(), cmd.partition_limit, opts))),
            ct);
}

} // anonymous namespace

future<std::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>> query_data_on_all_shards(
        distributed<database>& db,
        schema_ptr s,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        query::result_options opts,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout) {
    if (cmd.slice.options.contains(query::partition_slice::option::reversed)) {
        // FIXME: #1413
        // It is not worth it to add support for the current inefficient way of
        // doing reverse queries to the multishard reader, so just use the
        // reconcilable result result format and reverse individual partitions
        // when converting to the final query::result.
        return query_data_on_all_shards_in_reverse(db, std::move(s), cmd, ranges, opts, std::move(trace_state), timeout);
    }
    return do_query_on_all_shards<data_query_result_builder>(db, s, cmd, ranges, std::move(trace_state), timeout,
            [s, &cmd, opts] (query::result_memory_accounter&& accounter) {
        return data_query_result_builder(*s, cmd.slice, opts, std::move(accounter));
    });
}
