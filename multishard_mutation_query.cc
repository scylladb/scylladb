/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "schema/schema_registry.hh"
#include "multishard_mutation_query.hh"
#include "mutation_query.hh"
#include "replica/database.hh"
#include "db/config.hh"
#include "query-result-writer.hh"
#include "query_result_merger.hh"
#include "readers/multishard.hh"

#include <fmt/core.h>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>

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
class read_context : public reader_lifecycle_policy_v2 {

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
            lw_shared_ptr<const dht::partition_range> range;
            std::unique_ptr<const query::partition_slice> slice;
            utils::phased_barrier::operation read_operation;
            std::optional<reader_concurrency_semaphore::inactive_read_handle> handle;
            std::optional<mutation_reader::tracked_buffer> buffer;

            remote_parts(
                    reader_permit permit,
                    lw_shared_ptr<const dht::partition_range> range = nullptr,
                    std::unique_ptr<const query::partition_slice> slice = nullptr,
                    utils::phased_barrier::operation read_operation = {},
                    std::optional<reader_concurrency_semaphore::inactive_read_handle> handle = {})
                : permit(std::move(permit))
                , range(std::move(range))
                , slice(std::move(slice))
                , read_operation(std::move(read_operation))
                , handle(std::move(handle)) {
            }
        };

        reader_state state = reader_state::inexistent;
        foreign_unique_ptr<remote_parts> rparts;
        std::optional<mutation_reader::tracked_buffer> dismantled_buffer;

        reader_meta() = default;

        // Remote constructor.
        reader_meta(reader_state s, std::optional<remote_parts> rp = {})
            : state(s) {
            if (rp) {
                rparts = make_foreign(std::make_unique<remote_parts>(std::move(*rp)));
            }
        }

        mutation_reader::tracked_buffer& get_dismantled_buffer(const reader_permit& permit) {
            if (!dismantled_buffer) {
                dismantled_buffer.emplace(permit);
            }
            return *dismantled_buffer;
        }
    };

    struct dismantle_buffer_stats {
        size_t partitions = 0;
        size_t fragments = 0;
        size_t bytes = 0;
        size_t discarded_partitions = 0;
        size_t discarded_fragments = 0;
        size_t discarded_bytes = 0;

        void add(const mutation_fragment_v2& mf) {
            partitions += unsigned(mf.is_partition_start());
            ++fragments;
            bytes += mf.memory_usage();
        }
        void add(const schema& s, const range_tombstone_change& rtc) {
            ++fragments;
            bytes += rtc.memory_usage(s);
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
        void add_discarded(const mutation_fragment_v2& mf) {
            discarded_partitions += unsigned(mf.is_partition_start());
            ++discarded_fragments;
            discarded_bytes += mf.memory_usage();
        }
        void add_discarded(const schema& s, const range_tombstone_change& rtc) {
            ++discarded_fragments;
            discarded_bytes += rtc.memory_usage(s);
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
    };

    distributed<replica::database>& _db;
    schema_ptr _schema;
    locator::effective_replication_map_ptr _erm;
    reader_permit _permit;
    const query::read_command& _cmd;
    const dht::partition_range_vector& _ranges;
    tracing::trace_state_ptr _trace_state;

    // One for each shard. Index is shard id.
    std::vector<reader_meta> _readers;
    std::vector<reader_concurrency_semaphore*> _semaphores;

    static std::string_view reader_state_to_string(reader_state rs);

    dismantle_buffer_stats dismantle_combined_buffer(mutation_reader::tracked_buffer combined_buffer, const dht::decorated_key& pkey);
    dismantle_buffer_stats dismantle_compaction_state(detached_compaction_state compaction_state);
    future<> save_reader(shard_id shard, full_position_view last_pos);

    friend fmt::formatter<dismantle_buffer_stats>;

public:
    read_context(distributed<replica::database>& db, schema_ptr s, locator::effective_replication_map_ptr erm,
                 const query::read_command& cmd, const dht::partition_range_vector& ranges,
                 tracing::trace_state_ptr trace_state, db::timeout_clock::time_point timeout)
            : _db(db)
            , _schema(std::move(s))
            , _erm(std::move(erm))
            , _permit(_db.local().get_reader_concurrency_semaphore().make_tracking_only_permit(_schema, "multishard-mutation-query", timeout, trace_state))
            , _cmd(cmd)
            , _ranges(ranges)
            , _trace_state(std::move(trace_state))
            , _semaphores(smp::count, nullptr) {
        _readers.resize(smp::count);
        _permit.set_max_result_size(get_max_result_size());

        if (!_erm->get_replication_strategy().is_vnode_based()) {
            // The algorithm is full of assumptions about shard assignment being round-robin, static, and full.
            // This does not hold for tablets. We chose to avoid this algorithm rather than adapting it.
            // The coordinator should split ranges accordingly.
            on_internal_error(mmq_log, format("multishard reader cannot be used on tables with non-static sharding: {}.{}",
                              _schema->ks_name(), _schema->cf_name()));
        }
    }

    read_context(read_context&&) = delete;
    read_context(const read_context&) = delete;

    read_context& operator=(read_context&&) = delete;
    read_context& operator=(const read_context&) = delete;

    distributed<replica::database>& db() {
        return _db;
    }

    reader_permit permit() const {
        return _permit;
    }

    const locator::effective_replication_map_ptr& erm() const {
        return _erm;
    }

    query::max_result_size get_max_result_size() {
        return _cmd.max_result_size ? *_cmd.max_result_size : _db.local().get_query_max_result_size();
    }

    virtual mutation_reader create_reader(
            schema_ptr schema,
            reader_permit permit,
            const dht::partition_range& pr,
            const query::partition_slice& ps,
            tracing::trace_state_ptr trace_state,
            mutation_reader::forwarding fwd_mr) override;

    virtual const dht::partition_range* get_read_range() const override;

    virtual void update_read_range(lw_shared_ptr<const dht::partition_range> range) override;

    virtual future<> destroy_reader(stopped_reader reader) noexcept override;

    virtual reader_concurrency_semaphore& semaphore() override {
        const auto shard = this_shard_id();
        if (!_semaphores[shard]) {
            _semaphores[shard] = &_db.local().get_reader_concurrency_semaphore();
        }
        return *_semaphores[shard];
    }

    virtual future<reader_permit> obtain_reader_permit(schema_ptr schema, const char* const description, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr) override {
        const auto shard = this_shard_id();
        auto& rm = _readers[shard];
        if (rm.state == reader_state::successful_lookup) {
            rm.rparts->permit.set_max_result_size(get_max_result_size());
            co_return rm.rparts->permit;
        }
        auto permit = co_await _db.local().obtain_reader_permit(std::move(schema), description, timeout, std::move(trace_ptr));
        permit.set_max_result_size(get_max_result_size());
        co_return permit;
    }

    future<> lookup_readers(db::timeout_clock::time_point timeout) noexcept;

    future<> save_readers(mutation_reader::tracked_buffer unconsumed_buffer, std::optional<detached_compaction_state> compaction_state,
            full_position last_pos) noexcept;

    future<> stop();
};

template <> struct fmt::formatter<read_context::dismantle_buffer_stats> : fmt::formatter<string_view> {
    auto format(const read_context::dismantle_buffer_stats& s, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(),
                              "kept {} partitions/{} fragments/{} bytes, discarded {} partitions/{} fragments/{} bytes",
                              s.partitions,
                              s.fragments,
                              s.bytes,
                              s.discarded_partitions,
                              s.discarded_fragments,
                              s.discarded_bytes);
    }
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

mutation_reader read_context::create_reader(
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        tracing::trace_state_ptr trace_state,
        mutation_reader::forwarding fwd_mr) {
    const auto shard = this_shard_id();
    auto& rm = _readers[shard];

    if (rm.state != reader_state::used && rm.state != reader_state::successful_lookup && rm.state != reader_state::inexistent) {
        auto msg = format("Unexpected request to create reader for shard {}."
                " The reader is expected to be in either `used`, `successful_lookup` or `inexistent` state,"
                " but is in `{}` state instead.", shard, reader_state_to_string(rm.state));
        mmq_log.warn("{}", msg);
        throw std::logic_error(msg.c_str());
    }

    // The reader is either in inexistent or successful lookup state.
    if (rm.state == reader_state::successful_lookup) {
        if (auto reader_opt = semaphore().unregister_inactive_read(std::move(*rm.rparts->handle))) {
            rm.state = reader_state::used;
            // The saved reader permit is expected to be the same one passed to create_reader,
            // as returned from obtain_reader_permit()
            if (reader_opt->permit() != permit) {
                on_internal_error(mmq_log, "read_context::create_reader(): passed-in permit is different than saved reader's permit");
            }
            permit.set_trace_state(trace_state);
            return std::move(*reader_opt);
        }
    }

    auto& table = _db.local().find_column_family(schema);

    auto remote_parts = reader_meta::remote_parts(
            std::move(permit),
            make_lw_shared<const dht::partition_range>(pr),
            std::make_unique<const query::partition_slice>(ps),
            table.read_in_progress());

    if (!rm.rparts) {
        rm.rparts = make_foreign(std::make_unique<reader_meta::remote_parts>(std::move(remote_parts)));
    } else {
        *rm.rparts = std::move(remote_parts);
    }

    rm.state = reader_state::used;

    return table.as_mutation_source().make_reader_v2(std::move(schema), rm.rparts->permit, *rm.rparts->range, *rm.rparts->slice,
            std::move(trace_state), streamed_mutation::forwarding::no, fwd_mr);
}

const dht::partition_range* read_context::get_read_range() const {
    auto& rm = _readers[this_shard_id()];
    if (rm.rparts) {
        return rm.rparts->range.get();
    }
    return nullptr;
}

void read_context::update_read_range(lw_shared_ptr<const dht::partition_range> range) {
    auto& rm = _readers[this_shard_id()];
    rm.rparts->range = std::move(range);
}

future<> read_context::destroy_reader(stopped_reader reader) noexcept {
    auto& rm = _readers[this_shard_id()];

    if (rm.state == reader_state::used) {
        rm.state = reader_state::saving;
        rm.rparts->handle = std::move(reader.handle);
        rm.rparts->buffer = std::move(reader.unconsumed_fragments);
    } else {
        mmq_log.warn(
                "Unexpected request to dismantle reader in state `{}`."
                " Reader was not created nor is in the process of being created.",
                reader_state_to_string(rm.state));
    }
    return make_ready_future<>();
}

future<> read_context::stop() {
    return parallel_for_each(smp::all_cpus(), [this] (unsigned shard) {
        if (_readers[shard].rparts) {
            return _db.invoke_on(shard, [&rparts_fptr = _readers[shard].rparts] (replica::database& db) mutable {
                auto rparts = rparts_fptr.release();
                if (rparts->handle) {
                    auto reader_opt = rparts->permit.semaphore().unregister_inactive_read(std::move(*rparts->handle));
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

read_context::dismantle_buffer_stats read_context::dismantle_combined_buffer(mutation_reader::tracked_buffer combined_buffer,
        const dht::decorated_key& pkey) {
    auto& sharder = _erm->get_sharder(*_schema);

    std::vector<mutation_fragment_v2> tmp_buffer;
    dismantle_buffer_stats stats;

    auto rit = std::reverse_iterator(combined_buffer.end());
    const auto rend = std::reverse_iterator(combined_buffer.begin());
    for (;rit != rend; ++rit) {
        if (rit->is_partition_start()) {
            const auto shard = sharder.shard_for_reads(rit->as_partition_start().key().token());

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

            auto& shard_buffer = _readers[shard].get_dismantled_buffer(_permit);
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

    const auto shard = sharder.shard_for_reads(pkey.token());
    auto& shard_buffer = _readers[shard].get_dismantled_buffer(_permit);
    for (auto& smf : tmp_buffer) {
        stats.add(smf);
        shard_buffer.emplace_front(std::move(smf));
    }

    return stats;
}

read_context::dismantle_buffer_stats read_context::dismantle_compaction_state(detached_compaction_state compaction_state) {
    auto stats = dismantle_buffer_stats();
    auto& sharder = _erm->get_sharder(*_schema);
    const auto shard = sharder.shard_for_reads(compaction_state.partition_start.key().token());

    auto& rtc_opt = compaction_state.current_tombstone;

    // It is possible that the reader this partition originates from does not
    // exist anymore. Either because we failed stopping it or because it was
    // evicted.
    if (_readers[shard].state != reader_state::saving) {
        if (rtc_opt) {
            stats.add_discarded(*_schema, *rtc_opt);
        }
        if (compaction_state.static_row) {
            stats.add_discarded(*_schema, *compaction_state.static_row);
        }
        stats.add_discarded(*_schema, compaction_state.partition_start);
        return stats;
    }

    auto& shard_buffer = _readers[shard].get_dismantled_buffer(_permit);

    if (rtc_opt) {
        stats.add(*_schema, *rtc_opt);
        shard_buffer.emplace_front(*_schema, _permit, std::move(*rtc_opt));
    }

    if (compaction_state.static_row) {
        stats.add(*_schema, *compaction_state.static_row);
        shard_buffer.emplace_front(*_schema, _permit, std::move(*compaction_state.static_row));
    }

    stats.add(*_schema, compaction_state.partition_start);
    shard_buffer.emplace_front(*_schema, _permit, std::move(compaction_state.partition_start));

    return stats;
}

future<> read_context::save_reader(shard_id shard, full_position_view last_pos) {
  return do_with(std::exchange(_readers[shard], {}), [this, shard, last_pos] (reader_meta& rm) mutable {
    return _db.invoke_on(shard, [query_uuid = _cmd.query_uuid, query_ranges = _ranges, &rm,
            last_pos, gts = tracing::global_trace_state_ptr(_trace_state)] (replica::database& db) mutable {
        try {
            auto rparts = rm.rparts.release(); // avoid another round-trip when destroying rparts
            auto reader_opt = rparts->permit.semaphore().unregister_inactive_read(std::move(*rparts->handle));

            if (!reader_opt) {
                return make_ready_future<>();
            }
            mutation_reader_opt reader = std::move(*reader_opt);

            size_t fragments = 0;
            const auto size_before = reader->buffer_size();
            const auto& schema = *reader->schema();

            if (rparts->buffer) {
                fragments += rparts->buffer->size();
                auto rit = std::reverse_iterator(rparts->buffer->end());
                auto rend = std::reverse_iterator(rparts->buffer->begin());
                for (; rit != rend; ++rit) {
                    reader->unpop_mutation_fragment(std::move(*rit));
                }
            }
            if (rm.dismantled_buffer) {
                fragments += rm.dismantled_buffer->size();
                auto rit = std::reverse_iterator(rm.dismantled_buffer->cend());
                auto rend = std::reverse_iterator(rm.dismantled_buffer->cbegin());
                for (; rit != rend; ++rit) {
                    // Copy the fragment, the buffer is on another shard.
                    reader->unpop_mutation_fragment(mutation_fragment_v2(schema, rparts->permit, *rit));
                }
            }

            const auto size_after = reader->buffer_size();

            auto querier = query::shard_mutation_querier(
                    std::move(query_ranges),
                    std::move(rparts->range),
                    std::move(rparts->slice),
                    std::move(*reader),
                    std::move(rparts->permit),
                    last_pos);

            db.get_querier_cache().insert_shard_querier(query_uuid, std::move(querier), gts.get());

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

future<> read_context::lookup_readers(db::timeout_clock::time_point timeout) noexcept {
    if (!_cmd.query_uuid || _cmd.is_first_page) {
        return make_ready_future<>();
    }
    try {
        return _db.invoke_on_all([this, cmd = &_cmd, ranges = &_ranges, gs = global_schema_ptr(_schema),
                gts = tracing::global_trace_state_ptr(_trace_state), timeout] (replica::database& db) mutable -> future<> {
            auto schema = gs.get();
            auto& table = db.find_column_family(schema);
            auto& semaphore = this->semaphore();
            auto shard = this_shard_id();

            auto querier_opt = db.get_querier_cache().lookup_shard_mutation_querier(cmd->query_uuid, *schema, *ranges, cmd->slice, semaphore, gts.get(), timeout);

            if (!querier_opt) {
                _readers[shard] = reader_meta(reader_state::inexistent);
                co_return;
            }

            auto& q = *querier_opt;
            auto handle = semaphore.register_inactive_read(std::move(q).reader());
            _readers[shard] = reader_meta(
                    reader_state::successful_lookup,
                    reader_meta::remote_parts(q.permit(), std::move(q).reader_range(), std::move(q).reader_slice(), table.read_in_progress(),
                            std::move(handle)));
        });
    } catch (...) {
        return current_exception_as_future();
    }
}

future<> read_context::save_readers(mutation_reader::tracked_buffer unconsumed_buffer, std::optional<detached_compaction_state> compaction_state,
            full_position last_pos) noexcept {
    if (!_cmd.query_uuid) {
        co_return;
    }

    const auto cb_stats = dismantle_combined_buffer(std::move(unconsumed_buffer), dht::decorate_key(*_schema, last_pos.partition));
    tracing::trace(_trace_state, "Dismantled combined buffer: {}", cb_stats);

    auto cs_stats = dismantle_buffer_stats{};
    if (compaction_state) {
        cs_stats = dismantle_compaction_state(std::move(*compaction_state));
        tracing::trace(_trace_state, "Dismantled compaction state: {}", cs_stats);
    } else {
        tracing::trace(_trace_state, "No compaction state to dismantle, partition exhausted", cs_stats);
    }

    co_await parallel_for_each(boost::irange(0u, smp::count), [this, &last_pos] (shard_id shard) {
        auto& rm = _readers[shard];
        if (rm.state == reader_state::successful_lookup || rm.state == reader_state::saving) {
            return save_reader(shard, last_pos);
        }

        return make_ready_future<>();
    });
}

namespace {

template <typename ResultBuilder>
requires std::is_nothrow_move_constructible_v<typename ResultBuilder::result_type>
struct page_consume_result {
    typename ResultBuilder::result_type result;
    mutation_reader::tracked_buffer unconsumed_fragments;
    lw_shared_ptr<compact_for_query_state_v2> compaction_state;

    page_consume_result(typename ResultBuilder::result_type&& result, mutation_reader::tracked_buffer&& unconsumed_fragments,
            lw_shared_ptr<compact_for_query_state_v2>&& compaction_state) noexcept
        : result(std::move(result))
        , unconsumed_fragments(std::move(unconsumed_fragments))
        , compaction_state(std::move(compaction_state)) {
    }
};

// A special-purpose multi-range reader for multishard reads
//
// It is different from the "stock" multi-range reader
// (make_flat_multi_range_reader()) in the following ways:
// * It guarantees that a buffer never crosses two ranges.
// * It guarantees that after calling `fill_buffer()` the underlying reader's
//   buffer's *entire* content is moved into its own buffer. In other words,
//   calling detach_buffer() after fill_buffer() is guranted to get all
//   fragments fetched in that call, none will be left in the underlying
//   reader's one.
class multi_range_reader : public mutation_reader::impl {
    mutation_reader _reader;
    dht::partition_range_vector::const_iterator _it;
    const dht::partition_range_vector::const_iterator _end;

public:
    multi_range_reader(schema_ptr s, reader_permit permit, mutation_reader rd, const dht::partition_range_vector& ranges)
        : impl(std::move(s), std::move(permit)) , _reader(std::move(rd)) , _it(ranges.begin()) , _end(ranges.end()) { }

    virtual future<> fill_buffer() override {
        if (is_end_of_stream()) {
            co_return;
        }
        while (is_buffer_empty()) {
            if (_reader.is_buffer_empty() && _reader.is_end_of_stream()) {
                if (++_it == _end) {
                    _end_of_stream = true;
                    break;
                } else {
                    co_await _reader.fast_forward_to(*_it);
                }
            }
            if (_reader.is_buffer_empty()) {
                co_await _reader.fill_buffer();
            }
            _reader.move_buffer_content_to(*this);
        }
    }

    virtual future<> fast_forward_to(const dht::partition_range&) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }

    virtual future<> fast_forward_to(position_range) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }

    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (is_buffer_empty() && !is_end_of_stream()) {
            return _reader.next_partition();
        }
        return make_ready_future<>();
    }

    virtual future<> close() noexcept override {
        return _reader.close();
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
        noncopyable_function<ResultBuilder()> result_builder_factory) {
    auto compaction_state = make_lw_shared<compact_for_query_state_v2>(*s, cmd.timestamp, cmd.slice, cmd.get_row_limit(),
            cmd.partition_limit);

    auto reader = make_multishard_combining_reader_v2(ctx, s, ctx->erm(), ctx->permit(), ranges.front(), cmd.slice,
            trace_state, mutation_reader::forwarding(ranges.size() > 1));
    if (ranges.size() > 1) {
        reader = make_mutation_reader<multi_range_reader>(s, ctx->permit(), std::move(reader), ranges);
    }

    // Use coroutine::as_future to prevent exception on timesout.
    auto f = co_await coroutine::as_future(query::consume_page(reader, compaction_state, cmd.slice, result_builder_factory(), cmd.get_row_limit(),
                cmd.partition_limit, cmd.timestamp));
    if (!f.failed()) {
        // no exceptions are thrown in this block
        auto result = std::move(f).get();
        if (compaction_state->are_limits_reached() || result.is_short_read()) {
            ResultBuilder::maybe_set_last_position(result, compaction_state->current_full_position());
        }
        const auto& cstats = compaction_state->stats();
        tracing::trace(trace_state, "Page stats: {} partition(s), {} static row(s) ({} live, {} dead), {} clustering row(s) ({} live, {} dead) and {} range tombstone(s)",
                cstats.partitions,
                cstats.static_rows.total(),
                cstats.static_rows.live,
                cstats.static_rows.dead,
                cstats.clustering_rows.total(),
                cstats.clustering_rows.live,
                cstats.clustering_rows.dead,
                cstats.range_tombstones);
        auto buffer = reader.detach_buffer();
        co_await reader.close();
        // page_consume_result cannot fail so there's no risk of double-closing reader.
        co_return page_consume_result<ResultBuilder>(std::move(result), std::move(buffer), std::move(compaction_state));
    }

    co_await reader.close();
    co_return coroutine::exception(f.get_exception());
}

template <typename ResultBuilder>
future<foreign_ptr<lw_shared_ptr<typename ResultBuilder::result_type>>> do_query_vnodes(
        distributed<replica::database>& db,
        schema_ptr s,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout,
        noncopyable_function<ResultBuilder()> result_builder_factory) {
    auto& table = db.local().find_column_family(s);
    auto erm = table.get_effective_replication_map();
    auto ctx = seastar::make_shared<read_context>(db, s, erm, cmd, ranges, trace_state, timeout);

    // Use coroutine::as_future to prevent exception on timesout.
    auto f = co_await coroutine::as_future(ctx->lookup_readers(timeout).then([&, result_builder_factory = std::move(result_builder_factory)] () mutable {
        return read_page<ResultBuilder>(ctx, s, cmd, ranges, trace_state, std::move(result_builder_factory));
    }).then([&] (page_consume_result<ResultBuilder> r) -> future<foreign_ptr<lw_shared_ptr<typename ResultBuilder::result_type>>> {
        if (r.compaction_state->are_limits_reached() || r.result.is_short_read()) {
            // Must call before calling `detach_state()`.
            auto last_pos = *r.compaction_state->current_full_position();
            co_await ctx->save_readers(std::move(r.unconsumed_fragments), std::move(*r.compaction_state).detach_state(), std::move(last_pos));
        }
        co_return make_foreign(make_lw_shared<typename ResultBuilder::result_type>(std::move(r.result)));
    }));
    co_await ctx->stop();
    if (f.failed()) {
        co_return coroutine::exception(f.get_exception());
    }
    co_return f.get();
}

template <typename ResultBuilder>
future<foreign_ptr<lw_shared_ptr<typename ResultBuilder::result_type>>> do_query_tablets(
        distributed<replica::database>& db,
        schema_ptr s,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout,
        noncopyable_function<ResultBuilder()> result_builder_factory) {
    auto& table = db.local().find_column_family(s);
    auto erm = table.get_effective_replication_map();
    const auto& token_metadata = erm->get_token_metadata();
    const auto& tablets = token_metadata.tablets().get_tablet_map(s->id());
    const auto this_node_id = token_metadata.get_topology().this_node()->host_id();

    auto query_cmd = cmd;
    auto result_builder = result_builder_factory();

    std::vector<foreign_ptr<lw_shared_ptr<typename ResultBuilder::result_type>>> results;
    locator::tablet_range_splitter range_splitter{s, tablets, this_node_id, ranges};
    while (auto range_opt = range_splitter()) {
        auto& r = *results.emplace_back(co_await db.invoke_on(range_opt->shard,
                    [&result_builder, gs = global_schema_ptr(s), &query_cmd, &range_opt, gts = tracing::global_trace_state_ptr(trace_state), timeout] (replica::database& db) {
            return result_builder.query(db, gs, query_cmd, range_opt->range, gts, timeout);
        }));

        // Subtract result from limit, watch for underflow
        query_cmd.partition_limit -= std::min(query_cmd.partition_limit, ResultBuilder::get_partition_count(r));
        query_cmd.set_row_limit(query_cmd.get_row_limit() - std::min(query_cmd.get_row_limit(), ResultBuilder::get_row_count(r)));

        if (!query_cmd.partition_limit || !query_cmd.get_row_limit() || r.is_short_read()) {
            break;
        }
    }

    co_return result_builder.merge(std::move(results));
}

template <typename ResultBuilder>
static future<std::tuple<foreign_ptr<lw_shared_ptr<typename ResultBuilder::result_type>>, cache_temperature>> do_query_on_all_shards(
        distributed<replica::database>& db,
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

    const auto& keyspace = local_db.find_keyspace(s->ks_name());
    auto query_method = keyspace.get_replication_strategy().uses_tablets() ? do_query_tablets<ResultBuilder> : do_query_vnodes<ResultBuilder>;

    try {
        auto accounter = co_await local_db.get_result_memory_limiter().new_mutation_read(*cmd.max_result_size, short_read_allowed);

        auto result = co_await query_method(db, s, cmd, ranges, std::move(trace_state), timeout,
                [result_builder_factory, accounter = std::move(accounter)] () mutable {
			return result_builder_factory(std::move(accounter));
		});

        ++stats.total_reads;
        stats.short_mutation_queries += bool(result->is_short_read());
        auto hit_rate = local_db.find_column_family(s).get_global_cache_hit_rate();
        co_return std::tuple(std::move(result), hit_rate);
    } catch (...) {
        ++stats.total_reads_failed;
        throw;
    }
}

namespace {

class mutation_query_result_builder {
public:
    using result_type = reconcilable_result;

private:
    reconcilable_result_builder _builder;
    schema_ptr _s;

public:
    mutation_query_result_builder(const schema& s, const query::partition_slice& slice, query::result_memory_accounter&& accounter)
        : _builder(s, slice, std::move(accounter))
        , _s(s.shared_from_this())
     { }

    void consume_new_partition(const dht::decorated_key& dk) { _builder.consume_new_partition(dk); }
    void consume(tombstone t) { _builder.consume(t); }
    stop_iteration consume(static_row&& sr, tombstone t, bool is_alive) { return _builder.consume(std::move(sr), t, is_alive); }
    stop_iteration consume(clustering_row&& cr, row_tombstone t, bool is_alive) { return _builder.consume(std::move(cr), t, is_alive); }
    stop_iteration consume(range_tombstone_change&& rtc) { return _builder.consume(std::move(rtc)); }
    stop_iteration consume_end_of_partition()  { return _builder.consume_end_of_partition(); }
    result_type consume_end_of_stream() { return _builder.consume_end_of_stream(); }

    future<foreign_ptr<lw_shared_ptr<result_type>>> query(
            replica::database& db,
            schema_ptr schema,
            const query::read_command& cmd,
            const dht::partition_range& range,
            tracing::trace_state_ptr trace_state,
            db::timeout_clock::time_point timeout) {
        auto res = co_await db.query_mutations(std::move(schema), cmd, range, std::move(trace_state), timeout);
        co_return make_foreign(make_lw_shared<result_type>(std::get<0>(std::move(res))));
    }

    foreign_ptr<lw_shared_ptr<result_type>> merge(std::vector<foreign_ptr<lw_shared_ptr<result_type>>> results) {
        if (results.empty()) {
            return make_foreign(make_lw_shared<result_type>());
        }
        auto& first = results.front();
        for (auto it = results.begin() + 1; it != results.end(); ++it) {
            first->merge_disjoint(_s, **it);
        }
        return std::move(first);
    }

    static void maybe_set_last_position(result_type& r, std::optional<full_position> full_position) { }
    static uint32_t get_partition_count(result_type& r) { return r.partitions().size(); }
    static uint64_t get_row_count(result_type& r) { return r.row_count(); }
};

class data_query_result_builder {
public:
    using result_type = query::result;

private:
    std::unique_ptr<query::result::builder> _res_builder;
    query_result_builder _builder;
    query::result_options _opts;

public:
    data_query_result_builder(const schema& s, const query::partition_slice& slice, query::result_options opts,
            query::result_memory_accounter&& accounter, uint64_t tombstone_limit)
        : _res_builder(std::make_unique<query::result::builder>(slice, opts, std::move(accounter), tombstone_limit))
        , _builder(s, *_res_builder)
        , _opts(opts)
    { }

    void consume_new_partition(const dht::decorated_key& dk) { _builder.consume_new_partition(dk); }
    void consume(tombstone t) { _builder.consume(t); }
    stop_iteration consume(static_row&& sr, tombstone t, bool is_alive) { return _builder.consume(std::move(sr), t, is_alive); }
    stop_iteration consume(clustering_row&& cr, row_tombstone t, bool is_alive) { return _builder.consume(std::move(cr), t, is_alive); }
    stop_iteration consume(range_tombstone_change&& rtc) { return _builder.consume(std::move(rtc)); }
    stop_iteration consume_end_of_partition()  { return _builder.consume_end_of_partition(); }
    result_type consume_end_of_stream() {
        _builder.consume_end_of_stream();
        return _res_builder->build();
    }

    future<foreign_ptr<lw_shared_ptr<result_type>>> query(
            replica::database& db,
            schema_ptr schema,
            const query::read_command& cmd,
            const dht::partition_range& range,
            tracing::trace_state_ptr trace_state,
            db::timeout_clock::time_point timeout) {
        dht::partition_range_vector ranges;
        ranges.emplace_back(range);
        auto res = co_await db.query(std::move(schema), cmd, _opts, ranges, std::move(trace_state), timeout);
        co_return std::get<0>(std::move(res));
    }

    foreign_ptr<lw_shared_ptr<result_type>> merge(std::vector<foreign_ptr<lw_shared_ptr<result_type>>> results) {
        if (results.empty()) {
            return make_foreign(make_lw_shared<result_type>());
        }
        query::result_merger merger(query::max_rows, query::max_partitions);
        merger.reserve(results.size());
        for (auto&& r: results) {
            merger(std::move(r));
        }
        return merger.get();
    }

    static void maybe_set_last_position(result_type& r, std::optional<full_position> full_position) {
        r.set_last_position(std::move(full_position));
    }
    static uint32_t get_partition_count(result_type& r) {
        r.ensure_counts();
        return *r.partition_count();
    }
    static uint64_t get_row_count(result_type& r) {
        r.ensure_counts();
        return *r.row_count();
    }
};

} // anonymous namespace

future<std::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>> query_mutations_on_all_shards(
        distributed<replica::database>& db,
        schema_ptr query_schema,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout) {
    return do_query_on_all_shards<mutation_query_result_builder>(db, query_schema, cmd, ranges, std::move(trace_state), timeout,
            [query_schema, &cmd] (query::result_memory_accounter&& accounter) {
        return mutation_query_result_builder(*query_schema, cmd.slice, std::move(accounter));
    });
}

future<std::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>> query_data_on_all_shards(
        distributed<replica::database>& db,
        schema_ptr query_schema,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        query::result_options opts,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout) {
    return do_query_on_all_shards<data_query_result_builder>(db, query_schema, cmd, ranges, std::move(trace_state), timeout,
            [query_schema, &cmd, opts] (query::result_memory_accounter&& accounter) {
        return data_query_result_builder(*query_schema, cmd.slice, opts, std::move(accounter), cmd.tombstone_limit);
    });
}
