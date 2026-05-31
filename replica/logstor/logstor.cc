/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/logstor.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>
#include <seastar/core/future.hh>
#include "query/query-request.hh"
#include "readers/from_mutations.hh"
#include "readers/forwardable.hh"
#include "readers/empty.hh"
#include "keys/keys.hh"
#include "replica/logstor/key_utils.hh"
#include "replica/logstor/segment_manager.hh"
#include "replica/logstor/types.hh"
#include <seastar/core/when_all.hh>
#include "utils/managed_bytes.hh"
#include <seastar/util/defer.hh>
#include <openssl/evp.h>
#include <algorithm>
#include <queue>
#include <vector>

namespace replica::logstor {

seastar::logger logstor_logger("logstor");

primary_index_key::primary_index_key(const schema& s, const dht::decorated_key& dk)
    : primary_index_key(dk.token(), compute_key_hash(s, dk.key().view())) {
}

static api::timestamp_type extract_logstor_record_timestamp(const mutation& m) {
    const auto& partition = m.partition();

    for (const auto& row_entry : partition.clustered_rows()) {
        if (row_entry.dummy()) {
            continue;
        }
        if (!row_entry.row().marker().is_missing()) {
            return row_entry.row().marker().timestamp();
        }
    }

    if (const auto partition_tombstone = partition.partition_tombstone(); partition_tombstone) {
        return partition_tombstone.timestamp;
    }

    throw std::runtime_error("logstor mutation has no row marker or partition tombstone timestamp");
}

logstor::logstor(logstor_config config, ::cache_tracker& shared_cache_tracker)
    : _segment_manager(config.segment_manager_cfg)
    , _write_buffer(buffered_writer_config{
            .buffer_size = _segment_manager.get_segment_size(),
            .ring_size = config.write_buffer_ring_size,
            .flush_sg = config.flush_sg,
            .max_queued_write_bytes = config.max_queued_write_bytes,
        }, [&sm = _segment_manager] (write_buffer& buf) { return sm.write(buf); })
    , _cache_tracker(shared_cache_tracker) {

    namespace sm = seastar::metrics;

    _metrics.add_group("logstor", {
        sm::make_gauge("queued_write_count", [this] { return _write_buffer.queued_write_count(); },
                       sm::description("Number of writes currently queued in the write buffer.")),
        sm::make_counter("write_failures", [this] { return _stats.write_failures; },
                       sm::description("Number of writes that failed to be persisted.")),
    });
}

future<> logstor::do_recovery(replica::database& db) {
    co_await _segment_manager.do_recovery(db);
}

future<> logstor::start() {
    logstor_logger.info("Starting logstor");

    co_await _segment_manager.start();
    co_await _write_buffer.start();

    logstor_logger.info("logstor started");
}

future<> logstor::stop() {
    if (_async_gate.is_closed()) {
        co_return;
    }
    logstor_logger.info("Stopping logstor");

    co_await _async_gate.close();
    co_await _write_buffer.stop();
    co_await _segment_manager.stop();

    logstor_logger.info("logstor stopped");
}

size_t logstor::get_memory_usage() const {
    return _segment_manager.get_memory_usage();
}

segment_manager& logstor::get_segment_manager() noexcept {
    return _segment_manager;
}

const segment_manager& logstor::get_segment_manager() const noexcept {
    return _segment_manager;
}

compaction_manager& logstor::get_compaction_manager() noexcept {
    return _segment_manager.get_compaction_manager();
}

const compaction_manager& logstor::get_compaction_manager() const noexcept {
    return _segment_manager.get_compaction_manager();
}

std::unique_ptr<primary_index> logstor::make_primary_index(schema_ptr schema, bool cache_enabled) {
    return std::make_unique<primary_index>(schema, _segment_manager, cache_enabled ? &_cache_tracker : nullptr);
}

future<> logstor::write(const mutation& m, compaction_group& cg, seastar::gate::holder cg_holder, db::timeout_clock::time_point timeout) {
    auto gate_holder = _async_gate.hold();

    primary_index_key key(*m.schema(), m.decorated_key());
    table_id table = m.schema()->id();
    auto& index = cg.get_logstor_index();

    const auto ts = extract_logstor_record_timestamp(m);

    log_record record {
        .header = {
            .key = key,
            .timestamp = ts,
            .table = table,
        },
        .mut = canonical_mutation(m)
    };

    auto writer = log_record_writer(std::move(record));

    auto result_f = co_await coroutine::as_future(_write_buffer.write(std::move(writer), timeout, &cg, std::move(cg_holder)));
    if (result_f.failed()){
        _stats.write_failures++;
        co_await coroutine::return_exception_ptr(result_f.get_exception());
    }
    auto [location, op] = result_f.get();
    index_entry new_entry {
        .location = location,
        .timestamp = ts,
    };
    index.insert(key, std::move(new_entry));
}

future<std::optional<mutation>> logstor::read(schema_ptr s, const primary_index& index, const dht::decorated_key& dk, const query::partition_slice& slice) {
    auto gate_holder = _async_gate.hold();

    auto op = index.start_read();

    primary_index_key pk(*s, dk);

    const auto bypass_cache = slice.options.contains(query::partition_slice::option::bypass_cache);
    auto lookup = index.lookup_for_read(pk, s, !bypass_cache);
    if (!lookup) {
        co_return std::nullopt;
    }

    if (lookup->cached_mutation) {
        auto& cached = *lookup->cached_mutation;
        if (cached.decorated_key().key() != dk.key()) [[unlikely]] {
            co_await coroutine::return_exception(key_mismatch_error(dk.key(), cached.decorated_key().key(), key_mismatch_error::cache_location{}));
        }
        co_return std::move(cached);
    }

    auto record = co_await _segment_manager.read(lookup->entry.location);

    if (record.mut.key() != dk.key()) [[unlikely]] {
        co_await coroutine::return_exception(key_mismatch_error(dk.key(), record.mut.key(), lookup->entry.location));
    }

    mutation m = record.mut.to_mutation(s);

    if (!bypass_cache) {
        index.populate_cache(pk, lookup->entry.location, m);
    }

    co_return std::move(m);
}

mutation_reader logstor::make_reader(schema_ptr schema, const primary_index& index, reader_permit permit, const dht::partition_range& pr,
        const query::partition_slice& slice, tracing::trace_state_ptr trace_state, streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr) {

    class logstor_single_key_reader : public mutation_reader::impl {
        logstor* _logstor;
        const primary_index& _index;
        dht::decorated_key _dk;
        query::partition_slice _slice;
        tracing::trace_state_ptr _trace_state;
        mutation_reader_opt _current_partition_reader;
        // Accounts for the mutation held by _current_partition_reader. The fragments it
        // produces are accounted by the reader's buffer, the mutation waiting to be
        // fragmented is not.
        reader_permit::resource_units _partition_memory;
        // Set once the partition was read and its stream was opened. Until then the reader
        // is positioned before the partition, which next_partition() must not skip over.
        bool _partition_started = false;

        future<> reset_current_partition_reader() {
            if (!_current_partition_reader) {
                return make_ready_future<>();
            }

            auto fut = _current_partition_reader->close();
            _current_partition_reader = std::nullopt;
            _partition_memory.reset_to_zero();
            return fut;
        }

    public:
        logstor_single_key_reader(schema_ptr s, const primary_index& idx, reader_permit p,
                logstor* ls, dht::decorated_key dk,
                query::partition_slice slice, tracing::trace_state_ptr ts)
            : impl(std::move(s), std::move(p))
            , _logstor(ls), _index(idx), _dk(std::move(dk))
            , _slice(std::move(slice)), _trace_state(std::move(ts))
            , _partition_memory(_permit.consume_memory()) {
        }

        virtual future<> fill_buffer() override {
            while (!is_buffer_full() && !_end_of_stream) {
                if (_current_partition_reader) {
                    co_await _current_partition_reader->fill_buffer();
                    _current_partition_reader->move_buffer_content_to(*this);
                    if (_current_partition_reader->is_end_of_stream()) {
                        co_await reset_current_partition_reader();
                        _end_of_stream = true;
                    }
                    continue;
                }

                auto guard = reader_permit::awaits_guard(_permit);
                auto mut = co_await _logstor->read(_schema, _index, _dk, _slice);
                if (!mut) {
                    _end_of_stream = true;
                    co_return;
                }

                tracing::trace(_trace_state, "logstor_single_key_reader: fetched key {}", _dk);

                _partition_started = true;
                _partition_memory = _permit.consume_memory(mut->memory_usage(*_schema));
                _current_partition_reader = make_mutation_reader_from_mutations(
                    _schema, _permit, std::move(*mut),
                    _slice, streamed_mutation::forwarding::no
                );
            }
        }

        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            if (!is_buffer_empty()) {
                return make_ready_future<>();
            }
            if (!_partition_started) {
                // Positioned before the partition, nothing to skip over.
                return make_ready_future<>();
            }
            // The reader produces a single partition, so skipping it ends the stream.
            _end_of_stream = true;
            return reset_current_partition_reader();
        }

        virtual future<> fast_forward_to(const dht::partition_range&) override {
            // This reader can only produce its own key, so it is used only when the caller
            // will not move it to another partition range, see make_reader(). That also
            // means _partition_started can never go stale.
            return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
        }

        virtual future<> fast_forward_to(position_range) override {
            // Clustering forwarding is served by the make_forwardable() wrapper, which
            // never forwards the underlying reader, see make_reader().
            return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
        }

        virtual future<> close() noexcept override {
            return reset_current_partition_reader();
        }
    };

    class logstor_range_reader : public mutation_reader::impl {
        logstor* _logstor;
        const primary_index& _index;
        dht::partition_range _pr;
        primary_index::token_range_scan _scan;
        query::partition_slice _slice;
        tracing::trace_state_ptr _trace_state;

        // A mutation read from the log, together with the permit memory it is accounted
        // with. Mutations are held until they are fragmented into the reader's buffer,
        // which does its own accounting, so they are charged for from the moment they
        // are read until the partition reader holding them is closed.
        struct pending_mutation {
            mutation mut;
            reader_permit::resource_units memory;
        };

        std::queue<pending_mutation> _pending_mutations;
        mutation_reader_opt _current_partition_reader;
        reader_permit::resource_units _current_partition_memory;

        struct mutation_batch {
            std::vector<log_location> locations;
            dht::token first_token;
            dht::token last_token;
            bool exhausted;
        };

        static dht::token_range partition_range_to_token_range(const dht::partition_range& pr) {
            using token_range_bound = dht::token_range::bound;

            std::optional<token_range_bound> start;
            std::optional<token_range_bound> end;

            if (pr.start()) {
                const auto& pos = pr.start()->value();
                const bool inclusive = pos.has_key() || pos.bound() == dht::ring_position::token_bound::start;
                start = token_range_bound(pos.token(), inclusive);
            }

            if (pr.end()) {
                const auto& pos = pr.end()->value();
                const bool inclusive = pos.has_key() || pos.bound() == dht::ring_position::token_bound::end;
                end = token_range_bound(pos.token(), inclusive);
            }

            return dht::token_range(std::move(start), std::move(end));
        }

        future<std::vector<pending_mutation>> read_mutations_for_batch(const std::vector<log_location>& locations) {
            auto guard = reader_permit::awaits_guard(_permit);

            std::vector<future<pending_mutation>> reads;
            reads.reserve(locations.size());

            for (const auto location : locations) {
                reads.push_back([this, location] () -> future<pending_mutation> {
                    return _logstor->_segment_manager.read(location).then([this] (log_record record) {
                        auto mut = record.mut.to_mutation(_schema);
                        auto memory = _permit.consume_memory(mut.memory_usage(*_schema));
                        return pending_mutation{std::move(mut), std::move(memory)};
                    });
                }());
            }

            auto read_mutations = co_await when_all_succeed(reads.begin(), reads.end());
            co_return std::move(read_mutations);
        }

        // Primary-index scan order is by (token, key hash). That means the batch already arrives
        // in token order, but entries that share a token are only ordered by the hash stored in
        // the index. Partition ranges and reader output use ring order instead: first by token,
        // then by the full partition key. We can only restore that order after reading the log
        // records, because only then do we have the full decorated key rather than just its hash.
        // After sorting each same-token run by ring order, filtering against the non-wrapping
        // partition range can only remove a prefix and/or suffix of the batch.
        void sort_and_filter_mutations_for_range(std::vector<pending_mutation>& mutations) const {
            auto cmp = dht::ring_position_comparator(*_schema);
            auto in_range = [&] (const pending_mutation& pending) {
                return _pr.contains(dht::ring_position(pending.mut.decorated_key()), cmp);
            };

            auto run_begin = mutations.begin();
            while (run_begin != mutations.end()) {
                const auto& token = run_begin->mut.decorated_key().token();
                auto run_end = std::ranges::find_if(run_begin, mutations.end(), [&] (const pending_mutation& pending) {
                    return pending.mut.decorated_key().token() != token;
                });
                if (std::distance(run_begin, run_end) > 1) {
                    std::ranges::sort(run_begin, run_end, [&] (const pending_mutation& lhs, const pending_mutation& rhs) {
                        return cmp(lhs.mut.decorated_key(), rhs.mut.decorated_key()) < 0;
                    });
                }
                run_begin = run_end;
            }

            auto first_in_range = std::ranges::find_if(mutations, in_range);
            if (first_in_range == mutations.end()) {
                mutations.clear();
                return;
            }

            auto last_in_range = std::ranges::find_if(mutations.rbegin(), mutations.rend(), in_range).base();
            mutations.erase(last_in_range, mutations.end());
            mutations.erase(mutations.begin(), first_in_range);
        }

        std::optional<mutation_batch> collect_batch(size_t max_entries) {
            auto index_batch = _scan.next_batch(max_entries);
            if (!index_batch) {
                return std::nullopt;
            }

            mutation_batch batch{
                .first_token = index_batch->first_token,
                .last_token = index_batch->last_token,
                .exhausted = index_batch->exhausted,
            };
            batch.locations.reserve(index_batch->entry_count);
            for (const auto& entry : index_batch->entries) {
                batch.locations.push_back(entry.get().entry().location);
            }
            return batch;
        }

        future<bool> load_next_token_mutations() {
            static constexpr size_t read_ahead_entries = 10;

            if (_scan.exhausted()) {
                co_return false;
            }

            auto op = _index.start_read();
            auto batch = collect_batch(read_ahead_entries);
            if (!batch) {
                co_return false;
            }

            auto mutations = co_await read_mutations_for_batch(batch->locations);
            sort_and_filter_mutations_for_range(mutations);

            tracing::trace(_trace_state,
                    "logstor_range_reader: fetched {} keys for token range [{}, {}]",
                    mutations.size(), batch->first_token, batch->last_token);

            for (auto& m : mutations) {
                _pending_mutations.push(std::move(m));
            }
            co_return true;
        }

        bool has_pending_mutations_for_current_token() const {
            return !_pending_mutations.empty();
        }

        future<bool> open_next_partition_reader() {
            while (!has_pending_mutations_for_current_token()) {
                if (!co_await load_next_token_mutations()) {
                    co_return false;
                }
            }

            auto pending = std::move(_pending_mutations.front());
            _pending_mutations.pop();
            _current_partition_memory = std::move(pending.memory);
            _current_partition_reader = make_mutation_reader_from_mutations(
                _schema, _permit, std::move(pending.mut),
                _slice, streamed_mutation::forwarding::no
            );
            co_return true;
        }

        future<> reset_current_partition_reader() {
            if (!_current_partition_reader) {
                return make_ready_future<>();
            }

            auto fut = _current_partition_reader->close();
            _current_partition_reader = std::nullopt;
            _current_partition_memory.reset_to_zero();
            return fut;
        }

    public:
        logstor_range_reader(schema_ptr s, const primary_index& idx, reader_permit p,
                    logstor* ls, dht::partition_range pr,
                    query::partition_slice slice, tracing::trace_state_ptr ts)
            : impl(std::move(s), std::move(p))
            , _logstor(ls), _index(idx), _pr(std::move(pr))
            , _scan(_index.scan(partition_range_to_token_range(_pr)))
            , _slice(std::move(slice)), _trace_state(std::move(ts))
            , _current_partition_memory(_permit.consume_memory())
        {}

        virtual future<> fill_buffer() override {
            while (!is_buffer_full() && !_end_of_stream) {
                // Drain current partition's reader first
                if (_current_partition_reader) {
                    co_await _current_partition_reader->fill_buffer();
                    _current_partition_reader->move_buffer_content_to(*this);
                    if (!_current_partition_reader->is_end_of_stream()) {
                        continue;
                    }
                    co_await reset_current_partition_reader();
                    // Open the next partition only when the buffer has room for it. Its
                    // reader is closed by next_partition(), so a partition that was opened
                    // but not produced yet would be skipped.
                    continue;
                }

                if (!co_await open_next_partition_reader()) {
                    _end_of_stream = true;
                    break;
                }
            }
        }

        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            if (!is_buffer_empty()) {
                return make_ready_future<>();
            }
            _end_of_stream = false;
            if (_current_partition_reader) {
                return reset_current_partition_reader();
            }
            return make_ready_future<>();
        }

        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            clear_buffer();
            _end_of_stream = false;
            _pr = pr;
            _scan = _index.scan(partition_range_to_token_range(_pr));
            _pending_mutations = {};
            if (_current_partition_reader) {
                return reset_current_partition_reader();
            }
            return make_ready_future<>();
        }

        virtual future<> fast_forward_to(position_range) override {
            // Clustering forwarding is served by the make_forwardable() wrapper, which
            // never forwards the underlying reader, see make_reader().
            return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
        }

        virtual future<> close() noexcept override {
            return reset_current_partition_reader();
        }
    };

    auto maybe_make_forwardable = [] (mutation_reader reader, streamed_mutation::forwarding fwd) {
        if (fwd) {
            return make_forwardable(std::move(reader));
        }
        return reader;
    };

    // The single-partition reader can only ever produce its own key, so it must not be
    // given to a caller that may fast-forward it to later partition ranges. The range
    // reader serves a singular range correctly too, it just does more work for it.
    if (!fwd_mr && pr.is_singular() && pr.start()->value().has_key()) {
        return maybe_make_forwardable(make_mutation_reader<logstor_single_key_reader>(
            std::move(schema), index, std::move(permit), this, pr.start()->value().as_decorated_key(), slice, std::move(trace_state)
        ), fwd);
    } else {
        return maybe_make_forwardable(make_mutation_reader<logstor_range_reader>(
            std::move(schema), index, std::move(permit), this, pr, slice, std::move(trace_state)
        ), fwd);
    }
}

void logstor::set_trigger_compaction_hook(std::function<void()> fn) {
    _segment_manager.set_trigger_compaction_hook(std::move(fn));
}

void logstor::set_trigger_separator_flush_hook(std::function<void(segment_sequence)> fn) {
    _segment_manager.set_trigger_separator_flush_hook(std::move(fn));
}

}
