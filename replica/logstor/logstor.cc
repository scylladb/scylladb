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
#include <seastar/core/metrics.hh>
#include "query/query-request.hh"
#include "readers/from_mutations.hh"
#include "keys/keys.hh"
#include "replica/logstor/segment_manager.hh"
#include "replica/logstor/types.hh"
#include "utils/managed_bytes.hh"
#include <openssl/ripemd.h>
#include <openssl/evp.h>

namespace replica::logstor {

seastar::logger logstor_logger("logstor");

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

future<> logstor::do_recovery_for_test() {
    co_await _segment_manager.do_recovery_for_test();
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
    auto index = std::make_unique<primary_index>(schema, _segment_manager);
    if (cache_enabled) {
        index->set_cache_tracker(&_cache_tracker);
    }
    return index;
}

future<> logstor::write(const mutation& m, write_target target, db::timeout_clock::time_point timeout) {
    auto gate_holder = _async_gate.hold();

    auto& cg = *target.cg;
    primary_index_key key(m.decorated_key());
    table_id table = m.schema()->id();
    auto& index = cg.logstor_index();

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

    auto result_f = co_await coroutine::as_future(_write_buffer.write(std::move(writer), timeout, std::move(target)));
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

future<std::optional<mutation>> logstor::read(const schema& s, const primary_index& index, const dht::decorated_key& dk, const query::partition_slice& slice) {
    auto gate_holder = _async_gate.hold();

    auto op = index.start_read();

    const auto bypass_cache = slice.options.contains(query::partition_slice::option::bypass_cache);
    auto* cache = bypass_cache ? nullptr : index.cache_tracker();

    auto it = index.find(dk);
    if (it == index.end()) {
        co_return std::nullopt;
    }

    // lookup in cache
    if (cache) {
        auto cached_mut = cache->lookup(*it, s.shared_from_this());
        if (cached_mut) {
            co_return std::move(*cached_mut);
        }
    }

    // Cache miss (or bypass): read from disk using the entry we already have.
    // copy the entry. we want to remember the original entry that we use for the read. the entry may change while we read.
    const index_entry entry_for_read = it->entry();
    auto record = co_await _segment_manager.read(entry_for_read.location);

    if (record.mut.key() != dk.key()) [[unlikely]] {
        on_internal_error(logstor_logger, format("Key mismatch reading log entry: expected {}, got {}", dk.key(), record.mut.key()));
    }

    mutation m = record.mut.to_mutation(s.shared_from_this());

    // Populate the cache with the freshly deserialized mutation.
    // Skipped when bypass_cache is set.
    // We must re-find the entry because the iterator may have been invalidated
    // across the co_await above.
    if (cache) {
        auto it = index.find(dk);
        if (it != index.end() && it->entry().location == entry_for_read.location) {
            cache->populate(*it, m);
        }
    }

    co_return std::move(m);
}

std::map<sstring, mutation_source> logstor::make_mutation_sources_for_dump(schema_ptr s, const primary_index& index,
        const dht::decorated_key& dk, reader_permit permit) {
    // The snapshot of the cached mutation, held until the dump is done with its source.
    // The source is copyable, so the snapshot and the memory it is accounted with are
    // shared between its copies.
    struct cache_snapshot {
        mutation mut;
        reader_permit::resource_units memory;
    };

    std::map<sstring, mutation_source> sources;

    auto lookup = index.lookup_for_dump(dk, s);
    if (!lookup) {
        return sources;
    }

    // The cached mutation is snapshotted by the lookup above; the cache entry may be
    // evicted by the time the source is read from.
    if (lookup->cached_mutation) {
        auto memory = permit.consume_memory(lookup->cached_mutation->memory_usage(*s));
        auto snapshot = make_lw_shared<cache_snapshot>(cache_snapshot{std::move(*lookup->cached_mutation), std::move(memory)});
        sources.emplace("logstor-cache", mutation_source([snapshot = std::move(snapshot)] (
                schema_ptr s,
                reader_permit permit,
                const dht::partition_range&,
                const query::partition_slice& slice,
                tracing::trace_state_ptr,
                streamed_mutation::forwarding fwd,
                mutation_reader::forwarding) {
            return make_mutation_reader_from_mutations(std::move(s), std::move(permit), mutation(snapshot->mut), slice, fwd);
        }));
    }

    // The name identifies the segment holding the record, the way an sstable source is
    // named after the sstable holding the row, and not the record's position inside it.
    // All the records of a segment therefore share one source name, so that a dump can be
    // restricted to the contents of a single segment. The name changes when compaction or
    // an overwrite moves the record to another segment.
    const auto location = lookup->entry.location;
    auto name = format("logstor-log:{}:{}", _segment_manager.get_segment_file_path(location.segment), location.segment.value);

    // The record is read through the regular read path, with the cache bypassed so that
    // inspecting a table neither reads from nor populates it. The read happens on the
    // first fill_buffer() call and resolves the record location from the index again, so
    // a compaction which moved the record in the meantime is followed (in which case the
    // source name, computed above, names the record's old location).
    sources.emplace(std::move(name), mutation_source([this, &index] (
            schema_ptr s,
            reader_permit permit,
            const dht::partition_range& pr,
            const query::partition_slice& slice,
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding,   // The dump consumes whole partitions and never
            mutation_reader::forwarding) {   // forwards the readers it creates.
        auto dump_slice = slice;
        dump_slice.options.set<query::partition_slice::option::bypass_cache>();
        return make_reader(std::move(s), index, std::move(permit), pr, dump_slice, std::move(trace_state));
    }));

    return sources;
}

mutation_reader logstor::make_reader(schema_ptr schema, const primary_index& index, reader_permit permit, const dht::partition_range& pr,
        const query::partition_slice& slice, tracing::trace_state_ptr trace_state) {

    class logstor_range_reader : public mutation_reader::impl {
        logstor* _logstor;
        const primary_index& _index;
        dht::partition_range _pr;
        query::partition_slice _slice;
        tracing::trace_state_ptr _trace_state;
        std::optional<dht::decorated_key> _last_key; // owns the key, safe across yields
        mutation_reader_opt _current_partition_reader;
        dht::ring_position_comparator _cmp;

        // Finds the next iterator to process, safe to call after any co_await
        primary_index::partitions_type::const_iterator find_next() const {
            auto it = _last_key
                ? _index.upper_bound(*_last_key)                        // strictly after last key
                : position_at_range_start();                            // initial positioning
            // If start was exclusive and we haven't yet seen a key
            return it;
        }

        primary_index::partitions_type::const_iterator position_at_range_start() const {
            if (!_pr.start()) {
                return _index.begin();
            }
            auto it = _index.lower_bound(_pr.start()->value());
            if (!_pr.start()->is_inclusive() && it != _index.end()) {
                if (_cmp(it->key(), _pr.start()->value()) == 0) {
                    ++it;
                }
            }
            return it;
        }

        bool exceeds_range_end(const primary_index_entry& e) const {
            if (!_pr.end()) return false;
            auto c = _cmp(e.key(), _pr.end()->value());
            return _pr.end()->is_inclusive() ? c > 0 : c >= 0;
        }

    public:
        logstor_range_reader(schema_ptr s, const primary_index& idx, reader_permit p,
                    logstor* ls, dht::partition_range pr,
                    query::partition_slice slice, tracing::trace_state_ptr ts)
            : impl(std::move(s), std::move(p))
            , _logstor(ls), _index(idx), _pr(std::move(pr))
            , _slice(std::move(slice)), _trace_state(std::move(ts))
            , _cmp(*_schema)
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
                    co_await _current_partition_reader->close();
                    _current_partition_reader = std::nullopt;
                    // _last_key was already set when we opened the reader
                }

                // Find next key in range (safe after co_await since we use _last_key)
                auto it = find_next();
                if (it == _index.end() || exceeds_range_end(*it)) {
                    _end_of_stream = true;
                    break;
                }

                // Snapshot the key before yielding
                auto current_key = it->key();

                auto guard = reader_permit::awaits_guard(_permit);
                auto mut = co_await _logstor->read(*_schema, _index, current_key, _slice);

                _last_key = current_key; // mark as visited even if not found (tombstoned)

                if (!mut) {
                    continue; // key was removed between index lookup and read
                }

                tracing::trace(_trace_state, "logstor_range_reader: fetched key {}", current_key);

                _current_partition_reader = make_mutation_reader_from_mutations(
                    _schema, _permit, std::move(*mut),
                    _slice, streamed_mutation::forwarding::no
                );
            }
        }

        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            if (!is_buffer_empty()) return make_ready_future<>();
            _end_of_stream = false;
            if (_current_partition_reader) {
                auto fut = _current_partition_reader->close();
                _current_partition_reader = std::nullopt;
                return fut;
            }
            return make_ready_future<>();
        }

        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            clear_buffer();
            _end_of_stream = false;
            _pr = pr;
            _last_key = std::nullopt;      // re-position from new range start
            if (_current_partition_reader) {
                auto fut = _current_partition_reader->close();
                _current_partition_reader = std::nullopt;
                return fut;
            }
            return make_ready_future<>();
        }

        virtual future<> fast_forward_to(position_range pr) override {
            if (_current_partition_reader) {
                clear_buffer();
                return _current_partition_reader->fast_forward_to(std::move(pr));
            }
            return make_ready_future<>();
        }

        virtual future<> close() noexcept override {
            if (_current_partition_reader) {
                return _current_partition_reader->close();
            }
            return make_ready_future<>();
        }
    };

    return make_mutation_reader<logstor_range_reader>(
        std::move(schema), index, std::move(permit), this, pr, slice, std::move(trace_state)
    );
}

future<> logstor::flush_to_separator() {
    auto gate_holder = _async_gate.hold();
    co_await _write_buffer.flush();
    co_await _segment_manager.await_pending_writes();
}

}
