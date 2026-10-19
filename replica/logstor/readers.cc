/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// The readers of a logstor table. The single-key reader reads one partition. The range reader
// scans the primary index for a partition range. logstor::make_reader() selects one of the two.
// logstor::make_mutation_sources_for_dump() makes the sources for a MUTATION_FRAGMENTS() query.

#include "replica/logstor/logstor.hh"
#include <algorithm>
#include <queue>
#include <vector>
#include <seastar/core/coroutine.hh>
#include <seastar/core/when_all.hh>
#include "mutation/mutation_fragment.hh"
#include "query/query-request.hh"
#include "readers/forwardable.hh"
#include "readers/from_mutations.hh"
#include "replica/logstor/record_value.hh"
#include "replica/logstor/segment_manager.hh"
#include "replica/logstor/single_row_partition.hh"

namespace replica::logstor {

namespace {

// What the readers share: a single_row_partition going into the buffer as a partition start, its
// row and a partition end. Only as far as the buffer bound allows, though - a caller that shrank
// its buffer is owed a stop between any two fragments - so the fragments not pushed yet wait for
// the next fill. The fragments account for themselves as they are pushed, so the reader charges a
// partition to the permit only while its key or its row waits outside the buffer, and releases the
// charge when it pushes the row.
class logstor_reader_base : public mutation_reader::impl {
protected:
    struct pending_partition {
        dht::decorated_key key;
        single_row_partition partition;
        // The charge for the partition while it waits outside the buffer, or none.
        reader_permit::resource_units memory;
    };

    // The query's schema, _schema, is what the fragments are pushed under. The table's is what the
    // cache is asked and a record decoded under. They differ for a reversed read, whose query
    // schema is the reversed one: a table without clustering columns has nothing to reverse, and a
    // cached partition upgraded to the reversed schema would only be upgraded back by the next
    // forward read.
    schema_ptr _table_schema;
    query::partition_slice _slice;
    // Decided once, from the slice: a slice with bypass_cache neither looks the cache up nor admits
    // what it reads from a segment.
    const use_cache _use_cache;

private:
    // The partition whose fragments are going into the buffer, from its start being pushed until
    // its end is.
    std::optional<pending_partition> _emitting;
    bool _row_pushed = false;

protected:
    logstor_reader_base(schema_ptr s, reader_permit p, const query::partition_slice& slice)
        : impl(std::move(s), std::move(p))
        , _table_schema(slice.is_reversed() ? _schema->get_reversed() : _schema)
        , _slice(slice)
        , _use_cache(!_slice.options.contains(query::partition_slice::option::bypass_cache))
    { }

    bool emitting() const noexcept {
        return bool(_emitting);
    }

    // What a partition holds while it waits outside the buffer: the pending_partition, the bytes of
    // its key until the partition start takes the key, and the cells of its row.
    size_t waiting_memory_usage(const dht::decorated_key& key, const single_row_partition& partition) const {
        size_t usage = sizeof(pending_partition) + key.key().external_memory_usage();
        if (partition.row) {
            usage += partition.row->cells().external_memory_usage(*_table_schema, column_kind::regular_column);
        }
        return usage;
    }

    // Begins a partition: pushes its start at once, so that a partition is either not begun or
    // begun - and next_partition() drops what was begun - and then as much of the rest as the
    // buffer takes. The buffer must have room for the start.
    void start_partition(pending_partition&& pending) {
        _emitting.emplace(std::move(pending));
        auto& p = *_emitting;
        // A slice whose row ranges exclude the one row a logstor partition has. A logstor schema
        // has no clustering columns for a range to be built on, so a range that exists is the full
        // one, and only an empty list excludes the row.
        if (p.partition.row && _slice.row_ranges(*_schema, p.key.key()).empty()) {
            p.partition.row.reset();
        }
        _row_pushed = false;
        push_mutation_fragment(*_schema, _permit, partition_start(std::move(p.key), p.partition.partition_tombstone));
        emit_rest();
    }

    // Pushes what is left of the partition being emitted, its end included, as far as the buffer
    // allows. Returns whether the end was pushed.
    bool emit_rest() {
        auto& p = *_emitting;
        if (!_row_pushed) {
            if (is_buffer_full()) {
                // The row waits for the next fill. The charge is set again because the partition
                // start took the key, and the slice can have dropped the row.
                p.memory.reset_to(reader_resources::with_memory(waiting_memory_usage(p.key, p.partition)));
                return false;
            }
            // The fragment of the row accounts for the row from now. Release the charge before the
            // push, or the push charges the row a second time.
            p.memory.reset_to_zero();
            if (p.partition.row) {
                push_mutation_fragment(*_schema, _permit, clustering_row(clustering_key::make_empty(), std::move(*p.partition.row)));
            }
            _row_pushed = true;
        }
        if (is_buffer_full()) {
            return false;
        }
        push_mutation_fragment(*_schema, _permit, partition_end{});
        _emitting.reset();
        return true;
    }

    void drop_partition() noexcept {
        _emitting.reset();
    }

public:
    virtual future<> fast_forward_to(position_range) override {
        // Clustering forwarding is served by the make_forwardable() wrapper, which
        // never forwards the underlying reader, see make_reader().
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }

    virtual future<> close() noexcept override {
        drop_partition();
        return make_ready_future<>();
    }
};

class logstor_single_key_reader : public logstor_reader_base {
    segment_manager& _segment_manager;
    const primary_index& _index;
    // The key read. It goes into the partition start once the partition is found: its last use.
    dht::decorated_key _dk;
    tracing::trace_state_ptr _trace_state;
    // Set once the partition was found and begun. Until then the reader is positioned before
    // the partition, which next_partition() must not skip over.
    bool _started = false;

    // The partition, found: begun in the buffer, and the stream ends with it. It goes into the
    // buffer at once, so it has no charge unless its row has to wait for the next fill.
    void begin(single_row_partition&& partition) {
        _started = true;
        start_partition(pending_partition{std::move(_dk), std::move(partition), _permit.consume_memory()});
        _end_of_stream = !emitting();
    }

    future<> read_miss(primary_index_key key, primary_index::guarded_location where) {
        // The record is charged from the read until the end of this function, which frees it.
        auto record_memory = _permit.consume_memory(where.location.size);
        // The permit awaits only while the record is read from the disk, and not while it is
        // decoded and put in the cache, so that the semaphore does not admit readers for CPU work.
        std::optional<reader_permit::awaits_guard> awaits(std::in_place, _permit);
        auto record = co_await _segment_manager.read(where.guard, where.location);
        awaits.reset();
        if (record.header.key.key() != _dk.key()) [[unlikely]] {
            co_await coroutine::return_exception(key_mismatch_error(_dk.key(), record.header.key.key(), where.location));
        }
        auto partition = decode_record_value(record.value, *_table_schema, record.header);
        if (_use_cache) {
            _index.populate_cache(key, where.location, _table_schema, _dk.key(), partition);
        }
        tracing::trace(_trace_state, "logstor_single_key_reader: fetched key {}", _dk);
        begin(std::move(partition));
    }

public:
    logstor_single_key_reader(schema_ptr s, const primary_index& idx, reader_permit p,
            segment_manager& sm, dht::decorated_key dk,
            const query::partition_slice& slice, tracing::trace_state_ptr ts)
        : logstor_reader_base(std::move(s), std::move(p), slice)
        , _segment_manager(sm), _index(idx), _dk(std::move(dk))
        , _trace_state(std::move(ts)) {
    }

    virtual future<> fill_buffer() override {
        if (emitting()) {
            if (emit_rest()) {
                _end_of_stream = true;
            }
            return make_ready_future<>();
        }
        if (_started || _end_of_stream) {
            return make_ready_future<>();
        }

        try {
            primary_index_key key(_dk);
            auto found = _index.lookup_for_read(key, *_table_schema, _use_cache);
            if (!found) {
                _end_of_stream = true;
                return make_ready_future<>();
            }
            if (auto* cached = std::get_if<keyed_single_row_partition>(&*found)) {
                if (cached->key.key() != _dk.key()) [[unlikely]] {
                    throw key_mismatch_error(_dk.key(), cached->key.key(), key_mismatch_error::cache_location{});
                }
                tracing::trace(_trace_state, "logstor_single_key_reader: fetched key {} from the cache", _dk);
                begin(std::move(cached->partition));
                return make_ready_future<>();
            }
            return read_miss(key, std::get<primary_index::guarded_location>(std::move(*found)));
        } catch (...) {
            return current_exception_as_future();
        }
    }

    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (!is_buffer_empty()) {
            return make_ready_future<>();
        }
        if (!_started) {
            // Positioned before the partition, nothing to skip over.
            return make_ready_future<>();
        }
        // The reader produces a single partition, so skipping it ends the stream.
        _end_of_stream = true;
        drop_partition();
        return make_ready_future<>();
    }

    virtual future<> fast_forward_to(const dht::partition_range&) override {
        // This reader can only produce its own key, so it is used only when the caller
        // will not move it to another partition range, see make_reader(). That also
        // means _started can never go stale.
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
};

class logstor_range_reader : public logstor_reader_base {
    segment_manager& _segment_manager;
    const primary_index& _index;
    dht::partition_range _pr;
    primary_index::token_range_scan _scan;
    tracing::trace_state_ptr _trace_state;

    // The partitions of a batch that are not in the buffer yet, in ring order. Charged to the
    // permit from the moment they were read or copied out of the cache until their fragments
    // account for themselves: the buffer does its own accounting, and they are not in it.
    std::queue<pending_partition> _pending;

    // One entry of a batch: where its record lives, and the partition once it is available -
    // taken from the cache while the index entry was still in hand, or read from the log.
    struct batch_slot {
        record_location location;
        std::optional<pending_partition> partition;
        // The record read from the log, kept from the read until it is decoded into partition.
        std::optional<log_record> record;
    };

    struct partition_batch {
        std::vector<batch_slot> slots;
        // Keeps the locations of the slots readable until the batch is read.
        index_read_guard guard;
        dht::token first_token;
        dht::token last_token;
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

    // A partition of a batch, charged from now: it waits in the batch and then in
    // _pending, across fills, before it goes into the buffer.
    pending_partition make_pending(dht::decorated_key key, single_row_partition partition) {
        auto memory = _permit.consume_memory(waiting_memory_usage(key, partition));
        return pending_partition{std::move(key), std::move(partition), std::move(memory)};
    }

    // Reads the records the cache did not have, filling each slot in place so that the batch
    // keeps the index order the scan produced. A range read does not populate the cache: a
    // scan would pull a whole table through it and evict what the point reads rely on.
    // The records are all read first and then decoded, so that the permit awaits only while
    // the reads are in flight, and not while a record is decoded.
    future<std::vector<pending_partition>> read_partitions_for_batch(partition_batch& batch) {
        auto& slots = batch.slots;

        // The records are charged in one call until all of them are decoded, so a record decoded
        // early stays charged next to its partition until then. The charge can fail, so it is made
        // before the first read starts: a read in flight must not be abandoned.
        size_t record_bytes = 0;
        for (const auto& slot : slots) {
            if (!slot.partition) {
                record_bytes += slot.location.size;
            }
        }
        auto record_memory = _permit.consume_memory(record_bytes);

        {
            auto awaits = reader_permit::awaits_guard(_permit);
            std::vector<future<>> reads;
            reads.reserve(slots.size());

            for (auto& slot : slots) {
                if (slot.partition) {
                    continue;
                }
                reads.push_back(_segment_manager.read(batch.guard, slot.location).then([&slot] (log_record record) {
                    slot.record = std::move(record);
                }));
            }

            co_await when_all_succeed(reads.begin(), reads.end());
        }

        std::vector<pending_partition> partitions;
        partitions.reserve(slots.size());
        for (auto& slot : slots) {
            if (slot.record) {
                auto partition = decode_record_value(slot.record->value, *_table_schema, slot.record->header);
                slot.partition = make_pending(std::move(slot.record->header.key), std::move(partition));
                slot.record.reset();
            }
            partitions.push_back(std::move(*slot.partition));
        }
        co_return partitions;
    }

    // Primary-index scan order is by (token, key hash). That means the batch already arrives
    // in token order, but entries that share a token are only ordered by the hash stored in
    // the index. Partition ranges and reader output use ring order instead: first by token,
    // then by the full partition key. We can only restore that order after reading the log
    // records, because only then do we have the full decorated key rather than just its hash.
    //
    // The batch is also coarser than the requested range, because
    // partition_range_to_token_range() keeps the whole token of a key-bearing bound. Sorting
    // each same-token run first means that filtering against the non-wrapping partition range
    // can only remove a prefix and/or a suffix of the batch.
    void sort_and_filter_partitions_for_range(std::vector<pending_partition>& partitions) const {
        auto cmp = dht::ring_position_comparator(*_schema);
        auto in_range = [&] (const pending_partition& p) {
            return _pr.contains(dht::ring_position(p.key), cmp);
        };

        auto run_begin = partitions.begin();
        while (run_begin != partitions.end()) {
            const auto& token = run_begin->key.token();
            auto run_end = std::ranges::find_if(run_begin, partitions.end(), [&] (const pending_partition& p) {
                return p.key.token() != token;
            });
            if (std::distance(run_begin, run_end) > 1) {
                std::ranges::sort(run_begin, run_end, [&] (const pending_partition& lhs, const pending_partition& rhs) {
                    return cmp(lhs.key, rhs.key) < 0;
                });
            }
            run_begin = run_end;
        }

        auto first_in_range = std::ranges::find_if(partitions, in_range);
        if (first_in_range == partitions.end()) {
            partitions.clear();
            return;
        }

        auto last_in_range = std::ranges::find_if(partitions.rbegin(), partitions.rend(), in_range).base();
        partitions.erase(last_in_range, partitions.end());
        partitions.erase(partitions.begin(), first_in_range);
    }

    // Must not yield: the entries the scan returns are only valid until the next yield, and a
    // cache lookup needs the entry itself, since that is what the cached partition hangs off.
    std::optional<partition_batch> collect_batch(size_t max_entries) {
        auto index_batch = _scan.next_batch(max_entries);
        if (!index_batch) {
            return std::nullopt;
        }

        partition_batch batch{
            .guard = std::move(index_batch->guard),
            .first_token = index_batch->first_token,
            .last_token = index_batch->last_token,
        };
        batch.slots.reserve(index_batch->entry_count);
        for (const auto& entry : index_batch->entries) {
            batch_slot slot{.location = entry.get().entry().location};
            if (auto cached = _index.lookup_cached(entry.get(), *_table_schema, _use_cache)) {
                slot.partition = make_pending(std::move(cached->key), std::move(cached->partition));
            }
            batch.slots.push_back(std::move(slot));
        }
        return batch;
    }

    // Takes the next batch of entries from the scan, which may span many tokens, and queues their
    // partitions in ring order. Returns false when the scan has no more entries.
    future<bool> load_next_batch() {
        // The entries one batch takes from the scan, and so the records read from their segments
        // concurrently: a scan holds at most this many partitions ahead of its buffer.
        static constexpr size_t read_ahead_entries = 10;

        if (_scan.exhausted()) {
            co_return false;
        }

        auto batch = collect_batch(read_ahead_entries);
        if (!batch) {
            co_return false;
        }

        auto partitions = co_await read_partitions_for_batch(*batch);
        sort_and_filter_partitions_for_range(partitions);

        tracing::trace(_trace_state,
                "logstor_range_reader: fetched {} keys for token range [{}, {}]",
                partitions.size(), batch->first_token, batch->last_token);

        for (auto& p : partitions) {
            _pending.push(std::move(p));
        }
        co_return true;
    }

public:
    logstor_range_reader(schema_ptr s, const primary_index& idx, reader_permit p,
                segment_manager& sm, dht::partition_range pr,
                const query::partition_slice& slice, tracing::trace_state_ptr ts)
        : logstor_reader_base(std::move(s), std::move(p), slice)
        , _segment_manager(sm), _index(idx), _pr(std::move(pr))
        , _scan(_index.scan(partition_range_to_token_range(_pr)))
        , _trace_state(std::move(ts))
    {}

    virtual future<> fill_buffer() override {
        while (!is_buffer_full() && !_end_of_stream) {
            if (emitting()) {
                emit_rest();
                continue;
            }
            if (_pending.empty() && !co_await load_next_batch()) {
                _end_of_stream = true;
                break;
            }
            // A batch filtered down to nothing leaves the queue empty, and the next round loads
            // the one after it.
            if (!_pending.empty()) {
                start_partition(std::move(_pending.front()));
                _pending.pop();
            }
        }
    }

    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (!is_buffer_empty()) {
            return make_ready_future<>();
        }
        _end_of_stream = false;
        drop_partition();
        return make_ready_future<>();
    }

    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        clear_buffer();
        _end_of_stream = false;
        _pr = pr;
        _scan = _index.scan(partition_range_to_token_range(_pr));
        _pending = {};
        drop_partition();
        return make_ready_future<>();
    }
};

} // anonymous namespace

mutation_reader logstor::make_reader(schema_ptr schema, const primary_index& index, reader_permit permit, const dht::partition_range& pr,
        const query::partition_slice& slice, tracing::trace_state_ptr trace_state, streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr) {
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
            std::move(schema), index, std::move(permit), _segment_manager, pr.start()->value().as_decorated_key(), slice, std::move(trace_state)
        ), fwd);
    } else {
        return maybe_make_forwardable(make_mutation_reader<logstor_range_reader>(
            std::move(schema), index, std::move(permit), _segment_manager, pr, slice, std::move(trace_state)
        ), fwd);
    }
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

    primary_index_key pk(dk);
    auto lookup = index.lookup_for_dump(pk, s);
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

} // namespace replica::logstor
