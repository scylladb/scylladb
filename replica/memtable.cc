/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include "memtable.hh"
#include "replica/database.hh"
#include "mutation/frozen_mutation.hh"
#include "partition_snapshot_reader.hh"
#include "partition_builder.hh"
#include "mutation/mutation_partition_view.hh"
#include "readers/empty_v2.hh"
#include "readers/forwardable_v2.hh"
#include "sstables/types.hh"

namespace replica {

static mutation_reader make_partition_snapshot_flat_reader_from_snp_schema(
        bool is_reversed,
        reader_permit permit,
        dht::decorated_key dk,
        query::clustering_key_filter_ranges crr,
        partition_snapshot_ptr snp,
        bool digest_requested,
        logalloc::region& region,
        logalloc::allocating_section& read_section,
        std::any pointer_to_container,
        streamed_mutation::forwarding fwd, memtable& memtable);

void memtable::memtable_encoding_stats_collector::update_timestamp(api::timestamp_type ts) noexcept {
    if (ts != api::missing_timestamp) {
        encoding_stats_collector::update_timestamp(ts);
        min_max_timestamp.update(ts);
    }
}

memtable::memtable_encoding_stats_collector::memtable_encoding_stats_collector() noexcept
    : min_max_timestamp(0, 0)
{}

void memtable::memtable_encoding_stats_collector::update(atomic_cell_view cell) noexcept {
    update_timestamp(cell.timestamp());
    if (cell.is_live_and_has_ttl()) {
        update_ttl(cell.ttl());
        update_local_deletion_time(cell.expiry());
    } else if (!cell.is_live()) {
        update_local_deletion_time(cell.deletion_time());
    }
}

void memtable::memtable_encoding_stats_collector::update(tombstone tomb) noexcept {
    if (tomb) {
        update_timestamp(tomb.timestamp);
        update_local_deletion_time(tomb.deletion_time);
    }
}

void memtable::memtable_encoding_stats_collector::update(const ::schema& s, const row& r, column_kind kind) {
    r.for_each_cell([this, &s, kind](column_id id, const atomic_cell_or_collection& item) {
        auto& col = s.column_at(kind, id);
        if (col.is_atomic()) {
            update(item.as_atomic_cell(col));
        } else {
            item.as_collection_mutation().with_deserialized(*col.type, [&] (collection_mutation_view_description mview) {
            // Note: when some of the collection cells are dead and some are live
            // we need to encode a "live" deletion_time for the living ones.
            // It is not strictly required to update encoding_stats for the latter case
            // since { <int64_t>.min(), <int32_t>.max() } will not affect the encoding_stats
            // minimum values.  (See #4035)
            update(mview.tomb);
            for (auto& entry : mview.cells) {
                update(entry.second);
            }
            });
        }
    });
}

void memtable::memtable_encoding_stats_collector::update(const range_tombstone& rt) noexcept {
    update(rt.tomb);
}

void memtable::memtable_encoding_stats_collector::update(const row_marker& marker) noexcept {
    update_timestamp(marker.timestamp());
    if (!marker.is_missing()) {
        if (!marker.is_live()) {
            update_ttl(gc_clock::duration(sstables::expired_liveness_ttl));
            update_local_deletion_time(marker.deletion_time());
        } else if (marker.is_expiring()) {
            update_ttl(marker.ttl());
            update_local_deletion_time(marker.expiry());
        }
    }
}

void memtable::memtable_encoding_stats_collector::update(const ::schema& s, const deletable_row& dr) {
    update(dr.marker());
    row_tombstone row_tomb = dr.deleted_at();
    update(row_tomb.regular());
    update(row_tomb.tomb());
    update(s, dr.cells(), column_kind::regular_column);
}

void memtable::memtable_encoding_stats_collector::update(const ::schema& s, const mutation_partition& mp) {
    update(mp.partition_tombstone());
    update(s, mp.static_row().get(), column_kind::static_column);
    for (auto&& row_entry : mp.clustered_rows()) {
        update(s, row_entry.row());
    }
    for (auto&& rt : mp.row_tombstones()) {
        update(rt.tombstone());
    }
}

memtable::memtable(schema_ptr schema, dirty_memory_manager& dmm,
    memtable_table_shared_data& table_shared_data,
    replica::table_stats& table_stats,
    memtable_list* memtable_list, seastar::scheduling_group compaction_scheduling_group)
        : dirty_memory_manager_logalloc::size_tracked_region()
        , _dirty_mgr(dmm)
        , _cleaner(*this, no_cache_tracker, table_stats.memtable_app_stats, compaction_scheduling_group,
                   [this] (size_t freed) { remove_flushed_memory(freed); })
        , _memtable_list(memtable_list)
        , _schema(std::move(schema))
        , _table_shared_data(table_shared_data)
        , partitions(dht::raw_token_less_comparator{})
        , _table_stats(table_stats) {
    logalloc::region::listen(&dmm.region_group());
}

static thread_local dirty_memory_manager mgr_for_tests;
static thread_local replica::table_stats stats_for_tests;
static thread_local memtable_table_shared_data memtable_shared_data_for_tests;

memtable::memtable(schema_ptr schema)
        : memtable(std::move(schema), mgr_for_tests,
                 memtable_shared_data_for_tests, stats_for_tests)
{ }

memtable::~memtable() {
    revert_flushed_memory();
    clear();
    logalloc::region::unlisten();
}

uint64_t memtable::dirty_size() const {
    return occupancy().total_space();
}

void memtable::evict_entry(memtable_entry& e, mutation_cleaner& cleaner) noexcept {
    e.partition().evict(cleaner);
    nr_partitions--;
}

void memtable::clear() noexcept {
    auto dirty_before = dirty_size();
    with_allocator(allocator(), [this] {
        partitions.clear_and_dispose([this] (memtable_entry* e) noexcept {
            evict_entry(*e, _cleaner);
        });
    });
    remove_flushed_memory(dirty_before - dirty_size());
}

future<> memtable::clear_gently() noexcept {
    return futurize_invoke([this] {
        auto t = std::make_unique<seastar::thread>([this] {
            auto& alloc = allocator();

            auto p = std::move(partitions);
            nr_partitions = 0;
            while (!p.empty()) {
                auto dirty_before = dirty_size();
                with_allocator(alloc, [&] () noexcept {
                    while (!p.empty()) {
                        if (p.begin()->clear_gently() == stop_iteration::no) {
                            break;
                        }
                        p.begin().erase(dht::raw_token_less_comparator{});
                        if (need_preempt()) {
                            break;
                        }
                    }
                });
                remove_flushed_memory(dirty_before - dirty_size());
                seastar::thread::yield();
            }

            /*
             * The collection is not guaranteed to free everything
             * with the last erase. If anything gets freed in destructor,
             * it will be unaccounted from wrong allocator, so handle it
             */
            with_allocator(alloc, [&p] { p.clear(); });
        });
        auto f = t->join();
        return f.then([t = std::move(t)] {});
    }).handle_exception([this] (auto e) {
        this->clear();
    });
}

partition_entry&
memtable::find_or_create_partition_slow(partition_key_view key) {
    SCYLLA_ASSERT(!reclaiming_enabled());

    // FIXME: Perform lookup using std::pair<token, partition_key_view>
    // to avoid unconditional copy of the partition key.
    // We can't do it right now because std::map<> which holds
    // partitions doesn't support heterogeneous lookup.
    // We could switch to boost::intrusive_map<> similar to what we have for row keys.
    auto& outer = current_allocator();
    return with_allocator(standard_allocator(), [&, this] () -> partition_entry& {
        auto dk = dht::decorate_key(*_schema, key);
        return with_allocator(outer, [&dk, this] () -> partition_entry& {
            return find_or_create_partition(dk);
        });
    });
}

partition_entry&
memtable::find_or_create_partition(const dht::decorated_key& key) {
    SCYLLA_ASSERT(!reclaiming_enabled());

    // call lower_bound so we have a hint for the insert, just in case.
    partitions_type::bound_hint hint;
    auto i = partitions.lower_bound(key, dht::ring_position_comparator(*_schema), hint);
    if (i == partitions.end() || !hint.match) {
        partitions_type::iterator entry = partitions.emplace_before(i,
                key.token().raw(), hint,
                _schema, dht::decorated_key(key), mutation_partition(*_schema));
        ++nr_partitions;
        ++_table_stats.memtable_partition_insertions;
        if (!hint.emplace_keeps_iterators()) {
            current_allocator().invalidate_references();
        }
        return entry->partition();
    } else {
        ++_table_stats.memtable_partition_hits;
        upgrade_entry(*i);
    }
    return i->partition();
}

bool
memtable::contains_partition(const dht::decorated_key& key) const {
    return partitions.find(key, dht::ring_position_comparator(*_schema)) != partitions.end();
}

boost::iterator_range<memtable::partitions_type::const_iterator>
memtable::slice(const dht::partition_range& range) const {
    if (query::is_single_partition(range)) {
        const query::ring_position& pos = range.start()->value();
        auto i = partitions.find(pos, dht::ring_position_comparator(*_schema));
        if (i != partitions.end()) {
            return boost::make_iterator_range(i, std::next(i));
        } else {
            return boost::make_iterator_range(i, i);
        }
    } else {
        auto cmp = dht::ring_position_comparator(*_schema);

        auto i1 = range.start()
                  ? (range.start()->is_inclusive()
                        ? partitions.lower_bound(range.start()->value(), cmp)
                        : partitions.upper_bound(range.start()->value(), cmp))
                  : partitions.cbegin();

        auto i2 = range.end()
                  ? (range.end()->is_inclusive()
                        ? partitions.upper_bound(range.end()->value(), cmp)
                        : partitions.lower_bound(range.end()->value(), cmp))
                  : partitions.cend();

        return boost::make_iterator_range(i1, i2);
    }
}

class iterator_reader {
    lw_shared_ptr<memtable> _memtable;
    schema_ptr _schema;
    const dht::partition_range* _range;
    std::optional<dht::decorated_key> _last;
    memtable::partitions_type::iterator _i;
    memtable::partitions_type::iterator _end;
    uint64_t _last_reclaim_counter;
    size_t _last_partition_count = 0;

    memtable::partitions_type::iterator lookup_end() {
        auto cmp = dht::ring_position_comparator(*_memtable->_schema);
        return _range->end()
            ? (_range->end()->is_inclusive()
                ? _memtable->partitions.upper_bound(_range->end()->value(), cmp)
                : _memtable->partitions.lower_bound(_range->end()->value(), cmp))
            : _memtable->partitions.end();
    }
    void update_iterators() {
        // We must be prepared that iterators may get invalidated during compaction.
        auto current_reclaim_counter = _memtable->reclaim_counter();
        auto cmp = dht::ring_position_comparator(*_memtable->_schema);
        if (_last) {
            if (current_reclaim_counter != _last_reclaim_counter ||
                  _last_partition_count != _memtable->partition_count()) {
                _i = _memtable->partitions.upper_bound(*_last, cmp);
                _end = lookup_end();
                _last_partition_count = _memtable->partition_count();
            }
        } else {
            // Initial lookup
            _i = _range->start()
                 ? (_range->start()->is_inclusive()
                    ? _memtable->partitions.lower_bound(_range->start()->value(), cmp)
                    : _memtable->partitions.upper_bound(_range->start()->value(), cmp))
                 : _memtable->partitions.begin();
            _end = lookup_end();
            _last_partition_count = _memtable->partition_count();
        }
        _last_reclaim_counter = current_reclaim_counter;
    }
protected:
    iterator_reader(schema_ptr s,
                    lw_shared_ptr<memtable> m,
                    const dht::partition_range& range)
        : _memtable(std::move(m))
        , _schema(std::move(s))
        , _range(&range)
    { }

    memtable_entry* fetch_entry() {
        update_iterators();
        if (_i == _end) {
            return nullptr;
        } else {
            memtable_entry& e = *_i;
            _memtable->upgrade_entry(e);
            return &e;
        }
    }

    void advance_iterator() {
        ++_i;
    }

    void update_last(dht::decorated_key last) {
        _last = std::move(last);
    }

    logalloc::allocating_section& read_section() {
        return _memtable->_table_shared_data.read_section;
    }

    lw_shared_ptr<memtable> mtbl() {
        return _memtable;
    }

    schema_ptr schema() {
        return _schema;
    }

    logalloc::region& region() {
        return *_memtable;
    };

    std::optional<dht::partition_range> get_delegate_range() {
        // We cannot run concurrently with row_cache::update().
        if (_memtable->is_flushed()) {
            return _last ? _range->split_after(*_last, dht::ring_position_comparator(*_memtable->_schema)) : *_range;
        }
        return {};
    }

    mutation_reader delegate_reader(reader_permit permit,
                                    const dht::partition_range& delegate,
                                    const query::partition_slice& slice,
                                    streamed_mutation::forwarding fwd,
                                    mutation_reader::forwarding fwd_mr) {
        auto ret = _memtable->_underlying->make_reader_v2(_schema, std::move(permit), delegate, slice, nullptr, fwd, fwd_mr);
        _memtable = {};
        _last = {};
        return ret;
    }
    future<> fast_forward_to(const dht::partition_range& pr) {
        _range = &pr;
        _last = { };
        return make_ready_future<>();
    }
};

class partition_snapshot_read_accounter {
    memtable& _mt;
public:
    explicit partition_snapshot_read_accounter(memtable& mt): _mt(mt) {}

    void operator()(const clustering_row& cr) {
        if (cr.tomb()) {
            ++_mt._table_stats.memtable_row_tombstone_reads;
        }
    }
    void operator()(const static_row& sr) {}
    void operator()(const range_tombstone_change& rt) {
        ++_mt._table_stats.memtable_range_tombstone_reads;
    }
    void operator()(const partition_start& ph) {}
    void operator()(const partition_end& eop) {}
};

class scanning_reader final : public mutation_reader::impl, private iterator_reader {
    std::optional<dht::partition_range> _delegate_range;
    mutation_reader_opt _delegate;
    const query::partition_slice& _slice;
    mutation_reader::forwarding _fwd_mr;

    struct consumer {
        scanning_reader* _reader;
        explicit consumer(scanning_reader* r) : _reader(r) {}
        stop_iteration operator()(mutation_fragment_v2 mf) {
            _reader->push_mutation_fragment(std::move(mf));
            return stop_iteration(_reader->is_buffer_full());
        }
    };

    future<> fill_buffer_from_delegate() {
        return _delegate->consume_pausable(consumer(this)).then([this] {
            if (_delegate->is_end_of_stream() && _delegate->is_buffer_empty()) {
                if (_delegate_range) {
                    _end_of_stream = true;
                } else {
                    return close_delegate();
                }
            }
            return make_ready_future<>();
        });
    }

    future<> close_delegate() noexcept {
        return _delegate ? _delegate->close() : make_ready_future<>();
    };

public:
     scanning_reader(schema_ptr s,
                     lw_shared_ptr<memtable> m,
                     reader_permit permit,
                     const dht::partition_range& range,
                     const query::partition_slice& slice,
                     mutation_reader::forwarding fwd_mr)
         : impl(s, std::move(permit))
         , iterator_reader(s, std::move(m), range)
         , _slice(slice)
         , _fwd_mr(fwd_mr)
     { }

    virtual future<> fill_buffer() override {
        return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
            if (!_delegate) {
                _delegate_range = get_delegate_range();
                if (_delegate_range) {
                    _delegate = delegate_reader(_permit, *_delegate_range, _slice, streamed_mutation::forwarding::no, _fwd_mr);
                } else {
                    auto key_and_snp = read_section()(region(), [&] () -> std::optional<std::pair<dht::decorated_key, partition_snapshot_ptr>> {
                        memtable_entry *e = fetch_entry();
                        if (!e) {
                            return { };
                        } else {
                            // FIXME: Introduce a memtable specific reader that will be returned from
                            // memtable_entry::read and will allow filling the buffer without the overhead of
                            // virtual calls, intermediate buffers and futures.
                            auto key = e->key();
                            auto snp = e->snapshot(*mtbl());
                            advance_iterator();
                            return std::pair(std::move(key), std::move(snp));
                        }
                    });
                    if (key_and_snp) {
                        update_last(key_and_snp->first);

                        auto cr = query::clustering_key_filter_ranges::get_ranges(*schema(), _slice, key_and_snp->first.key());
                        bool digest_requested = _slice.options.contains<query::partition_slice::option::with_digest>();
                        bool is_reversed = _slice.is_reversed();
                        _delegate = make_partition_snapshot_flat_reader_from_snp_schema(is_reversed, _permit, std::move(key_and_snp->first), std::move(cr), std::move(key_and_snp->second), digest_requested, region(), read_section(), mtbl(), streamed_mutation::forwarding::no, *mtbl());
                        _delegate->upgrade_schema(schema());
                    } else {
                        _end_of_stream = true;
                    }
                }
            }

            return is_end_of_stream() ? make_ready_future<>() : fill_buffer_from_delegate();
        });
    }
    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (is_buffer_empty()) {
            if (!_delegate_range) {
                return close_delegate();
            } else {
                return _delegate->next_partition();
            }
        }
        return make_ready_future<>();
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        _end_of_stream = false;
        clear_buffer();
        if (_delegate_range) {
            return _delegate->fast_forward_to(pr);
        } else {
          return close_delegate().then([this, &pr] {
            return iterator_reader::fast_forward_to(pr);
          });
        }
    }
    virtual future<> fast_forward_to(position_range cr) override {
        throw std::runtime_error("This reader can't be fast forwarded to another partition.");
    };
    virtual future<> close() noexcept override {
        return close_delegate();
    };
};

void memtable::add_flushed_memory(uint64_t delta) {
    _flushed_memory += delta;
    _dirty_mgr.account_potentially_cleaned_up_memory(this, delta);
}

void memtable::remove_flushed_memory(uint64_t delta) {
    delta = std::min(_flushed_memory, delta);
    _flushed_memory -= delta;
    _dirty_mgr.revert_potentially_cleaned_up_memory(this, delta);
}

void memtable::on_detach_from_region_group() noexcept {
    _merged_into_cache = true;
    revert_flushed_memory();
}

void memtable::revert_flushed_memory() noexcept {
    _dirty_mgr.revert_potentially_cleaned_up_memory(this, _flushed_memory);
    _flushed_memory = 0;
}

class flush_memory_accounter {
    memtable& _mt;
public:
    void update_bytes_read(uint64_t delta) {
        _mt.add_flushed_memory(delta);
    }
    explicit flush_memory_accounter(memtable& mt)
        : _mt(mt)
	{}
    ~flush_memory_accounter() {
        SCYLLA_ASSERT(_mt._flushed_memory <= _mt.occupancy().total_space());
    }
    uint64_t compute_size(memtable_entry& e, partition_snapshot& snp) {
        return e.size_in_allocator_without_rows(_mt.allocator())
            + _mt.allocator().object_memory_size_in_allocator(&*snp.version());
    }
};

class partition_snapshot_flush_accounter {
    const schema& _schema;
    flush_memory_accounter& _accounter;
public:
    partition_snapshot_flush_accounter(const schema& s, flush_memory_accounter& acct)
        : _schema(s), _accounter(acct) {}

    // We will be passed mutation fragments here, and they are allocated using the standard
    // allocator. So we can't compute the size in memtable precisely. However, precise accounting is
    // hard anyway, since we may be holding multiple snapshots of the partitions, and the
    // partition_snapshot_reader may compose them. In doing so, we move memory to the standard
    // allocation. As long as our size read here is lesser or equal to the size in the memtables, we
    // are safe, and worst case we will allow a bit fewer requests in.
    void operator()(const range_tombstone_change& rtc) {
        _accounter.update_bytes_read(rtc.minimal_memory_usage(_schema));
    }

    void operator()(const static_row& sr) {
        _accounter.update_bytes_read(sr.external_memory_usage(_schema));
    }

    void operator()(const partition_start& ph) {}

    void operator()(const partition_end& eop) {}

    void operator()(const clustering_row& cr) {
        // Every clustering row is stored in a rows_entry object, and that has some significant
        // overhead - so add it here. We will be a bit short on our estimate because we can't know
        // what is the size in the allocator for this rows_entry object: we may have many snapshots,
        // and we don't know which one(s) contributed to the generation of this mutation fragment.
        //
        // We will add the size of the struct here, and that should be good enough.
        _accounter.update_bytes_read(sizeof(rows_entry) + cr.minimal_external_memory_usage(_schema));
    }
};

static mutation_reader make_partition_snapshot_flat_reader_from_snp_schema(
        bool is_reversed,
        reader_permit permit,
        dht::decorated_key dk,
        query::clustering_key_filter_ranges crr,
        partition_snapshot_ptr snp,
        bool digest_requested,
        logalloc::region& region,
        logalloc::allocating_section& read_section,
        std::any pointer_to_container,
        streamed_mutation::forwarding fwd, memtable& memtable) {
    if (is_reversed) {
        schema_ptr rev_snp_schema = snp->schema()->make_reversed();
        return make_partition_snapshot_flat_reader<true, partition_snapshot_read_accounter>(std::move(rev_snp_schema), std::move(permit), std::move(dk), std::move(crr), std::move(snp), digest_requested, region, read_section, pointer_to_container, fwd, memtable);
    } else {
        schema_ptr snp_schema = snp->schema();
        return make_partition_snapshot_flat_reader<false, partition_snapshot_read_accounter>(std::move(snp_schema), std::move(permit), std::move(dk), std::move(crr), std::move(snp), digest_requested, region, read_section, pointer_to_container, fwd, memtable);
    }
}

class flush_reader final : public mutation_reader::impl, private iterator_reader {
    // FIXME: Similarly to scanning_reader we have an underlying
    // mutation_reader for each partition. This is suboptimal.
    // Partition snapshot reader should be devirtualised and called directly
    // without using any intermediate buffers.
    mutation_reader_opt _partition_reader;
    flush_memory_accounter _flushed_memory;
public:
    flush_reader(schema_ptr s, reader_permit permit, lw_shared_ptr<memtable> m)
        : impl(s, std::move(permit))
        , iterator_reader(std::move(s), m, query::full_partition_range)
        , _flushed_memory(*m)
    {}
    flush_reader(const flush_reader&) = delete;
    flush_reader(flush_reader&&) = delete;
    flush_reader& operator=(flush_reader&&) = delete;
    flush_reader& operator=(const flush_reader&) = delete;
private:
    void get_next_partition() {
        uint64_t component_size = 0;
        auto key_and_snp = read_section()(region(), [&] () -> std::optional<std::pair<dht::decorated_key, partition_snapshot_ptr>> {
            memtable_entry* e = fetch_entry();
            if (e) {
                auto dk = e->key();
                auto snp = e->snapshot(*mtbl());
                component_size = _flushed_memory.compute_size(*e, *snp);
                advance_iterator();
                return std::pair(std::move(dk), std::move(snp));
            }
            return { };
        });
        if (key_and_snp) {
            _flushed_memory.update_bytes_read(component_size);
            update_last(key_and_snp->first);
            auto cr = query::clustering_key_filter_ranges::get_ranges(*schema(), schema()->full_slice(), key_and_snp->first.key());
            auto snp_schema = key_and_snp->second->schema();
            _partition_reader = make_partition_snapshot_flat_reader<false, partition_snapshot_flush_accounter>(snp_schema, _permit, std::move(key_and_snp->first), std::move(cr),
                            std::move(key_and_snp->second), false, region(), read_section(), mtbl(), streamed_mutation::forwarding::no, *snp_schema, _flushed_memory);
            _partition_reader->upgrade_schema(schema());
        }
    }
    future<> close_partition_reader() noexcept {
        return _partition_reader ? _partition_reader->close() : make_ready_future<>();
    }
public:
    virtual future<> fill_buffer() override {
        return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
            if (!_partition_reader) {
                get_next_partition();
                if (!_partition_reader) {
                    _end_of_stream = true;
                    return make_ready_future<>();
                }
            }
            return _partition_reader->consume_pausable([this] (mutation_fragment_v2 mf) {
                push_mutation_fragment(std::move(mf));
                return stop_iteration(is_buffer_full());
            }).then([this] {
                if (_partition_reader->is_end_of_stream() && _partition_reader->is_buffer_empty()) {
                    return _partition_reader->close();
                }
                return make_ready_future<>();
            });
        });
    }
    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (is_buffer_empty()) {
            return close_partition_reader();
        }
        return make_ready_future<>();
    }
    virtual future<> fast_forward_to(const dht::partition_range&) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> fast_forward_to(position_range) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> close() noexcept override {
        return close_partition_reader();
    }
};

partition_snapshot_ptr memtable_entry::snapshot(memtable& mtbl) {
    return _pe.read(mtbl.region(), mtbl.cleaner(), no_cache_tracker);
}

mutation_reader_opt
memtable::make_flat_reader_opt(schema_ptr query_schema,
                      reader_permit permit,
                      const dht::partition_range& range,
                      const query::partition_slice& slice,
                      tracing::trace_state_ptr trace_state_ptr,
                      streamed_mutation::forwarding fwd,
                      mutation_reader::forwarding fwd_mr) {
    bool is_reversed = slice.is_reversed();
    if (query::is_single_partition(range) && !fwd_mr) {
        const query::ring_position& pos = range.start()->value();
        auto snp = _table_shared_data.read_section(*this, [&] () -> partition_snapshot_ptr {
            auto i = partitions.find(pos, dht::ring_position_comparator(*_schema));
            if (i != partitions.end()) {
                upgrade_entry(*i);
                return i->snapshot(*this);
            } else {
                return { };
            }
        });
        if (!snp) {
            return {};
        }
        auto dk = pos.as_decorated_key();
        auto cr = query::clustering_key_filter_ranges::get_ranges(*query_schema, slice, dk.key());
        bool digest_requested = slice.options.contains<query::partition_slice::option::with_digest>();
        auto rd = make_partition_snapshot_flat_reader_from_snp_schema(is_reversed, std::move(permit), std::move(dk), std::move(cr), std::move(snp), digest_requested, *this, _table_shared_data.read_section, shared_from_this(), fwd, *this);
        rd.upgrade_schema(query_schema);
        return rd;
    } else {
        auto res = make_mutation_reader<scanning_reader>(std::move(query_schema), shared_from_this(), std::move(permit), range, slice, fwd_mr);
        if (fwd == streamed_mutation::forwarding::yes) {
            return make_forwardable(std::move(res));
        } else {
            return res;
        }
    }
}

mutation_reader
memtable::make_flush_reader(schema_ptr s, reader_permit permit) {
    if (!_merged_into_cache) {
        return make_mutation_reader<flush_reader>(std::move(s), std::move(permit), shared_from_this());
    } else {
        auto& full_slice = s->full_slice();
        return make_mutation_reader<scanning_reader>(std::move(s), shared_from_this(), std::move(permit),
                      query::full_partition_range, full_slice, mutation_reader::forwarding::no);
    }
}

void
memtable::update(db::rp_handle&& h) {
    db::replay_position rp = h;
    if (_replay_position < rp) {
        _replay_position = rp;
    }
    _rp_set.put(std::move(h));
}

future<>
memtable::apply(memtable& mt, reader_permit permit) {
    if (auto reader_opt = mt.make_flat_reader_opt(_schema, std::move(permit), query::full_partition_range, _schema->full_slice())) {
        return with_closeable(std::move(*reader_opt), [this] (auto&& rd) mutable {
            return consume_partitions(rd, [self = this->shared_from_this()] (mutation&& m) {
                self->apply(m);
                return stop_iteration::no;
            });
        });
    }
    [[unlikely]] return make_ready_future<>();
}

void
memtable::apply(const mutation& m, db::rp_handle&& h) {
    with_allocator(allocator(), [this, &m] {
        _table_shared_data.allocating_section(*this, [&, this] {
            auto& p = find_or_create_partition(m.decorated_key());
            _stats_collector.update(*m.schema(), m.partition());
            p.apply(region(), cleaner(), *_schema, m.partition(), *m.schema(), _table_stats.memtable_app_stats);
        });
    });
    update(std::move(h));
}

void
memtable::apply(const frozen_mutation& m, const schema_ptr& m_schema, db::rp_handle&& h) {
    with_allocator(allocator(), [this, &m, &m_schema] {
        _table_shared_data.allocating_section(*this, [&, this] {
            auto& p = find_or_create_partition_slow(m.key());
            mutation_partition mp(*m_schema);
            partition_builder pb(*m_schema, mp);
            m.partition().accept(*m_schema, pb);
            _stats_collector.update(*m_schema, mp);
            p.apply(region(), cleaner(), *_schema, std::move(mp), *m_schema, _table_stats.memtable_app_stats);
        });
    });
    update(std::move(h));
}

logalloc::occupancy_stats memtable::occupancy() const noexcept {
    return logalloc::region::occupancy();
}

mutation_source memtable::as_data_source() {
    return mutation_source([mt = shared_from_this()] (schema_ptr s,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding fwd_mr) {
        return mt->make_flat_reader(std::move(s), std::move(permit), range, slice, std::move(trace_state), fwd, fwd_mr);
    });
}

memtable_entry::memtable_entry(memtable_entry&& o) noexcept
    : _key(std::move(o._key))
    , _pe(std::move(o._pe))
    , _flags(o._flags)
{ }

stop_iteration memtable_entry::clear_gently() noexcept {
    return _pe.clear_gently(no_cache_tracker);
}

void memtable::mark_flushed(mutation_source underlying) noexcept {
    _underlying = std::move(underlying);
}

bool memtable::is_flushed() const noexcept {
    return bool(_underlying);
}

void memtable_entry::upgrade_schema(logalloc::region& r, const schema_ptr& s, mutation_cleaner& cleaner) {
    if (schema() != s) {
        partition().upgrade(r, s, cleaner, no_cache_tracker);
    }
}

void memtable::upgrade_entry(memtable_entry& e) {
    if (e.schema() != _schema) {
        SCYLLA_ASSERT(!reclaiming_enabled());
        e.upgrade_schema(region(), _schema, cleaner());
    }
}

void memtable::set_schema(schema_ptr new_schema) noexcept {
    _schema = std::move(new_schema);
}

size_t memtable_entry::object_memory_size(allocation_strategy& allocator) {
    return memtable::partitions_type::estimated_object_memory_size_in_allocator(allocator, this);
}

}

auto fmt::formatter<replica::memtable_entry>::format(const replica::memtable_entry& mt,
                                                     fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{{{}: {}}}", mt.key(), partition_entry::printer(mt.partition()));
}

auto fmt::formatter<replica::memtable>::format(replica::memtable& mt,
                                        fmt::format_context& ctx) const -> decltype(ctx.out()) {
    logalloc::reclaim_lock rl(mt);
    return fmt::format_to(ctx.out(), "{{memtable: [{}]}}", fmt::join(mt.partitions, ",\n"));
}
