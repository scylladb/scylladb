/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// This file is part of the object storage page cache (the 'disk_cache': 'pages'
// keyspace storage option): the schema of the system_cache.pagecache table,
// which stores the cache, and the implementation of the cache's interface over
// it. See db/object_storage_page_cache_table.hh.

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>
#include "db/object_storage_page_cache_table.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "dht/fixed_shard.hh"
#include "mutation/frozen_mutation.hh"
#include "mutation/mutation.hh"
#include "mutation/mutation_fragment_v2.hh"
#include "partition_slice_builder.hh"
#include "readers/mutation_reader.hh"
#include "replica/database.hh"
#include "schema/caching_options.hh"
#include "schema/schema_builder.hh"
#include "service/storage_proxy.hh"
#include "sstables/compressor.hh"
#include "tombstone_gc_options.hh"
#include "types/types.hh"
#include "utils/fragment_range.hh"

namespace db {

// The object storage page cache: byte ranges ("pages") of sstable components,
// which were read from the object store.
//
// The table is in the system_cache keyspace, not in the system keyspace:
// the cache may fill most of the node's disk, while the system keyspace
// holds small metadata tables. Moreover, the system_cache keyspace has
// durable_writes = false, so writes to this table skip the commitlog. A
// crash may then lose the most recently cached pages, which is fine: they
// are only copies of data that the object store holds.
schema_ptr object_storage_page_cache_table::schema() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(SYSTEM_CACHE, PAGECACHE);
        return schema_builder(this_smp_shard_count(), SYSTEM_CACHE, PAGECACHE, id)
            // The shard that owns the sstable. The fixed_shard_partitioner
            // (below) stores the partition on that shard, so a shard reads
            // and writes the cache of its sstables without crossing shards.
            .with_column("shard", short_type, column_kind::partition_key)
            // The sstable's sstable_id (sstable::sstable_identifier()), which
            // is also in the names of its objects on the object store.
            .with_column("sstable", uuid_type, column_kind::partition_key)
            // Every lookup specifies the component, but it is a clustering
            // column, so all the pages of an sstable are in one partition,
            // and dropping the cache of an sstable takes a single partition
            // tombstone.
            .with_column("component", utf8_type, column_kind::clustering_key)
            // The pages of each component are sorted by offset, so reading a
            // range of consecutive pages is a sequential scan.
            .with_column("offset", long_type, column_kind::clustering_key)
            .with_column("page", bytes_type)
            .set_comment("The object storage page cache: pages of sstables on object storage, cached on the local disk")
            .with_hash_version()
            .with_partitioner(dht::fixed_shard_partitioner::classname)
            .with_sharder(dht::fixed_shard_sharder::instance())
            // A page read from this table is cached in memory anyway, by the
            // user table's caches (its row cache for Data pages, its index
            // cache for index pages), so the row cache would only hold a
            // second copy. Unlike BYPASS CACHE, disabling the row cache in the
            // schema keeps this table's own index cached.
            .set_caching_options(caching_options::get_disabled_caching_options())
            // Most pages are already compressed - the compressed chunks of
            // a Data component - or encrypted, so compressing them again
            // would cost CPU on every write, read and compaction, and save
            // almost nothing.
            .set_compressor_params(compression_parameters::no_compression())
            // The table is local, so it has no other replicas which a
            // tombstone needs to reach before it may be dropped.
            .with_tombstone_gc_options(tombstone_gc_options(tombstone_gc_mode::immediate))
            .build();
    }();
    return schema;
}

// The cache of each shard is in the partitions whose "shard" column is
// that shard, which the fixed_shard_partitioner stores on that shard. So
// the queries below, which always name the calling shard, never leave it.
// The shard column is a smallint, which the fixed_shard_partitioner
// requires, so the shard number is passed as an int16_t.
static int16_t this_shard() {
    return int16_t(this_shard_id());
}

// The key of the table: ((shard, sstable), component, offset).
static partition_key cache_partition_key(const schema& s, int16_t shard, utils::UUID sstable) {
    return partition_key::from_exploded(s, {data_value(shard).serialize_nonnull(), data_value(sstable).serialize_nonnull()});
}

static clustering_key cache_clustering_key(const schema& s, const sstring& component, int64_t offset) {
    return clustering_key::from_exploded(s, {data_value(component).serialize_nonnull(), data_value(offset).serialize_nonnull()});
}

api::timestamp_type object_storage_page_cache_table::next_timestamp() noexcept {
    static thread_local api::timestamp_type last = api::min_timestamp;
    last = std::max(api::new_timestamp(), last + 1);
    return last;
}

// A mutation which writes a cached page, like an INSERT of it.
static mutation page_mutation(int16_t shard, utils::UUID sstable, const sstring& component, int64_t offset, bytes page) {
    auto s = object_storage_page_cache_table::schema();
    auto ts = object_storage_page_cache_table::next_timestamp();
    auto ck = cache_clustering_key(*s, component, offset);
    mutation m(s, cache_partition_key(*s, shard, sstable));
    m.partition().clustered_row(*s, ck).apply(row_marker(ts));
    m.set_clustered_cell(ck, "page", data_value(std::move(page)), ts);
    return m;
}

// A mutation which deletes all the rows of an sstable in the given table.
static mutation sstable_deletion(schema_ptr s, int16_t shard, utils::UUID sstable) {
    mutation m(s, cache_partition_key(*s, shard, sstable));
    m.partition().apply(tombstone(object_storage_page_cache_table::next_timestamp(), gc_clock::now()));
    return m;
}

// The cache writes its table by applying mutations to it directly, not
// with internal CQL statements: a statement costs much more than the write
// itself - its execution, and the coordinator's write path - and the cache
// writes a page for each page which a read misses. The mutations are of
// this shard's partitions, so they are applied on this shard.
future<> object_storage_page_cache_table::apply(mutation m) {
    auto s = m.schema();
    co_await _db.apply(std::move(s), freeze(m), {}, db::commitlog_force_sync::no, db::no_timeout);
}

future<> object_storage_page_cache_table::put_page(sstables::sstable_id sst, sstables::component_type component,
        uint64_t offset, temporary_buffer<char> data) {
    co_await apply(page_mutation(this_shard(), sst.uuid(), fmt::to_string(component), int64_t(offset),
            bytes(reinterpret_cast<const int8_t*>(data.get()), data.size())));
}

// Reads the pages of a range of an sstable's component from the cache's
// table, in offset order, with a mutation reader of the table - which the
// reader keeps open between pages, so reading consecutive pages looks the
// range up in the table's sstables only once.
//
// The reader reads the table directly, not with an internal CQL query. The
// sstable read which looks up its pages holds a permit of the reader
// concurrency semaphore of its scheduling group, and a CQL query would wait
// for admission on the same semaphore, behind other reads - which also hold
// their permits while waiting for their own lookups. So with more concurrent
// reads than the semaphore admits, all of them would wait until they time
// out. Instead, the reader has a tracking-only permit, which isn't admitted:
// it is part of a read which was already admitted.
//
// But skipping admission isn't enough: while a read runs, it requests memory
// from its permit's semaphore (through tracked files), which, when its reads
// use too much memory, lets only one of them proceed, and makes the others
// wait. If the read which proceeds were the one waiting for this reader, a
// permit of the same semaphore would wait behind it, and so would all the
// reads. So the permit is of a semaphore without limits, which never makes a
// read wait - for admission, or for memory
// (database::object_storage_page_cache_semaphore()). The reader's memory,
// which its permit tracks, is bounded anyway, by the size of its buffers.
//
// The table's mutation reader returns the rows as they are stored, with the
// tombstones which delete them - evict_sstable() evicts all of an sstable's
// pages with a partition tombstone - so the reader skips the pages which a
// tombstone deletes, as a query would. A page which is cached again after it was
// evicted is written with a later timestamp, so the tombstone doesn't delete
// it.
namespace {

class cached_pages_reader {
    replica::table& _table;
    schema_ptr _schema;
    const column_definition& _page_column;
    // The ranges which the mutation reader reads, which must live as long as
    // it does.
    dht::partition_range _range;
    query::partition_slice _slice;
    mutation_reader _reader;
    tombstone _partition_tombstone;
    tombstone _range_tombstone;
public:
    cached_pages_reader(replica::database& db, sstables::sstable_id sst, const sstring& component_name,
            uint64_t from, uint64_t to, const char* op_name)
        : _table(db.find_column_family(object_storage_page_cache_table::SYSTEM_CACHE, object_storage_page_cache_table::PAGECACHE))
        , _schema(_table.schema())
        , _page_column(*_schema->get_column_definition("page"))
        , _range(dht::partition_range::make_singular(dht::decorate_key(*_schema, cache_partition_key(*_schema, this_shard(), sst.uuid()))))
        , _slice(partition_slice_builder(*_schema).with_range(query::clustering_range::make(
                {cache_clustering_key(*_schema, component_name, int64_t(from)), true},
                {cache_clustering_key(*_schema, component_name, int64_t(to)), false})).build())
        , _reader(_table.make_mutation_reader(_schema,
                db.object_storage_page_cache_semaphore().make_tracking_only_permit(_schema, op_name, db::no_timeout, {}),
                _range, _slice, nullptr, streamed_mutation::forwarding::no, mutation_reader::forwarding::no))
    {}
    // The mutation reader refers to _range and _slice.
    cached_pages_reader(cached_pages_reader&&) = delete;

    // Returns the next page which no tombstone deletes, or nothing after the
    // last one.
    future<std::optional<sstables::object_storage_page_cache::page>> next() {
        while (auto mf = co_await _reader()) {
            if (mf->is_partition_start()) {
                _partition_tombstone = mf->as_partition_start().partition_tombstone();
            } else if (mf->is_range_tombstone_change()) {
                _range_tombstone = mf->as_range_tombstone_change().tombstone();
            } else if (mf->is_clustering_row()) {
                if (auto page = live_page(mf->as_clustering_row())) {
                    co_return page;
                }
            }
        }
        co_return std::nullopt;
    }

    future<> close() noexcept {
        return _reader.close();
    }
private:
    std::optional<sstables::object_storage_page_cache::page> live_page(const clustering_row& row) const {
        auto cell = row.cells().find_cell(_page_column.id);
        if (!cell) {
            return std::nullopt;
        }
        auto t = std::max({_partition_tombstone, _range_tombstone, row.tomb().tomb()});
        auto page = cell->as_atomic_cell(_page_column);
        if (!page.is_live(t, false)) {
            return std::nullopt;
        }
        auto value = page.value();
        temporary_buffer<char> data(value.size_bytes());
        auto out = data.get_write();
        for (bytes_view fragment : fragment_range(value)) {
            out = std::copy_n(reinterpret_cast<const char*>(fragment.data()), fragment.size(), out);
        }
        return sstables::object_storage_page_cache::page{
            .offset = uint64_t(value_cast<int64_t>(long_type->deserialize_value(row.key().get_component(*_schema, 1)))),
            .data = std::move(data),
        };
    }
};

} // anonymous namespace

// A cursor of read_pages(), which reads the pages with a cached_pages_reader.
class object_storage_page_cache_table::page_cursor_impl : public sstables::object_storage_page_cache::page_cursor {
    // The longest gap before the page which the cursor is asked for that it
    // reads through: reading through a gap reads the cached pages in it from
    // the table, while a new lookup reads only the indexes of the table's
    // sstables, so a longer gap is cheaper to look up anew. The limit is the
    // size of an sstable reader's buffer (default_sstable_buffer_size): the
    // pages of a shorter gap may already be in the buffer.
    static constexpr uint64_t max_read_through_gap = 128 * 1024;

    object_storage_page_cache_table& _cache;
    sstables::sstable_id _sst;
    sstring _component_name;
    uint64_t _from;
    uint64_t _to;
    std::unique_ptr<cached_pages_reader> _reader;
    // The offset after which _reader returns the next page: the start of its
    // range, or the offset of the last page which it returned.
    uint64_t _reader_pos = 0;
    // Whether the reader returned all the pages up to the end of the range.
    bool _exhausted = false;
    // The page which the reader returned after the offset that get_page()
    // was asked for, which a later get_page() may ask for.
    std::optional<page> _next;
public:
    page_cursor_impl(object_storage_page_cache_table& cache, sstables::sstable_id sst, sstables::component_type component,
            uint64_t from, uint64_t to)
        : _cache(cache)
        , _sst(sst)
        , _component_name(fmt::to_string(component))
        , _from(from)
        , _to(to)
        , _reader_pos(from)
    {}

    virtual future<temporary_buffer<char>> get_page(uint64_t offset) override {
        if (offset < _from || offset >= _to) {
            co_return temporary_buffer<char>();
        }
        if (!_next || _next->offset < offset) {
            _next.reset();
            if (!_exhausted) {
                if (!_reader || (offset > _reader_pos && offset - _reader_pos > max_read_through_gap)) {
                    co_await close_reader();
                    _reader = std::make_unique<cached_pages_reader>(_cache._db, _sst, _component_name,
                            offset, _to, "object_storage_page_cache_read_pages");
                    _reader_pos = offset;
                }
                while (auto p = co_await _reader->next()) {
                    _reader_pos = p->offset;
                    if (p->offset >= offset) {
                        _next = std::move(p);
                        break;
                    }
                }
                if (!_next) {
                    // No page is left in the range, so the reader isn't needed.
                    _exhausted = true;
                    co_await close_reader();
                }
            }
        }
        if (!_next || _next->offset != offset) {
            co_return temporary_buffer<char>();
        }
        auto data = std::move(_next->data);
        _next.reset();
        co_return data;
    }

    virtual future<> close() noexcept override {
        return close_reader();
    }
private:
    future<> close_reader() noexcept {
        if (_reader) {
            co_await _reader->close();
            _reader.reset();
        }
    }
};

std::unique_ptr<sstables::object_storage_page_cache::page_cursor>
object_storage_page_cache_table::read_pages(sstables::sstable_id sst, sstables::component_type component,
        uint64_t from, uint64_t to) {
    return std::make_unique<page_cursor_impl>(*this, sst, component, from, to);
}

future<std::vector<sstables::object_storage_page_cache::page>>
object_storage_page_cache_table::get_pages(sstables::sstable_id sst, sstables::component_type component,
        uint64_t from, uint64_t to, size_t max_pages) {
    auto component_name = fmt::to_string(component);
    cached_pages_reader reader(_db, sst, component_name, from, to, "object_storage_page_cache_get_pages");
    std::vector<page> pages;
    std::exception_ptr ex;
    try {
        while (pages.size() < max_pages) {
            auto p = co_await reader.next();
            if (!p) {
                break;
            }
            pages.push_back(std::move(*p));
        }
    } catch (...) {
        ex = std::current_exception();
    }
    co_await reader.close();
    if (ex) {
        std::rethrow_exception(ex);
    }
    co_return pages;
}

future<> object_storage_page_cache_table::evict_sstable(sstables::sstable_id sst) {
    co_await apply(sstable_deletion(schema(), this_shard(), sst.uuid()));
}

} // namespace db
