/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/memory.hh>
#include "sstables/parquet/reader.hh"
#include "sstables/parquet/encryption_keys.hh"

#include <array>
#include <seastar/core/when_all.hh>
#include <chrono>
#include <cstdlib>
#include "sstables/parquet/writer_impl.hh"
#include "sstables/parquet/schema_mapping.hh"
#include "sstables/parquet/format/parquet_reader.hh"
#include "sstables/sstables.hh"
#include "sstables/index_reader.hh"
#include "sstables/mutation_fragment_filter.hh"
#include "readers/forwardable.hh"
#include "mutation/collection_mutation.hh"
#include "mutation/counters.hh"
#include "types/collection.hh"
#include "mutation/mutation_fragment.hh"
#include "mutation/mutation.hh"
#include "keys/keys.hh"
#include "schema/schema.hh"
#include "types/types.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>

#include <bit>
#include <cstring>

namespace sstables::parquet {

static seastar::logger pqlog("pq_reader");

namespace {

// How many rows are decoded at a time. This, plus one row group's compressed
// bytes, is the reader's memory footprint -- neither grows with the sstable,
// which is what R-13 asks for.
constexpr int64_t scan_window_rows = 16384;
// A point read usually wants one partition, so it starts small: the cost of
// looping is a page header walk, while the cost of over-decoding is real work.
//
// This is *not* the window a bounded range gets -- that was the defect §10.26 named. It is the
// window a read gets when fetching pages is measurably cheaper than fetching the row group, which
// next_window() decides per row group from the OffsetIndex.
constexpr int64_t point_window_rows = 512;

// Inverse of the `decode` in writer_impl.cc. Scylla serialises fixed-width
// scalars big-endian, so these rebuild exactly the bytes the shredder took
// apart -- anything that arrived as an opaque BYTE_ARRAY goes back verbatim.
bytes encode(cql_type t, const value& v) {
    auto be = [] (uint64_t x, size_t n) {
        bytes b(bytes::initialized_later(), n);
        for (size_t i = 0; i < n; ++i) { b[i] = int8_t(uint8_t(x >> (8 * (n - 1 - i)))); }
        return b;
    };
    switch (t) {
    case cql_type::int32:
        return be(uint32_t(std::get<int32_t>(v)), 4);
    case cql_type::bigint:
    case cql_type::timestamp:
        return be(uint64_t(std::get<int64_t>(v)), 8);
    case cql_type::dbl:
        return be(std::bit_cast<uint64_t>(std::get<double>(v)), 8);
    default: {
        const auto& s = std::get<std::string>(v);
        return bytes(reinterpret_cast<const int8_t*>(s.data()), s.size());
    }
    }
}

// The inverse of read_counter_cell(): rebuild a counter cell from the elements
// standing in for its shards. An element-less collection carrying a tombstone is
// a dead counter cell; anything else is live, and every element's timestamp is
// the cell's own so the first one serves.
//
// Shards go back through add_maybe_unsorted_shard() rather than add_shard():
// counter_cell_view requires them sorted by id, and while our writer emits them
// in the order the source cell held them -- already sorted -- relying on that
// would make the reader depend on an invariant it does not enforce.
static atomic_cell_or_collection build_counter(const collection_cell& cc) {
    if (cc.elements.empty()) {
        const auto ts = cc.tomb ? cc.tomb->timestamp : api::missing_timestamp;
        const auto ldt = gc_clock::time_point(
                gc_clock::duration(cc.tomb ? cc.tomb->local_deletion_time : 0));
        return atomic_cell_or_collection(atomic_cell::make_dead(ts, ldt));
    }
    counter_cell_builder b(cc.elements.size());
    for (const auto& e : cc.elements) {
        int64_t msb = 0, lsb = 0, value = 0, clock = 0;
        if (!unpack_i64_pair(e.key, msb, lsb) ||
            !e.value || !unpack_i64_pair(*e.value, value, clock)) {
            throw std::runtime_error("pq: malformed counter shard");
        }
        b.add_maybe_unsorted_shard(counter_shard(
                counter_id(utils::UUID(msb, lsb)), value, clock));
    }
    b.sort_and_remove_duplicates();
    return atomic_cell_or_collection(b.build(cc.elements.front().timestamp));
}

// Rebuild a collection cell from the mapping's element list. Keys and values are
// the serialised forms the writer took apart, so they go back verbatim; the
// element's own value type is used for the atomic cell so that a fixed-width
// element is stored the way the rest of Scylla stores it.
atomic_cell_or_collection build_collection(const column_definition& cdef,
                                           const collection_cell& cc) {
    // Not every multi-cell column is a collection. A non-frozen UDT is multi-cell -- columns_of()
    // marks it so as `!is_atomic() || is_counter()` -- and is shredded and reassembled by this same
    // path, with the field index standing in for the element key. But `user_type_impl` derives from
    // `tuple_type_impl`, not from `collection_type_impl`, so the reference dynamic_cast this line
    // used to be threw `std::bad_cast` on one: every read of a pq sstable holding a non-frozen UDT
    // column failed outright.
    //
    // The type is wanted only for atomic_cell::make_live() below, which ignores it entirely
    // (mutation/atomic_cell.cc:18 -- the parameter is unnamed and unused), so there is nothing to
    // reconstruct for the UDT case and no need for a per-field lookup: the fix is to stop demanding
    // a cast that a correct caller can fail. It stays the collection's value_comparator() where
    // there is one, so the call below reads the same as it does everywhere else in the tree.
    const auto* ctype = dynamic_cast<const collection_type_impl*>(cdef.type.get());
    auto vtype = ctype ? ctype->value_comparator() : cdef.type;

    tombstone t;
    if (cc.tomb) {
        t = tombstone(cc.tomb->timestamp,
                      gc_clock::time_point(gc_clock::duration(cc.tomb->local_deletion_time)));
    }
    collection_mutation_writer w(t);
    for (const auto& e : cc.elements) {
        auto key = managed_bytes(reinterpret_cast<const int8_t*>(e.key.data()), e.key.size());
        if (!e.value) {
            auto ldt = gc_clock::time_point(
                    gc_clock::duration(e.local_deletion_time.value_or(0)));
            w.push_back(managed_bytes_view(key), atomic_cell::make_dead(e.timestamp, ldt));
            continue;
        }
        auto vb = bytes(reinterpret_cast<const int8_t*>(e.value->data()), e.value->size());
        if (e.ttl && e.local_deletion_time) {
            w.push_back(managed_bytes_view(key),
                    atomic_cell::make_live(*vtype, e.timestamp, bytes_view(vb),
                            gc_clock::time_point(gc_clock::duration(*e.local_deletion_time)),
                            gc_clock::duration(*e.ttl)));
        } else {
            w.push_back(managed_bytes_view(key),
                    atomic_cell::make_live(*vtype, e.timestamp, bytes_view(vb)));
        }
    }
    return atomic_cell_or_collection(std::move(w).finish());
}

// ------------------------------------------------------------------ footer cache
// Bytes a std::string holds on the heap. libstdc++ keeps up to 15 characters inline, so a string
// at or below that costs nothing beyond the object itself -- which the enclosing vector's
// capacity has already accounted for.
size_t heap_size(const std::string& s) {
    return s.capacity() > 15 ? s.capacity() + 1 : 0;
}

template <typename T>
size_t heap_size_of_pod_vector(const std::vector<T>& v) {
    return v.capacity() * sizeof(T);
}

} // namespace

// The retained form of one sstable's footer: the plaintext Thrift blob, the lazily-parsed
// metadata whose row-group extents index into it, and the two derived tables every read needs.
//
// Immutable once published. In particular materialise_row_group() is never called on `md`: a
// reader materialises into its own single-group copy instead (pq_reader::need_columns). Two
// reasons. It keeps the entry a fixed size -- otherwise a full scan would grow it into the eager
// parse the lazy mode exists to avoid, tens of megabytes on a large sstable. And it keeps
// concurrent readers of the same sstable off each other's state.
class cached_footer final : public cached_footer_base {
public:
    std::vector<uint8_t>  footer;           // plaintext, even when the file on disk is encrypted
    format::file_metadata md;               // metadata_mode::lazy: no per-column metadata
    std::vector<int64_t>  rg_start;         // cumulative first row of each row group
    int64_t               total_rows = 0;

    // The encryption envelope, minus the key -- deliberately. This entry outlives the read that
    // created it, and a key that outlives its read is a key the provider no longer controls. A
    // reader on a cache hit asks the provider again; what it saves is the footer I/O, the
    // decrypt and the parse, not the key lookup.
    bool                  encrypted = false;
    format::cipher        algo = format::cipher::aes_gcm_v1;
    std::string           aad_file_unique;
    std::string           aad_prefix;
    seastar::sstring      key_id;
    // For a file with per-column keys: the leaf name of each column the file says has its own key,
    // and the provider id that column's key_metadata carried. Harvested once, from row group 0,
    // because the writer sets the column keys for the whole file -- so every group repeats the same
    // ids, and a file that somehow did not would fail authentication rather than misread.
    //
    // Ids, not keys: the same reason `key_id` above is here and the footer key is not. An id is not
    // a capability, so caching it hands out nothing; a key would outlive the read that fetched it
    // and stop being the provider's to revoke. A reader on a cache hit still asks the provider for
    // every one of these, which is what §11.1 B3 is about -- see read_crypto_for().
    std::map<std::string, seastar::sstring> column_key_ids;

    // Row group -> the parsed OffsetIndex of each leaf, filled in on first use of that group.
    //
    // The point of caching this is *not* the Thrift parse, which is 3.2 us. It is the read: the
    // page index sits near the end of the file, so fetching it is one device round trip that has
    // to complete before any data page can be located -- a point read's I/O depth is 2 with a
    // miss and 1 with a hit, and that first read measured 264 us against 107 us for the nine
    // parallel page fetches that follow it. Data files are opened O_DIRECT, so there is no page
    // cache underneath to make the repeat free.
    //
    // Lazy, not eager: `md` is parsed in lazy mode and the offset_index offsets live in the
    // per-column metadata, so filling every group up front would mean materialising every row
    // group -- the eager parse the lazy mode exists to avoid, and 16 us per group on a file with
    // thousands of them.
    //
    // Mutable behind a shared_ptr<const>, which is safe here and nowhere near as loose as it
    // looks: entries are only ever *added*, an entry's content is a pure function of immutable
    // file bytes, the insert itself is synchronous (the fetch happens on the reader's stack and
    // publishes only after its co_await has returned), and a reader that races another to the
    // same group produces a value equal to what it overwrites. Readers hold the entry by
    // shared_ptr, so growth never invalidates a view someone is using.
    mutable std::unordered_map<size_t, std::vector<std::optional<format::offset_index>>> oi;
    // Counted separately because it is added after publication; see sstable::grow_pq_footer_cache.
    mutable size_t oi_retained = 0;

    // Decompressed data pages, by absolute file offset of their value bytes. See
    // format::page_cache for why this is where a point read's time actually goes.
    //
    // unordered_map because references to its elements survive a rehash, which is what lets the
    // decode hold a pointer into it while another column inserts.
    //
    // Full means "stop accepting", not "evict something". A page is only worth keeping while the
    // read pattern keeps coming back to it, and the entry as a whole is already subject to the
    // manager's reclaim -- so the useful policy at this level is a cap, and the useful policy for
    // pressure is the one that already exists. An LRU here would be a second eviction policy
    // competing with the first, which is what 10.22 decided against.
    mutable std::unordered_map<int64_t, std::vector<uint8_t>> pages;
    mutable size_t pages_retained = 0;

    // Releases this entry's share of the shard-wide read-cache budget. Clamped rather than
    // asserted: an accounting slip should cost a suboptimal caching decision, not a counter that
    // has wrapped and reads as exabytes held.
    void on_dropped() const noexcept override {
        auto& b = read_cache_bytes_local();
        b.pages -= std::min<uint64_t>(b.pages, pages_retained);
        b.extents -= std::min<uint64_t>(b.extents, extents_retained);
    }

    // The *compressed* extents a paged read fetches: one per column for the dictionary, one for
    // the run of wanted data pages. Cached alongside the decompressed values rather than instead
    // of them, because the two remove different costs -- this one removes the read, the one above
    // removes the codec -- and after the codec cost went away the read is what is left:
    // `page_fetch` is 73 % of a point read once pages are cached (§10.41).
    //
    // Holding both forms costs roughly 1.3x the decompressed size. That is the deliberate trade:
    // serving a page entirely from its decoded form would mean a second decode path that bypasses
    // the page walk, duplicating the most delicate thirty lines in the reader. Caching the bytes
    // the walk reads leaves the walk untouched.
    //
    // Keyed by offset *and* length: the wanted run of pages depends on which rows a read wants, so
    // the same offset can be asked for with different lengths. At shipping defaults the run is one
    // page and the length is stable, but a smaller page_rows makes it vary.
    // Plain bytes, deliberately, and not the temporary_buffer that data_read() returned.
    //
    // Retaining that buffer keeps whatever it is backed by alive, and an sstable then never
    // finishes closing: the test body of test_pq_compaction_aborted_while_reading_does_not_fault
    // completed in 14 s and the process then sat in teardown until it was killed. The copy costs a
    // memcpy per *miss*, against the device read it is replacing.
    struct cached_extent {
        size_t len = 0;
        std::vector<uint8_t> bytes;
    };
    mutable std::unordered_map<uint64_t, cached_extent> extents;
    mutable size_t extents_retained = 0;

    static size_t oi_heap_size(const std::vector<std::optional<format::offset_index>>& v) {
        size_t n = v.capacity() * sizeof(std::optional<format::offset_index>);
        for (const auto& o : v) {
            if (o) { n += o->pages.capacity() * sizeof(format::page_loc); }
        }
        return n;
    }

    size_t memory_size() const noexcept override {
        return _retained + oi_retained + pages_retained + extents_retained;
    }

    // Call once, after filling everything in. Measured rather than estimated: every container is
    // asked for its capacity, so the number does not depend on believing anything about the
    // relationship between the on-disk footer length and the parsed form.
    void measure() {
        size_t n = sizeof(cached_footer);
        n += footer.capacity();
        n += heap_size_of_pod_vector(rg_start);
        n += md.schema.capacity() * sizeof(format::schema_element);
        for (const auto& e : md.schema) {
            n += heap_size(e.name);
        }
        n += md.row_groups.capacity() * sizeof(format::row_group);
        for (const auto& g : md.row_groups) {
            // Zero in a pristine entry. A materialised group here would mean the entry had been
            // mutated after publication, which the reader-local one-group copy exists to prevent;
            // it is summed anyway so that the number stays honest if that ever changes.
            n += g.columns.capacity() * sizeof(format::column_chunk);
        }
        n += md.key_value_metadata.capacity() * sizeof(format::key_value);
        for (const auto& kv : md.key_value_metadata) {
            n += heap_size(kv.key) + heap_size(kv.value);
        }
        if (md.created_by) {
            n += heap_size(*md.created_by);
        }
        n += heap_size(aad_file_unique) + heap_size(aad_prefix);
        for (const auto& [leaf, id] : column_key_ids) {
            n += heap_size(leaf) + heap_size(id) + sizeof(decltype(column_key_ids)::value_type);
        }
        _retained = n;
    }

private:
    size_t _retained = 0;
};

namespace {

// Streams a pq sstable: footer once, then one row group's bytes at a time,
// decoded in fixed-size row windows and turned straight into fragments.
//
// Two things make the bounded behaviour possible, and both were built for it:
// the index entry written by pq_writer_impl carries a *row ordinal* rather than
// a byte offset (design doc 5.4, option A), and read_row_range() steps over
// pages outside the requested rows using the V2 header's num_rows without
// decompressing them.
// ---------------------------------------------------------------- profiling
// Phases chosen to match the candidates in design doc 10.4g so the report answers the question that
// was actually asked rather than whatever was easy to instrument.
//
// **The phases are non-overlapping, and that is a property the report depends on** -- its share
// column is each phase against the sum, which means nothing if one phase contains another. Two
// corrections were needed to make it true, both recorded in design doc 10.27:
//
//   * `page_decode` used to wrap the whole of decode_paged() and so *contained* page_fetch and
//     decode_cpu, which the report then also added into its own total. Every share was diluted by
//     the double count. It is gone; its two halves were always the interesting part of it, and what
//     is left of it is extent planning.
//   * the **streaming** path had no timer at all. That was harmless when only a scan streamed, and
//     wrong the moment 10.26's window fix made a point read at the shipping defaults stream too:
//     the row-group fetch and its decode -- by then the whole of the read's data cost -- were
//     invisible, so a profile of a shipping-default point read attributed essentially all of it to
//     the footer by omission. rg_fetch and rg_decode are that path.
enum class rphase : size_t {
    footer_io,      // the two bounded reads that fetch the footer
    footer_parse,   // Thrift decode of FileMetaData, plus the footer decrypt for a PARE file
    schema_recover, // rebuilding the mapped schema from the footer
    index_lookup,   // partition key -> row ordinal, via the sstable index
    rg_materialise, // one row group's column metadata, decoded out of the retained footer bytes
    offset_index_io,    // fetching the OffsetIndex blobs: one read covering every projected column
    offset_index_parse, // Thrift-decoding those blobs into PageLocation vectors
    rg_fetch,       // streaming path: one sequential read of a whole row group
    rg_decode,      // streaming path: decode of that row group's wanted rows
    page_fetch,     // paged path: the data_read calls that pull dictionary and data pages
    decode_cpu,     // paged path: header parse and value decode, no I/O
    reassemble,     // levels and values -> CQL rows, common to both paths
    _count
};

struct rprof {
    static inline bool enabled = [] {
        const char* e = std::getenv("PQ_READER_PROFILE");
        return e && *e && *e != '0';
    }();
    static inline std::array<uint64_t, size_t(rphase::_count)> ns{};
    static inline std::array<uint64_t, size_t(rphase::_count)> hits{};
};

// Scoped. The I/O phases deliberately span their co_await, because fetch time is the thing being
// attributed; the honest caveat is that they therefore include any scheduler delay before
// resumption, so treat them as an upper bound on the I/O itself. CPU phases contain no
// suspension point and need no such caveat.
class rtimer {
    rphase _p;
    std::chrono::steady_clock::time_point _t0;
public:
    explicit rtimer(rphase p) : _p(p) {
        if (rprof::enabled) { _t0 = std::chrono::steady_clock::now(); }
    }
    ~rtimer() {
        if (!rprof::enabled) { return; }
        const auto dt = std::chrono::steady_clock::now() - _t0;
        rprof::ns[size_t(_p)] += uint64_t(std::chrono::duration_cast<std::chrono::nanoseconds>(dt).count());
        ++rprof::hits[size_t(_p)];
    }
};

// One budget for both read caches, shard-wide. Default 2 % of this shard's memory: enough that a
// point-read working set of a few hundred pages fits, small enough that a node is never surprised
// by it. Overridable because the right fraction is a workload question and nobody has measured it
// on a real one yet.
size_t read_cache_cap() {
    static const size_t cap = [] () -> size_t {
        if (const char* e = std::getenv("PQ_READ_CACHE_MB")) {
            return size_t(std::max(0L, std::atol(e))) << 20;
        }
        return size_t(double(seastar::memory::stats().total_memory()) * 0.02);
    }();
    return cap;
}

bool read_cache_has_room(size_t bytes) noexcept {
    return read_cache_bytes_local().total() + bytes <= read_cache_cap();
}

bool extent_cache_enabled() {
    static const bool on = [] {
        const char* e = std::getenv("PQ_EXTENT_CACHE");
        return !(e && *e == '0');
    }();
    return on;
}

// Binds the entry's page store to the format layer's interface. Holds the entry by shared_ptr so
// that the store cannot go away under a decode, and the sstable so that growth can be accounted.
class entry_page_cache final : public format::page_cache {
    seastar::shared_ptr<const cached_footer> _cf;
    sstables::shared_sstable _sst;
    // Accumulated here and handed over once, in the destructor, rather than per page.
    //
    // Not a micro-optimisation: sstables_manager::adjust_total_reclaimable_memory() signals the
    // reclaim fiber on every call, so accounting each page separately woke the reclaimer ~16 times
    // per point read where the footer cache had woken it once per sstable. At 240 row groups across
    // 10 sstables that turned test_pq_compaction_aborted_while_reading_does_not_fault from seconds
    // into more than ten minutes -- the reclaimer re-examining the whole sstable set on each
    // signal. One flush per decode restores the original order of magnitude.
    size_t _grown = 0;
public:
    entry_page_cache(seastar::shared_ptr<const cached_footer> cf, sstables::shared_sstable sst)
        : _cf(std::move(cf)), _sst(std::move(sst)) {}

    ~entry_page_cache() {
        if (_grown) { _sst->grow_pq_footer_cache(_grown); }
    }

    const std::vector<uint8_t>* get(int64_t off) const noexcept override {
        auto it = _cf->pages.find(off);
        if (it == _cf->pages.end()) {
            ++page_cache_stats_local().misses;
            return nullptr;
        }
        ++page_cache_stats_local().hits;
        return &it->second;
    }

    bool accepts(size_t bytes) const noexcept override {
        return read_cache_has_room(bytes);
    }

    const std::vector<uint8_t>* put(int64_t off, std::vector<uint8_t> v) override {
        const size_t n = v.capacity();
        auto& slot = _cf->pages[off];
        slot = std::move(v);
        _cf->pages_retained += n;
        read_cache_bytes_local().pages += n;
        _grown += n;
        ++page_cache_stats_local().populations;
        return &slot;
    }
};

class pq_reader : public mutation_reader::impl {
    sstables::shared_sstable _sst;
    const dht::partition_range* _pr;
    // Held by value. The caller's slice can be a temporary -- the reversed path
    // in sstable::make_reader builds one with reverse_slice() -- and a reader
    // outlives the call that made it.
    query::partition_slice _slice;
    sstables::read_monitor& _mon;
    const bool _use_index;
    // What the read monitor watches. mx keeps this inside its read context, which advances it as
    // the data file is consumed sequentially; this reader has no such context and its reads are not
    // sequential, so `position` accumulates bytes actually fetched. For the one consumer that
    // matters -- compaction_read_monitor::compacted(), which feeds the backlog tracker -- "bytes of
    // this sstable consumed so far" is the intended quantity, and on a compaction (which reads every
    // column) the two definitions converge.
    sstables::reader_position_tracker _tracker;
    bool _read_started = false;
    bool _read_completed = false;

    bool _init = false;
    // The sstable's parsed footer, shared and immutable. Held by shared_ptr rather than looked up
    // per use, so that a reclaim in the middle of a read cannot pull it out from under us: the
    // entry leaves the sstable and stays alive here until this reader is done with it.
    seastar::shared_ptr<const cached_footer> _cf;
    // One row group's column metadata, materialised into a reader-local metadata object holding
    // exactly that group at index 0. This is what keeps the shared entry immutable; see
    // cached_footer.
    format::file_metadata _rgmd;
    size_t _rgmd_rg = size_t(-1);
    mapped_schema _ms;
    std::vector<cql_column> _cols;
    size_t _n_pk = 0, _n_ck = 0, _static_base = 0;
    std::span<const int64_t> _rg_start;   // cumulative first row of each row group, from _cf

    // Ordinal window this read is confined to, from the partition index.
    int64_t _row_lo = 0, _row_hi = 0;

    // Currently loaded row group and its bytes. A streaming window holds the whole row group
    // (sequential reads are what a scan wants); a paged window instead holds two small extents
    // per column and never touches the pages in between.
    size_t _cur_rg = size_t(-1);
    temporary_buffer<char> _rg_buf;
    int64_t _rg_base = 0;               // file offset of _rg_buf[0]
    size_t _oi_rg = size_t(-1);
    // Reader-owned storage, used only when there is no footer cache entry to grow.
    std::vector<std::optional<format::offset_index>> _oi;
    // What every reader of the offset indexes goes through. Points either at `_oi` or into the
    // cached footer entry, which `_cf` keeps alive. Null until load_offset_indexes() has run.
    const std::vector<std::optional<format::offset_index>>* _oi_view = nullptr;
    // Leaves of the materialised row group that need not be read at all: see elidable_leaves().
    // One entry per leaf, recomputed per row group because it is a property of the chunk
    // statistics rather than of the file.
    std::vector<uint8_t> _elide;
    // Leaves the *query* does not want, from the slice, when it has said projecting is allowed.
    // Unioned into _elide, which is already the "do not read this leaf" channel.
    std::vector<uint8_t> _projection_skip;
    size_t _elide_rg = size_t(-1);
    // Per-reader, never cached: see cached_footer for why the key does not go in the entry.
    std::optional<format::read_crypto> _crypto;

    // Decoded window.
    std::vector<row> _rows;
    size_t _pos = 0;
    int64_t _cursor = 0;                // next row ordinal to decode

    // Partition being assembled across windows.
    std::vector<value> _open_pk;
    std::optional<dht::decorated_key> _open_dk;
    // Applies the clustering slice, and -- the part that cannot be done by
    // filtering rows alone -- clips range tombstone changes to the slice's
    // boundaries, re-opening a range at the start of each range it spans.
    std::optional<mutation_fragment_filter> _filter;
    bool _open = false;
    // True while sitting inside a partition the range excludes.
    bool _skipping = false;

    future<> init();
    // Every data-file fetch goes through this so the read monitor's position keeps advancing.
    future<temporary_buffer<char>> tracked_read(uint64_t off, size_t len);
    future<> load_footer();             // cache hit, or fetch-decrypt-parse-publish
    // Ask the key provider for this file's key and build the per-read crypto context around the
    // envelope the footer declared. Separate from the footer load because the envelope is cached
    // and the key is not.
    future<format::read_crypto> read_crypto_for(format::cipher algo,
                                                std::string_view aad_file_unique,
                                                std::string_view aad_prefix,
                                                const seastar::sstring& key_id,
                                                const std::map<std::string, seastar::sstring>&
                                                        column_key_ids);
    // The per-column half of the above, separate because the two happen at different points on a
    // footer *miss*: the footer key is needed to decrypt the footer at all, and which columns have
    // their own key -- and under which id -- is only legible once that footer is parsed.
    future<> resolve_column_keys(format::read_crypto&,
                                const std::map<std::string, seastar::sstring>& column_key_ids);
    // Decrypt the per-column-key chunks of the currently materialised row group. Synchronous:
    // read_crypto_for() has already fetched every key this can need.
    void decrypt_column_metadata(size_t rg);
    void prepare_row_group_metadata() {
        _rgmd = format::file_metadata{};
        // walk_leaves() needs the schema tree, not the row groups, so this is the whole of what
        // the reader-local metadata carries besides the one group it materialises.
        _rgmd.schema = _cf->md.schema;
        _rgmd_rg = size_t(-1);
    }
    future<bool> next_window();         // false at end of the ordinal range
    future<> load_row_group(size_t rg);
    future<> load_offset_indexes(size_t rg);
    // Byte span of one row group: the first dictionary or data page of any of its chunks to the
    // end of the last. This is exactly what load_row_group() fetches, in one read.
    std::pair<int64_t, int64_t> row_group_span() const;
    // True when every chunk of the materialised row group has a usable OffsetIndex, which is
    // what decode_paged() needs in order to fetch pages rather than the whole group.
    bool have_page_index() const;
    // Whether this row group's rows all carry their existence in the shared marker channel, which
    // is what makes dropping their other columns safe. See the definition.
    bool projection_is_safe(size_t rg) const;
    // Would the paged path fetch at least as many bytes as one sequential read of the row group,
    // to cover group-local rows [lo, hi)? See the long comment at the call site.
    bool paged_fetch_is_not_cheaper(int64_t lo, int64_t hi, std::span<const uint8_t> elide) const;
    // Which leaves of the materialised row group carry nothing for any row in it, and so need be
    // neither fetched nor decoded. need_columns(rg) first.
    const std::vector<uint8_t>& elidable_leaves(size_t rg);

    // The plaintext footer, which the cached entry always holds whether or not the file on disk
    // was encrypted. Row-group column metadata is decoded from it on demand.
    std::span<const uint8_t> footer_bytes() const { return _cf->footer; }
    const format::read_crypto* crypto() const { return _crypto ? &*_crypto : nullptr; }
    // Decode this row group's column metadata if it has not been decoded yet, into the
    // reader-local single-group metadata. Cheap and idempotent; called before anything reads
    // columns(). The group's own index inside _rgmd is always 0.
    void need_columns(size_t rg) {
        if (_rgmd_rg == rg) { return; }
        rtimer _t{rphase::rg_materialise};
        _rgmd.row_groups.assign(1, _cf->md.row_groups.at(rg));
        format::materialise_row_group(_rgmd, 0, footer_bytes());
        // Any chunk with its own key arrives here with `meta` empty and its bytes in
        // encrypted_column_metadata; nothing downstream tolerates that, by design.
        decrypt_column_metadata(rg);
        _rgmd_rg = rg;
    }
    // The column metadata of the currently materialised row group. need_columns(rg) first.
    const std::vector<format::column_chunk>& columns() const { return _rgmd.row_groups[0].columns; }
    // Reads only the pages covering [lo, hi). Falls back to the whole row group
    // when the file carries no OffsetIndex.
    future<std::vector<format::column_data>> decode_paged(size_t rg, int64_t lo, int64_t hi);
    void emit_row(const row& r);
    void close_partition();

    dht::decorated_key key_of(std::span<const value> pk) const {
        std::vector<bytes> parts;
        parts.reserve(_n_pk);
        for (size_t i = 0; i < _n_pk; ++i) { parts.push_back(encode(_cols[i].type, pk[i])); }
        return dht::decorate_key(*_schema, partition_key::from_exploded(*_schema, parts));
    }

public:
    // Always constructed non-forwarding. Intra-partition forwarding is provided
    // by wrapping in make_forwardable() -- see make_reader() -- because seeking
    // by clustering position inside a row group is not implemented, and honouring
    // the interface while ignoring the position range would silently return rows
    // the caller did not ask for.
    pq_reader(sstables::shared_sstable sst, schema_ptr s, reader_permit permit,
              const dht::partition_range& pr, const query::partition_slice& slice,
              sstables::read_monitor& mon, bool use_index)
        : impl(std::move(s), std::move(permit))
        , _sst(std::move(sst)), _pr(&pr), _slice(slice), _mon(mon), _use_index(use_index) {}

    future<> fill_buffer() override {
        if (_end_of_stream) { co_return; }
        co_await init();
        while (!is_buffer_full() && !_end_of_stream) {
            if (_pos == _rows.size()) {
                if (!co_await next_window()) {
                    close_partition();
                    _end_of_stream = true;
                    break;
                }
                continue;
            }
            emit_row(_rows[_pos++]);
        }
    }

    future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (is_buffer_empty() && !_end_of_stream) {
            // Skip the rest of the open partition's rows.
            while (_open) {
                if (_pos == _rows.size()) {
                    if (!co_await next_window()) { _open = false; _end_of_stream = true; break; }
                    continue;
                }
                std::vector<value> pk(_rows[_pos].key.begin(),
                                      _rows[_pos].key.begin() + long(_n_pk));
                if (pk != _open_pk) { _open = false; break; }
                ++_pos;
            }
        }
        co_return;
    }

    future<> fast_forward_to(const dht::partition_range& pr) override {
        clear_buffer();
        _end_of_stream = false;
        _pr = &pr;
        _init = false;          // recompute the ordinal window for the new range
        _open = false;
        _rows.clear();
        _pos = 0;
        return make_ready_future<>();
    }

    future<> fast_forward_to(position_range) override {
        // Unreachable: this reader is never handed to a caller that forwards.
        // make_reader() wraps it in make_forwardable() instead.
        on_internal_error(sstlog, "pq_reader: intra-partition forwarding should be "
                                  "handled by the forwardable adapter");
    }

    future<> close() noexcept override {
        // on_read_completed() used to be called at the end of init(), i.e. when the read was just
        // beginning. With the compaction monitor that was a no-op -- on_read_started() was never
        // called, so its _tracker was null -- and the visible effect was that
        // compaction_read_monitor::compacted() returned 0 for every pq sstable, so the backlog
        // tracker could not discount work already done and overestimated a pq table's backlog for
        // the whole of every compaction.
        if (_read_started && !_read_completed) {
            _read_completed = true;
            _mon.on_read_completed();
        }
        return make_ready_future<>();
    }
};

// Every data-file fetch goes through here so the monitor's position cannot silently stop
// advancing when a new read site is added. on_read_started() is deferred to the first fetch rather
// than done in the constructor: the monitor expects a tracker that is about to move, and a reader
// that is constructed and closed without reading anything should not register at all.
future<temporary_buffer<char>> pq_reader::tracked_read(uint64_t off, size_t len) {
    if (!_read_started) {
        _read_started = true;
        _tracker.total_read_size = _sst->ondisk_data_size();
        _mon.on_read_started(_tracker);
    }
    auto buf = co_await _sst->data_read(off, len, _permit);
    _tracker.position += buf.size();
    co_return std::move(buf);
}

future<format::read_crypto> pq_reader::read_crypto_for(
        format::cipher algo,
        std::string_view aad_file_unique,
        std::string_view aad_prefix,
        const seastar::sstring& key_id,
        const std::map<std::string, seastar::sstring>& column_key_ids) {
    // The key id carries what the key provider issued for this file, or nothing at all if the
    // provider issues no ids. Trusting the file to name its own key is safe: it is an id, not a
    // capability, and a wrong one simply fails to authenticate.
    //
    // The provider *options* come from the schema rather than from the file. That is a real
    // difference from the whole-component encryption path, which stores them in the sstable's own
    // extension attributes: changing the key provider on a table with existing sstables will make
    // them unreadable, where scylla_encryption_options would still open them. Recorded in the
    // design doc as a known limitation -- key_metadata is a single opaque string, and packing the
    // option map into it would break the "id, verbatim" contract that makes the provider-neutral
    // shape work.
    auto* ksrc = key_source_ptr();
    if (!ksrc) {
        throw std::runtime_error(seastar::format(
                "pq: {} is encrypted, but this node has no encryption key provider registered",
                _sst->get_filename()));
    }
    const parquet_parameters pp{_schema->parquet_options()};
    const auto key_opts = pp.key_opts();
    format::read_crypto rc;
    try {
        // Resolving a key is not this reader's own CPU work, and for several providers it is not
        // even local: the replicated provider reads the key out of a system table, so a key
        // lookup *is another read*, admitted by the very same reader_concurrency_semaphore that
        // admitted this one. Say so, or the semaphore counts this permit as running on the CPU
        // for the whole round trip and refuses to admit anything new once
        // `reader_concurrency_semaphore_cpu_concurrency` permits are in that state -- including
        // the key lookup this reader is blocked on. The cycle is only broken when the permit's
        // own TTL expires (range_request_timeout_in_ms), so the symptom is that the first range
        // scan of an encrypted table stalls for the whole range timeout and then fails, with the
        // semaphore's "Identified bottleneck(s): CPU" diagnostics arriving only at the very end.
        //
        // Point reads escaped it by arithmetic rather than by design: one read is one need_cpu
        // permit and the default cpu_concurrency is 2, which leaves exactly one slot for the
        // nested lookup. A range scan runs two reads at a time and closes that slot, which is
        // why the asymmetry looked like a property of the scan path.
        // B3: resolve the key on the SYSTEM semaphore, not the user one.
        //
        // awaits_guard alone does not close the cycle. It exempts this permit from the CPU limit
        // (mark_awaits() touches no resources, and awaits_permits is consulted only by
        // cpu_concurrency_limit_reached()), but an awaiting permit still holds one COUNT unit, and
        // admission for the nested read still needs has_available_units(base_resources()). At
        // max_count_concurrent_reads concurrent first-time lookups the deadlock reforms and waits
        // out the permit's TTL.
        //
        // classify_request() keys off the CURRENT SCHEDULING GROUP, and default_scheduling_group()
        // classifies as request_class::system (replica/database.cc:1797). Running the lookup there
        // means it admits against the system semaphore, so it no longer competes with the user
        // permit this reader is holding. The write path has always had this for free: a flush or
        // compaction is already in a group that classifies as system, which is why its key lookup
        // never contended with its own permit.
        //
        // The trade, stated because it is a policy change and not just a fix: the provider round
        // trip's CPU is billed to main rather than to the user's service level. It is a rare,
        // cache-populating call, and it is the same trade the write path already makes.
        reader_permit::awaits_guard awaiting{_permit};
        rc.key = co_await seastar::with_scheduling_group(seastar::default_scheduling_group(),
                [&ksrc, &key_opts, &key_id] { return ksrc->key_for_read(key_opts, key_id); });
    } catch (...) {
        std::throw_with_nested(std::runtime_error(seastar::format(
                "pq: {} is encrypted, but its key could not be obtained from the key provider "
                "(id '{}')", _sst->get_filename(), key_id)));
    }
    if (!rc.key.valid()) {
        throw std::runtime_error(seastar::format(
                "pq: {}: the key provider returned a {}-byte key; AES needs 16, 24 or 32",
                _sst->get_filename(), rc.key.bytes.size()));
    }
    rc.algo = algo;
    rc.aad_file_unique = std::string(aad_file_unique);
    rc.aad_prefix = std::string(aad_prefix);

    if (!column_key_ids.empty()) {
        co_await resolve_column_keys(rc, column_key_ids);
    }
    co_return rc;
}

future<> pq_reader::resolve_column_keys(
        format::read_crypto& rc, const std::map<std::string, seastar::sstring>& column_key_ids) {
    auto* ksrc = key_source_ptr();
    if (!ksrc) {
        throw std::runtime_error(seastar::format(
                "pq: {} is encrypted, but this node has no encryption key provider registered",
                _sst->get_filename()));
    }
    const parquet_parameters pp{_schema->parquet_options()};
    // Driven off the *file*, not off the schema. The file states which columns it encrypted under
    // their own key; the schema only says where to find those keys. That asymmetry is deliberate
    // and it is what makes ALTER safe in the one direction that matters: dropping
    // `encryption_key.<col>` from the property does not make existing files unreadable, because
    // they still declare the column key and we still go looking for it. The reverse -- adding one
    // -- affects new files only, since old ones declare the footer key for that column.
    //
    // A column the file says has its own key and the schema cannot locate is a hard error. That is
    // the whole point: the alternative is `read_crypto::key_for()` falling back to the footer key,
    // which authenticates against nothing and would surface as a decode failure pages later, or --
    // worse and the failure mode this feature must never have -- an empty column that reads as
    // "no data" rather than "no access".
    const auto& col_opts = pp.column_key_opts();
    // Deduplicated by option set, exactly as on the write side: several columns under one key must
    // cost one lookup. See the B3 note below for why the count matters and not just the latency.
    std::map<key_options, format::encryption_key> resolved;
    for (const auto& [leaf, col_key_id] : column_key_ids) {
        auto oi = col_opts.find(seastar::sstring(leaf));
        if (oi == col_opts.end()) {
            throw std::runtime_error(seastar::format(
                    "pq: {} encrypts column '{}' under its own key, but the table's 'parquet' "
                    "option has no 'encryption_key.{}' saying where to find it. Restore that "
                    "sub-option (the key provider options are read from the schema, not from the "
                    "file) -- refusing to read rather than return the column as empty",
                    _sst->get_filename(), leaf, leaf));
        }
        if (auto ri = resolved.find(oi->second); ri != resolved.end()) {
            rc.column_keys[leaf] = ri->second;
            continue;
        }
        format::encryption_key ck;
        try {
            // Same reasoning, and the same guard, as the footer key above: this suspends on a
            // provider round trip -- for the replicated provider, on another read admitted by the
            // very semaphore that admitted this one -- so the permit must say it is awaiting and
            // not burning CPU.
            //
            // §11.1 B3 is not fixed by that guard and this makes its window longer. An awaiting
            // permit still holds one *count* unit, so a file with K distinct column keys keeps its
            // permit for K+1 sequential provider round trips instead of one, multiplying the time
            // each concurrent reader spends in the state that reforms the cycle. The lookups are
            // sequential on purpose: issuing them concurrently would put K nested lookups in flight
            // from a single reader and make the count limit reachable from far fewer readers.
            // Same reasoning as the footer key above: on the system semaphore, so K column keys
            // are K round trips that do not contend with the user permit being held.
            reader_permit::awaits_guard awaiting{_permit};
            ck = co_await seastar::with_scheduling_group(seastar::default_scheduling_group(),
                    [&ksrc, &oi, &col_key_id] { return ksrc->key_for_read(oi->second, col_key_id); });
        } catch (...) {
            std::throw_with_nested(std::runtime_error(seastar::format(
                    "pq: {} encrypts column '{}' under its own key, which could not be obtained "
                    "from the key provider (id '{}')",
                    _sst->get_filename(), leaf, col_key_id)));
        }
        if (!ck.valid()) {
            throw std::runtime_error(seastar::format(
                    "pq: {}: the key provider returned a {}-byte key for column '{}'; AES needs "
                    "16, 24 or 32", _sst->get_filename(), ck.bytes.size(), leaf));
        }
        resolved.emplace(oi->second, ck);
        rc.column_keys[leaf] = std::move(ck);
    }
}

// Fill in the ColumnMetaData of any chunk whose metadata was encrypted under a column key. Pure
// CPU: every key this needs was resolved in read_crypto_for() before any row group was touched.
//
// A chunk we cannot decrypt is an error and not a skip. format::parse_encrypted_footer tolerates
// leaving `meta` empty -- correct for a general-purpose reader that may legitimately hold only some
// keys -- but for Scylla reading its own sstable it is not a partial-access case, it is a missing
// key, and an empty `meta` would flow on as a column with no pages, i.e. as an all-null column.
// "No access" must never render as "no data".
void pq_reader::decrypt_column_metadata(size_t rg) {
    if (!_crypto) { return; }
    auto& chunks = _rgmd.row_groups[0].columns;
    for (size_t c = 0; c < chunks.size(); ++c) {
        auto& ch = chunks[c];
        if (ch.meta || !ch.encrypted_column_metadata || !ch.crypto_metadata) { continue; }
        const auto& path = ch.crypto_metadata->path_in_schema;
        const std::string leaf = path.empty() ? std::string() : path.back();
        auto it = _crypto->column_keys.find(leaf);
        if (it == _crypto->column_keys.end()) {
            throw std::runtime_error(seastar::format(
                    "pq: {}: row group {} column '{}' is encrypted under its own key, which this "
                    "reader does not hold", _sst->get_filename(), rg,
                    leaf.empty() ? "<unnamed>" : leaf));
        }
        // The AAD binds the module to its row group and column ordinal, which is why the writer
        // must emit RowGroup.ordinal: a reader that substitutes -1 for an absent one derives a
        // different AAD and every per-column decrypt fails.
        auto aad = format::build_aad(_crypto->aad_prefix, _crypto->aad_file_unique,
                                    format::module_type::column_metadata, int(rg), int(c));
        auto blob = std::span<const uint8_t>(
                reinterpret_cast<const uint8_t*>(ch.encrypted_column_metadata->data()),
                ch.encrypted_column_metadata->size());
        auto plain = format::decrypt_module(blob, it->second, aad, nullptr, _crypto->algo, false);
        ch.meta = format::parse_column_metadata_blob(plain);
    }
}

// Get the parsed footer into _cf, from the sstable's cache if it is there and by reading,
// decrypting and parsing the file if it is not.
//
// The cache makes this once per sstable rather than once per read, which is the whole point: the
// footer is 1.4 kB per row group (design doc 10.22) and both fetching it and walking it scale
// with row groups, which is where a cold point read's 1.11 us per row group goes (10.21).
future<> pq_reader::load_footer() {
    if (_cf) {
        // fast_forward_to() re-runs init() for the new range. The file's footer has not changed.
        co_return;
    }
    auto& stats = footer_cache_stats_local();
    if (auto& cached = _sst->pq_footer_cache()) {
        ++stats.hits;
        // Downcast rather than dynamic_cast: reader.cc is the only thing that ever publishes an
        // entry, so there is exactly one concrete type it can be.
        _cf = seastar::static_pointer_cast<const cached_footer>(cached);
        if (_cf->encrypted) {
            // The envelope was cached; the key was not, on purpose. Fetch it again.
            _crypto = co_await read_crypto_for(_cf->algo, _cf->aad_file_unique, _cf->aad_prefix,
                                               _cf->key_id, _cf->column_key_ids);
        }
        prepare_row_group_metadata();
        co_return;
    }
    ++stats.misses;

    auto entry = seastar::make_shared<cached_footer>();

    // Footer only: the last 8 bytes give its length, then one bounded read.
    const uint64_t len = _sst->ondisk_data_size();
    if (len < 12) { throw std::runtime_error("pq: data component too small"); }
    // Local, per-miss accumulators alongside the global rprof ones: see the log line at the end.
    std::chrono::steady_clock::duration t_fetch{}, t_parse{};
    temporary_buffer<char> tail;
    {
        rtimer _t{rphase::footer_io};
        const auto t0 = std::chrono::steady_clock::now();
        tail = co_await tracked_read(len - 8, 8);
        t_fetch += std::chrono::steady_clock::now() - t0;
    }
    uint32_t flen;
    std::memcpy(&flen, tail.get(), 4);
    if (uint64_t(flen) + 12 > len) { throw std::runtime_error("pq: bad footer length"); }
    // "PARE" rather than "PAR1" means the footer is a ciphertext preceded by a plaintext
    // FileCryptoMetaData. The tail read above is the same either way -- length then magic --
    // which is why only this branch is new.
    const bool encrypted = std::memcmp(tail.get() + 4, format::magic_encrypted, 4) == 0;
    temporary_buffer<char> raw;
    {
        rtimer _t{rphase::footer_io};
        const auto t0 = std::chrono::steady_clock::now();
        raw = co_await tracked_read(len - 8 - flen, flen);
        t_fetch += std::chrono::steady_clock::now() - t0;
    }
    entry->encrypted = encrypted;
    if (!encrypted) {
        entry->footer.assign(reinterpret_cast<const uint8_t*>(raw.get()),
                             reinterpret_cast<const uint8_t*>(raw.get()) + raw.size());
    } else {
        rtimer _t{rphase::footer_parse};
        const auto t0 = std::chrono::steady_clock::now();
        auto region = std::span<const uint8_t>(
                reinterpret_cast<const uint8_t*>(raw.get()), raw.size());
        size_t consumed = 0;
        auto fcm = format::parse_file_crypto_metadata(region, &consumed);
        // key_metadata carries the id the key provider issued for this file, or nothing at all if
        // the provider issues no ids. Trusting the file to name its own key is safe: it is an id,
        // not a capability, and a wrong one simply fails to authenticate.
        //
        // The provider *options* come from the schema rather than from the file. That is a real
        // difference from the whole-component encryption path, which stores them in the sstable's
        // own extension attributes: changing the key provider on a table with existing sstables
        // will make them unreadable, where scylla_encryption_options would still open them.
        // Recorded in the design doc as a known limitation -- key_metadata is a single opaque
        // string, and packing the option map into it would break the "id, verbatim" contract that
        // makes the provider-neutral shape work.
        entry->key_id = fcm.key_metadata
                ? key_id_from_metadata(seastar::sstring(*fcm.key_metadata))
                : seastar::sstring();
        entry->algo = fcm.algo;
        entry->aad_file_unique = fcm.aad_file_unique;
        // The writer stores the prefix, so this normally comes from the file. The fallback
        // reconstructs what the writer would have used, which keeps a file written before
        // store_aad_prefix was set readable.
        entry->aad_prefix = fcm.aad_prefix;
        if (entry->aad_prefix.empty()) {
            entry->aad_prefix = fmt::format("{}.{}", _schema->ks_name(), _schema->cf_name());
        }
        // Column keys cannot be resolved yet: which columns have their own, and under which id,
        // is stated inside the footer this key is about to decrypt. Harvested and resolved below.
        auto rc = co_await read_crypto_for(entry->algo, entry->aad_file_unique, entry->aad_prefix,
                                           entry->key_id, {});
        auto aad = format::build_aad(rc.aad_prefix, rc.aad_file_unique,
                                     format::module_type::footer);
        entry->footer = format::decrypt_module(region.subspan(consumed), rc.key, aad, nullptr,
                                               rc.algo, false);
        _crypto = std::move(rc);
        // Includes the key fetch, which is a provider round trip rather than CPU. The log line
        // says so; a PARE breakdown that hid it would misattribute a KMS call as decrypt cost.
        t_parse += std::chrono::steady_clock::now() - t0;
    }
    {
        // Lazy: decode the schema and each row group's row count, but not the per-column
        // metadata, which is 4.3 us per row group and irrelevant to every group but the one
        // this read touches (design doc 10.4j). The footer bytes are retained so the wanted
        // group can be decoded on demand.
        rtimer _t{rphase::footer_parse};
        const auto t0 = std::chrono::steady_clock::now();
        entry->md = format::parse_file_metadata(entry->footer, {}, format::semantic_check::yes,
                                                format::metadata_mode::lazy);
        t_parse += std::chrono::steady_clock::now() - t0;
    }
    // Which columns the file encrypted under their own key, and the provider id each one recorded.
    //
    // One row group is materialised into a throwaway copy to read it: the ids are the same in every
    // group, because the writer fixes the file's column keys once, and one materialisation is 4.3 us
    // (design doc 10.4j) against the provider round trips it is about to inform. Cached on the
    // entry so a later reader pays neither this nor the parse -- only the lookups themselves, which
    // are deliberately never cached.
    if (encrypted && !entry->md.row_groups.empty()) {
        format::file_metadata probe;
        probe.schema = entry->md.schema;
        probe.row_groups.assign(1, entry->md.row_groups.front());
        format::materialise_row_group(probe, 0, entry->footer);
        for (const auto& ch : probe.row_groups[0].columns) {
            if (!ch.crypto_metadata || ch.crypto_metadata->with_footer_key) { continue; }
            const auto& path = ch.crypto_metadata->path_in_schema;
            if (path.empty()) {
                throw std::runtime_error(seastar::format(
                        "pq: {}: a column chunk declares its own encryption key but names no "
                        "column, so the key cannot be looked up", _sst->get_filename()));
            }
            entry->column_key_ids[path.back()] = ch.crypto_metadata->key_metadata
                    ? key_id_from_metadata(seastar::sstring(*ch.crypto_metadata->key_metadata))
                    : seastar::sstring();
        }
        if (!entry->column_key_ids.empty()) {
            co_await resolve_column_keys(*_crypto, entry->column_key_ids);
        }
    }

    entry->rg_start.reserve(entry->md.row_groups.size());
    for (const auto& g : entry->md.row_groups) {
        entry->rg_start.push_back(entry->total_rows);
        entry->total_rows += g.num_rows;
    }
    entry->measure();

    // Per-miss breakdown, not an aggregate, and the reason it exists is that no aggregate could
    // answer the question. A footer miss happens once per sstable, so in any run long enough to
    // measure, the global rprof counters have summed one miss with thousands of hits and the miss
    // is lost in them -- while the estimator every cold figure in the design doc uses (min over 400
    // probes after one restart) discards it outright, because only the first of the 400 pays it.
    // This is what §10.27 was measured with: at the shipping defaults a 568 kB footer over 399 row
    // groups costs ~1 150 us to fetch and ~1 400 us to walk, and those 2.5 ms are 69 % of a genuine
    // first read -- none of which appears in the 1 149 us the same read costs once cached.
    if (rprof::enabled) {
        pqlog.info("footer miss: {} bytes={} groups={} fetch={:.0f}us parse={:.0f}us{}",
                   _sst->get_filename(), flen, entry->md.row_groups.size(),
                   double(std::chrono::duration_cast<std::chrono::microseconds>(t_fetch).count()),
                   double(std::chrono::duration_cast<std::chrono::microseconds>(t_parse).count()),
                   encrypted ? " (PARE: parse includes decrypt)" : "");
    }

    _cf = entry;
    // Published last, and only after the entry is complete and immutable. A concurrent reader
    // that missed at the same time simply parsed it too and overwrites this with an identical
    // entry; both are correct, and the loser's copy dies with its reader.
    _sst->set_pq_footer_cache(_cf);
    prepare_row_group_metadata();
}

future<> pq_reader::init() {
    if (_init) { co_return; }
    _init = true;

    co_await load_footer();

    _cols = columns_of(*_schema);
    {
        rtimer _t{rphase::schema_recover};
        _ms = recover_mapped_schema(_cf->md, _cols);
    }
    // Projection, when the query has said it projects anyway (see may_project_columns). Built once:
    // the slice does not change over a reader's life.
    //
    // The row format ignores the slice entirely and projects above storage. pq can do better
    // because a column chunk is its own extent -- but only when told, because a consumer that needs
    // whole rows (a view update, a mutation dump, compaction, or anything populating the row cache)
    // must not be handed a partial one. Row *existence* is safe either way: every row's key leaves
    // are written and keys are never projected, so the row set does not depend on which value
    // columns are read -- test_pq_projection_and_row_existence pins that, including for a
    // marker-less row whose only live cell is projected away.
    if (_slice.options.contains(query::partition_slice::option::may_project_columns)) {
        // By id, not by name. A regular column's `column_id` is its index among the regular
        // columns, which is the order columns_of() emits them in, so the id *is* the offset into
        // the value columns. Statics follow the regulars, starting at static_base().
        //
        // Matching by name instead looks more obviously correct and is not: columns_of() renames a
        // static to "__s_<name>", so a name lookup for a selected static silently found nothing and
        // projected it away. test_pq_may_project_columns_skips_unwanted_and_keeps_the_rest caught
        // that, which is why it reads a static both ways rather than only asserting the fast path.
        std::vector<bool> want(_ms.n_regular, false);
        bool any_unwanted = false;
        auto mark_want = [&] (size_t k) {
            if (k < want.size()) { want[k] = true; }
        };
        for (column_id id : _slice.regular_columns) { mark_want(size_t(id)); }
        // static_base(*_schema) rather than _static_base: that member is assigned further down this
        // function, so reading it here would silently use zero and mark a *regular* column instead.
        const size_t sbase = static_base(*_schema);
        for (column_id id : _slice.static_columns) { mark_want(sbase + size_t(id)); }
        for (size_t k = 0; k < want.size(); ++k) { if (!want[k]) { any_unwanted = true; break; } }
        // A `SELECT *` wants everything, so there is nothing to skip and no mask to carry.
        if (any_unwanted) {
            _projection_skip = projection_skip_mask(_ms, want);
        }
        if (std::getenv("PQ_PROJ_DEBUG")) {
            std::string w;
            for (size_t k = 0; k < want.size(); ++k) { w += want[k] ? '1' : '0'; }
            std::string names;
            for (size_t k = 0; k < _ms.n_regular && _ms.n_key + k < _cols.size(); ++k) {
                names += _cols[_ms.n_key + k].name; names += ',';
            }
            pqlog.info("proj debug: n_key={} n_regular={} cols={} slice.reg={} slice.static={} "
                       "want={} value_cols=[{}]",
                       _ms.n_key, _ms.n_regular, _cols.size(), _slice.regular_columns.size(),
                       _slice.static_columns.size(), w, names);
        }
    }

    _n_pk = _schema->partition_key_size();
    _n_ck = _schema->clustering_key_size();
    _static_base = static_base(*_schema);

    _rg_start = _cf->rg_start;
    const int64_t acc = _cf->total_rows;
    _row_lo = 0;
    _row_hi = acc;

    // A bounded range becomes a bounded ordinal window, via the partition index.
    // This is what turns a point read from "decode the file" into "decode a page".
    if (_use_index && (_pr->start() || _pr->end()) && _sst->has_component(component_type::Index)) {
        auto ir = _sst->make_index_reader(_permit, {}, use_caching::yes, _pr->is_singular());
        std::exception_ptr ex;
        try {
            bool present = true;
            std::optional<int64_t> singular_end;
            if (_pr->is_singular()) {
                // A singular range needs the exact-key lookup, not advance_to():
                // advance_to() positions for a *range* and leaves both bounds at
                // the start for a point, which reads back as an empty window.
                // This is the same call mx makes for a single-partition read.
                {
                    rtimer _t{rphase::index_lookup};
                    present = co_await ir->advance_lower_and_check_if_present(
                            dht::ring_position_view(_pr->start()->value()));
                }
                // ...but that call leaves the *upper* bound unset, and an unset upper bound is
                // not a missing optimisation here -- it is a window that runs to the end of the
                // file. `data_file_positions().end` is engaged only when an upper bound exists,
                // so a point read used to come back with [first row of the partition, total
                // rows), and next_window() would then decode and reassemble min(window, rest of
                // the row group) rows in order to return the handful the partition holds. At the
                // shipping defaults that is a whole row group: ~2 500 rows on average to answer
                // a five-row question, and it was 90 % of the read (design doc 10.28).
                //
                // Stepping the lower bound on by one partition costs an increment of an index
                // page cursor -- the page is already in memory, read_partition_data() having
                // just been awaited inside the call above -- and gives the ordinal the partition
                // ends at. `_row_lo` is captured *before* the step, because the step moves the
                // bound this reads the start from.
                if (present && !ir->eof()) {
                    _row_lo = std::min<int64_t>(int64_t(ir->data_file_positions().start), acc);
                    rtimer _t{rphase::index_lookup};
                    co_await ir->advance_to_next_partition();
                    // At the last partition of the file this is data_file_end(), which is a byte
                    // count rather than a row ordinal -- so it is clamped to `acc` like every
                    // other position here, and lands on "to the end of the file", which for the
                    // last partition is the right answer anyway.
                    singular_end = std::min<int64_t>(int64_t(ir->data_file_positions().start), acc);
                }
            } else {
                rtimer _t{rphase::index_lookup};
                co_await ir->advance_to(*_pr);
            }
            if (!present) { _sst->get_filter_tracker().add_false_positive(); }
            if (singular_end) {
                _row_hi = *singular_end;
            } else {
                auto pos = present ? ir->data_file_positions() : sstables::data_file_positions_range{0, 0};
                _row_lo = std::min<int64_t>(int64_t(pos.start), acc);
                _row_hi = pos.end ? std::min<int64_t>(int64_t(*pos.end), acc) : acc;
            }
            if (_row_hi < _row_lo) { _row_hi = _row_lo; }
        } catch (...) { ex = std::current_exception(); }
        co_await ir->close();
        if (ex) { std::rethrow_exception(ex); }
    }
    _cursor = _row_lo;
}

// The materialised row group's byte span: [first page of any chunk, end of the last). One call
// site fetches it and the other compares against its size, and they must agree, so there is one
// definition. need_columns(rg) first.
std::pair<int64_t, int64_t> pq_reader::row_group_span() const {
    int64_t lo = std::numeric_limits<int64_t>::max(), hi = 0;
    for (const auto& cc : columns()) {
        if (!cc.meta) { continue; }
        const auto& cm = *cc.meta;
        const int64_t s = cm.dictionary_page_offset ? *cm.dictionary_page_offset
                                                    : cm.data_page_offset;
        lo = std::min(lo, s);
        hi = std::max(hi, s + cm.total_compressed_size);
    }
    return {lo, hi};
}

future<> pq_reader::load_row_group(size_t rg) {
    // Before the early return, not after: a fast_forward_to() resets the materialised group while
    // leaving the buffered bytes in place, and the callers below index columns() either way.
    need_columns(rg);
    if (_cur_rg == rg) { co_return; }
    const auto [lo, hi] = row_group_span();
    if (lo >= hi) { throw std::runtime_error("pq: empty row group extent"); }
    {
        rtimer _t{rphase::rg_fetch};
        _rg_buf = co_await tracked_read(uint64_t(lo), size_t(hi - lo));
    }
    _rg_base = lo;
    _cur_rg = rg;
}

// Leaves that cannot contribute a cell to any row of this row group, taken from the chunk's own
// statistics: `null_count == num_values` says every slot in the chunk is null, so a reader that
// decodes it learns only that -- at the cost of a page walk, a decompress, and (on the paged
// path) two read operations per window.
//
// This is where a working column projection would have paid off, and it pays off here without
// the query needing to say anything: on Scylla's shredded schemas the leaves that dominate the
// *operation* count are the metadata channels, and they are all-null except when the data uses
// them. On the 28-leaf time-series schema of design doc §10.26, 23 leaves -- every `__ttl_*`,
// `__ldt_*`, `__rt*`, `__pt_*` -- are all-null and cost 23/28 of the per-leaf work while holding
// 5 % of the bytes. That is the read-operations prize §10.26 identified, and unlike a projection
// it is also claimed by `SELECT *`, by compaction and by repair, because it is a statement about
// the file rather than about the query.
//
// Restrictions, both of which exist to keep reassembly exact:
//
//   * repeated leaves are never elided. `num_values` counts *slots*, and for a repeated leaf a
//     slot whose definition level says "present but empty" is counted as a null -- so all-null
//     does not mean absent there, and read_collection() distinguishes the two.
//   * a leaf inside a collection group is never elided, even when flat: the group's leaves are
//     consumed together, slot by slot, by one cursor.
//
// Key leaves and `__ts` are REQUIRED, so their null_count is zero and they never qualify; that is
// relied on by reassemble(), which rejects rather than trusts it.
const std::vector<uint8_t>& pq_reader::elidable_leaves(size_t rg) {
    if (_elide_rg == rg) { return _elide; }
    const auto& cols = columns();
    _elide.assign(cols.size(), false);
    _elide_rg = rg;

    // Leaves belonging to a collection group, which travel together or not at all.
    std::vector<bool> in_group(cols.size(), false);
    for (size_t k = 0; k < _ms.value_is_collection.size(); ++k) {
        if (!_ms.value_is_collection[k]) { continue; }
        const size_t v = _ms.value_leaf[k];
        const size_t n = _ms.value_is_counter[k] ? 6 : 5;
        for (size_t i = v; i < std::min(v + n, in_group.size()); ++i) { in_group[i] = true; }
    }

    for (size_t c = 0; c < cols.size(); ++c) {
        if (in_group[c]) { continue; }
        if (c < _ms.columns.size() && _ms.columns[c].max_rep > 0) { continue; }
        const auto& cc = cols[c];
        if (!cc.meta) { continue; }
        const auto& cm = *cc.meta;
        if (!cm.stats || !cm.stats->null_count) { continue; }   // no statistics: read it
        if (cm.num_values > 0 && *cm.stats->null_count == cm.num_values) { _elide[c] = 1; }
    }
    // The query's projection rides the same channel, but only where it is safe for this group --
    // see projection_is_safe(). Unioned rather than replacing: a leaf the file proves all-null is
    // skippable whether or not the query wanted it, and a leaf the query does not want is skippable
    // whether or not it is null.
    if (projection_is_safe(rg)) {
        for (size_t c = 0; c < _elide.size() && c < _projection_skip.size(); ++c) {
            if (_projection_skip[c]) { _elide[c] = 1; }
        }
        ++projection_stats_local().groups_projected;
    } else if (!_projection_skip.empty()) {
        ++projection_stats_local().groups_declined;
    }
    return _elide;
}

// Whether the query's projection may be applied to this row group.
//
// The constraint is not about bytes, it is about row existence. `deletable_row::is_live()` is "a
// live marker, or any live cell" -- there is no third channel -- so a row whose only live cell sits
// in a projected-away column stops being live, and the query layer drops it. A `SELECT a` that
// should return that row with `a = null` returns nothing instead, which is what
// test_parquet_bypass_cache_projection_matches_row_format caught: 5 rows against the row format's 6.
//
// A per-row "has a live cell" bit in the file would not fix it either, and that is worth writing
// down: liveness is a *merged* property. Whether a cell is live depends on tombstones that may live
// in another sstable or in a memtable, so a bit computed when this file was written cannot decide
// it, and a synthetic row marker invented to carry it would not merge correctly against a row
// tombstone either.
//
// What does work needs no format change, because the information is already here. The row marker is
// its own leaf and it is a *shared* channel -- projection_skip_mask() only ever skips per-column
// leaves -- so it is read whatever the projection. If every row in this group has a marker, every
// row's existence is carried by data the projection keeps, and it merges correctly because it is
// real data rather than something reconstructed. Parquet's own statistics answer that: null_count
// on the marker chunk.
//
// The two extra conditions are the ways a present marker can still fail to be live. A marker with a
// TTL can expire, and a marker can itself be deleted; in either case the row might be alive only
// through some other cell, which is exactly the cell a projection would drop. Both are statistics
// questions too -- the TTL and local-deletion-time leaves must be entirely null for this group.
bool pq_reader::projection_is_safe(size_t rg) const {
    if (_projection_skip.empty()) { return false; }
    const auto& cols = columns();
    auto chunk = [&] (std::optional<size_t> idx) -> const format::column_metadata* {
        if (!idx || *idx >= cols.size() || !cols[*idx].meta) { return nullptr; }
        return &*cols[*idx].meta;
    };
    // Every row carries a marker.
    const auto* mk = chunk(_ms.rm_index);
    if (!mk || !mk->stats || !mk->stats->null_count) { return false; }
    if (*mk->stats->null_count != 0) { return false; }
    // No marker in this group expires or is dead: those leaves are entirely absent from it.
    for (auto idx : {_ms.rm_ttl_index, _ms.rm_ldt_index}) {
        if (!idx) { continue; }                       // the schema does not materialise it at all
        const auto* c = chunk(idx);
        if (!c) { continue; }
        if (!c->stats || !c->stats->null_count) { return false; }   // cannot prove it, so decline
        if (*c->stats->null_count != c->num_values) { return false; }
    }
    return true;
}

bool pq_reader::have_page_index() const {
    if (!_oi_view || _oi_view->size() != columns().size()) { return false; }
    for (const auto& o : *_oi_view) { if (!o || o->pages.empty()) { return false; } }
    return true;
}

bool pq_reader::paged_fetch_is_not_cheaper(int64_t lo, int64_t hi,
                                           std::span<const uint8_t> elide) const {
    const auto [span_lo, span_hi] = row_group_span();
    const int64_t extent = span_lo >= span_hi ? 0 : span_hi - span_lo;
    if (extent <= 0) { return true; }
    const auto& cols = columns();
    int64_t paged = 0;
    // Every window the paged path would still issue inside this row group, because streaming
    // pays for the group once and then serves the rest of it from _rg_buf. Bails out as soon as
    // the paged path has spent the group's own size, which is the only thing the answer turns on
    // -- so the loop is short in exactly the case where it would otherwise be long.
    for (int64_t w = lo; w < hi; w += point_window_rows) {
        const int64_t wend = std::min(hi, w + point_window_rows);
        for (size_t c = 0; c < cols.size(); ++c) {
            if (!cols[c].meta) { continue; }
            // Leaves the paged path would not fetch do not count against it. Note the asymmetry
            // this creates in the reader's favour and against streaming: eliding leaves makes
            // paging cheaper without making the group's extent smaller, because the chunks are
            // contiguous.
            if (c < elide.size() && elide[c]) { continue; }
            const auto& pages = (*_oi_view)[c]->pages;
            const size_t i0 = (*_oi_view)[c]->page_for_row(w);
            const size_t i1 = (*_oi_view)[c]->page_for_row(wend > w ? wend - 1 : w);
            if (i0 >= pages.size() || i1 >= pages.size() || i1 < i0) { return true; }
            paged += pages[i1].offset + pages[i1].compressed_page_size - pages[i0].offset;
            if (cols[c].meta->dictionary_page_offset) {
                const int64_t d0 = *cols[c].meta->dictionary_page_offset;
                if (pages.front().offset > d0) { paged += pages.front().offset - d0; }
            }
        }
        if (paged >= extent) { return true; }
    }
    return paged >= extent;
}

future<bool> pq_reader::next_window() {
    _rows.clear();
    _pos = 0;
    if (_cursor >= _row_hi) { co_return false; }

    // Which row group holds _cursor. Binary search rather than a walk: _rg_start has one entry
    // per row group, and a linear scan of 8 000 of them is not free on a path this hot.
    const size_t rg = size_t(std::distance(
            _rg_start.begin(),
            std::prev(std::upper_bound(_rg_start.begin(), _rg_start.end(), _cursor))));
    const int64_t rg_first = _rg_start[rg];
    const int64_t rg_end = rg_first + _cf->md.row_groups[rg].num_rows;

    const int64_t lo = _cursor;
    // Rows of this row group the read still wants. The ordinal window [_row_lo, _row_hi) is
    // contiguous, so every row group strictly inside it is wanted whole and only the two at the
    // ends can be partial -- which is why the cheap test below settles a scan without touching
    // the page index at all.
    const int64_t grp_hi = std::min(rg_end, _row_hi);

    // Streaming or paging, decided per row group by which one fetches fewer bytes.
    //
    // This used to be decided by `_pr->start() || _pr->end()` -- the *presence* of a bound rather
    // than the *width* of the range -- and since the coordinator splits every range scan at
    // tablet boundaries before a replica sees it, every scan a client can issue is bounded and
    // every scan took the point-read path: 512 rows at a time, re-fetching each window's
    // containing page for every leaf. At the shipping defaults that pulled 460 MB through a 23 MB
    // file for one aggregate, in ~497 000 reads against the row format's 1 792 (design doc
    // §10.26).
    //
    // The predicate is now the comparison the cost model actually turns on, with no constant to
    // tune: the paged path is worth it only while the pages covering the wanted rows are smaller
    // than the row group holding them. Two cases, and the first is the common one:
    //
    //   * the whole group is wanted -- any scan, and every interior group of any range -- so
    //     streaming reads it once, sequentially, in one operation. No page index needed.
    //   * the group is wanted in part -- a point read, or a boundary group of a range -- so the
    //     OffsetIndex (which the paged path has to read anyway) says exactly what paging would
    //     fetch, and it wins unless the re-fetching makes it cost the group's own size.
    //
    // That second test is what keeps a point read on the paged path where paging is genuinely
    // cheaper, and it is also why a point read at the shipping defaults now streams: with
    // page_rows >= rows_per_row_group the containing "page" *is* the chunk, so paging fetched the
    // whole group anyway -- the same bytes in 2 x leaves operations instead of one.
    bool stream;
    if (lo == rg_first && grp_hi == rg_end) {
        stream = true;
    } else {
        need_columns(rg);
        co_await load_offset_indexes(rg);
        const auto& elide = elidable_leaves(rg);
        stream = !have_page_index()
              || paged_fetch_is_not_cheaper(lo - rg_first, grp_hi - rg_first, elide);
    }

    const int64_t win = stream ? scan_window_rows : point_window_rows;
    const int64_t hi = std::min({rg_end, _row_hi, lo + win});
    if (hi <= lo) { co_return false; }

    std::vector<format::column_data> colsdata;
    if (!stream) {
        colsdata = co_await decode_paged(rg, lo - _rg_start[rg], hi - _rg_start[rg]);
    } else {
        co_await load_row_group(rg);
        auto img = std::span<const uint8_t>(
                reinterpret_cast<const uint8_t*>(_rg_buf.get()), _rg_buf.size());
        // The whole group is in `img` either way -- eliding a leaf here saves the decode, not
        // the read, because the chunks are contiguous and splitting the read around them would
        // cost more operations than it saves.
        const auto& elide = elidable_leaves(rg);
        rtimer _t{rphase::rg_decode};
        colsdata = format::read_row_range(img, _rg_base, _rgmd, 0,
                                          lo - _rg_start[rg], hi - _rg_start[rg], crypto(),
                                          elide);
    }
    rtimer _tr{rphase::reassemble};
    _rows = reassemble(_ms, _cols, colsdata, size_t(hi - lo));
    _cursor = hi;
    co_return true;
}

future<> pq_reader::load_offset_indexes(size_t rg) {
    if (_oi_rg == rg && _oi_view) { co_return; }
    need_columns(rg);
    _oi_rg = rg;
    _oi_view = nullptr;

    // Cached from an earlier read of this sstable? Then the whole of this function -- a device
    // round trip that gates every page fetch behind it -- is skipped. See cached_footer::oi.
    //
    // The kill switch exists to measure it. This box is shared, so run-to-run comparison is worth
    // little; the only trustworthy A/B is one that flips the cache inside a single process while
    // everything else stays fixed.
    static const bool cache_enabled = [] {
        const char* e = std::getenv("PQ_OFFSET_INDEX_CACHE");
        return !(e && *e == '0');
    }();
    if (_cf && cache_enabled) {
        if (auto it = _cf->oi.find(rg); it != _cf->oi.end()) {
            _oi_view = &it->second;
            ++offset_index_cache_stats_local().hits;
            co_return;
        }
    }
    ++offset_index_cache_stats_local().misses;

    _oi.assign(columns().size(), std::nullopt);
    _oi_view = &_oi;

    // The per-column OffsetIndex blobs sit together near the end of the file, so
    // one read covers all of them.
    int64_t lo = std::numeric_limits<int64_t>::max(), hi = 0;
    for (const auto& cc : columns()) {
        if (!cc.offset_index_offset || !cc.offset_index_length) { continue; }
        lo = std::min(lo, *cc.offset_index_offset);
        hi = std::max(hi, *cc.offset_index_offset + *cc.offset_index_length);
    }
    if (lo >= hi) { co_return; }        // file has no page index; caller falls back
    temporary_buffer<char> buf;
    {
        rtimer _t{rphase::offset_index_io};
        buf = co_await tracked_read(uint64_t(lo), size_t(hi - lo));
    }
    rtimer _tp{rphase::offset_index_parse};
    auto img = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(buf.get()), buf.size());
    for (size_t c = 0; c < _oi.size(); ++c) {
        const auto& cc = columns()[c];
        if (!cc.offset_index_offset || !cc.offset_index_length) { continue; }
        try {
            _oi[c] = format::parse_offset_index_blob(
                    img.subspan(size_t(*cc.offset_index_offset - lo), size_t(*cc.offset_index_length)));
        } catch (...) { _oi[c] = std::nullopt; }
    }

    // Publish, so the next reader of this row group does no I/O at all. Only a complete set is
    // worth keeping: a partial one would send have_page_index() down the fallback path forever
    // on a file whose index failed to parse, which is a slow answer cached as if it were a fast
    // one. The insert is synchronous -- the co_await above has already returned -- so no other
    // reader observes a half-built vector.
    if (_cf && cache_enabled && have_page_index()) {
        auto& slot = _cf->oi[rg];
        slot = _oi;
        const size_t grew = cached_footer::oi_heap_size(slot);
        _cf->oi_retained += grew;
        _sst->grow_pq_footer_cache(grew);
        _oi_view = &slot;
        ++offset_index_cache_stats_local().populations;
    }
}

future<std::vector<format::column_data>> pq_reader::decode_paged(size_t rg, int64_t lo, int64_t hi) {
    co_await load_offset_indexes(rg);
    need_columns(rg);
    const auto& cols = columns();

    bool all = true;
    for (const auto& o : *_oi_view) { if (!o || o->pages.empty()) { all = false; break; } }
    if (!all) {
        // No page index: nothing to seek with, so read the row group whole.
        co_await load_row_group(rg);
        auto img = std::span<const uint8_t>(
                reinterpret_cast<const uint8_t*>(_rg_buf.get()), _rg_buf.size());
        const auto& elide = elidable_leaves(rg);
        rtimer _t{rphase::rg_decode};
        co_return format::read_row_range(img, _rg_base, _rgmd, 0, lo, hi, crypto(), elide);
    }

    // Two extents per column: the dictionary page at the head of the chunk, and the
    // contiguous run of data pages covering the wanted rows. Everything else in the chunk is
    // never read.
    //
    // All of them are issued at once. They were awaited one at a time, which for this schema
    // meant 35 sequential round trips costing 338 us -- 43 % of the whole point read -- for
    // reads that have no dependency on each other whatsoever (design doc 10.4i). Planning the
    // extents first and fetching second also keeps the OffsetIndex arithmetic in one place.
    struct extent {
        size_t  col;
        bool    is_dict;
        uint64_t off;
        size_t  len;
    };
    std::vector<extent> want;
    want.reserve(cols.size() * 2);
    std::vector<format::column_input> in(cols.size());

    // This is where eliding an all-null leaf pays in read *operations* rather than bytes: two
    // extents per leaf per window are not issued at all. On the 28-leaf schema of §10.26 that is
    // 23 of the 28 leaves, so a point read's extent count falls by roughly three quarters.
    const auto& elide = elidable_leaves(rg);

    for (size_t c = 0; c < cols.size(); ++c) {
        if (c < elide.size() && elide[c]) {
            in[c].absent = true;
            continue;
        }
        const auto& cm = *cols[c].meta;
        const auto& pages = (*_oi_view)[c]->pages;
        const size_t i0 = (*_oi_view)[c]->page_for_row(lo);
        const size_t i1 = (*_oi_view)[c]->page_for_row(hi > lo ? hi - 1 : lo);
        if (i0 >= pages.size() || i1 >= pages.size() || i1 < i0) {
            throw std::runtime_error("pq: OffsetIndex does not cover the requested rows");
        }
        if (cm.dictionary_page_offset) {
            const int64_t d0 = *cm.dictionary_page_offset;
            const int64_t d1 = pages.front().offset;    // first data page
            if (d1 > d0) {
                want.push_back({c, true, uint64_t(d0), size_t(d1 - d0)});
            }
        }
        const int64_t p0 = pages[i0].offset;
        const int64_t p1 = pages[i1].offset + pages[i1].compressed_page_size;
        want.push_back({c, false, uint64_t(p0), size_t(p1 - p0)});
        in[c].first_row = pages[i0].first_row_index;
        in[c].pages_file_offset = p0;
    }

    // Which of these extents this sstable already has in memory. Not merely an optimisation of the
    // read: it removes the read. `page_fetch` scales with the number of extents -- 137 us at 7
    // leaves, 324 us at 31, so ~82 us fixed plus ~7.8 us an extent -- so the batch being issued in
    // parallel does not hide them (§10.42).
    //
    // Only ever consulted for an unencrypted file, for the same reason the decompressed-page cache
    // is: what is retained here is ciphertext, but the key that opens it is not this layer's to
    // hold, and the simpler rule is one rule.
    std::vector<std::span<const uint8_t>> spans(want.size());
    std::vector<size_t> misses;
    const bool use_extents = _cf && !crypto() && extent_cache_enabled();
    if (use_extents) {
        for (size_t i = 0; i < want.size(); ++i) {
            auto it = _cf->extents.find(want[i].off);
            if (it != _cf->extents.end() && it->second.len == want[i].len) {
                spans[i] = std::span<const uint8_t>(it->second.bytes);
                ++extent_cache_stats_local().hits;
            } else {
                ++extent_cache_stats_local().misses;
                misses.push_back(i);
            }
        }
    } else {
        for (size_t i = 0; i < want.size(); ++i) { misses.push_back(i); }
    }

    std::vector<temporary_buffer<char>> held;
    if (!misses.empty()) {
        rtimer _tf{rphase::page_fetch};
        std::vector<future<temporary_buffer<char>>> fs;
        fs.reserve(misses.size());
        for (size_t i : misses) {
            fs.push_back(tracked_read(want[i].off, want[i].len));
        }
        held = co_await when_all_succeed(fs.begin(), fs.end());
    }
    // Published after the co_await, synchronously, so nothing observes a half-filled map. The map
    // gives reference stability, so a span into it stays valid for the decode that follows.
    size_t extents_grown = 0;
    for (size_t k = 0; k < misses.size(); ++k) {
        const size_t i = misses[k];
        if (use_extents && read_cache_has_room(held[k].size())) {
            const size_t n = held[k].size();
            auto& slot = _cf->extents[want[i].off];
            slot.len = want[i].len;
            const auto* p = reinterpret_cast<const uint8_t*>(held[k].get());
            slot.bytes.assign(p, p + n);
            _cf->extents_retained += n;
            read_cache_bytes_local().extents += n;
            // Summed and handed over once below, not per extent: see entry_page_cache::_grown.
            extents_grown += n;
            ++extent_cache_stats_local().populations;
            spans[i] = std::span<const uint8_t>(slot.bytes);
        } else {
            spans[i] = std::span<const uint8_t>(
                    reinterpret_cast<const uint8_t*>(held[k].get()), held[k].size());
        }
    }
    if (extents_grown) { _sst->grow_pq_footer_cache(extents_grown); }
    for (size_t i = 0; i < want.size(); ++i) {
        if (want[i].is_dict) {
            in[want[i].col].dict = spans[i];
        } else {
            in[want[i].col].pages = spans[i];
        }
    }

    rtimer _td{rphase::decode_cpu};
    // The cache only makes sense on this path. A scan streams each page once and moves on, so
    // caching there would retain the whole file to serve nothing; a point read comes back to the
    // same page from a fresh reader, which is the case that pays.
    std::optional<entry_page_cache> pcache;
    static const bool pages_enabled = [] {
        const char* e = std::getenv("PQ_PAGE_CACHE");
        return !(e && *e == '0');
    }();
    if (_cf && pages_enabled) { pcache.emplace(_cf, _sst); }
    co_return format::decode_columns(in, _rgmd, 0, lo, hi, crypto(),
                                     pcache ? &*pcache : nullptr);
}

void pq_reader::close_partition() {
    if (!_open) { return; }
    if (_filter && _filter->current_tombstone()) {
        auto res = _filter->apply(position_in_partition_view::after_all_clustered_rows(), {});
        for (auto&& rt : res.rts) {
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(rt)));
        }
    }
    push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, partition_end()));
    _open = false;
}

void pq_reader::emit_row(const row& r) {
    bool just_opened = false;
    std::vector<value> pk(r.key.begin(), r.key.begin() + long(_n_pk));
    if (!_open || pk != _open_pk) {
        close_partition();
        auto dk = key_of(pk);
        // The index gives a lower bound but not always an upper one, and the
        // window it yields is a superset anyway. Rows are in token order, so once
        // a partition sorts past the range's end there is nothing further to
        // find -- that is what keeps a point read from decoding to EOF.
        dht::ring_position_comparator cmp(*_schema);
        if (_pr->end() && cmp(dht::ring_position_view(dk), _pr->end()->value()) > 0) {
            _end_of_stream = true;
            _open_pk = std::move(pk);
            _skipping = true;
            return;
        }
        if (!_pr->contains(dk, cmp)) {
            _open_pk = std::move(pk);
            _open = false;
            _skipping = true;
            return;
        }
        // The partition tombstone rides on every row of the partition, so the
        // first row of the run carries it.
        tombstone pt;
        if (r.part_del) {
            pt = tombstone(r.part_del->timestamp,
                           gc_clock::time_point(gc_clock::duration(r.part_del->local_deletion_time)));
        }
        _open_dk = dk;
        _filter.emplace(*_schema,
                query::clustering_key_filter_ranges::get_ranges(*_schema, _slice, dk.key()),
                streamed_mutation::forwarding::no);
        push_mutation_fragment(mutation_fragment_v2(*_schema, _permit,
                partition_start(std::move(dk), pt)));
        _open_pk = std::move(pk);
        _open = true;
        _skipping = false;
        just_opened = true;
    }
    if (_skipping) { return; }

    // Static cells were replayed onto every row of the partition; they belong to
    // the partition, not the row, so they come back out as a static_row emitted
    // once, immediately after the partition_start.
    if (just_opened) {
        ::row st;
        for (const auto& [k, c] : r.cells) {
            if (k < _static_base) { continue; }
            const column_definition& cdef = _schema->static_column_at(column_id(k - _static_base));
            if (!c.v && c.live) { continue; }
            if (!c.live) {
                auto ldt = gc_clock::time_point(gc_clock::duration(c.local_deletion_time.value_or(0)));
                st.append_cell(column_id(k - _static_base),
                               atomic_cell::make_dead(c.timestamp, ldt));
                continue;
            }
            auto raw = encode(_cols[_n_pk + _n_ck + k].type, *c.v);
            if (c.ttl) {
                auto expiry = gc_clock::time_point(
                        gc_clock::duration(c.local_deletion_time.value_or(0)));
                st.append_cell(column_id(k - _static_base),
                        atomic_cell::make_live(*cdef.type, c.timestamp, bytes_view(raw),
                                               expiry, gc_clock::duration(*c.ttl)));
            } else {
                st.append_cell(column_id(k - _static_base),
                        atomic_cell::make_live(*cdef.type, c.timestamp, bytes_view(raw)));
            }
        }
        for (const auto& [k, cc] : r.collections) {
            if (k < _static_base) { continue; }
            const column_definition& cdef =
                    _schema->static_column_at(column_id(k - _static_base));
            st.append_cell(column_id(k - _static_base),
                    cdef.is_counter() ? build_counter(cc) : build_collection(cdef, cc));
        }
        if (!st.empty()) {
            // The filter has to be told about the static row even though the slice
            // cannot exclude it, because the walker tracks position and every
            // later advance must be monotonic. mx does the same; skipping it left
            // the walker behind the first clustering position and it then reported
            // out-of-range, silently dropping every row in the partition.
            ::static_row sr(std::move(st));
            if (_filter->apply(sr) == mutation_fragment_filter::result::emit) {
                push_mutation_fragment(mutation_fragment_v2(*_schema, _permit,
                        std::move(sr)));
            }
        }
    }

    // A range tombstone change: rebuild its bound from the stored prefix length,
    // weight and region, and emit it in place. Everything past prefix_len in the
    // clustering columns is padding the writer put there.
    if (r.rtc) {
        // The prefix must always be present, even when it is empty: the bounds
        // that cover a whole partition -- before_all_clustered_rows and
        // after_all_clustered_rows -- are an *empty* clustering prefix carrying
        // weight -1 or +1, not an absent one. A position with no key at all is
        // not a valid clustered position, and comparing one against those bounds
        // silently yields nonsense rather than failing, which sends the
        // clustering_ranges_walker past every range and drops the whole partition.
        std::vector<bytes> parts;
        parts.reserve(size_t(std::max<int32_t>(r.rtc->prefix_len, 0)));
        for (int32_t i = 0; i < r.rtc->prefix_len && size_t(i) < _n_ck; ++i) {
            parts.push_back(encode(_cols[_n_pk + size_t(i)].type, r.key[_n_pk + size_t(i)]));
        }
        auto pos = position_in_partition(partition_region(r.rtc->region),
                                         bound_weight(r.rtc->weight),
                                         clustering_key_prefix::from_exploded(*_schema, std::move(parts)));
        tombstone t;
        if (r.rtc->tomb) {
            t = tombstone(r.rtc->tomb->timestamp,
                          gc_clock::time_point(gc_clock::duration(r.rtc->tomb->local_deletion_time)));
        }
        // The filter decides what actually goes out: a change outside the slice
        // is swallowed, and one that opens a range spanning into the slice is
        // re-emitted at the slice boundary instead.
        auto res = _filter->apply(pos, t);
        for (auto&& rt : res.rts) {
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(rt)));
        }
        return;
    }

    // A placeholder row exists only to carry the static row or the partition
    // tombstone; it has no clustering row of its own.
    if (r.no_ck) { return; }

    std::vector<bytes> ck_parts;
    ck_parts.reserve(_n_ck);
    for (size_t i = 0; i < _n_ck; ++i) {
        ck_parts.push_back(encode(_cols[_n_pk + i].type, r.key[_n_pk + i]));
    }
    auto ck = clustering_key::from_exploded(*_schema, ck_parts);

    // Honour the query's clustering slice. The reader does not *seek* to it --
    // the ColumnIndex's per-page clustering bounds would let a later version do
    // that -- but it must not emit rows outside it, and any range tombstone the
    // slice boundary cuts has to be re-opened there.
    auto row_res = _filter->apply(position_in_partition_view::for_key(ck));
    for (auto&& rt : row_res.rts) {
        push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(rt)));
    }
    if (row_res.action != mutation_fragment_filter::result::emit) { return; }

    ::row cells;
    for (const auto& [k, c] : r.cells) {
        if (k >= _static_base) { continue; }       // static: already emitted above
        if (!c.v && c.live) { continue; }          // absent, not deleted
        const column_definition& cdef = _schema->regular_column_at(column_id(k));
        if (!c.live) {
            auto ldt = gc_clock::time_point(gc_clock::duration(c.local_deletion_time.value_or(0)));
            cells.append_cell(column_id(k), atomic_cell::make_dead(c.timestamp, ldt));
            continue;
        }
        auto raw = encode(_cols[_n_pk + _n_ck + k].type, *c.v);
        if (c.ttl) {
            auto expiry = gc_clock::time_point(
                    gc_clock::duration(c.local_deletion_time.value_or(0)));
            cells.append_cell(column_id(k),
                    atomic_cell::make_live(*cdef.type, c.timestamp, bytes_view(raw),
                                           expiry, gc_clock::duration(*c.ttl)));
        } else {
            cells.append_cell(column_id(k),
                    atomic_cell::make_live(*cdef.type, c.timestamp, bytes_view(raw)));
        }
    }

    // Non-frozen collections. Static ones were emitted with the static row above.
    for (const auto& [k, cc] : r.collections) {
        if (k >= _static_base) { continue; }
        const column_definition& cdef = _schema->regular_column_at(column_id(k));
        cells.append_cell(column_id(k),
                cdef.is_counter() ? build_counter(cc) : build_collection(cdef, cc));
    }

    row_marker rm;
    if (r.marker) {
        rm = (r.marker->ttl && r.marker->expiry)
           ? row_marker(r.marker->timestamp,
                        gc_clock::duration(*r.marker->ttl),
                        gc_clock::time_point(gc_clock::duration(*r.marker->expiry)))
           : row_marker(r.marker->timestamp);
    }
    row_tombstone rt;
    if (r.row_del) {
        auto sh = tombstone(r.row_del->timestamp,
                gc_clock::time_point(gc_clock::duration(r.row_del->local_deletion_time)));
        // Rebuild both halves. Passing only one makes the regular tombstone as
        // strong as the shadowable one, which deletes cells that should survive.
        auto reg = r.row_del_regular
                ? tombstone(r.row_del_regular->timestamp,
                        gc_clock::time_point(
                                gc_clock::duration(r.row_del_regular->local_deletion_time)))
                : tombstone();
        rt = row_tombstone(reg, shadowable_tombstone(sh));
    }
    push_mutation_fragment(mutation_fragment_v2(*_schema, _permit,
            clustering_row(std::move(ck), rt, rm, std::move(cells))));
}

} // namespace

mutation_reader make_reader(
        sstables::shared_sstable sst,
        schema_ptr query_schema,
        reader_permit permit,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding,
        sstables::read_monitor& mon) {
    // The *row* slice is honoured, in pq_reader::emit_row. The *column* selection deliberately is
    // not, and this is the one place to record why, because §10.26 named it as a fix and it turns
    // out to be a fix that cannot be made here.
    //
    // A CQL row exists if its marker is live *or* any of its cells is. Scylla's compacting reader
    // decides that from the fragment it is handed, so a reader that drops the cells of a column
    // the query did not select turns a live row into an empty one whenever that column was the
    // only thing keeping it alive -- and a row created by UPDATE has no marker at all, which makes
    // the case ordinary rather than exotic. `SELECT b` on a partition whose rows only ever had `a`
    // written must return rows with b = null; dropping `a` returns nothing. Cassandra hits the
    // same wall and solves it by distinguishing *fetched* from *queried* columns -- it reads every
    // regular column from storage and projects afterwards -- and mx does the same thing by simply
    // never consulting the slice. Diverging here would make pq answer differently from every other
    // format in the tree.
    //
    // What §10.26 measured as the prize -- read *operations*, which on a shredded schema are
    // dominated by the all-null metadata leaves rather than by user columns -- is claimed instead
    // by pq_reader::elidable_leaves(), which is driven by the file's own statistics and is
    // therefore both safe and available to every path, including `SELECT *` and compaction.
    // See test_pq_restricted_slice_still_returns_every_cell for the case that pins this.
    auto rd = make_mutation_reader<pq_reader>(std::move(sst), std::move(query_schema),
                                              std::move(permit), range, slice, mon, true);
    if (fwd) {
        rd = make_forwardable(std::move(rd));
    }
    return rd;
}

mutation_reader make_full_scan_reader(
        sstables::shared_sstable sst,
        schema_ptr schema,
        reader_permit permit,
        tracing::trace_state_ptr,
        sstables::read_monitor& mon) {
    // A full scan wants everything, so the schema's full slice is the right one.
    auto& s_ref = *schema;
    return make_mutation_reader<pq_reader>(std::move(sst), schema,
            std::move(permit), query::full_partition_range, s_ref.full_slice(), mon, false);
}

void reader_profile_reset() {
    rprof::ns.fill(0);
    rprof::hits.fill(0);
    format::decode_profile_reset();
}

std::string reader_profile_report() {
    if (!rprof::enabled) {
        return "reader profile disabled (set PQ_READER_PROFILE=1)\n";
    }
    // In enum order, and every one of them disjoint from the others -- see the note on rphase.
    static constexpr const char* names[] = {
        "footer_io", "footer_parse", "schema_recover", "index_lookup",
        "rg_materialise", "offset_index_io", "offset_index_parse",
        "rg_fetch", "rg_decode",
        "page_fetch", "decode_cpu",
        "reassemble",
    };
    static_assert(std::size(names) == size_t(rphase::_count));
    uint64_t total = 0;
    for (auto v : rprof::ns) { total += v; }
    std::string out = "  reader phase        total ms    calls     us/call     share\n";
    for (size_t i = 0; i < size_t(rphase::_count); ++i) {
        const double ms = double(rprof::ns[i]) / 1e6;
        const double per = rprof::hits[i] ? double(rprof::ns[i]) / 1e3 / double(rprof::hits[i]) : 0.0;
        const double share = total ? 100.0 * double(rprof::ns[i]) / double(total) : 0.0;
        out += fmt::format("  {:<18} {:>9.1f} {:>8} {:>11.2f} {:>8.1f} %\n",
                           names[i], ms, rprof::hits[i], per, share);
    }
    out += fmt::format("  {:<18} {:>9.1f}\n", "instrumented", double(total) / 1e6);
    // Appended rather than merged: these are *inside* rg_decode and decode_cpu, so folding them
    // into the table above would double-count them and dilute every share in it -- which is
    // exactly the defect 10.27 records against the old page_decode phase.
    out += format::decode_profile_report();
    return out;
}

} // namespace sstables::parquet
