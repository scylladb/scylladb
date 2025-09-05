/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "bti_index.hh"
#include "sstables/consumer.hh"
#include "sstables/processing_result_generator.hh"
#include "bti_node_reader.hh"
#include "trie_traversal.hh"
#include "bti_key_translation.hh"
#include <seastar/core/fstream.hh>
#include <fmt/format.h>
#include <fmt/std.h>

template <>
struct fmt::formatter<sstables::trie::trail_entry> : fmt::formatter<string_view> {
    auto format(const sstables::trie::trail_entry& r, fmt::format_context& ctx) const
            -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(), "trail_entry(id={} child_idx={} payload_bits={})", r.pos, r.child_idx, r.payload_bits);
    }
};

namespace sstables::trie {

// Each row index trie in Rows.db is followed by a header containing some partition metadata.
// The partition's entry in Partitions.db points into that header.
struct row_index_header {
    // The partiition key, in BIG serialization format (i.e. the same as in Data.db or Index.db).
    sstables::key partition_key = bytes();
    // The global position of the root node of this partition's row index within Rows.db.
    uint64_t trie_root;
    // The global position of the partition inside Data.db.
    uint64_t data_file_offset;
    // The partition tombstone of this partition.
    sstables::deletion_time partition_tombstone;
};

// A partitions entry in Rows.db contains the full partition key.
// This means that the entry has variable size, and can span multiple pages.
// So, unlike with every other part of BTI, an asynchronous parser is needed for it.
//
// The header parsed here is produced by `bti_row_index_writer::finish()`.
struct row_index_header_parser : public data_consumer::continuous_data_consumer<row_index_header_parser> {
    using processing_result = data_consumer::processing_result;
    using proceed = data_consumer::proceed;

    row_index_header _result;
    bool _finished = false;
    temporary_buffer<char> _pk;
    temporary_buffer<char>* _processing_data = nullptr;
    processing_result_generator _gen;

    row_index_header_parser(reader_permit rp, input_stream<char>&& input, uint64_t start, uint64_t maxlen)
        : continuous_data_consumer(std::move(rp), std::move(input), start, maxlen)
        , _gen(do_process_state())
    {}
    void verify_end_state() {
        if (!_finished) {
            throw sstables::malformed_sstable_exception(fmt::format("row_index_header_parser: verify_end_state: not finished"));
        }
    }
    bool non_consuming() const {
        return false;
    }
    data_consumer::processing_result process_state(temporary_buffer<char>& data) {
        _processing_data = &data;
        return _gen.generate();
    }
    processing_result_generator do_process_state() {
        expensive_log("row_index_header_parser: reading short length bytes for partition key");
        co_yield this->read_short_length_bytes(*_processing_data, _pk);
        _result.partition_key = sstables::key(to_bytes(to_bytes_view(_pk)));
        expensive_log("row_index_header_parser: read partition key, size={}", _pk.size());

        // The start of of this vint is used as a reference point
        // for the delta encoded in the next vint.
        uint64_t trie_root_delta_base = this->position() - (*_processing_data).size();
        expensive_log("row_index_header_parser: reading unsigned vint for data file offset, delta_base={}", trie_root_delta_base);
        co_yield this->read_unsigned_vint(*_processing_data);
        _result.data_file_offset = this->_u64;
        expensive_log("row_index_header_parser: read data file offset={}", _result.data_file_offset);

        // This vint is the offset from the root of the intra-partition index
        // to the start of the previous vint.
        expensive_log("row_index_header_parser: reading unsigned vint for trie root delta");
        co_yield this->read_unsigned_vint(*_processing_data);
        _result.trie_root = trie_root_delta_base - this->_u64;
        expensive_log("row_index_header_parser: read trie root delta={}, calculated trie_root={}", this->_u64, _result.trie_root);

        // Note: the partition tombstone in BTI files (here: Rows.db) has a different
        // encoding than in BIG files (e.g. Data.db).
        expensive_log("row_index_header_parser: reading 8 bytes for tombstone marker");
        co_yield this->read_8(*_processing_data);
        expensive_log("row_index_header_parser: read tombstone marker=0x{:02x}", this->_u8);
        if (this->_u8 == 0x80) {
            // If the partition tombstone starts with 0x80 then the partition tombstone is empty.
            // (And there are no more bytes in the header).
            _result.partition_tombstone = sstables::deletion_time::make_live();
            _finished = true;
            expensive_log("row_index_header_parser: empty tombstone, finished parsing");
            co_return;
        }

        if (this->_u8 == 0xc0) [[unlikely]] {
            // If the partition tombstone starts with 0xc0,
            // then the next 8 bytes contain a negative timestamp.
            //
            // Note: this is a Scylla-specific extension.
            // In Cassandra, the partition tombstone may only start with
            // 0x80 or with a byte in range [0x00;0x7f].
            // (And Cassandra crashes if you write a negative timestamp to a BTI sstable).
            //
            // It's likely that nobody ever uses negative timestamps in practice.
            // (Our tests do, though).
            expensive_log("row_index_header_parser: reading 64 bytes for negative timestamp");
            co_yield this->read_64(*_processing_data);
            _result.partition_tombstone.marked_for_delete_at = this->_u64;
            expensive_log("row_index_header_parser: read negative timestamp={}", _result.partition_tombstone.marked_for_delete_at);
        } else {
            // If the first byte isn't special, then the first bit must be 0
            // (i.e. we could assert here that `_u8 & 0x80 == 0`)
            // and the next 63 bits contain a positive timestamp.
            expensive_log("row_index_header_parser: reading 56 bytes for positive timestamp continuation");
            co_yield this->read_56(*_processing_data);
            _result.partition_tombstone.marked_for_delete_at = (uint64_t(this->_u8 & 0x7f) << 56) | this->_u64;
            expensive_log("row_index_header_parser: read timestamp parts, calculated positive timestamp={}", _result.partition_tombstone.marked_for_delete_at);
        }
        expensive_log("row_index_header_parser: reading 32 bytes for deletion time");
        co_yield this->read_32(*_processing_data);
        _result.partition_tombstone.local_deletion_time = this->_u32;
        expensive_log("row_index_header_parser: read deletion time={}, finished parsing", this->_u32);
        _finished = true;
    }
};

static future<row_index_header> read_row_index_header(cached_file& _file, uint64_t pos, reader_permit rp) {
    // TODO: Should there be a fast path for the case where the entire
    // parse fits into the current page?
    // We're involving so much code in parsing something that's usually just several bytes long...
    auto ctx = row_index_header_parser(
        std::move(rp),
        make_file_input_stream(file(make_shared<cached_file_impl>(_file)), pos, _file.size() - pos, {.buffer_size = 4096, .read_ahead = 0}),
        pos,
        _file.size() - pos);
    std::exception_ptr ex;
    try {
        co_await ctx.consume_input();
        co_await ctx.close();
        expensive_log("read_row_index_header result={}", ctx._result.data_file_offset);
        co_return std::move(ctx._result);
    } catch (...) {
        ex = std::current_exception();
    }
    co_await ctx.close();
    std::rethrow_exception(ex);
}

struct payload_result {
    uint8_t bits;
    // Note: our payload-reading helpers return the payload bytes by view,
    // and this view might be longer than the actual payload.
    // (It extends to the end of the page).
    // The caller is supposed to know the actual size based on `bits`
    // and the context where this is used.
    const_bytes bytes;
};

// Note: the cursor can be uninitialized, or it can be pointing at:
// - Exactly at a node.
// - Before the first child of a node.
// - Between two children of a node.
// - After all children of a node.
//
// A node might be carrying a payload.
// A node carrying a payload is also called a "payloaded" node,
// or a "key" node. (Because each payloaded node corresponds to some
// key inserted into the trie).
//
// In Partitions.db, a node is payloaded iff it's a leaf.
// Every partition entry added to the index is represented by
// a leaf.
// The byte chain leading from root to the payloaded leaf is
// a prefix of the partition key pointed-to by this node.
// 
// In Rows.db, every leaf has a payload, but inner nodes can
// have a payload too.
// Every clustering key block is represented by a payloaded node,
// and the byte chain leading from root to that node
// is some arbitrary separator string,
// smaller-or-equal to the first clustering key in the block,
// but strictly greater than the last clustering key in the preceding block.  
class trie_cursor {
    bti_node_reader _in;
    // A stack holding the path from the root to the currently visited node.
    // When initialized, _path[0] is root.
    decltype(traversal_state::trail) _trail;
public:
    trie_cursor(bti_node_reader);
    ~trie_cursor() = default;
    trie_cursor& operator=(const trie_cursor&) = default;
    // Checks whether the cursor has been initialized (by `set_to`).
    // Preconditions: none.
    bool initialized() const;
    // Walks down from the given root along the given key,
    // until the key is fully traversed or there's no child matching the next key byte.
    // Postcondition: initialized()
    future<> set_to(uint64_t root, comparable_bytes_iterator auto&& key);
    // Moves the cursor forward to the closest payloaded node to the
    // right of the current position.
    // (Or to EOF, if there's no such node).
    // Precondition: initialized() && !eof()
    future<> step();
    // Moves the cursor to the closest payloaded node to the left
    // of the current position.
    // (Or, if there's no such node, moves to the first payloaded node).
    // (Or, if there's no payloaded node, moves to EOF).
    // If there is no previous key, doesn't do anything.
    future<> step_back();
    // Returns the payload (if any) of the current node.
    payload_result payload() const;
    // Checks whether the cursor in the EOF position.
    //
    // Preconditions: points at eof or a node.
    bool eof() const;
    // Preconditions: none.
    // Postconditions: !initialized()
    void reset();
    // Checks whether the cursor is at a leaf position,
    // i.e. points exactly at a leaf node or points past past a leaf node.
    //
    // By construction, the Partitions.db represents each inserted partition key,
    // with a leaf node that corresponds to some prefix of the encoded key.
    // So if `at_leaf()` is false after a `set_to(..., pk)`, then the queried partiion
    // key is not present in the trie.
    //
    // Precondition: initialized()
    bool at_leaf() const;
    // If the cursor points past a leaf node, makes it point
    // exactly at the leaf node instead.
    // 
    // Precondition: at_leaf()
    void snap_to_leaf();
    // Checks if the cursor points at a payloaded node.
    //
    // Precondition: initialized()
    bool at_key() const;
    // Direct accessor to the internal state, for debug logs.
    const ancestor_trail& trail() const;
};

enum class set_result {
    definitely_not_a_match,
    possible_match,
};

// An index cursor, which is effectively a pair of trie cursors -- one into Partitions.db, one into Rows.db.
class index_cursor {
    // A cursor into Partitions.db.
    // Starts unintialized, gets initialized by set_before_partition/set_after_partition.
    trie_cursor _partition_cursor;
    // A cursor into Rows.db.
    // Starts unintialized, gets initialized by set_before_row/set_after_row,
    // becomes unitialized again after partition cursor is moved.
    trie_cursor _row_cursor;
    // Holds the row index header for the current partition, iff the current partition has a row index.
    // This is kept in sync with the partition cursor.
    // (I.e. filling it doesn't require a separate method call).
    std::optional<row_index_header> _partition_metadata;
    // _row_cursor reads the tries written in Rows.db.
    // Reference wrapper to allow for copying the cursor.
    bti_node_reader _in_row;
    // It's supposed to account for the memory consumed by the cursor.
    // FIXME: it accounts for the memory used by _in_row,
    // but doesn't account for the 8 kiB used by the trie cursors.
    // Does it need to?
    reader_permit _permit;
    uint64_t _par_root;
private:
    // If the current partition has a row index, reads its header.
    future<> maybe_read_metadata();
    // The colder part of set_after_row, just to hint at inlining the hotter part.
    future<> set_after_row_cold(lazy_comparable_bytes_from_clustering_position&);
public:
    index_cursor(uint64_t par_root, bti_node_reader par, bti_node_reader row, reader_permit);
    index_cursor& operator=(const index_cursor&) = default;
    // Returns the data file position of the cursor. Can only be called after the cursor is set.
    //
    // If the row cursor is set, returns the position of the pointed-to clustering key block.
    // Otherwise, if the partition cursor is set to a partition, returns the position of the partition.
    // Otherwise, if the partition cursor is set to EOF, returns `file_size`.
    uint64_t data_file_pos(uint64_t file_size) const;
    // If the row index is set to a clustering key block, returns the range tombstone active at the start of the block.
    // Otherwise returns an empty tombstone.
    tombstone open_tombstone() const;
    // Returns the current partition's row index header, if it has one.
    const std::optional<row_index_header>& partition_metadata() const;
    // Sets the partition cursor to *some* position smaller-or-equal to all entries greater-or-equal to K.
    // See the comments at trie_cursor::set_before for more elaboration.
    //
    // Resets the row cursor.
    future<> set_before_partition(lazy_comparable_bytes_from_ring_position& K);
    // Used for single-partition reads.
    // Tries to set the partition cursor to the given partition key.
    //
    // If it returns `set_result::definitely_not_a_match`,
    // then the key is definitely not present in the sstable.
    // the cursor becomes broken and cannot be used again.
    //
    // If it returns `set_result::possible_match`,
    // then the key might be present and might not be present.
    // The user of the cursor must read the partition key
    // (from Rows.db, if the partition has an entry there,
    // or from Data.db otherwise) to know.
    //
    // Resets the row cursor.
    future<set_result> set_to_partition(lazy_comparable_bytes_from_ring_position& K, std::byte hash_byte);
    // Sets the partition cursor to *some* position strictly greater than all entries smaller-or-equal to K.
    // See the comments at trie_cursor::set_after for details.
    //
    // Resets the row cursor.
    future<> set_after_partition(lazy_comparable_bytes_from_ring_position& K);
    // Moves the partition cursor to the next position (next partition or eof).
    // Resets the row cursor.
    future<> next_partition();
    // Sets the row cursor to *some* position (within the current partition)
    // smaller-or-equal to all present entries greater-or-equal to K.
    future<> set_before_row(lazy_comparable_bytes_from_clustering_position& K);
    // Sets the row cursor to *some* position (within the current partition)
    // greater than all present entries smaller-or-equal to K.
    //
    // If the row cursor would go beyond the end of the partition,
    // it instead resets moves the partition cursor to the next partition
    // and resets the row cursor.
    future<> set_after_row(lazy_comparable_bytes_from_clustering_position& K);
    // Checks if the row cursor is initialized.
    bool row_cursor_set() const;
    // Checks if the partition cursor is initialized.
    bool partition_cursor_set() const;
    // Return the offset to the last clustering block
    // (i.e. the offset of the second-to-last payloaded node in the row index)
    // in the current partition, if a row index exists.
    future<std::optional<uint64_t>> last_block_offset() const;
    // Return the hash byte (if present) of the partition entry.
    std::optional<std::byte> partition_hash() const;
};

class bti_index_reader : public sstables::abstract_index_reader {
    bti_node_reader _in_row;
    // We need the schema solely to parse the partition keys serialized in row index headers.
    schema_ptr _s;
    // Supposed to account the memory usage of the index reader.
    // FIXME: it doesn't actually do that yet.
    // FIXME: it should also mark the permit as blocked on disk?
    reader_permit _permit;
    // The index is, in essence, a pair of cursors.
    index_cursor _lower;
    index_cursor _upper;
    // Partitions.db doesn't store the size of Data.db, so the cursor doesn't know by itself
    // what Data.db position to return when its position is EOF. We need to pass the file size
    // from the above.
    uint64_t _total_file_size;

    // FIXME: by contract, the index reader is immediately initialized to position 0
    // after construction.
    //
    // But we don't really want to actually walk down to the leftmost node in the trie,
    // because that would be a waste of work. Nothing actually uses the "initialized to 0"
    // property. The first thing the MX reader does with the index reader is to advance it
    // to the actual target, via `advance_to(partition_range)` or `advance_lower_and_check_if_present()`.
    //
    // So to respect the contract, in every method which in theory might assume the
    // "initialized to 0" property, we have to check if the index reader is initialized to 0,
    // and if not, do so.
    //
    // That's awkward. It would be better to just demand that the caller
    // doesn't do anything with the reader before initializing it.
    // 
    // It would get rid of those two methods below,
    // and also of the `partition_data_ready()` dance which is only ever
    // false when the index reader is in the never-used
    // "initialized-to-0-but-not-really" state.
    // (This applies to both the BIG and the BTI index readers).
    future<> init_lower_bound();
    future<> maybe_init_lower_bound();
public:
    bti_index_reader(bti_node_reader partitions_db, bti_node_reader rows_db, uint64_t root_pos, uint64_t total_file_size, schema_ptr s, reader_permit);
    // Implementation of the `abstract_index_reader` interface.
    virtual future<> close() noexcept override;
    virtual sstables::data_file_positions_range data_file_positions() const override;
    virtual future<std::optional<uint64_t>> last_block_offset() override;
    virtual future<bool> advance_lower_and_check_if_present(dht::ring_position_view key) override;
    virtual future<> advance_to_next_partition() override;
    virtual sstables::indexable_element element_kind() const override;
    virtual future<> advance_past_definitely_present_partition(const dht::decorated_key& dk) override;
    virtual future<> advance_to_definitely_present_partition(const dht::decorated_key& dk) override;
    virtual future<> advance_to(position_in_partition_view pos) override;
    virtual std::optional<sstables::deletion_time> partition_tombstone() override;
    virtual std::optional<partition_key> get_partition_key() override;
    virtual bool partition_data_ready() const override;
    virtual future<> read_partition_data() override;
    virtual future<> advance_reverse(position_in_partition_view pos) override;
    virtual future<> advance_to(const dht::partition_range& range) override;
    virtual future<> advance_reverse_to_next_partition() override;
    virtual future<> advance_upper_past(position_in_partition_view pos) override;
    virtual std::optional<sstables::open_rt_marker> end_open_marker() const override;
    virtual std::optional<sstables::open_rt_marker> reverse_end_open_marker() const override;
    virtual bool eof() const override;
    virtual future<> prefetch_lower_bound(position_in_partition_view pos) override;
};

trie_cursor::trie_cursor(bti_node_reader in)
    : _in(std::move(in))
{
}

// Documented near the declaration.
future<> trie_cursor::set_to(uint64_t root, comparable_bytes_iterator auto&& key) {
    auto result = co_await trie::traverse(_in, std::move(key), root);
    _trail = std::move(result.trail);
}

// Documented near the declaration.
future<> trie_cursor::step() {
    co_await trie::step(_in, _trail);
}

// Documented near the declaration.
future<> trie_cursor::step_back() {
    co_await trie::step_back(_in, _trail);
}

payload_result trie_cursor::payload() const {
    return payload_result{
        .bits = _trail.back().payload_bits,
        .bytes = _in.get_payload(_trail.back().pos),
    };
}

bool trie_cursor::eof() const {
    return _trail[0].child_idx == _trail[0].n_children;
}

bool trie_cursor::initialized() const {
    return !_trail.empty();
}

void trie_cursor::reset() {
    _trail.clear();
}

void trie_cursor::snap_to_leaf() {
    _trail.back().child_idx = -1;
}

bool trie_cursor::at_leaf() const {
    return _trail.back().n_children == 0;
}

bool trie_cursor::at_key() const {
    return _trail.back().payload_bits && _trail.back().child_idx == -1;
}

const ancestor_trail& trie_cursor::trail() const {
    return _trail;
}

index_cursor::index_cursor(uint64_t par_root, bti_node_reader par, bti_node_reader row, reader_permit permit)
    : _partition_cursor(par)
    , _row_cursor(row)
    , _in_row(row)
    , _permit(permit)
    , _par_root(par_root)
{}

bool index_cursor::row_cursor_set() const {
    return _row_cursor.initialized();
}

bool index_cursor::partition_cursor_set() const {
    return _partition_cursor.initialized();
}

static int64_t partition_payload_to_pos(const payload_result& p) {
    constexpr uint8_t HASH_BYTE_FLAG = 0x8;
    uint64_t hash_byte_skip = p.bits & HASH_BYTE_FLAG ? 1 : 0;
    uint64_t bytewidth = (p.bits & HASH_BYTE_FLAG - 1) + 1;
    [[assume(bytewidth <= 8)]];
    uint64_t be_result = 0;
    std::memcpy(&be_result, p.bytes.data() + hash_byte_skip, bytewidth);
    // Note the `int64_t` cast.
    // We extracted a *signed* big-endian integer of width n:=`bytewidth` from the payload into
    // the first bytes of `be_result`,
    // and here we are sign-extending it into a `int64_t`.
    auto result = int64_t(seastar::be_to_cpu(be_result)) >> 8*(8 - bytewidth);
    expensive_log("payload_to_offset: be_result={:016x} bits={}, bytes={}, result={:016x}", be_result, p.bits, fmt_hex(p.bytes), uint64_t(result));
    return result;
}

static int64_t row_payload_to_offset(const payload_result& p) {
    constexpr uint8_t TOMBSTONE_FLAG = 0x8;
    uint64_t bits = p.bits & ~TOMBSTONE_FLAG;
    uint64_t be_result = 0;
    std::memcpy(&be_result, p.bytes.data(), bits);
    auto result = seastar::be_to_cpu(be_result) >> 8*(8 - bits);
    return result;
}

static inline std::generator<std::span<const std::byte>> single_fragment_generator(std::span<const std::byte> only_frag) {
    co_yield only_frag;
}

future<std::optional<uint64_t>> index_cursor::last_block_offset() const {
    if (!_partition_metadata) {
        expensive_log("last_block_offset: no partition metadata");
        co_return std::nullopt;
    }

    auto cur = _row_cursor;
    const std::byte past_all_keys[] = {std::byte(0xff)};

    co_await cur.set_to(_partition_metadata->trie_root, single_fragment_generator(past_all_keys).begin());
    // Sic. The last payloaded node points to the end of the partition.
    // The second-to-last payloaded node points to the last clustering key block.
    co_await cur.step_back();
    co_await cur.step_back();

    auto result = _partition_metadata->data_file_offset + row_payload_to_offset(cur.payload());
    expensive_log("last_block_offset: {}", result);
    co_return result;
}

std::optional<std::byte> index_cursor::partition_hash() const {
    auto p = _partition_cursor.payload();
    if (p.bits & 0x8) {
        return p.bytes[0];
    }
    return std::nullopt;
}

uint64_t index_cursor::data_file_pos(uint64_t file_size) const {
    expensive_log("index_cursor::data_file_pos this={} initialized={}", fmt::ptr(this), _partition_cursor.initialized());
    expensive_assert(_partition_cursor.initialized());
    if (_partition_metadata) {
        if (!_row_cursor.initialized()) {
            expensive_log("index_cursor::data_file_pos this={} from empty row cursor: {}", fmt::ptr(this), _partition_metadata->data_file_offset);
            return _partition_metadata->data_file_offset;
        }
        const auto p = _row_cursor.payload();
        auto res = _partition_metadata->data_file_offset + row_payload_to_offset(p);
        expensive_log("index_cursor::data_file_pos this={} from row cursor: {} bytes={} bits={}", fmt::ptr(this), res, fmt_hex(p.bytes), p.bits & 0x7);
        return res;
    }
    if (!_partition_cursor.eof()) {
        auto res = partition_payload_to_pos(_partition_cursor.payload());
        expensive_assert(res < 0);
        expensive_log("index_cursor::data_file_pos this={} from partition cursor: {}", fmt::ptr(this), ~res);
        return ~res;
    }
    expensive_log("index_cursor::data_file_pos this={} from eof: {}", fmt::ptr(this), file_size);
    return file_size;
}

tombstone index_cursor::open_tombstone() const {
    expensive_log("index_cursor::open_tombstone this={}", fmt::ptr(this));
    expensive_assert(_partition_cursor.initialized());
    expensive_assert(_partition_metadata.has_value());
    if (!_row_cursor.initialized() || _row_cursor.eof()) {
        // This branch never happens in practice.
        // This method isn't called with an uninitialized cursor and
        // there are no methods which can set the cursor to EOF.
        auto res = tombstone();
        expensive_log("index_cursor::open_tombstone this={} from eof: {}", fmt::ptr(this), tombstone());
        return res;
    } else {
        const auto p = _row_cursor.payload();
        constexpr uint8_t TOMBSTONE_FLAG = 0x8;
        if (p.bits & TOMBSTONE_FLAG) {
            uint64_t bits = p.bits & ~TOMBSTONE_FLAG;
            auto marked = seastar::be_to_cpu(read_unaligned<int64_t>(p.bytes.data() + bits));
            auto deletion_time = seastar::be_to_cpu(read_unaligned<int32_t>(p.bytes.data() + bits + 8));
            auto result = tombstone(marked, gc_clock::time_point(gc_clock::duration(deletion_time)));
            expensive_log("index_cursor::open_tombstone this={} from payload: {}", fmt::ptr(this), result);
            return result;
        } else {
            expensive_log("index_cursor::open_tombstone this={} from empty payload: {}", fmt::ptr(this), tombstone());
            return tombstone();
        }
    }
}

const std::optional<row_index_header>& index_cursor::partition_metadata() const {
    return _partition_metadata;
}

future<> index_cursor::maybe_read_metadata() {
    if (_partition_cursor.eof()) {
        return make_ready_future<>();
    }
    if (auto res = partition_payload_to_pos(_partition_cursor.payload()); res >= 0) {
        return read_row_index_header(_in_row._file.get(), res, _permit).then([this] (auto result) {
            _partition_metadata = result;
        });
    }
    return make_ready_future<>();
}

future<> index_cursor::set_before_partition(lazy_comparable_bytes_from_ring_position& key) {
    _row_cursor.reset();
    _partition_metadata.reset();
    co_await _partition_cursor.set_to(_par_root, key.begin());
    if (_partition_cursor.at_leaf()) {
        _partition_cursor.snap_to_leaf();
    } else {
        co_await _partition_cursor.step();
    }
    co_await maybe_read_metadata();
}

future<set_result> index_cursor::set_to_partition(lazy_comparable_bytes_from_ring_position& key, std::byte expected_hash_byte) {
    _row_cursor.reset();
    _partition_metadata.reset();
    co_await _partition_cursor.set_to(_par_root, key.begin());
    if (!_partition_cursor.at_leaf()) {
        expensive_log("index_cursor::set_to_partition, not at leaf, trail={}", fmt::join(_partition_cursor.trail(), ", "));
        co_return set_result::definitely_not_a_match;
    }
    _partition_cursor.snap_to_leaf();
    std::optional<std::byte> actual_hash_byte = partition_hash();
    expensive_log("index_cursor::set_to_partition, points at key, actual_hash_byte={} trail={}", actual_hash_byte, fmt::join(_partition_cursor.trail(), ", "));
    if (actual_hash_byte && *actual_hash_byte != expected_hash_byte) {
        co_return set_result::definitely_not_a_match;
    }
    co_await maybe_read_metadata();
    co_return set_result::possible_match;
}

future<> index_cursor::set_after_partition(lazy_comparable_bytes_from_ring_position& key) {
    _row_cursor.reset();
    _partition_metadata.reset();
    co_await _partition_cursor.set_to(_par_root, key.begin());
    co_await _partition_cursor.step();
    co_await maybe_read_metadata();
}
future<> index_cursor::next_partition() {
    _row_cursor.reset();
    _partition_metadata.reset();
    return _partition_cursor.step().then([this] {
        return maybe_read_metadata();
    });
}
future<> index_cursor::set_before_row(lazy_comparable_bytes_from_clustering_position& key) {
    if (!_partition_metadata) {
        co_return;
    }
    _row_cursor.reset();
    co_await _row_cursor.set_to(_partition_metadata->trie_root, key.begin());
    if (!_row_cursor.at_key()) [[likely]] {
        // Note: in practice this branch is always taken
        // because of our choice of clustering block separators.
        //
        // To get here, `key` would have to lead exactly to a payloaded node.
        // The format allows for that in general.
        // But it doesn't happen with our writers.
        //
        // Our separators are encoded clustering key prefixes,
        // trimmed and "nudged" by incrementing their last byte.
        // Something like this never looks like another encoded position_in_partition.
        co_await _row_cursor.step_back();
    }
}

future<> index_cursor::set_after_row_cold(lazy_comparable_bytes_from_clustering_position& key) {
    co_await _row_cursor.set_to(_partition_metadata->trie_root, key.begin());
    co_await _row_cursor.step();
    if (_row_cursor.eof()) {
        co_await next_partition();
    }
}

future<> index_cursor::set_after_row(lazy_comparable_bytes_from_clustering_position& key) {
    if (!_partition_metadata) {
        return next_partition();
    }
    return set_after_row_cold(key);
}

sstables::data_file_positions_range bti_index_reader::data_file_positions() const {
    auto lo = _lower.partition_cursor_set() ? _lower.data_file_pos(_total_file_size) : 0;
    std::optional<uint64_t> hi;
    if (_upper.partition_cursor_set()) {
        hi = _upper.data_file_pos(_total_file_size);
    }
    trie_logger.debug("bti_index_reader::data_file_positions this={} result=({}, {})", fmt::ptr(this), lo, hi);
    return {lo, hi};
}
future<std::optional<uint64_t>> bti_index_reader::last_block_offset() {
    trie_logger.debug("bti_index_reader::last_block_offset this={}", fmt::ptr(this));
    return _lower.last_block_offset();
}
future<> bti_index_reader::close() noexcept {
    trie_logger.debug("bti_index_reader::close this={}", fmt::ptr(this));
    return make_ready_future<>();
}
bti_index_reader::bti_index_reader(
    bti_node_reader partitions_db,
    bti_node_reader rows_db,
    uint64_t root_offset,
    uint64_t total_file_size,
    schema_ptr s,
    reader_permit rp
)
    : _in_row(rows_db)
    , _s(std::move(s))
    , _permit(std::move(rp))
    // Note that each cursor gets its own copy of the `bti_node_reader`s,
    // not a reference to some shared one, because `bti_node_reader`
    // holds the currently-active page, and each bound might be in a different page.
    // (And both are needed to read lower and upper Data.db bounds).
    , _lower(root_offset, partitions_db, rows_db, _permit)
    , _upper(root_offset, partitions_db, rows_db, _permit)
    , _total_file_size(total_file_size)
{
    trie_logger.debug("bti_index_reader::constructor: this={} root_offset={} total_file_size={} table={}.{}",
        fmt::ptr(this), root_offset, total_file_size, _s->ks_name(), _s->cf_name());
}

std::byte hash_byte_from_token(const dht::token& x) {
    return static_cast<std::byte>(static_cast<uint8_t>(x.unbias()));
}

future<bool> bti_index_reader::advance_lower_and_check_if_present(dht::ring_position_view key) {
    trie_logger.debug("bti_index_reader::advance_lower_and_check_if_present: this={} key={}", fmt::ptr(this), key);
    auto k = lazy_comparable_bytes_from_ring_position(*_s, key);
    std::byte expected_hash = hash_byte_from_token(key.token());
    auto res = co_await _lower.set_to_partition(k, expected_hash);
    co_return res == set_result::possible_match;
    co_return true;
}
future<> bti_index_reader::init_lower_bound() {
    auto k = lazy_comparable_bytes_from_ring_position(*_s, dht::ring_position_view::min());
    if (_lower.partition_cursor_set()) {
        on_internal_error(trie_logger, "bti_index_reader::init_lower_bound: partition cursor already set");
    }
    co_await _lower.set_before_partition(k);
}
future<> bti_index_reader::maybe_init_lower_bound() {
    if (!_lower.partition_cursor_set()) {
        return init_lower_bound();
    }
    return make_ready_future<>();
}
future<> bti_index_reader::advance_to_next_partition() {
    trie_logger.debug("bti_index_reader::advance_to_next_partition this={}", fmt::ptr(this));
    co_await maybe_init_lower_bound();
    co_await _lower.next_partition();
}
sstables::indexable_element bti_index_reader::element_kind() const {
    trie_logger.debug("bti_index_reader::element_kind");
    return _lower.row_cursor_set() ? sstables::indexable_element::cell : sstables::indexable_element::partition;
}
future<> bti_index_reader::advance_to_definitely_present_partition(const dht::decorated_key& dk) {
    trie_logger.debug("bti_index_reader::advance_after_existing(partition) this={} pos={}", fmt::ptr(this), dk);
    auto k = lazy_comparable_bytes_from_ring_position(*_s, dht::ring_position_view(dk.token(), &dk.key(), 0));
    co_await _lower.set_before_partition(k);
}
future<> bti_index_reader::advance_past_definitely_present_partition(const dht::decorated_key& dk) {
    trie_logger.debug("bti_index_reader::advance_after_existing(partition) this={} pos={}", fmt::ptr(this), dk);
    auto k = lazy_comparable_bytes_from_ring_position(*_s, dht::ring_position_view(dk.token(), &dk.key(), 0));
    co_await _lower.set_after_partition(k);
}
future<> bti_index_reader::advance_to(position_in_partition_view pos) {
    trie_logger.debug("bti_index_reader::advance_to(row) this={} pos={}", fmt::ptr(this), pos);
    co_await maybe_init_lower_bound();
    auto k = lazy_comparable_bytes_from_clustering_position(*_s, pos);
    co_await _lower.set_before_row(k);
}
std::optional<sstables::deletion_time> bti_index_reader::partition_tombstone() {
    std::optional<sstables::deletion_time> res;
    if (const auto& hdr = _lower.partition_metadata()) {
        res = hdr->partition_tombstone;
        trie_logger.debug("bti_index_reader::partition_tombstone this={} res={}", fmt::ptr(this), hdr->partition_tombstone);
    } else {
        trie_logger.debug("bti_index_reader::partition_tombstone this={} res=none", fmt::ptr(this));
    }
    return res;
}
std::optional<partition_key> bti_index_reader::get_partition_key() {
    std::optional<partition_key> res;
    if (const auto& hdr = _lower.partition_metadata()) {
        res = hdr->partition_key.to_partition_key(*_s);
    }
    trie_logger.debug("bti_index_reader::get_partition_key this={} res={}", fmt::ptr(this), bool(res));
    return res;
}
bool bti_index_reader::partition_data_ready() const {
    trie_logger.debug("bti_index_reader::partition_data_ready this={}", fmt::ptr(this));
    return _lower.partition_cursor_set();
}
future<> bti_index_reader::advance_reverse(position_in_partition_view pos) {
    trie_logger.debug("bti_index_reader::advance_reverse this={} pos={}", fmt::ptr(this), pos);
    co_await maybe_init_lower_bound();
    _upper = _lower;
    auto k = lazy_comparable_bytes_from_clustering_position(*_s, pos);
    co_await _upper.set_after_row(k);
}
future<> bti_index_reader::read_partition_data() {
    trie_logger.debug("bti_index_reader::read_partition_data this={}", fmt::ptr(this));
    co_await maybe_init_lower_bound();
}
future<> bti_index_reader::advance_to(const dht::partition_range& range) {
    trie_logger.debug("bti_index_reader::advance_to(range) this={} range={}", fmt::ptr(this), range);
    {
        auto k = lazy_comparable_bytes_from_ring_position(*_s, dht::ring_position_view::for_range_start(range));
        co_await _lower.set_before_partition(k);
    }
    {
        auto k = lazy_comparable_bytes_from_ring_position(*_s, dht::ring_position_view::for_range_end(range));
        co_await _upper.set_after_partition(k);
    }
}
future<> bti_index_reader::advance_reverse_to_next_partition() {
    trie_logger.debug("bti_index_reader::advance_reverse_to_next_partition() this={}", fmt::ptr(this));
    co_await maybe_init_lower_bound();
    _upper = _lower;
    co_await _upper.next_partition();
}
future<> bti_index_reader::advance_upper_past(position_in_partition_view pos) {
    trie_logger.debug("bti_index_reader::advance_upper_past() this={} pos={}", fmt::ptr(this), pos);
    co_await maybe_init_lower_bound();
    _upper = _lower;
    if (!_upper.partition_metadata() || pos.is_after_all_clustered_rows(*_s)) {
        // Common special case. Happens for queries with full clustering range
        // and for queries with no intra-partition index. 
        co_await _upper.next_partition();
        co_return;
    }
    auto k = lazy_comparable_bytes_from_clustering_position(*_s, pos);
    co_await _upper.set_after_row(k);
}
std::optional<sstables::open_rt_marker> bti_index_reader::end_open_marker() const {
    trie_logger.debug("bti_index_reader::end_open_marker() this={}", fmt::ptr(this));
    std::optional<sstables::open_rt_marker> res;
    if (_lower.row_cursor_set()) {
        if (auto tomb = _lower.open_tombstone()) {
            res = sstables::open_rt_marker{.pos = {position_in_partition::after_static_row_tag_t()}, .tomb = tomb};
        }
    }
    trie_logger.debug("bti_index_reader::end_open_marker this={} res={}", fmt::ptr(this), res ? res->tomb : tombstone());
    return res;
}
std::optional<sstables::open_rt_marker> bti_index_reader::reverse_end_open_marker() const {
    trie_logger.debug("bti_index_reader::reverse_end_open_marker() this={}", fmt::ptr(this));
    std::optional<sstables::open_rt_marker> res;
    if (_upper.row_cursor_set()) {
        if (auto tomb = _upper.open_tombstone()) {
            res = sstables::open_rt_marker{.pos = {position_in_partition::after_static_row_tag_t()}, .tomb = tomb};
        }
    }
    trie_logger.debug("bti_index_reader::reverse_end_open_marker this={} res={}", fmt::ptr(this), res ? res->tomb : tombstone());
    return res;
}
bool bti_index_reader::eof() const {
    trie_logger.debug("bti_index_reader::eof() this={}", fmt::ptr(this));
    if (!_lower.partition_cursor_set()) {
        // The index reader is not initialized, so it's logically at data position 0,
        // so it can't be at EOF.
        // (There's an assumption that the data file is non empty).
        return false;
    }
    return _lower.data_file_pos(_total_file_size) >= _total_file_size;
}

future<> bti_index_reader::prefetch_lower_bound(position_in_partition_view pos) {
    // In the BIG index reader, the following heuristic optimization is used:
    // when the index is queried for a clustering range,
    // it looks up the lower bound with maximum accuracy available in the index file,
    // and then it looks up the upper bound with the maximum granularity available
    // *in RAM cache*.
    //
    // In other words, the BIG index reader prefers to return a lower-quality
    // result for the upper bound rather than do any I/O in the index file
    // to get a more accurate bound. The assumption (I'm not sure where it came from,
    // or whether it's true) is that reading too much from the Data file is
    // generally cheaper in this case than the extra Index file reads.
    //
    // In the BIG index, this behavior is implemented by first fetching the page
    // needed for the accurate lower bound lookup,
    // and only then querying the upper bound within the set of RAM-cached pages.
    // The order is important because, in most cases, the pages needed by lower_bound
    // lookup are very much relevant to the upper bound lookup, and improve its quality.
    //
    // But it so happens that the MX reader queries the upper bound before the lower bound.
    // So to fetch the lower bound pages before doing the upper bound lookup,
    // the `prefetch_lower_bound` was introduced, and it's called before the upper bound lookup.
    //
    // To some degree this discussion could also apply to the BTI index.
    // But it has different performance characteristcs, and it's hard to decide
    // if this heuristic optimization would be an improvement in practice.
    //
    // For now, in the BTI index we just look up the upper bound with full accuracy,
    // (potentially paying for extra index file reads).
    // Therefore the upper bound lookup doesn't benefit from any fetches in advance,
    // and this method is a no-op.
    return make_ready_future<>();
}

// Creates a BTI index reader over the Partitions.db file and the matching Rows.db file.
std::unique_ptr<sstables::abstract_index_reader> make_bti_index_reader(
    cached_file& partitions_db,
    cached_file& rows_db,
    uint64_t partitions_db_root_pos,
    uint64_t total_data_db_file_size,
    schema_ptr s,
    reader_permit permit
) {
    // The reader currently assumes that every index page (as chosen by the writer)
    // is fully contained within a single `cached_file` page.
    static_assert(cached_file::page_size % BTI_PAGE_SIZE == 0);
    return std::make_unique<bti_index_reader>(
        bti_node_reader(partitions_db),
        bti_node_reader(rows_db),
        partitions_db_root_pos,
        total_data_db_file_size,
        std::move(s),
        std::move(permit)
    );
}

} // namespace sstables::trie
