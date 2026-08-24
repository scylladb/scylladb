/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "bti_index.hh"
#include "bti_index_internal.hh" // IWYU pragma: keep
#include "trie_writer.hh"
#include "bti_key_translation.hh"
#include "utils/div_ceil.hh"
#include "bti_node_sink.hh"
#include "sstables/mx/types.hh"
#include "sstables/writer.hh"
#include "utils/assert.hh"

#include <array>
#include <bit>

namespace sstables::trie {

class row_index_writer_impl {
public:
    row_index_writer_impl(sstable_version_types, bti_node_sink&, bti_position_encoding);
    ~row_index_writer_impl();
    row_index_writer_impl(row_index_writer_impl&&) = delete;
private:
    // Writes _last_key to the trie.
    void flush_last_key(size_t mismatch, size_t last_key_size, const trie_payload& payload);
    trie_payload make_payload(
        sstable_position partition_data_start,
        sstable_position block_start,
        sstables::deletion_time range_tombstone_before_first_ck);
public:
    void add(
        const schema& s,
        const sstables::clustering_info& first_ck,
        const sstables::clustering_info& last_ck,
        sstable_position partition_data_start,
        sstable_position block_start,
        sstables::deletion_time range_tombstone_before_first_ck);
    bti_partition_index_target finish(
        const schema&,
        sstable_position partition_data_start,
        sstable_position partition_data_end,
        const sstables::key& pk,
        const sstables::deletion_time& partition_tombstone);
    using buf = std::vector<std::byte>;

private:
    trie_writer<bti_node_sink> _wr;
    sstable_version_types _sst_ver;
    // Selects the payload format, and checks the kind of the positions given to us.
    bti_position_encoding _encoding;
    size_t _added_blocks = 0;
    // Storage for _last_key, _last_separator and _tmp_key.
    // 
    // They are pointers to an array of lazy_comparable_bytes_from_rpv
    // instead of being individual objects in order to avoid
    // moving around `lazy_comparable_bytes_from_clustering_position`
    // objects in std::swap<_last_key, _tmp_key>,
    // both for performance reasons and because it allows making
    // `lazy_comparable_bytes_from_clustering_position` self-referential if we need that later.
    std::optional<lazy_comparable_bytes_from_clustering_position> _keys[3];
    // The separator chosen by the last `add()` call.
    // During the next `add()` call, it will be compared with its successor
    // separator
    std::remove_reference_t<decltype(_keys[0])>* _last_separator = &_keys[0];
    // The last clustering key of the clustering block added in the previous `add()` call.
    // During the next `add()` call, it will be used to choose a separator between that
    // block and its successor.
    // It will become `_last_separator` after the next `add()` call returns. 
    std::remove_reference_t<decltype(_keys[0])>* _last_key = &_keys[1];
    // The key added in the ongoing `add()` call.
    // It will become `_last_key` when the call returns.
    std::remove_reference_t<decltype(_keys[0])>* _tmp_key = &_keys[2];
};

row_index_writer_impl::row_index_writer_impl(sstable_version_types sst_ver, bti_node_sink& out, bti_position_encoding encoding)
    : _wr(out)
    , _sst_ver(sst_ver)
    , _encoding(std::move(encoding))
{}
row_index_writer_impl::~row_index_writer_impl() {
}

void row_index_writer_impl::flush_last_key(size_t mismatch, size_t last_key_size, const trie_payload& payload) {
    size_t i = 0;
    // We find the first fragment of `_last_key` that contains the mismatch position,
    // and then insert the `last_key_size - mismatch` bytes starting from that point
    // into the writer, at depth `mismatch`.
    for (auto frag : **_last_key) {
        if (i + frag.size() <= mismatch) {
            i += frag.size();
            continue;
        }

        if (i < mismatch) {
            auto skip = mismatch - i;
            i += skip;
            frag = frag.subspan(skip);
        }

        if (i + frag.size() < last_key_size) {
            _wr.add_partial(i, frag);
        } else {
            _wr.add(i, frag, payload);
            break;
        }

        i += frag.size();
    }
}
constexpr uint8_t TOMBSTONE_FLAG = 0x8;

// Appends the (optional) range tombstone to the payload buffer.
static std::byte* write_optional_tombstone(std::byte* it, sstables::deletion_time t) {
    if (!t.live()) {
        it = write_unaligned(it, seastar::cpu_to_be(t.marked_for_delete_at));
        it = write_unaligned(it, seastar::cpu_to_be(t.local_deletion_time));
    }
    return it;
}

// Builds the legacy (`ms`/`mt`) row index payload: the variable-width
// (pre-compression) offset, followed by an optional tombstone.
static trie_payload make_legacy_row_payload(
    sstable_position_offset offset_from_partition_start,
    sstables::deletion_time range_tombstone_before_first_ck
) {
    std::array<std::byte, 20> payload_bytes;
    std::byte* it = payload_bytes.data();

    // The (pre-compression) offset serialized into the payload.
    uint64_t offset = offset_from_partition_start.to_logical();

    // byte width of the offset integer.
    // Cassandra expects this to be a signed integer (for no good reason)
    // so we waste one bit (note the `+ 1`) for compatibility.
    auto pos_bytewidth = div_ceil(std::bit_width<uint64_t>(offset) + 1, 8);

    // The 4 bits of metadata included in the first byte of the BTI node.
    uint8_t payload_bits = pos_bytewidth;

    // Write n:=`pos_bytewidth` least significant bytes of `offset` to the payload buffer,
    // in big endian order.
    uint64_t offset_be = seastar::cpu_to_be<uint64_t>(offset << 8*(8 - pos_bytewidth));
    // sic. We only need `sizeof(pos_bytewidth)` bytes, but we copy 8 bytes to have a fixed-size copy.
    std::memcpy(it, &offset_be, 8);
    it += pos_bytewidth;

    if (!range_tombstone_before_first_ck.live()) {
        payload_bits |= TOMBSTONE_FLAG;
    }
    it = write_optional_tombstone(it, range_tombstone_before_first_ck);
    return trie_payload(payload_bits, {payload_bytes.data(), it});
}

// Builds the `mu` row index payload: a compact unsigned chunk position (relative
// to the partition start's chunk, so it is 0 when the block lies in the same chunk
// as the partition start), block count of the extent, offset-within-chunk, and a
// tombstone flag bit, followed by an optional tombstone. The payload bits carry the
// byte size of the non-tombstone part of the payload. The block-count
// encoding uses the block's absolute chunk position for block alignment, so that
// the decoded chunk end is block-aligned. The reader recovers the absolute chunk
// position from the partition start and decodes the block count against it (see
// bti_extent_to_block_count / bti_extent_hint_from_block_count).
static trie_payload make_physical_row_payload(
    sstable_position partition_data_start,
    sstable_position block_start,
    sstables::deletion_time range_tombstone_before_first_ck,
    const bti_physical_layout& layout
) {
    std::array<std::byte, 16 + sizeof(int64_t) + sizeof(int32_t)> payload_bytes;
    std::byte* it = payload_bytes.data();

    auto pc = (block_start - partition_data_start).as_physical();
    SCYLLA_ASSERT(pc.chunk_position >= 0);
    SCYLLA_ASSERT(pc.extent_hint >= 1);

    // extent_hint is a byte length; the index stores it as a block count.
    const auto block_count = bti_extent_to_block_count(block_start.as_physical().chunk_position, pc.extent_hint);

    const auto pos_bits = std::bit_width(uint64_t(pc.chunk_position));
    const auto packed_bits = pos_bits + layout.packed_bits() + 1;
    const auto packed_bytes = div_ceil(packed_bits, 8u);
    SCYLLA_ASSERT(packed_bytes < 16);

    unsigned __int128 packed = uint64_t(pc.chunk_position);
    packed <<= layout.packed_bits();
    packed |= layout.pack(block_count, pc.offset_within_chunk);
    packed <<= 1;
    packed |= uint64_t(!range_tombstone_before_first_ck.live());

    it = write_fixed_be(it, packed, packed_bytes);
    const auto payload_bits = static_cast<uint8_t>(packed_bytes);
    it = write_optional_tombstone(it, range_tombstone_before_first_ck);
    return trie_payload(payload_bits, {payload_bytes.data(), it});
}

trie_payload row_index_writer_impl::make_payload(
    sstable_position partition_data_start,
    sstable_position block_start,
    sstables::deletion_time range_tombstone_before_first_ck
) {
    _encoding.check_kind(partition_data_start);
    _encoding.check_kind(block_start);
    return !_encoding.is_physical()
        ? make_legacy_row_payload(block_start - partition_data_start, range_tombstone_before_first_ck)
        : make_physical_row_payload(partition_data_start, block_start, range_tombstone_before_first_ck, _encoding.layout());
}

void row_index_writer_impl::add(
    const schema& s,
    const sstables::clustering_info& first_ck_info,
    const sstables::clustering_info& last_ck_info,
    sstable_position partition_data_start,
    sstable_position block_start,
    sstables::deletion_time range_tombstone_before_first_ck
) {
    expensive_log("row_index_writer_impl::add() this={} first_ck={},{} last_ck={},{} partition_data_start={} block_start={} range_tombstone_before_first_ck={}",
        fmt::ptr(this),
        first_ck_info.clustering, first_ck_info.kind,
        last_ck_info.clustering, last_ck_info.kind,
        partition_data_start,
        block_start,
        range_tombstone_before_first_ck
    );
    auto first_ck = lazy_comparable_bytes_from_clustering_position(s, first_ck_info);
    _tmp_key->emplace(s, std::move(last_ck_info));

    auto payload = make_payload(partition_data_start, block_start, range_tombstone_before_first_ck);

    if (_added_blocks == 0) {
        // For the first separator, which points to the first clustering key,
        // we use an empty string.
        // This is an arbitrary choice. (Mirrored from Cassandra).
        // If you wish to change this, adjust the `_added_blocks == 1` branch in `finish()` accordingly.
        _wr.add(0, {}, payload);
    } else {
        auto [separator_mismatch_idx, separator_mismatch_ptr] = lcb_mismatch(first_ck.begin(), (**_last_key).begin());
        // We assume a prefix-free encoding here.
        // expensive_assert(separator_mismatch_idx < _last_key_size);
        //
        // When we are processing the first block, _last_key is empty.
        // We leave it that way.
        // The key we insert into the trie to represent the first block is empty.
        //
        // For later blocks, we need to insert some separator S which is greater than the last key (A) of the previous
        // block and not smaller than the first key (B) of the current block.
        //
        // The choice of this separator will affect the efficiency of lookups for range queries starting at any X within the keyless range (A, B).
        // Such queries lie entirely after the previous block, so the optimal answer from the index is the current block.
        // But whether the index returns the previous or the current block,
        // it depends on whether X is smaller or not smaller than the separator.
        //
        // For example, assume that A=0 and B=9.
        // Imagine a query for the range (5, +∞). If S=1, then index will return the current block, which is optimal.
        // If S=9, then index will return the previous block, and the reader will waste time scanning through it.
        //
        // Therefore it is good to construct S to be as close as possible to A (not to B) as possible.
        // In this regard, the optimal separator is A concatenated with a zero byte.
        //
        // But at the same time, we don't want to use a separator as long as a full key if much shorter possible separators exist.
        //
        // Therefore, as an arbitrary compromise, we use the optimal-distance separator in the set
        // of optimal-length separators. Which means we just nudge the byte at the point of mismatch by 1.
        //
        // The byte at the point of mismatch must be greater in the next key than in the previous key.
        // So the byte in the previous key can't possibly be 0xff.
        expensive_assert(*separator_mismatch_ptr != std::byte(0xff));
        *separator_mismatch_ptr = std::byte(uint8_t(*separator_mismatch_ptr) + 1);
        (**_last_key).trim(separator_mismatch_idx + 1);

        size_t mismatch = _added_blocks > 1 ? lcb_mismatch((**_last_key).begin(), (**_last_separator).begin()).first : 0;
        flush_last_key(mismatch, separator_mismatch_idx + 1, payload);
    }

    _added_blocks += 1;
    std::swap(_last_separator, _last_key);
    std::swap(_last_key, _tmp_key);
}

// Write partition tombstone in the bti-da encoding.
//
// If the tombstone is live, the encoding is a single byte 0x80.
// Otherwise, the encoding is 8 bytes of timestamp followed by 4 bytes of local deletion time.
// 
// Negative timestamps cannot be encoded in that encoding,
// so we have a Scylla-specific encoding extension:
// if the first byte is 0xc0, then the encoding is
// `0xc0` followed by 8 bytes of the negative timestamp, followed by 4 bytes of local deletion time..
void write_da_partition_tombstone(sstables::file_writer& fw, const sstables::deletion_time& dt) {
    if (dt.live()) {
        trie_logger.trace("consume_end_of_partition: deletime: live");
        uint8_t flag = 0x80;
        fw.write(reinterpret_cast<const char*>(&flag), sizeof(flag));
    } else if (dt.marked_for_delete_at < 0) {
        uint8_t flag = 0xc0;
        uint64_t mfda = seastar::cpu_to_be(dt.marked_for_delete_at);
        uint32_t ldt = seastar::cpu_to_be(dt.local_deletion_time);
        fw.write(reinterpret_cast<const char*>(&flag), sizeof(flag));
        fw.write(reinterpret_cast<const char*>(&mfda), sizeof(mfda));
        fw.write(reinterpret_cast<const char*>(&ldt), sizeof(ldt));
    } else {
        uint64_t mfda = seastar::cpu_to_be(dt.marked_for_delete_at);
        uint32_t ldt = seastar::cpu_to_be(dt.local_deletion_time);
        fw.write(reinterpret_cast<const char*>(&mfda), sizeof(mfda));
        fw.write(reinterpret_cast<const char*>(&ldt), sizeof(ldt));
    }
}

size_t nudge(lazy_comparable_bytes_from_clustering_position& _last_key, const size_t mismatch_idx) {
    auto it = _last_key.begin();
    size_t idx = 0;
    while (true) {
        expensive_assert(it != _last_key.end());
        if (mismatch_idx < idx + (*it).size()) {
            *it = (*it).subspan(mismatch_idx - idx);
            idx = mismatch_idx;
            break;
        }
        idx += (*it).size();
        ++it;
    }
    while (true) {
        expensive_assert(it != _last_key.end());
        auto sp = *it;
        for (size_t i = 0; i < sp.size(); ++i) {
            if (sp[i] != std::byte(0xff)) {
                sp[i] = std::byte(uint8_t(sp[i]) + 1);
                idx += i;
                return idx;
            }
        }
        idx += sp.size();
        ++it;
    }
}

void write_row_index_header(
    sstable_version_types sst_ver,
    sstables::file_writer& fw,
    const sstables::key& pk,
    sstable_position partition_data_start,
    const bti_position_encoding& encoding,
    uint64_t added_blocks,
    uint64_t root_pos,
    const sstables::deletion_time& partition_tombstone
) {
    trie_logger.trace("consume_end_of_partition: key: offset={} pk={}", fw.offset(), fmt_hex(bytes_view(pk)));
    write(sst_ver, fw, disk_string_view<uint16_t>(bytes_view(pk)));

    auto pos_datapos = fw.offset();
    trie_logger.trace("consume_end_of_partition: pos: {} {}", fw.offset(), partition_data_start);
    encoding.check_kind(partition_data_start);
    if (!encoding.is_physical()) {
        write_unsigned_vint(fw, partition_data_start.to_logical());
    } else {
        const auto& layout = encoding.layout();
        auto start_pc = partition_data_start.as_physical();
        SCYLLA_ASSERT(start_pc.chunk_position >= 0);
        SCYLLA_ASSERT(start_pc.extent_hint >= 1);

        // extent_hint is a byte length; the index stores it as a block count.
        const auto block_count = bti_extent_to_block_count(start_pc.chunk_position, start_pc.extent_hint);

        write_unsigned_vint(fw, uint64_t(start_pc.chunk_position));
        const auto packed = layout.pack(block_count, start_pc.offset_within_chunk);
        std::array<std::byte, sizeof(uint64_t)> packed_bytes;
        const auto packed_width = layout.packed_bytes();
        write_fixed_be(packed_bytes.data(), packed, packed_width);
        fw.write(reinterpret_cast<const char*>(packed_bytes.data()), packed_width);
    }

    trie_logger.trace("consume_end_of_partition: root_offset: {} {}", fw.offset(), pos_datapos - root_pos);
    write_signed_vint(fw, int64_t(uint64_t(root_pos) - uint64_t(pos_datapos)));

    trie_logger.trace("consume_end_of_partition: added_blocks_for_header: {} {}", fw.offset(), added_blocks);
    write_unsigned_vint(fw, added_blocks);

    trie_logger.trace("consume_end_of_partition: deletime: {}", fw.offset());
    write_da_partition_tombstone(fw, partition_tombstone);
}

bti_partition_index_target row_index_writer_impl::finish(
    const schema& s,
    sstable_position partition_data_start,
    sstable_position partition_data_end,
    const sstables::key& pk,
    const sstables::deletion_time& partition_tombstone
) {
    expensive_log("row_index_writer_impl::finish() this={}", fmt::ptr(this));

    if (_added_blocks >= 1) {
        // Write a separator that lies after the last key in the partition.
        // This separator points to the last byte (END_OF_PARTITION byte) at the end of the partition.
        // This might allow the reader to filter out (i.e. avoid reading Data.db)
        // queries that lie entirely after the partition.
        // 
        // It's not something necessary, but we do this because Cassandra does it.
        // 
        // For this separator, we arbitrarily choose: the last key, trimmed to the previously inserted separator's length, with last byte nudged by 1.
        // This really is arbitrary. But this choice is reasonable because we assume that, since the last separator was of this length,
        // this length has some decent separating power.
        //
        // bti_index_reader::last_block_offset() assumes that this final separator exists.
        size_t mismatch_idx = 0;
        if (_added_blocks > 1) [[likely]] {
            auto [idx, ptr] = lcb_mismatch((**_last_separator).begin(), (**_last_key).begin());
            mismatch_idx = idx;
        }
        auto nudge_idx = nudge(**_last_key, mismatch_idx);
        expensive_assert(nudge_idx >= mismatch_idx);
        (**_last_key).trim(nudge_idx + 1);
        // The final separator points to the end-of-partition byte. Its offset
        // relative to the partition start is serialized into the payload: the chunk
        // position is relative to the partition start's chunk, while extent_hint
        // and offset_within_chunk are carried from `partition_data_end`.
        auto final_payload = make_payload(partition_data_start, partition_data_end, sstables::deletion_time::make_live());
        expensive_log("row_index_writer_impl::finish() final_payload: {}", partition_data_end);
        flush_last_key(mismatch_idx, nudge_idx + 1, final_payload);
    }

    auto result = _wr.finish();
    auto added_blocks_for_header = _added_blocks;
    _added_blocks = 0;
    _last_separator->reset();
    _last_key->reset();
    _tmp_key->reset();
    if (!result.valid()) {
        // No intra-partition index: the partition index will point directly into Data.db,
        // at the partition start position. (The bit-negation that discriminates Data.db
        // positions from Rows.db positions on disk is applied by the partition index
        // writer during serialization.)
        return partition_data_start;
    }

    // The header we write here is parsed during reads by `row_index_header_parser`.
    auto root = result.value;
    auto& fw = _wr.sink().file_writer();

    expensive_log("row_index_writer_impl::finish: writing header at {}", fw.offset());
    int64_t pos_header = fw.offset();
    write_row_index_header(_sst_ver, fw, pk, partition_data_start, _encoding, added_blocks_for_header, root, partition_tombstone);
    // The partition index entry points into Rows.db, at the header we just wrote.
    // This is a plain offset into Rows.db, not a Data.db position.
    return rows_db_position{static_cast<uint64_t>(pos_header)};
}

// Instantiation of row_index_writer_impl with `Output` == `bti_node_sink`.
//
// This is the instantiation `row_index_writer_impl` actually used in practice.
// Other substitutions of `Output` are only used by tests.
struct bti_row_index_writer::impl
    : bti_node_sink
    , row_index_writer_impl
{
    impl(sstable_version_types sst_ver, sstables::file_writer& fw, bti_position_encoding encoding)
        : bti_node_sink(fw, BTI_PAGE_SIZE)
        , row_index_writer_impl(sst_ver, static_cast<bti_node_sink&>(*this), std::move(encoding))
    {}
    impl(impl&&) = delete;
};

bti_row_index_writer::bti_row_index_writer() noexcept = default;

bti_row_index_writer::~bti_row_index_writer() noexcept = default;

bti_row_index_writer::bti_row_index_writer(sstable_version_types sst_ver, sstables::file_writer& fw, bti_position_encoding encoding)
    : _impl(std::make_unique<impl>(sst_ver, fw, std::move(encoding)))
{}
bti_row_index_writer::bti_row_index_writer(bti_row_index_writer&&) noexcept = default;
bti_row_index_writer& bti_row_index_writer::operator=(bti_row_index_writer&&) noexcept = default;

bti_partition_index_target bti_row_index_writer::finish(
    const schema& s,
    sstable_position partition_data_start,
    sstable_position partition_data_end,
    const sstables::key& pk,
    const sstables::deletion_time& partition_tombstone) {
    return _impl->finish(s, partition_data_start, partition_data_end, pk, partition_tombstone);
}

void bti_row_index_writer::add(
    const schema& s,
    const sstables::clustering_info& first_ck,
    const sstables::clustering_info& last_ck,
    sstable_position partition_data_start,
    sstable_position block_start,
    const sstables::deletion_time& range_tombstone_before_first_ck
) {
    return _impl->add(s, first_ck, last_ck, partition_data_start, block_start, range_tombstone_before_first_ck);
}

} // namespace sstables::trie
