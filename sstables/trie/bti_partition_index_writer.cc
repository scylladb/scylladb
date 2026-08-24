/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "bti_index.hh"
#include "bti_index_internal.hh"
#include "trie_writer.hh"
#include "bti_key_translation.hh"
#include "utils/div_ceil.hh"
#include "bti_node_sink.hh"
#include "sstables/compress.hh"
#include "sstables/exceptions.hh"
#include "sstables/writer.hh"
#include "utils/assert.hh"
#include "utils/i_filter.hh"

namespace sstables::trie {

// The job of this class is to take a stream of partition keys and their index payloads
// (Data.db or Rows.db position, and hash bits for filtering false positives out)
// and translate it to a minimal trie which still allows for efficient queries.
//
// For that, each key in the trie is trimmed to the shortest length which still results
// in a unique leaf.
// For example, assume that keys ABCXYZ, ABDXYZ, ACXYZ are added to the trie.
// A "naive" trie would look like this:
//     A
//     B---C
//     C-D X
//     X X Y
//     Y Y Z
//     Z Z
//
// This writer creates:
//     A
//     B---C
//     C-D
//
// Note: the trie-serialization format of partition keys is prefix-free,
// which guarantees that only leaf nodes carry a payload.
class bti_partition_index_writer_impl {
    void write_last_key(size_t needed_prefix);
public:
    bti_partition_index_writer_impl(sstable_version_types, bti_node_sink&, bti_position_encoding);
    bti_partition_index_writer_impl(bti_partition_index_writer_impl&&) = delete;
    // `pos` points either to the partition's row index header in Rows.db (if the
    // partition has an intra-partition index) or to the partition itself in Data.db.
    // The bit-negation that distinguishes the two on disk is applied here during
    // serialization.
    void add(const schema&, dht::decorated_key, const utils::hashed_key&, bti_partition_index_target pos);
    std::optional<bti_partitions_db_footer> finish(
        const sstables::key& first_key,
        const sstables::key& last_key);
private:
    // The lower trie-writing layer, oblivious to the semantics of the partition index.
    trie_writer<bti_node_sink> _wr;
    sstable_version_types _sst_ver;
    // Counter of added keys.
    size_t _added_keys = 0;

    // We "buffer" one key in the writer. To write an entry to the trie_writer,
    // we want to trim the key to the shortest prefix which is still enough
    // to differentiate the key from its neighbours. For that, we need to see
    // the *next* entry first.
    //
    // The first position in _last_key where it differs from its predecessor.
    // After we see the next entry, we will also know the first position where it differs
    // from the successor, and we will be able to determine the shortest unique prefix.
    size_t _last_key_mismatch = 0;
    // Storage for _last_key and _tmp_key.
    // 
    // They are pointers to an array of lazy_comparable_bytes_from_rpv
    // instead of being individual objects in order to avoid
    // moving around `lazy_comparable_bytes_from_rpv`
    // objects in std::swap<_last_key, _tmp_key>,
    // both for performance reasons and because it allows making
    // `lazy_comparable_bytes_from_rpv` self-referential if we need that later. 
    std::optional<lazy_comparable_bytes_from_ring_position> _keys[2];
    // The key added in the previous `add()` call.
    std::remove_reference_t<decltype(_keys[0])>* _last_key = &_keys[0];
    // The key added in the ongoing `add()` call.
    // It will become `_last_key` when the call returns.
    std::remove_reference_t<decltype(_keys[0])>* _tmp_key = &_keys[1];
    // The payload of _last_key: the index target (a Data.db or Rows.db position, with
    // an explicit discriminator) and the hash bits.
    // We have to keep them around here because they will only be written
    // to the output in the *next* `add()` after the one
    // which inserted `_last_key`, because only then it will be
    // determined how much `_last_key` can be trimmed.
    std::optional<bti_partition_index_target> _last_pos_payload;
    uint8_t _last_hash_bits;
    // Selects the payload format: physical (full chunk coordinates) or legacy
    // (variable-width pre-compression position). This is a per-sstable property,
    // so it is fixed at construction rather than derived from each payload.
    bti_position_encoding _encoding;
};

bti_partition_index_writer_impl::bti_partition_index_writer_impl(sstable_version_types sst_ver, bti_node_sink& out, bti_position_encoding encoding)
    : _wr(out)
    , _sst_ver(sst_ver)
    , _encoding(std::move(encoding))
{}

// Builds the legacy (`ms`/`mt`) partition index payload: a hash byte followed by
// the variable-width (pre-compression) file position.
static trie_payload make_legacy_partition_payload(uint8_t hash_bits, int64_t pos_payload) {
    std::array<std::byte, 9> payload_bytes;
    auto it = payload_bytes.data();
    // Write the hash byte to the payload buffer.
    *it++ = std::byte(hash_bits);

    // The (pre-compression) file position serialized into the payload.
    // It is either the position of the partition's entry in Rows.db
    // or the bit-negated position of the partition in Data.db.
    // The two are distinguished via the sign bit.
    uint64_t abs_file_pos = pos_payload >= 0 ? pos_payload : ~pos_payload;
    // Note 1 extra bit needed for the sign.
    uint8_t pos_bytewidth = div_ceil(std::bit_width<uint64_t>(abs_file_pos) + 1, 8);

    // Note: this flag says if a hash byte has been written to the payload.
    // (We always do that, so we always set the flag).
    constexpr uint8_t HASH_BYTE_FLAG = 0x8;
    // Build the 4 bits of metadata included in the first byte of the BTI node.
    uint8_t payload_bits = (pos_bytewidth - 1) | HASH_BYTE_FLAG;

    // Write n:=`pos_bytewidth` least significant bytes of `pos_payload` to the payload buffer,
    // in big endian order.
    uint64_t pos_be = seastar::cpu_to_be<uint64_t>(pos_payload << 8*(8 - pos_bytewidth));
    // sic. We only need `sizeof(pos_bytewidth)` bytes, but we copy 8 bytes to have a fixed-size copy.
    memcpy(it, &pos_be, 8);
    it += pos_bytewidth;
    return trie_payload(payload_bits, {payload_bytes.data(), it});
}

// Builds a `mu` partition index payload: a hash byte followed by `packed` as a
// big-endian integer of `packed_bytes` bytes.
static trie_payload make_physical_partition_payload_from_packed(uint8_t hash_bits, unsigned __int128 packed, unsigned packed_bytes) {
    std::array<std::byte, 16> payload_bytes;
    auto it = payload_bytes.data();
    *it++ = std::byte(hash_bits);

    const auto payload_size = 1 + packed_bytes;
    SCYLLA_ASSERT(payload_size < 16);

    it = write_fixed_be(it, packed, packed_bytes);
    return trie_payload(payload_size, {payload_bytes.data(), it});
}

// Builds the `mu` Data.db partition index payload: a hash byte followed by a
// set flag bit, the chunk position, then the packed block count of the extent
// and offset-within-chunk.
static trie_payload make_physical_data_partition_payload(
    uint8_t hash_bits,
    int64_t chunk_position,
    uint64_t block_count,
    uint64_t offset_within_chunk,
    const bti_physical_layout& layout
) {
    SCYLLA_ASSERT(chunk_position >= 0);

    // Note 1 extra bit needed for the flag.
    const auto packed_bytes = div_ceil(std::bit_width(uint64_t(chunk_position)) + 1 + layout.packed_bits(), 8u);
    unsigned __int128 packed = static_cast<unsigned __int128>(1) << (packed_bytes * 8 - 1);
    packed |= static_cast<unsigned __int128>(chunk_position) << layout.packed_bits();
    packed |= layout.pack(block_count, offset_within_chunk);

    return make_physical_partition_payload_from_packed(hash_bits, packed, packed_bytes);
}

// Builds the `mu` Rows.db partition index payload: a hash byte followed by a
// clear flag bit and the Rows.db position.
static trie_payload make_physical_rows_partition_payload(uint8_t hash_bits, uint64_t rows_position) {
    SCYLLA_ASSERT(rows_position <= uint64_t(std::numeric_limits<int64_t>::max()));
    // Note 1 extra bit needed for the flag.
    const auto packed_bytes = div_ceil(std::bit_width(rows_position) + 1, 8u);
    return make_physical_partition_payload_from_packed(hash_bits, rows_position, packed_bytes);
}

// Builds the partition index payload of either position encoding.
// In `ms`/`mt`, Data.db positions are bit-negated, which is how the reader tells
// them apart from Rows.db positions, which are serialized verbatim. In `mu`, the
// top bit of the position field tells them apart.
// See bti_partition_index_target.
static trie_payload make_partition_payload(const bti_position_encoding& encoding, uint8_t hash_bits, const bti_partition_index_target& target) {
    const auto* rows_pos = target.rows_db();
    if (!encoding.is_physical()) {
        const auto pos_payload = rows_pos
            ? static_cast<int64_t>(rows_pos->value)
            : ~target.data_db()->to_logical();
        return make_legacy_partition_payload(hash_bits, pos_payload);
    }
    if (rows_pos) {
        return make_physical_rows_partition_payload(hash_bits, rows_pos->value);
    }
    const auto pc = target.data_db()->as_physical();
    // extent_hint is a byte length; the index stores it as a block count.
    return make_physical_data_partition_payload(
        hash_bits,
        pc.chunk_position,
        bti_extent_to_block_count(pc.chunk_position, pc.extent_hint),
        pc.offset_within_chunk,
        encoding.layout());
}

void bti_partition_index_writer_impl::write_last_key(size_t needed_prefix) {
    trie_payload payload = make_partition_payload(_encoding, _last_hash_bits, *_last_pos_payload);

    size_t i = 0;
    for (auto frag : **_last_key) {
        // The first fragment contains the entire token,
        // and token collisions are rare, hence [[unlikely]].
        if (i + frag.size() <= _last_key_mismatch) [[unlikely]] {
            i += frag.size();
            continue;
        }

        // Skip bytes that were already written to the trie.
        // Move to the point from which the new branch will grow out of.
        // This branch will be taken for all cases except the first partition and token collisions,
        // hence [[likely]].
        if (i < _last_key_mismatch) [[likely]] {
            auto skip = _last_key_mismatch - i;
            i += skip;
            frag = frag.subspan(skip);
        }

        // We amost never need more than one fragment (which contains the entire token), hence [[unlikely]].
        if (i + frag.size() < needed_prefix) [[unlikely]] {
            _wr.add_partial(i, frag);
        } else {
            _wr.add(i, frag.subspan(0, needed_prefix - i), payload);
            break;
        }

        i += frag.size();
    }
}

void bti_partition_index_writer_impl::add(const schema& s, dht::decorated_key dk, const utils::hashed_key& murmur_hash, bti_partition_index_target pos_payload) {
    uint8_t hash_bits = static_cast<uint8_t>(murmur_hash.hash()[1]);
    if (auto* data_pos = pos_payload.data_db()) {
        _encoding.check_kind(*data_pos);
    }
    expensive_log("partition_index_writer_impl::add: this={} key={}, pos_payload={}",
        fmt::ptr(this), dk, pos_payload);
    (*_tmp_key).emplace(_sst_ver, s, dk);
    if (_added_keys > 0) {
        // First position where the new key differs from the last key.
        size_t mismatch = lcb_mismatch((**_tmp_key).begin(), (**_last_key).begin()).first;
        expensive_log("partition_index_writer_impl::add: mismatch={}", mismatch);
        // From `_last_key_mismatch` (mismatch position between `_last_key` and its predecessor)
        // and `mismatch` (mismatch position between `_last_key` and its successor),
        // compute the minimal needed prefix of `_last_key`.
        size_t needed_prefix = std::max(_last_key_mismatch, mismatch) + 1;
        write_last_key(needed_prefix);
        // Update _last_* variables with the new key.
        _last_key_mismatch = mismatch;
    }
    _added_keys += 1;
    // Update _last_* variables with the new entry.
    std::swap(_last_key, _tmp_key);
    _last_pos_payload = pos_payload;
    _last_hash_bits = hash_bits;
}

std::optional<bti_partitions_db_footer> bti_partition_index_writer_impl::finish(
    const sstables::key& first_key,
    const sstables::key& last_key
) {
    if (_added_keys > 0) {
        size_t needed_prefix = _last_key_mismatch + 1;
        write_last_key(needed_prefix);
    } else {
        return std::nullopt;
    }
    // Footer of Partitions.db.
    auto root = _wr.finish().value;
    auto& fw = _wr.sink().file_writer();
    auto keys_pos = fw.offset();
    write(_sst_ver, fw, disk_string_view<uint16_t>(bytes_view(first_key)));
    write(_sst_ver, fw, disk_string_view<uint16_t>(bytes_view(last_key)));
    write(_sst_ver, fw, static_cast<uint64_t>(keys_pos));
    write(_sst_ver, fw, static_cast<uint64_t>(_added_keys));
    write(_sst_ver, fw, static_cast<uint64_t>(root));
    return bti_partitions_db_footer{
        .first_key = first_key,
        .last_key = last_key,
        .partition_count = _added_keys,
        .trie_root_position = root,
    };
}

struct bti_partition_index_writer::impl
    : bti_node_sink
    , bti_partition_index_writer_impl
{
    impl(sstable_version_types sst_ver, sstables::file_writer& fw, bti_position_encoding encoding)
        : bti_node_sink(fw, BTI_PAGE_SIZE)
        , bti_partition_index_writer_impl(sst_ver, static_cast<bti_node_sink&>(*this), std::move(encoding))
    {}
    impl(impl&&) = delete;
};

bti_partition_index_writer::bti_partition_index_writer() noexcept = default;

bti_partition_index_writer::~bti_partition_index_writer() noexcept = default;

bti_partition_index_writer::bti_partition_index_writer(sstable_version_types sst_ver, sstables::file_writer& fw, bti_position_encoding encoding)
    : _impl(std::make_unique<impl>(sst_ver, fw, std::move(encoding)))
{}
void bti_partition_index_writer::add(const schema& s, dht::decorated_key dk, const utils::hashed_key& murmur_hash, bti_partition_index_target pos) {
    _impl->add(s, std::move(dk), murmur_hash, pos);
}
std::optional<bti_partitions_db_footer> bti_partition_index_writer::finish(
    const sstables::key& first_key,
    const sstables::key& last_key
) && {
    return _impl->finish(first_key, last_key);
}
bti_partition_index_writer::bti_partition_index_writer(bti_partition_index_writer&&) noexcept = default;
bti_partition_index_writer& bti_partition_index_writer::operator=(bti_partition_index_writer&&) noexcept = default;

bti_physical_layout::bti_physical_layout(const framed_chunk_codec& codec)
    : _uncompressed_chunk_length(codec.uncompressed_chunk_length())
{
    if (!codec.framed()) {
        on_internal_error(trie_logger, "Physical index positions need framed chunks");
    }
    const uint32_t uncompressed_chunk_length = codec.uncompressed_chunk_length();
    if (!std::has_single_bit(uncompressed_chunk_length)) {
        throw_malformed_sstable_exception(fmt::format(
            "Physical index positions need a power-of-two uncompressed chunk length, got {}", uncompressed_chunk_length));
    }
    const uint64_t max_extent = codec.extent(compressed_chunk_length_limit(uncompressed_chunk_length));
    _max_block_count = div_ceil(sstable_compressed_block_size - 1 + max_extent, uint64_t(sstable_compressed_block_size));
    _block_count_bits = std::bit_width(_max_block_count - 1);
    // O < L, and L is a power of two, so O fits in log2(L) bits.
    _offset_within_chunk_bits = std::countr_zero(uncompressed_chunk_length);
}

std::byte hash_byte_from_key(const schema &s, const partition_key& x) {
    auto hk = utils::make_hashed_key(static_cast<bytes_view>(key::from_partition_key(s, x)));
    return std::byte(hk.hash()[1]);
}

} // namespace sstables::trie
