/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "bti_index.hh"
#include "trie_writer.hh"
#include "bti_key_translation.hh"
#include "utils/div_ceil.hh"
#include "bti_node_sink.hh"
#include "sstables/writer.hh"

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
    bti_partition_index_writer_impl(bti_node_sink&);
    bti_partition_index_writer_impl(bti_partition_index_writer_impl&&) = delete;
    // If the partition has a intra-partition index in Rows.db,
    // `file_pos` is the bit-negated position of the relevant entry in Rows.db.
    // Otherwise, `file_pos` is the position of the partition in Data.db.
    void add(const schema&, dht::decorated_key, int64_t pos_payload);
    void finish(sstable_version_types ver, disk_string_view<uint16_t> first_key, disk_string_view<uint16_t> last_key);
private:
    // The lower trie-writing layer, oblivious to the semantics of the partition index.
    trie_writer<bti_node_sink> _wr;
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
    // The payload of _last_key: the Data.db position (or the bit-negated Rows.db position)
    // and the hash bits.
    // We have to keep them around here because they will only be written
    // to the output in the *next* `add()` after the one
    // which inserted `_last_key`, because only then it will be
    // determined how much `_last_key` can be trimmed.
    int64_t _last_pos_payload;
    uint8_t _last_hash_bits;
};

bti_partition_index_writer_impl::bti_partition_index_writer_impl(bti_node_sink& out)
    : _wr(out) {
}

void bti_partition_index_writer_impl::write_last_key(size_t needed_prefix) {
    std::array<std::byte, 9> payload_bytes;
    auto payload_bytes_it = payload_bytes.data();
    // Write the hash byte to the payload buffer.
    *payload_bytes_it++ = std::byte(_last_hash_bits);

    // _last_pos_payload is either the position of the partition in Data.db
    // or the bit-negated position of the partition's entry in Rows.db.
    // The are distinguished via the sign bit.
    uint64_t file_pos = _last_pos_payload >= 0 ? _last_pos_payload : ~_last_pos_payload;
    // Note 1 extra bit needed for the sign.
    uint8_t pos_bytewidth = div_ceil(std::bit_width<uint64_t>(file_pos) + 1, 8);

    // Note: this flag says if a hash byte has been written to the payload.
    // (We always do that, so we always set the flag).
    constexpr uint8_t HASH_BYTE_FLAG = 0x8;
    // Build the 4 bits of metadata included in the first byte of the BTI node.
    uint8_t payload_bits = (pos_bytewidth - 1) | HASH_BYTE_FLAG;

    // Write n:=`pos_bytewidth` least significant bytes of `_last_pos_payload` to the payload buffer,
    // in big endian order.
    uint64_t pos_be = seastar::cpu_to_be<uint64_t>(_last_pos_payload << 8*(8 - pos_bytewidth));
    // sic. We only need `sizeof(pos_bytewidth)` bytes, but we copy 8 bytes to have a fixed-size copy.
    memcpy(payload_bytes_it, &pos_be, 8);
    payload_bytes_it += pos_bytewidth;
    // Pass the new node chain and its payload to the lower layer.
    // Note: we pass (payload_bits | 0x8) because the additional 0x8 bit indicates that hash bits are present.
    // (Even though currently they are always present).
    size_t i = 0;
    for (auto frag : **_last_key) {
        // The first fragment contains the entire token,
        // and token collisions are rare, hence [[unlikely]].
        if (i + frag.size() < _last_key_mismatch) [[unlikely]] {
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
            auto payload = trie_payload(
                payload_bits,
                std::span(payload_bytes.data(), payload_bytes_it)
            );
            _wr.add(i, frag.subspan(0, needed_prefix - i), payload);
            break;
        }

        i += frag.size();
    }
}

uint8_t hash_bits_from_dk(dht::decorated_key dk) {
    return dk.token().unbias();
}

void bti_partition_index_writer_impl::add(const schema& s, dht::decorated_key dk, int64_t pos_payload) {
    uint8_t hash_bits = hash_bits_from_dk(dk);
    expensive_log("partition_index_writer_impl::add: this={} key={}, pos_payload={}", fmt::ptr(this), dk, pos_payload);
    (*_tmp_key).emplace(s, dk);
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

void bti_partition_index_writer_impl::finish(sstable_version_types ver, disk_string_view<uint16_t> first_key, disk_string_view<uint16_t> last_key) {
    if (_added_keys > 0) {
        size_t needed_prefix = _last_key_mismatch + 1;
        write_last_key(needed_prefix);
    } else {
        return;
    }
    // Footer of Partitions.db.
    auto root = _wr.finish().value;
    auto& fw = _wr.sink().file_writer();
    auto keys_pos = fw.offset();
    write(ver, fw, first_key);
    write(ver, fw, last_key);
    write(ver, fw, static_cast<uint64_t>(keys_pos));
    write(ver, fw, static_cast<uint64_t>(_added_keys));
    write(ver, fw, static_cast<uint64_t>(root));
}

struct bti_partition_index_writer::impl
    : bti_node_sink
    , bti_partition_index_writer_impl
{
    impl(sstables::file_writer& fw)
        : bti_node_sink(fw, BTI_PAGE_SIZE)
        , bti_partition_index_writer_impl(static_cast<bti_node_sink&>(*this))
    {}
    impl(impl&&) = delete;
};

bti_partition_index_writer::bti_partition_index_writer(sstables::file_writer& fw)
    : _impl(std::make_unique<impl>(fw))
{}
bti_partition_index_writer::~bti_partition_index_writer() = default;
void bti_partition_index_writer::add(const schema& s, dht::decorated_key dk, int64_t data_or_rowsdb_file_pos) {
    _impl->add(s, std::move(dk), data_or_rowsdb_file_pos);
}
void bti_partition_index_writer::finish(sstable_version_types ver, disk_string_view<uint16_t> first_key, disk_string_view<uint16_t> last_key) && {
    _impl->finish(ver, first_key, last_key);
}

} // namespace sstables::trie