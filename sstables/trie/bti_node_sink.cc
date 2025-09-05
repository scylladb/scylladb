/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "bti_node_sink.hh"
#include "utils/div_ceil.hh"

namespace sstables::trie {

sink_offset max_offset_from_child(const writer_node& x, sink_pos xpos) {
    expensive_log("max_offset_from_child: x={} xpos={}", fmt::ptr(&x), xpos.value);
    // We iterate over children in reverse order and, for the ones which aren't written yet,
    // we compute their expected output position based on the accumulated values of _node_size and _branch_size.

    // Total size of unwritten child branches which we iterated over so far.
    // This is the offset between `x` and the next unwritten child branch we are going to see.
    auto offset = sink_offset{0};
    // Max offset noticed so far during iteration.
    auto result = sink_offset{0};
    for (auto it = x.get_children().rbegin(); it != x.get_children().rend(); ++it) {
        if ((*it)->_pos.valid()) {
            // This child has already been written out. It's position is set in stone.
            expensive_log("max_offset_from_child: offset={} child={} _pos={} _node_size={}, _branch_size={}",
                          offset.value, fmt::ptr(&**it), (*it)->_pos.value, (*it)->_node_size.value, (*it)->_branch_size.value);
            expensive_assert((*it)->_pos < xpos);
            result = std::max<sink_offset>(result, xpos - (*it)->_pos);
        } else {
            // Child `*it` hasn't been written out yet.
            // We compute its offset from `x` based on the total size of all siblings after it
            // and the node size of `*it`.
            auto delta = offset;
            auto chain_length = (*it)->_transition_length;
            if (chain_length > 2) {
                delta = delta + sink_offset(2);
            } else if (chain_length == 2) {
                delta = delta + ((*it)->_node_size.value < 16 ? sink_offset(2) : sink_offset(3));
            } else {
                delta = delta + (*it)->_node_size;
            }
            expensive_log("max_offset_from_child: offset={} child={} _pos={} _node_size={}, _branch_size={} delta={}",
                          offset.value, fmt::ptr(&**it), (*it)->_pos.value, (*it)->_node_size.value, (*it)->_branch_size.value, delta.value);
            result = std::max<sink_offset>(result, delta);
        }
        offset = offset + (*it)->_node_size + (*it)->_branch_size;
    }
    expensive_log("max_offset_from_child: x={} xpos={} result={}", fmt::ptr(&x), xpos.value, result.value);
    return result;
}

// Chooses the best node type (i.e the one which will have the smallest valid on-disk representation)
// for the given writer_node, assuming that the values of _node_size, _branch_size and _output_pos
// in its children are accurate,
// and that the given node would be written at output position `xpos`.
static node_type choose_node_type_impl(const writer_node& x, sink_pos xpos) {
    const auto n_children = x.get_children().size();
    if (n_children == 0) {
        // If there is no children, the answer is obvious.
        return PAYLOAD_ONLY;
    }
    auto max_offset = max_offset_from_child(x, xpos);
    // For a given offset bitwidth, contains the index of the leanest node type
    // in singles[], sparses[] and denses[] which can be used to represent a node with
    // this offset bitwidth.
    constexpr static uint8_t widths_lookup[] = {
        0,
        0, 0, 0, 0,
        1, 1, 1, 1,
        2, 2, 2, 2,
        3, 3, 3, 3,
        4, 4, 4, 4,
        4, 4, 4, 4,
        5, 5, 5, 5,
        5, 5, 5, 5,
        6, 6, 6, 6,
        6, 6, 6, 6,
        7, 7, 7, 7,
        7, 7, 7, 7,
        7, 7, 7, 7,
        7, 7, 7, 7,
        7, 7, 7, 7,
        7, 7, 7, 7,
    };
    auto width_idx = widths_lookup[std::bit_width<uint64_t>(max_offset.value)];
    // Nodes with a single close child are very common, so they have dedicated node types.
    // For offset widths which don't fit into the dedicated types,
    // singles[] returns the smallest valid non-dedicated (multi-child) type.
    constexpr node_type singles[] = {SINGLE_NOPAYLOAD_4, SINGLE_8, SINGLE_NOPAYLOAD_12, SINGLE_16, DENSE_24, DENSE_32, DENSE_40, LONG_DENSE};
    if (n_children == 1) {
        const auto has_payload = x._payload._payload_bits;
        if (has_payload && (width_idx == 0 || width_idx == 2)) {
            // SINGLE_NOPAYLOAD_4 and SINGLE_NOPAYLOAD_12 can't hold nodes with a payload.
            // Fall back to the next smallest node type which can
            // (SINGLE_8 and SINGLE_16 respectively).
            return singles[width_idx + 1];
        }
        return singles[width_idx];
    }
    // For nodes with 2 or more children, we calculate the sizes that would result from choosing
    // either the leanest dense node or the leanest sparse node, and we pick the one which
    // results in smaller size.
    constexpr node_type sparses[] = {SPARSE_8, SPARSE_8, SPARSE_12, SPARSE_16, SPARSE_24, SPARSE_40, SPARSE_40, LONG_DENSE};
    constexpr node_type denses[] = {DENSE_12, DENSE_12, DENSE_12, DENSE_16, DENSE_24, DENSE_32, DENSE_40, LONG_DENSE};
    const size_t dense_span = 1 + size_t(x.get_children().back()->transition()) - size_t(x.get_children().front()->transition());
    auto sparse_size = 16 + div_ceil((bits_per_pointer_arr[sparses[width_idx]] + 8) * n_children, 8);
    auto dense_size = 24 + div_ceil(bits_per_pointer_arr[denses[width_idx]] * dense_span, 8);
    // If the sizes are equal, pick the dense type, because dense nodes are easier for the reader.
    if (sparse_size < dense_size) {
        return sparses[width_idx];
    } else {
        return denses[width_idx];
    }
}

// This wrapper exists just to log the return value.
// (Logging the return value of a function with multiple `return` points
// is awkward).
static node_type choose_node_type(const writer_node& x, sink_pos xpos) {
    auto res = choose_node_type_impl(x, xpos);
    expensive_log("choose_node_type: x={} res={}", fmt::ptr(&x), int(res));
    return res;
}

bti_node_sink::bti_node_sink(sstables::file_writer& w, size_t page_size) : _w(w), _page_size(page_size) {
    expensive_assert(_page_size <= max_page_size);
}

void bti_node_sink::write_int(uint64_t x, size_t bytes) {
    uint64_t be = cpu_to_be(x);
    expensive_log("write_int: {}", fmt_hex({reinterpret_cast<const signed char*>(&be) + sizeof(be) - bytes, bytes}));
    _w.write(reinterpret_cast<const char*>(&be) + sizeof(be) - bytes, bytes);
}

void bti_node_sink::write_bytes(const_bytes x) {
    expensive_log("write_bytes: {}", fmt_hex({reinterpret_cast<const signed char*>(x.data()), x.size()}));
    _w.write(reinterpret_cast<const char*>(x.data()), x.size());
}

[[gnu::always_inline]]
size_t bti_node_sink::write_sparse(const writer_node& x, node_type type, int bytes_per_pointer, sink_pos xpos) {
    write_int((type << 4) | x._payload._payload_bits, 1);
    write_int(x.get_children().size(), 1);
    for (const auto& c : x.get_children()) {
        write_int(uint8_t(c->transition()), 1);
    }
    for (const auto& c : x.get_children()) {
        uint64_t offset = (xpos - c->_pos).value;
        write_int(offset, bytes_per_pointer);
    }
    write_bytes(x._payload.blob());
    return 2 + x.get_children().size() * (1+bytes_per_pointer) + x._payload.blob().size();
}

[[gnu::always_inline]]
node_size bti_node_sink::size_sparse(const writer_node& x, int bits_per_pointer) const {
    return node_size(2 + div_ceil(x.get_children().size() * (8+bits_per_pointer), 8) + x._payload.blob().size());
}

[[gnu::always_inline]]
size_t bti_node_sink::write_dense(const writer_node& x, node_type type, int bytes_per_pointer, sink_pos xpos) {
    int start = int(x.get_children().front()->transition());
    auto dense_span = 1 + int(x.get_children().back()->transition()) - int(x.get_children().front()->transition());
    write_int((type << 4) | x._payload._payload_bits, 1);
    write_int(start, 1);
    write_int(dense_span - 1, 1);
    auto it = x.get_children().begin();
    auto end_it = x.get_children().end();
    for (int next = start; next < start + dense_span; ++next) {
        uint64_t offset = 0;
        if (it != end_it && int((*it)->transition()) == next) {
            offset = (xpos - (*it)->_pos).value;
            ++it;
        }
        write_int(offset, bytes_per_pointer);
    }
    write_bytes(x._payload.blob());
    return 3 + dense_span * (bytes_per_pointer) + x._payload.blob().size();
}

[[gnu::always_inline]]
node_size bti_node_sink::size_dense(const writer_node& x, int bits_per_pointer) const  {
    int first = int(x.get_children().front()->transition());
    int last = int(x.get_children().back()->transition());
    return node_size(3 + div_ceil(bits_per_pointer * (1 + last - first), 8) + x._payload.blob().size());
}

void bti_node_sink::write_body(const writer_node& x, sink_pos xpos, node_type type) {
    switch (type) {
    case PAYLOAD_ONLY: {
        write_int(type << 4 | x._payload._payload_bits, 1);
        write_bytes(x._payload.blob());
        return;
    }
    case SINGLE_NOPAYLOAD_4: {
        uint64_t offset = (xpos - x.get_children().front()->_pos).value;
        uint8_t transition = uint8_t(x.get_children().front()->transition());
        uint8_t arr[2];
        arr[0] = (type << 4) | offset;
        arr[1] = transition;
        write_bytes({reinterpret_cast<const std::byte*>(arr), 2});
        return;
    }
    case SINGLE_8: {
        uint64_t offset = (xpos - x.get_children().front()->_pos).value;
        uint8_t transition = uint8_t(x.get_children().front()->transition());
        uint8_t arr[3 + sizeof(trie_payload::_payload_buf)];
        arr[0] = (type << 4) | x._payload._payload_bits;
        arr[1] = transition;
        arr[2] = offset;
        auto sz = x._payload.blob().size();
        memcpy(&arr[3], x._payload.blob().data(), sz);
        write_bytes({reinterpret_cast<const std::byte*>(arr), 3 + sz});
        return;
    }
    case SINGLE_NOPAYLOAD_12: {
        uint64_t offset = (xpos - x.get_children().front()->_pos).value;
        uint8_t transition = uint8_t(x.get_children().front()->transition());
        write_int((type << 4) | (offset >> 8), 1);
        write_int(offset & 0xff, 1);
        write_int(transition, 1);
        return;
    }
    case SINGLE_16: {
        uint64_t offset = (xpos - x.get_children().front()->_pos).value;
        uint8_t transition = uint8_t(x.get_children().front()->transition());
        write_int((type << 4) | x._payload._payload_bits, 1);
        write_int(transition, 1);
        write_int(offset, 2);
        write_bytes(x._payload.blob());
        return;
    }
    case SPARSE_8: {
        write_sparse(x, type, 1, xpos);
        return;
    }
    case SPARSE_12: {
        write_int((type << 4) | x._payload._payload_bits, 1);
        write_int(x.get_children().size(), 1);
        for (const auto& c : x.get_children()) {
            write_int(uint8_t(c->transition()), 1);
        }
        size_t i;
        for (i = 0; i + 1 < x.get_children().size(); i += 2) {
            uint64_t offset1 = (xpos - x.get_children()[i]->_pos).value;
            uint64_t offset2 = (xpos - x.get_children()[i+1]->_pos).value;
            write_int(offset1 << 12 | offset2, 3);
        }
        if (i < x.get_children().size()) {
            uint64_t offset = (xpos - x.get_children()[i]->_pos).value;
            write_int(offset << 4, 2);
        }
        write_bytes(x._payload.blob());
        return;
    }
    case SPARSE_16: {
        write_sparse(x, type, 2, xpos);
        return;
    }
    case SPARSE_24: {
        write_sparse(x, type, 3, xpos);
        return;
    }
    case SPARSE_40: {
        write_sparse(x, type, 5, xpos);
        return;
    }
    case DENSE_12: {
        int start = int(x.get_children().front()->transition());
        auto dense_span = 1 + int(x.get_children().back()->transition()) - int(x.get_children().front()->transition());
        write_int((type << 4) | x._payload._payload_bits, 1);
        write_int(start, 1);
        write_int(dense_span - 1, 1);
        auto it = x.get_children().begin();
        auto end_it = x.get_children().end();
        int next = start;
        for (; next + 1 < start + dense_span; next += 2) {
            uint64_t offset_1 = 0;
            uint64_t offset_2 = 0;
            if (it != end_it && int((*it)->transition()) == next) {
                offset_1 = (xpos - (*it)->_pos).value;
                ++it;
            }
            if (it != end_it && int((*it)->transition()) == next + 1) {
                offset_2 = (xpos - (*it)->_pos).value;
                ++it;
            }
            write_int(offset_1 << 12 | offset_2, 3);
        }
        if (next < start + dense_span) {
            uint64_t offset = 0;
            if (it != end_it && int((*it)->transition()) == next) {
                offset = (xpos - (*it)->_pos).value;
                ++it;
            }
            write_int(offset << 4, 2);
        }
        write_bytes(x._payload.blob());
        return;
    }
    case DENSE_16: {
        write_dense(x, type, 2, xpos);
        return;
    }
    case DENSE_24: {
        write_dense(x, type, 3, xpos);
        return;
    }
    case DENSE_32: {
        write_dense(x, type, 4, xpos);
        return;
    }
    case DENSE_40: {
        write_dense(x, type, 5, xpos);
        return;
    }
    case LONG_DENSE: {
        write_dense(x, type, 8, xpos);
        return;
    }
    case NODE_TYPE_COUNT: {
        abort();
    }
    }
    abort();
}

sink_pos bti_node_sink::write_chain(const writer_node& x, node_size body_offset) {
    int i = x._transition_length;
    expensive_assert(i >= 2);

    const std::byte* __restrict__ transition = x._transition.get();
    sink_pos c1_pos = pos();

    // Second-to-last node in the chain (i.e. last node before the body) can have size 2 or 3 bytes,
    // depending on how big the last node (body) is.
    uint64_t offset = body_offset.value;
    if (offset >= 16) {
        expensive_assert(offset < 4096);
        write_int(uint64_t(transition[i - 1]) | offset << 8 | uint64_t(SINGLE_NOPAYLOAD_12 << 20), 3);
    } else {
        write_int(uint64_t(transition[i - 1]) | offset << 8 | uint64_t(SINGLE_NOPAYLOAD_4 << 12), 2);
    }

    i -= 1;
    if (i == 1) {
        return c1_pos;
    }

    // Third-to-last node in the chain has always size 2, but the offset can be equal to 3 or 2, depending
    // on how big the second-to-last node was.
    offset = (pos() - c1_pos).value;
    write_int(uint64_t(transition[i - 1]) | offset << 8 | uint64_t(SINGLE_NOPAYLOAD_4 << 12), 2);

    i -= 1;

    // Fourth-to-last and earlier nodes in the chain always have the form 0x12??, where ?? is the transition byte.
    // This is SIMDable -- we can load a vector of transition bytes from memory,
    // reverse the order (earlier bytes in the transition chain become later nodes in the file)
    // and add a 0x12 byte before every transition byte.
    constexpr int s = 16; // Vector size.
    typedef unsigned char vector2x __attribute__((__vector_size__(s*2))); // Output vector.
    typedef unsigned char vector1x __attribute__((__vector_size__(s))); // Input vector.
    auto z = uint8_t(SINGLE_NOPAYLOAD_4 << 4 | 2); // 0x12
    vector1x zv = {z};

    // An extra buffer above the file_writer. It allows us to do constexpr-sized
    // memcpy, but incurs an extra copy overall. Is it worth it?
    std::array<std::byte, 1024> outbuf;
    static_assert(std::size(outbuf) % s == 0);
    size_t outbuf_pos = 0;

    // Serialize the chain in SIMD blocks of `s` transition bytes.
    for (; i - s > 0; i -= s) {
        vector1x v;
        memcpy(&v, &transition[i - s], s);
        vector2x d = __builtin_shufflevector(v, zv,
            s, 15, s, 14, s, 13, s, 12, s, 11, s, 10, s, 9, s, 8,
            s, 7, s, 6, s, 5, s, 4, s, 3, s, 2, s, 1, s, 0
        );
        memcpy(&outbuf[outbuf_pos], &d, sizeof(d));
        outbuf_pos += sizeof(d);
        if (outbuf_pos == 1024) [[unlikely]] {
            write_bytes(outbuf);
            outbuf_pos = 0;
        }
    }

    // Write the remaining `i - 1` first bytes (excluding idx 0) of the transition chain.
    // This is separated from the previous loop so that the previous loop enjoys the benefit
    // of constexpr-sized memory ops.
    {
        vector1x v;
        // Load into the `i - 1` last bytes of `v`.
        memcpy(reinterpret_cast<char*>(&v) + sizeof(v) - (i - 1), &transition[1], (i - 1));
        vector2x d = __builtin_shufflevector(v, zv,
            s, 15, s, 14, s, 13, s, 12, s, 11, s, 10, s, 9, s, 8,
            s, 7, s, 6, s, 5, s, 4, s, 3, s, 2, s, 1, s, 0
        );
        // Write out the first `2*(i-1)` first bytes of `v`.
        memcpy(&outbuf[outbuf_pos], &d, 2 * (i - 1));
        outbuf_pos += 2 * (i - 1);
        write_bytes(std::span(outbuf).subspan(0, outbuf_pos));
    }

    return pos() - sink_offset(2);
}

sink_pos bti_node_sink::write(const writer_node& x, sink_pos xpos) {
    expensive_assert(x._transition_length >= 1);

    // Write last node in the chain.
    sink_pos start_pos = pos();
    auto type = choose_node_type(x, xpos);
    write_body(x, xpos, type);

    if (x._transition_length == 1) {
        return start_pos;
    }

    return write_chain(x, node_size((pos() - start_pos).value));
}

node_size bti_node_sink::serialized_size(const writer_node& x, sink_pos xpos) const {
    expensive_assert(x._transition_length >= 1);
    auto inner = serialized_size_body(x, xpos);
    auto outer = serialized_size_chain(x, inner);
    return node_size((sink_offset(inner) + outer).value);
}

node_size bti_node_sink::serialized_size_chain(const writer_node& x, node_size body_offset) const {
    return node_size(x._transition_length == 1 ? 0 : (body_offset.value >= 16 ? 1 : 0) + (x._transition_length - 1) * 2);
}

node_size bti_node_sink::serialized_size_body_type(const writer_node& x, node_type type) const {
    switch (type) {
    case PAYLOAD_ONLY: {
        return node_size(1 + x._payload.blob().size());
    }
    case SINGLE_NOPAYLOAD_4: {
        return node_size(2);
    }
    case SINGLE_8: {
        return node_size(3 + x._payload.blob().size());
    }
    case SINGLE_NOPAYLOAD_12: {
        return node_size(3);
    }
    case SINGLE_16: {
        return node_size(4 + x._payload.blob().size());
    }
    case SPARSE_8: {
        return size_sparse(x, 8);
    }
    case SPARSE_12: {
        return size_sparse(x, 12);
    }
    case SPARSE_16: {
        return size_sparse(x, 16);
    }
    case SPARSE_24: {
        return size_sparse(x, 24);
    }
    case SPARSE_40: {
        return size_sparse(x, 40);
    }
    case DENSE_12: {
        return size_dense(x, 12);
    }
    case DENSE_16: {
        return size_dense(x, 16);
    }
    case DENSE_24: {
        return size_dense(x, 24);
    }
    case DENSE_32: {
        return size_dense(x, 32);
    }
    case DENSE_40: {
        return size_dense(x, 40);
    }
    case LONG_DENSE: {
        return size_dense(x, 64);
    }
    case NODE_TYPE_COUNT: {
        abort();
    }
    }
    abort();
}

node_size bti_node_sink::serialized_size_body(const writer_node& x, sink_pos xpos) const {
    return serialized_size_body_type(x, choose_node_type(x, xpos));
}

uint64_t bti_node_sink::page_size() const {
    return _page_size;
}

uint64_t bti_node_sink::bytes_left_in_page() const {
    return round_up(pos().value + 1, page_size()) - pos().value;
};

void bti_node_sink::pad_to_page_boundary() {
    const static std::array<std::byte, max_page_size> zero_page = {};
    _w.write(reinterpret_cast<const char*>(zero_page.data()), bytes_left_in_page());
}

sink_pos bti_node_sink::pos() const {
    return sink_pos(_w.offset());
}

sstables::file_writer& bti_node_sink::file_writer() {
    return _w;
}

} // namespace sstables::trie
