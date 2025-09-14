/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "bti_node_reader.hh"
#include "bti_node_type.hh"

namespace sstables::trie {

get_child_result bti_get_child(uint64_t pos, const_bytes sp, int child_idx, bool forward) {
    auto type = uint8_t(sp[0]) >> 4;
    trie::get_child_result result;
    auto single = [&](uint64_t offset) {
        result.offset = offset;
        result.idx = 0;
        return result;
    };
    auto sparse = [&] [[gnu::always_inline]] (int type) {
        auto bpp = bits_per_pointer_arr[type];
        result.idx = child_idx;
        result.offset = read_offset(sp.subspan(2 + int(sp[1])), child_idx, bpp);
        return result;
    };
    auto dense = [&] [[gnu::always_inline]] (int type) {
        auto bpp = bits_per_pointer_arr[type];
        auto dense_span = uint64_t(sp[2]) + 1;
        int idx = child_idx;
        int increment = forward ? 1 : -1;
        while (idx < int(dense_span) && idx >= 0) {
            if (auto off = read_offset(sp.subspan(3), idx, bpp)) {
                result.idx = idx;
                result.offset = off;
                return result;
            } else {
                idx += increment;
            }
        }
        [[unlikely]] sstables::on_bti_parse_error(pos);
    };
    switch (type) {
    case PAYLOAD_ONLY:
        [[unlikely]] sstables::on_bti_parse_error(pos);
    case SINGLE_NOPAYLOAD_4:
        return single(uint64_t(sp[0]) & 0xf);
    case SINGLE_NOPAYLOAD_12:
        return single((uint64_t(sp[0]) & 0xf) << 8 | uint64_t(sp[1]));
    case SINGLE_8:
        return single(uint64_t(sp[2]));
    case SINGLE_16:
        return single(uint64_t(sp[2]) << 8 | uint64_t(sp[3]));
    case SPARSE_8:
        return sparse(type);
    case SPARSE_12:
        return sparse(type);
    case SPARSE_16:
        return sparse(type);
    case SPARSE_24:
        return sparse(type);
    case SPARSE_40:
        return sparse(type);
    case DENSE_12:
        return dense(type);
    case DENSE_16:
        return dense(type);
    case DENSE_24:
        return dense(type);
    case DENSE_32:
        return dense(type);
    case DENSE_40:
        return dense(type);
    case LONG_DENSE:
        return dense(type);
    }
    [[unlikely]] sstables::on_bti_parse_error(pos);
}

std::byte bti_get_child_transition(uint64_t pos, const_bytes raw, int idx) {
    auto type = uint8_t(raw[0]) >> 4;
    switch (type) {
    case PAYLOAD_ONLY:
        abort();
    case SINGLE_NOPAYLOAD_4:
        return raw[1];
    case SINGLE_8:
        return raw[1];
    case SINGLE_NOPAYLOAD_12:
        return raw[2];
    case SINGLE_16:
        return raw[1];
    case SPARSE_8:
    case SPARSE_12:
    case SPARSE_16:
    case SPARSE_24:
    case SPARSE_40:
        return raw[2 + idx];
    case DENSE_12:
    case DENSE_16:
    case DENSE_24:
    case DENSE_32:
    case DENSE_40:
    case LONG_DENSE:
        return std::byte(uint8_t(raw[1]) + idx);
    }
    [[unlikely]] sstables::on_bti_parse_error(pos);
}

load_final_node_result bti_read_node(int64_t pos, const_bytes sp) {
    load_final_node_result result;
    auto type = uint8_t(sp[0]) >> 4;
    auto single = [&](uint8_t payload_bits) {
        result.n_children = 1;
        result.payload_bits = payload_bits;
        return result;
    };
    auto sparse = [&] [[gnu::always_inline]] (int type) {
        int n_children = int(sp[1]);
        result.n_children = n_children;
        result.payload_bits = uint8_t(sp[0]) & 0xf;
        return result;
    };
    auto dense = [&] [[gnu::always_inline]] (int type) {
        auto dense_span = uint64_t(sp[2]) + 1;
        result.n_children = dense_span;
        result.payload_bits = uint8_t(sp[0]) & 0xf;
        return result;
    };
    switch (type) {
    case PAYLOAD_ONLY:
        result.payload_bits = uint8_t(sp[0]) & 0xf;
        result.n_children = 0;
        return result;
    case SINGLE_NOPAYLOAD_4:
        return single(0);
    case SINGLE_NOPAYLOAD_12:
        return single(0);
    case SINGLE_8:
        return single(uint8_t(sp[0]) & 0xf);
    case SINGLE_16:
        return single(uint8_t(sp[0]) & 0xf);
    case SPARSE_8:
        return sparse(type);
    case SPARSE_12:
        return sparse(type);
    case SPARSE_16:
        return sparse(type);
    case SPARSE_24:
        return sparse(type);
    case SPARSE_40:
        return sparse(type);
    case DENSE_12:
        return dense(type);
    case DENSE_16:
        return dense(type);
    case DENSE_24:
        return dense(type);
    case DENSE_32:
        return dense(type);
    case DENSE_40:
        return dense(type);
    case LONG_DENSE:
        return dense(type);
    }
    [[unlikely]] sstables::on_bti_parse_error(pos);
}

const_bytes bti_get_payload(int64_t pos, const_bytes sp) {
    auto type = uint8_t(sp[0]) >> 4;
    switch (type) {
    case PAYLOAD_ONLY:
        return sp.subspan(1);
    case SINGLE_NOPAYLOAD_4:
    case SINGLE_NOPAYLOAD_12:
        return sp.subspan(1 + div_ceil(bits_per_pointer_arr[type], 8));
    case SINGLE_8:
    case SINGLE_16:
        return sp.subspan(2 + div_ceil(bits_per_pointer_arr[type], 8));
    case SPARSE_8:
    case SPARSE_12:
    case SPARSE_16:
    case SPARSE_24:
    case SPARSE_40: {
        auto n_children = uint8_t(sp[1]);
        return sp.subspan(2 + div_ceil(n_children * (8 + bits_per_pointer_arr[type]), 8));
    }
    case DENSE_12:
    case DENSE_16:
    case DENSE_24:
    case DENSE_32:
    case DENSE_40:
    case LONG_DENSE: {
        auto dense_span = uint8_t(sp[2]) + 1;
        return sp.subspan(3 + div_ceil(dense_span * bits_per_pointer_arr[type], 8));
    }
    }
    [[unlikely]] sstables::on_bti_parse_error(pos);
}

node_traverse_result bti_walk_down_along_key(int64_t pos, const_bytes sp, const_bytes key) {
    auto type = uint8_t(sp[0]) >> 4;
    trie::node_traverse_result result;
    result.body_pos = pos;
    result.traversed_key_bytes = 0;
    auto single = [&](std::byte edge, uint64_t offset, uint8_t payload_bits) {
        result.n_children = 1;
        result.payload_bits = payload_bits;
        if (key[0] <= edge) {
            result.found_idx = 0;
            result.found_byte = int(edge);
            result.child_offset = offset;
        } else {
            result.found_idx = 1;
            result.found_byte = -1;
            result.child_offset = -1;
        }
        return result;
    };
    auto sparse = [&] [[gnu::always_inline]] (int type) {
        int n_children = int(sp[1]);
        auto idx = std::lower_bound(&sp[2], &sp[2 + n_children], key[0]) - &sp[2];
        result.n_children = n_children;
        result.payload_bits = uint8_t(sp[0]) & 0xf;
        result.found_idx = idx;
        if (idx < n_children) {
            auto bpp = bits_per_pointer_arr[type];
            result.child_offset = read_offset(sp.subspan(2 + n_children), idx, bpp);
            result.found_byte = int(sp[2 + idx]);
        } else {
            result.child_offset = -1;
            result.found_byte = -1;
        }
        return result;
    };
    auto dense = [&] [[gnu::always_inline]] (int type) {
        auto start = int(sp[1]);
        auto idx = std::max<int>(0, int(key[0]) - start);
        auto dense_span = uint64_t(sp[2]) + 1;
        auto bpp = bits_per_pointer_arr[type];
        result.n_children = dense_span;
        result.payload_bits = uint8_t(sp[0]) & 0xf;
        while (idx < int(dense_span)) {
            if (auto off = read_offset(sp.subspan(3), idx, bpp)) {
                result.child_offset = off;
                result.found_idx = idx;
                result.found_byte = start + idx;
                return result;
            } else {
                ++idx;
            }
        }
        result.found_idx = dense_span;
        result.child_offset = -1;
        result.found_byte = -1;
        return result;
    };
    switch (type) {
    case PAYLOAD_ONLY:
        result.payload_bits = uint8_t(sp[0]) & 0xf;
        result.n_children = 0;
        result.found_idx = 0;
        result.found_byte = -1;
        result.child_offset = -1;
        return result;
    case SINGLE_NOPAYLOAD_4:
        return single(sp[1], uint64_t(sp[0]) & 0xf, 0);
    case SINGLE_NOPAYLOAD_12:
        return single(sp[2], (uint64_t(sp[0]) & 0xf) << 8 | uint64_t(sp[1]), 0);
    case SINGLE_8:
        return single(sp[1], uint64_t(sp[2]), uint8_t(sp[0]) & 0xf);
    case SINGLE_16:
        return single(sp[1], uint64_t(sp[2]) << 8 | uint64_t(sp[3]), uint8_t(sp[0]) & 0xf);
    case SPARSE_8:
        return sparse(type);
    case SPARSE_12:
        return sparse(type);
    case SPARSE_16:
        return sparse(type);
    case SPARSE_24:
        return sparse(type);
    case SPARSE_40:
        return sparse(type);
    case DENSE_12:
        return dense(type);
    case DENSE_16:
        return dense(type);
    case DENSE_24:
        return dense(type);
    case DENSE_32:
        return dense(type);
    case DENSE_40:
        return dense(type);
    case LONG_DENSE:
        return dense(type);
    }
    [[unlikely]] sstables::on_bti_parse_error(pos);
}

node_traverse_sidemost_result bti_walk_down_leftmost_path(int64_t pos, const_bytes sp) {
    auto type = uint8_t(sp[0]) >> 4;
    trie::node_traverse_sidemost_result result;
    result.body_pos = pos;
    auto single = [&](uint64_t offset, uint8_t payload_bits) {
        result.n_children = 1;
        result.payload_bits = payload_bits;
        result.child_offset = offset;
        return result;
    };
    auto sparse = [&] [[gnu::always_inline]] (int type) {
        int n_children = int(sp[1]);
        auto bpp = bits_per_pointer_arr[type];
        result.n_children = n_children;
        result.payload_bits = uint8_t(sp[0]) & 0xf;
        result.child_offset = read_offset(sp.subspan(2 + n_children), 0, bpp);
        return result;
    };
    auto dense = [&] [[gnu::always_inline]] (int type) {
        auto dense_span = uint64_t(sp[2]) + 1;
        auto bpp = bits_per_pointer_arr[type];
        result.n_children = dense_span;
        result.payload_bits = uint8_t(sp[0]) & 0xf;
        result.child_offset = read_offset(sp.subspan(3), 0, bpp);
        return result;
    };
    switch (type) {
    case PAYLOAD_ONLY:
        result.payload_bits = uint8_t(sp[0]) & 0xf;
        result.n_children = 0;
        result.child_offset = -1;
        return result;
    case SINGLE_NOPAYLOAD_4:
        return single(uint64_t(sp[0]) & 0xf, 0);
    case SINGLE_NOPAYLOAD_12:
        return single((uint64_t(sp[0]) & 0xf) << 8 | uint64_t(sp[1]), 0);
    case SINGLE_8:
        return single(uint64_t(sp[2]), uint8_t(sp[0]) & 0xf);
    case SINGLE_16:
        return single(uint64_t(sp[2]) << 8 | uint64_t(sp[3]), uint8_t(sp[0]) & 0xf);
    case SPARSE_8:
        return sparse(type);
    case SPARSE_12:
        return sparse(type);
    case SPARSE_16:
        return sparse(type);
    case SPARSE_24:
        return sparse(type);
    case SPARSE_40:
        return sparse(type);
    case DENSE_12:
        return dense(type);
    case DENSE_16:
        return dense(type);
    case DENSE_24:
        return dense(type);
    case DENSE_32:
        return dense(type);
    case DENSE_40:
        return dense(type);
    case LONG_DENSE:
        return dense(type);
    }
    [[unlikely]] sstables::on_bti_parse_error(pos);
}

node_traverse_sidemost_result bti_walk_down_rightmost_path(int64_t pos, const_bytes sp) {
    auto type = uint8_t(sp[0]) >> 4;
    trie::node_traverse_sidemost_result result;
    result.body_pos = pos;
    auto single = [&](uint64_t offset, uint8_t payload_bits) {
        result.n_children = 1;
        result.payload_bits = payload_bits;
        result.child_offset = offset;
        return result;
    };
    auto sparse = [&] [[gnu::always_inline]] (int type) {
        int n_children = int(sp[1]);
        auto bpp = bits_per_pointer_arr[type];
        result.n_children = n_children;
        result.payload_bits = uint8_t(sp[0]) & 0xf;
        result.child_offset = read_offset(sp.subspan(2 + n_children), n_children - 1, bpp);
        return result;
    };
    auto dense = [&] [[gnu::always_inline]] (int type) {
        auto dense_span = uint64_t(sp[2]) + 1;
        auto bpp = bits_per_pointer_arr[type];
        result.n_children = dense_span;
        result.payload_bits = uint8_t(sp[0]) & 0xf;
        result.child_offset = read_offset(sp.subspan(3), dense_span - 1, bpp);
        return result;
    };
    switch (type) {
    case PAYLOAD_ONLY:
        result.payload_bits = uint8_t(sp[0]) & 0xf;
        result.n_children = 0;
        result.child_offset = -1;
        return result;
    case SINGLE_NOPAYLOAD_4:
        return single(uint64_t(sp[0]) & 0xf, 0);
    case SINGLE_NOPAYLOAD_12:
        return single((uint64_t(sp[0]) & 0xf) << 8 | uint64_t(sp[1]), 0);
    case SINGLE_8:
        return single(uint64_t(sp[2]), uint8_t(sp[0]) & 0xf);
    case SINGLE_16:
        return single(uint64_t(sp[2]) << 8 | uint64_t(sp[3]), uint8_t(sp[0]) & 0xf);
    case SPARSE_8:
        return sparse(type);
    case SPARSE_12:
        return sparse(type);
    case SPARSE_16:
        return sparse(type);
    case SPARSE_24:
        return sparse(type);
    case SPARSE_40:
        return sparse(type);
    case DENSE_12:
        return dense(type);
    case DENSE_16:
        return dense(type);
    case DENSE_24:
        return dense(type);
    case DENSE_32:
        return dense(type);
    case DENSE_40:
        return dense(type);
    case LONG_DENSE:
        return dense(type);
    }
    [[unlikely]] sstables::on_bti_parse_error(pos);
}

bti_node_reader::bti_node_reader(cached_file& f)
    : _file(f) {
}

bool bti_node_reader::cached(int64_t pos) const {
    return _cached_page && _cached_page->pos() / cached_file::page_size == pos / cached_file::page_size;
}

seastar::future<> bti_node_reader::load(int64_t pos) {
    if (cached(pos)) {
        return make_ready_future<>();
    }
    return _file.get().get_shared_page(pos, nullptr).then([this](auto page) {
        _cached_page = std::move(page.ptr);
    });
}

trie::load_final_node_result bti_node_reader::read_node(int64_t pos) {
    SCYLLA_ASSERT(cached(pos));
    auto sp = _cached_page->get_view().subspan(pos % cached_file::page_size);
    return bti_read_node(pos, sp);
}

trie::node_traverse_result bti_node_reader::walk_down_along_key(int64_t pos, const_bytes key) {
    SCYLLA_ASSERT(cached(pos));
    auto sp = _cached_page->get_view().subspan(pos % cached_file::page_size);
    return bti_walk_down_along_key(pos, sp, key);
}

trie::node_traverse_sidemost_result bti_node_reader::walk_down_leftmost_path(int64_t pos) {
    SCYLLA_ASSERT(cached(pos));
    auto sp = _cached_page->get_view().subspan(pos % cached_file::page_size);
    return bti_walk_down_leftmost_path(pos, sp);
}

trie::node_traverse_sidemost_result bti_node_reader::walk_down_rightmost_path(int64_t pos) {
    SCYLLA_ASSERT(cached(pos));
    auto sp = _cached_page->get_view().subspan(pos % cached_file::page_size);
    return bti_walk_down_rightmost_path(pos, sp);
}

trie::get_child_result bti_node_reader::get_child(int64_t pos, int child_idx, bool forward) const {
    SCYLLA_ASSERT(cached(pos));
    auto sp = _cached_page->get_view().subspan(pos % cached_file::page_size);
    return bti_get_child(pos, sp, child_idx, forward);
}

const_bytes bti_node_reader::get_payload(int64_t pos) const {
    SCYLLA_ASSERT(cached(pos));
    auto sp = _cached_page->get_view().subspan(pos % cached_file::page_size);
    return bti_get_payload(pos, sp);
}

} // namespace sstables::trie
