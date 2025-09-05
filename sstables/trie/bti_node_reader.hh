/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/cached_file.hh"
#include "node_reader.hh"

namespace sstables {
    [[noreturn, gnu::noinline]] void on_bti_parse_error(uint64_t pos);
}

namespace sstables::trie {

// Implementation of concept `node_reader`.
get_child_result bti_get_child(uint64_t pos, const_bytes sp, int child_idx, bool forward);
std::byte bti_get_child_transition(uint64_t pos, const_bytes raw, int idx);
load_final_node_result bti_read_node(int64_t pos, const_bytes sp);
const_bytes bti_get_payload(int64_t pos, const_bytes sp);
node_traverse_result bti_walk_down_along_key(int64_t pos, const_bytes sp, const_bytes key);
node_traverse_sidemost_result bti_walk_down_leftmost_path(int64_t pos, const_bytes sp);
node_traverse_sidemost_result bti_walk_down_rightmost_path(int64_t pos, const_bytes sp);

// Deals with BTI-specific parts of trie traversal.
// (I.e. the parts which understand the BTI serialization format).
//
// (We talk about "traversal" and not "deserialization" because we don't actually
// want to deserialize the full nodes, we only want to walk over them.)
//
// See comments for concept `node_reader` for a description of the methods.
struct bti_node_reader {
    // A copy-constructible wrapper over cached_file::ptr_type.
    // (Automatically calls share() on copy).
    struct page_ptr : cached_file::ptr_type {
        using parent = cached_file::ptr_type;
        page_ptr() noexcept = default;
        page_ptr(parent&& x) noexcept : parent(std::move(x)) {}
        page_ptr(const page_ptr& other) noexcept : parent(other ? other->share() : nullptr) {}
        page_ptr(page_ptr&&) noexcept = default;
        page_ptr& operator=(page_ptr&&) noexcept = default;
        page_ptr& operator=(const page_ptr& other) noexcept {
            parent::operator=(other ? other->share() : nullptr);
            return *this;
        }
    };

    page_ptr _cached_page;
    std::reference_wrapper<cached_file> _file;

    bti_node_reader(cached_file& f);
    bool cached(int64_t pos) const;
    future<> load(int64_t pos);
    trie::load_final_node_result read_node(int64_t pos);
    trie::node_traverse_result walk_down_along_key(int64_t pos, const_bytes key);
    trie::node_traverse_sidemost_result walk_down_leftmost_path(int64_t pos);
    trie::node_traverse_sidemost_result walk_down_rightmost_path(int64_t pos);
    trie::get_child_result get_child(int64_t pos, int child_idx, bool forward) const;
    const_bytes get_payload(int64_t pos) const;
};
static_assert(node_reader<bti_node_reader>);

} // namespace sstables::trie
