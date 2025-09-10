/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/future.hh>

// This file defines an interface between the format-agnostic part of a trie
// reader (the cursor which can be set to a specific key and then
// stepped forwards and backwards) and the format-specific part
// (which actually understand the bytes and bits of the nodes on disk).

namespace sstables::trie {

using const_bytes = std::span<const std::byte>;

// Contains information about the result of the (partial) walk down.
//
// For example, if the trie contains strings ("123", "124", "23"),
// then the trie looks like this (where * is the root node):
//
// *
// 1-----2
// 2     3
// 3-4
//
// and if node "124" is on a different page than node "12", then
// `walk_down_along_key(pos_of_root, "123")` might
// return node "12" as the result, with
//
// payload_bits = 0
// n_children = 2
// found_idx = idx_of_child_4_in_node_12
// found_byte = '4'
// traversed_key_bytes = 2
// body_pos = pos_of_node_12
// child_offset = pos_of_node_12 - pos_of_node_123
struct node_traverse_result {
    // Payload bits for this node.
    uint8_t payload_bits;
    // Number of child indexes (slots) for this node.
    // ("Slots" because not all indexes have to be occupied).
    int n_children;
    // Index of first child whose transition is greater or equal to the next key byte.
    // (Or, if such children don't exist, this is equal to n_children).
    int found_idx;
    // The transition byte of the child at found_idx.
    // (Or, if there is no such child, this is equal to -1).
    int found_byte;
    // The number of edges traversed while walking down.
    // I.e. the number of edges between this node and the ancestor
    // at which the walk started.
    //
    // E.g. if we started at node X, and the
    // at which the walk started.
    int traversed_key_bytes;
    // File position of this node.
    int64_t body_pos;
    // Position of this node minus position of the child at found_idx.
    // Only valid if found_idx is valid in range [0, n_children).
    int64_t child_offset;
};

// Fields have same meaning as in node_traverse_result.
struct node_traverse_sidemost_result {
    uint8_t payload_bits;
    int n_children;
    int64_t body_pos;
    int64_t child_offset;
};

// Contains information about the result of the get_child call.
struct get_child_result {
    // The index of the child selected by the call.
    int idx;
    // The offset of the selected child from the parent.
    // (A children always has position smaller than its parent).
    uint64_t offset;
};

// Fields have same meaning as in node_traverse_result.
struct load_final_node_result {
    uint16_t n_children;
    uint8_t payload_bits;
};

// This concept contains all operations which abstract trie nodes
// needs to provide in order so that a trie cursor can be implemented
// over them.
template <typename T>
concept node_reader = requires(T& o, int64_t pos, const_bytes key, int child_idx, bool forwards) {
    // Checks if the page containing the given file position has been loaded.
    //
    // Note: forcing the caller to explicitly call `load` before use
    // is error-prone, but it lets us keep the performance-sensitive methods
    // synchronous.
    { o.cached(pos) } -> std::same_as<bool>;
    // Loads the page containing the given file position, and potentially
    // unloads previously loaded pages.
    //
    // Ensures `cached(pos)` until the next `load(pos)` call.
    //
    // Precondition: pos lies within the file.
    { o.load(pos) } -> std::same_as<seastar::future<>>;
    // Reads some basic information (payload and number of children)
    // about the node.
    //
    // Precondition: cached(pos)
    { o.read_node(pos) } -> std::same_as<load_final_node_result>;
    // Walks some distance down the trie
    // from node at `pos` along `key`.
    // Might walk over any number of *unimportant* nodes,
    // but does not walk past the first *important* node.
    //
    // An *important* node is one which fulfills at least one of the following conditions:
    // 1. Has a payload.
    // 2. Has more than one child.
    //
    // (A trie iterator has to remember the important nodes on the path from the root
    // to the current position, because stepping forwards or backwards might involve
    // going up to the closest important node. Unimportant nodes don't have to be revisited).
    //
    // Note: we allow this method to traverse many *unimportant* nodes
    // at once for optimization reasons. As of this writing, the method always
    // walks over one node, but we might want to add an optimization which
    // walks over long chains of SINGLE_NOPAYLOAD_4 nodes in batches.
    //
    // Precondition: cached(pos), key.size() > 0
    { o.walk_down_along_key(pos, key) } -> std::same_as<node_traverse_result>;
    // Like walk_down_along_key, but always chooses the leftmost child instead
    // of following a key.
    //
    // Precondition: cached(pos)
    { o.walk_down_leftmost_path(pos) } -> std::same_as<node_traverse_sidemost_result>;
    // Like walk_down_along_key, but always chooses the rightmost child instead
    // of following a key.
    //
    // Precondition: cached(pos)
    { o.walk_down_rightmost_path(pos) } -> std::same_as<node_traverse_sidemost_result>;
    // Returns some information about a child of node at `pos`.
    //
    // The child is specified by an index, but not all child indexes must be occupied by a child.
    // If an unoccupied index is passed, the method will return information for the nearest
    // occupied child index which is greater (if `forwards`) or smaller (if `!forwards`)
    //
    // (This is used for stepping forwards/backwards over the trie).
    //
    // Precondition: cached(pos), child_idx lies between the smallest and the largest
    // occupied child indexes (inclusive) for this node.
    { o.get_child(pos, child_idx, forwards) } -> std::same_as<get_child_result>;
    // Returns the payload for the payload-carrying node at `pos`.
    //
    // The child is specified by an index, but not all child indexes must be occupied by a child.
    // If an unoccupied index is passed, the method will return information for the nearest
    // occupied child index which is greater (if `forwards`) or smaller (if `!forwards`)
    //
    // (This is used for stepping forwards/backwards over the trie).
    //
    // Precondition: cached(pos), the node carries a payload
    { o.get_payload(pos) } -> std::same_as<const_bytes>;
};

} // namespace sstables::trie
