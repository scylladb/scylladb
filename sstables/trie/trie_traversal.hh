/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "node_reader.hh"
#include "utils/small_vector.hh"
#include <seastar/core/future.hh>
#include <seastar/core/coroutine.hh>
#include "common.hh"
#include "utils/assert.hh"

namespace sstables::trie {

// To be able to step over the trie (forwards and backwards),
// the cursor has to keep a trail of all nodes between
// root and the currently-visited node which
// have a payload or more than one child.
//
// This type represents an entry in this trail.
//
// See also: node_reader::walk_down_along_key()
struct trail_entry {
    // Position/ID of the node in the file.
    uint64_t pos;
    // Position/ID of the node in the file.
    uint16_t n_children;
    // Index of the currently-visited child slot.
    // (I.e. the next node in the trail is in the branch starting at this child slot).
    // Special value -1 means that we are currently visiting *this* node,
    // not any of its children.
    int16_t child_idx;
    // Payload bits for this node.
    // (Non-zero if this node has a payload).
    uint8_t payload_bits;
    // For tests.
    std::strong_ordering operator<=>(const trail_entry&) const = default;
};

// Note: for the partition index, the trail should never be longer than ~6 entries.
// For the row index, it can get arbitrarily large.
struct ancestor_trail : utils::small_vector<trail_entry, 8> {
};

template <typename T>
concept bytespan_ref = std::same_as<T, std::span<std::byte>&&>
    || std::same_as<T, std::span<const std::byte>&&>;

// Converting keys from the regular Scylla format to the BTI byte-comparable format
// can be expensive, so we allow the conversion to happen lazily.
// To support that, the traversal routines take the key as an iterator/generator
// of fragments (byte spans).
//
// Note: a std::generator<const_bytes>::iterator fulfils this concept.
template <typename T>
concept comparable_bytes_iterator = requires (T it) {
    // Users of comparable_bytes_iterator want to modify the contents of `*it`
    // (for their convenience, not due to necessity), so operator* returns a reference.
    { *it } -> bytespan_ref;
    // Allowed to destroy `*it`;
    { ++it };
    // Since this concept is designed for lazy evaluation,
    // the "end" of the sequence is usually more naturally encoded
    // as something in the iterator itself, rather than some external "end" object.
    //
    // So we don't bother with iterator pairs and we instead
    // require the iterator itself to know if it's done.
    { it == std::default_sentinel } -> std::same_as<bool>;
    { it != std::default_sentinel } -> std::same_as<bool>;
};

// State of the "traverse" routine.
// Serves as both an input and an output argument.
struct traversal_state {
    // The next node we should look at.
    // Should be initialized to the position of the root node
    // when the search starts.
    // Special value `-1` means that the search is over.
    int64_t next_pos = -1;
    // The number of traversed trie edges (== number of traversed characters of the key).
    int edges_traversed = 0;
    // The trail of all "important" (cf. node_reader::walk_down_along_key)
    // ancestors of the currently-visited node/edge.
    //
    // (Note: after "traverse" return, the final ("found") node
    // is included here regardless of whether it's "important".
    ancestor_trail trail;
};

// The synchronous part of `traverse`.
// Walks down the trie,
// over the nodes inside the page currently loaded in `page`,
// along edges selected by `key_it`,
// appending all visited "important" (cf `node_reader::walk_down_along_key`)
// updating `state` accordingly,
// until `key_it` is exhausted, or leaf is reached, or there's no child matching the next byte of `key_it`.
inline void traverse_single_page(
    node_reader auto& page,
    comparable_bytes_iterator auto&& key_it,
    traversal_state& state
) {
    // Body of the loop wants to know the next character of `key_it`,
    // and accesses it as `(*key_it)[0]`.
    // We need to skip empty fragments somewhere for that to work.
    // We do it before each loop iteration.
    while (key_it != std::default_sentinel && (*key_it).empty()) {
        ++key_it;
    }
    while (page.cached(state.next_pos) && key_it != std::default_sentinel) {
        // Empty fragments should have been already skipped.
        expensive_assert(!(*key_it).empty());

        node_traverse_result traverse_one = page.walk_down_along_key(state.next_pos, *key_it);
        state.edges_traversed += traverse_one.traversed_key_bytes;
        // Note: the protocol between `walk_down_along_key` and `traverse_single_page`
        // is that `walk_down_along_key` can silently skip some "unimportant"
        // edges without giving `traverse_single_page` a chance to remember them,
        // and those skipped edges are counted into `traversed_key_bytes`,
        // but the last visited edge (i.e. the edge which best matches `(*key_it)[traversed_key_bytes]`)
        // is returned to the caller and is not counted into `traversed_key_bytes`.
        // (even if it's just as "skippable" as the skipped edges).
        //
        // So with a valid `node_reader`, `traversed_key_bytes` must have been smaller
        // than key_it->.size()
        *key_it = (*key_it).subspan(traverse_one.traversed_key_bytes);
        expensive_assert(!(*key_it).empty());

        // walk_down_along_key finds the first child edge greater or equal
        // to `(*key_it)[0]`.
        // If it's greater, then the traversal stops here. If it's equal, we will continue walking down.
        //
        // Note: this doesn't mean that we can continue walking down *in the current page*,
        // just that we can continue in general. It might require loading a new page.
        bool can_continue = traverse_one.found_byte == int((*key_it)[0]);

        if (can_continue) {
            // The chosen edge wasn't "traversed" by `walk_down_along_key`,
            // (just because that's the protocol we chose).
            // We "traverse" this edge ourselves, here.
            state.next_pos = traverse_one.body_pos - traverse_one.child_offset;
            state.edges_traversed += 1;
            *key_it = (*key_it).subspan(1);
        } else {
            // Special value -1 means that the traversal is finished.
            state.next_pos = -1;
        }
        // We add the node to the trail if if's an "important" node (has payload or children)
        // or if it's the last node reachable along the passed key.
        bool add_to_trail = traverse_one.payload_bits || traverse_one.n_children > 1 || !can_continue;
        expensive_log("traversing pos={} add_to_trail={} found_byte={} can_continue={} body_pos={} child_offset={}",
                state.next_pos, add_to_trail, traverse_one.found_byte, can_continue, traverse_one.body_pos, traverse_one.child_offset);
        if (add_to_trail) {
            state.trail.push_back(trail_entry{
                .pos = traverse_one.body_pos,
                .n_children = traverse_one.n_children,
                .child_idx = traverse_one.found_idx,
                .payload_bits = traverse_one.payload_bits});
        }

        // Body of the loop wants to know the next character of `key_it`,
        // and accesses it as `(*key_it)[0]`.
        // We need to skip empty fragments somewhere for that to work.
        // We do it before each loop iteration.
        while (key_it != std::default_sentinel && (*key_it).empty()) {
            ++key_it;
        }
    }
}

// Walks down the trie, from the given root,
// along the edges matching `key_it`,
// until the key is exhausted or there's no child matching
// the next key byte.
//
// The returned trail contains the last visited node
// and all its "important" ancestors.
//
// The alst visited node has `.child_idx` set to -1 if it matches the key exactly,
// or to "lower_bound(child_edges, first_mismatching_key_byte)" otherwise.
inline seastar::future<traversal_state> traverse(
    node_reader auto& input,
    comparable_bytes_iterator auto&& key_it,
    int64_t root_pos
) {
    traversal_state state = {.next_pos = root_pos};
    while (state.next_pos >= 0 && key_it != std::default_sentinel) {
        co_await input.load(state.next_pos);
        traverse_single_page(input, key_it, state);
    }
    if (state.next_pos >= 0) {
        co_await input.load(state.next_pos);
        load_final_node_result final_node = input.read_node(state.next_pos);
        state.trail.push_back(trail_entry{
                .pos = state.next_pos,
                .n_children = final_node.n_children,
                .child_idx = -1,
                .payload_bits = final_node.payload_bits});
    }
    co_await input.load(state.trail.back().pos);
    co_return std::move(state);
}

// The synchronous part of descend_leftmost.
inline void descend_leftmost_single_page(
    node_reader auto& page,
    ancestor_trail& trail,
    int64_t& next_pos
) {
    while (page.cached(next_pos)) {
        node_traverse_sidemost_result traverse_one = page.walk_down_leftmost_path(next_pos);
        if (traverse_one.payload_bits || traverse_one.n_children > 1) {
            if (!(trail.back().payload_bits || trail.back().n_children > 1)) {
                trail.pop_back();
            }
            trail.push_back(trail_entry{
                .pos = traverse_one.body_pos,
                .n_children = traverse_one.n_children,
                .child_idx = 0,
                .payload_bits = traverse_one.payload_bits});
        }

        if (traverse_one.payload_bits) {
            next_pos = -1;
            trail.back().child_idx = -1;
        } else {
            SCYLLA_ASSERT(traverse_one.n_children >= 1);
            next_pos = traverse_one.body_pos - traverse_one.child_offset;
        }
    }
}

// Walks down the trie to the leaf, from a node with the given
// trail and position, always taking the leftmost path.
inline seastar::future<> descend_leftmost(
    node_reader auto& input,
    ancestor_trail& trail,
    int64_t next_pos
) {
    while (next_pos >= 0) {
        co_await input.load(next_pos);
        // Note: next_pos passed by reference.
        descend_leftmost_single_page(input, trail, next_pos);
    }
}

// Ascends to the first node which has a child edge to the right
// of the current path, and visits that edge.
inline bool ascend_to_right_edge(ancestor_trail& trail) {
    if (trail.back().child_idx != -1) {
        trail.back().child_idx -= 1;
    }
    for (int i = trail.size() - 1; i >= 0; --i) {
        if (trail[i].child_idx + 1 < trail[i].n_children) {
            trail[i].child_idx += 1;
            trail.resize(i + 1);
            return true;
        }
    }
    trail[0].child_idx = trail[0].n_children;
    trail.resize(1);
    return false;
}

// Steps the cursor represented by `trail` forwards until
// the first payloaded node.
// If there's no such node, visits EOF.
// (I.e. visits the root of the trail and sets its child idx past the end).
inline seastar::future<> step(
    node_reader auto& input,
    ancestor_trail& trail
) {
    if (ascend_to_right_edge(trail)) {
        co_await input.load(trail.back().pos);
        get_child_result child = input.get_child(trail.back().pos, trail.back().child_idx, true);

        trail.back().child_idx = child.idx;
        int64_t next_pos = trail.back().pos - child.offset;
        co_await descend_leftmost(input, trail, next_pos);
    }
    co_await input.load(trail.back().pos);
}

// The synchronous part of descend_rightmost.
inline void descend_rightmost_single_page(
    node_reader auto& page,
    ancestor_trail& trail,
    int64_t& next_pos
) {
    while (page.cached(next_pos)) {
        node_traverse_sidemost_result traverse_one = page.walk_down_rightmost_path(next_pos);
        if (traverse_one.payload_bits || traverse_one.n_children > 1) {
            if (!(trail.back().payload_bits || trail.back().n_children > 1)) {
                trail.pop_back();
            }
            trail.push_back(trail_entry{
                .pos = traverse_one.body_pos,
                .n_children = traverse_one.n_children,
                .child_idx = traverse_one.n_children - 1,
                .payload_bits = traverse_one.payload_bits});
        }

        if (traverse_one.n_children >= 1) {
            next_pos = traverse_one.body_pos - traverse_one.child_offset;
        } else {
            next_pos = -1;
            trail.back().child_idx = -1;
        }
    }
}

// Like descend_leftmost, but always takes the rightmost path.
inline seastar::future<> descend_rightmost(
    node_reader auto& input,
    ancestor_trail& trail,
    int64_t next_pos
) {
    while (next_pos >= 0) {
        co_await input.load(next_pos);
        // Note: next_pos passed by reference.
        descend_rightmost_single_page(input, trail, next_pos);
    }
}

// If the currently visited edge-or-node has a proper ancestor node
// with a child edge to the left of the trail, visits that edge.
// Otherwise, if the currently visited edge-or-node has a proper ancestor node
// with a payload, visits that node.
// Otherwise, if the trail root has a payload, visits the root.
// Otherwise, visits the first child edge of the root.
inline bool ascend_to_left_edge(ancestor_trail& trail) {
    for (int i = trail.size() - 1; i >= 0; --i) {
        if (trail[i].child_idx > 0 || (trail[i].child_idx == 0 && trail[i].payload_bits)) {
            trail[i].child_idx -= 1;
            trail.resize(i + 1);
            return true;
        }
    }
    trail[0].child_idx = trail[0].payload_bits ? -1 : 0;
    trail.resize(1);
    return false;
}

// Steps the cursor represented by `trail` backwards until
// the first payloaded node.
// If there's no such node, visits the first payloaded node in the trie.
// Otherwise visits EOF. (I.e. visits the root of the trail and sets its child idx past the end).
inline seastar::future<> step_back(
    node_reader auto& input,
    ancestor_trail& trail
) {
    bool didnt_go_past_start = ascend_to_left_edge(trail);
    if (trail.back().child_idx >= 0 && trail.back().child_idx < trail.back().n_children) {
        co_await input.load(trail.back().pos);
        get_child_result child = input.get_child(trail.back().pos, trail.back().child_idx, false);

        trail.back().child_idx = child.idx;
        int64_t next_pos = trail.back().pos - child.offset;
        if (didnt_go_past_start) {
            co_await descend_rightmost(input, trail, next_pos);
        } else {
            co_await descend_leftmost(input, trail, next_pos);
        }
    }
    co_await input.load(trail.back().pos);
}

} // namespace sstables::trie
