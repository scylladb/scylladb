/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "writer_node.hh"
#include "utils/small_vector.hh"

namespace sstables::trie {

template <trie_writer_sink Output>
sink_offset writer_node::recalc_sizes(ptr<writer_node> self, const Output& out, sink_pos global_pos) {
    // Note: this routine is very similar to `write` in its overall structure.

    // A trie can be arbitrarily deep, so we can't afford to use recursion.
    // We have to maintain a walk stack manually.
    struct local_state {
        ptr<writer_node> _node;
        // The index of the next child to be visited.
        // When equal to the number of children, it's time to walk out of the node.
        int _stage;
        // The start position of the node's entire subtree.
        sink_pos _startpos;
    };
    utils::small_vector<local_state, 8> stack;
    // Depth-first walk.
    // To calculate the sizes, we essentially simulate a write of the tree.
    stack.push_back({self, 0, global_pos});
    while (!stack.empty()) {
        // Caution: note that `node`, `pos`, `stage` are references to contents of `stack`.
        // Any pushing or popping will invalidate them, so be sure not to use them afterwards!
        auto& [node, stage, startpos] = stack.back();
        // If the node has out of page grandchildren, the sizes of children might have changed from the estimate,
        // so we have to recurse into the children and update them.
        if (stage < static_cast<int>(node->get_children().size()) && node->_has_out_of_page_descendants) {
            stage += 1;
            stack.push_back({node->get_children()[stage - 1], 0, global_pos});
            continue;
        }
        // If we got here, then either we have recursed into children
        // (and then global_pos was updated accordingly),
        // or we skipped that because there was no need. In the latter case,
        // we have to update global_pos manually here.
        if (!node->_has_out_of_page_descendants) {
            global_pos = global_pos + node->_branch_size;
        }
        node->_branch_size = global_pos - startpos;
        if (node->_has_out_of_page_children || node->_has_out_of_page_descendants) {
            // The size of children might have changed, which might have in turn changed
            // our offsets to children, which might have changed our size.
            // We have to recalculate.
            node->_node_size = out.serialized_size(*node, sink_pos(global_pos));
            expensive_assert(uint64_t(node->_node_size.value) <= out.page_size());
        }
        global_pos = global_pos + node->_node_size;
        stack.pop_back();
    }
    return self->_branch_size + self->_node_size;
}

template <trie_writer_sink Output>
void writer_node::write(ptr<writer_node> self, Output& out, bool guaranteed_fit) {
    expensive_log(
        "writer_node::write: subtree={} pos={}, ps={} sz={} ",
        fmt::ptr(self.get()), out.pos().value, out.page_size(), (self->_branch_size + self->_node_size).value);
    expensive_assert(!self->_pos.valid());
    expensive_assert(self->_node_size.valid());
    if (guaranteed_fit) {
        auto page_of_first_byte = round_down(out.pos().value, out.page_size());
        auto page_of_last_byte = round_down((out.pos() + self->_branch_size + self->_node_size).value - 1, out.page_size());
        expensive_assert(page_of_first_byte == page_of_last_byte);
    }
    auto starting_pos = out.pos();
    // A trie can be arbitrarily deep, so we can't afford to use recursion.
    // We have to maintain a walk stack manually.
    struct local_state {
        ptr<writer_node> _node;
        // The index of the next child to be visited.
        // When equal to the number of children, it's time to walk out of the node.
        int _stage;
        // The start position of the node's entire subtree.
        sink_pos _startpos;
    };
    // Note: partition index should almost always have depth smaller than 6.
    // Row index can have depth of up to several thousand.
    utils::small_vector<local_state, 8> stack;
    // Depth-first walk.
    // We write out the subtree in postorder.
    stack.push_back({self, 0, starting_pos});
    while (!stack.empty()) {
        // Caution: note that `node`, `pos`, `stage` are references to contents of `stack`.
        // Any pushing or popping will invalidate them, so be sure not to use them afterwards!
        auto& [node, stage, startpos] = stack.back();
        if (stage < static_cast<int>(node->get_children().size())) {
            stage += 1;
            if (!node->get_children()[stage - 1]->_pos.valid()) {
                stack.push_back({node->get_children()[stage - 1], 0, out.pos()});
            }
            continue;
        }

        // Leaf nodes must have a payload.
        expensive_assert(node->get_children().size() || node->_payload._payload_bits);

        expensive_log("writer_node::write: node={} pos={} n_children={} size={} transition_length={}",
            fmt::ptr(node.get()), out.pos().value, node->get_children().size(), node->_node_size.value, node->_transition_length);

        if (guaranteed_fit) {
            SCYLLA_ASSERT(out.pos() - startpos == node->_branch_size);
            node->_pos = sink_pos(out.write(*node, sink_pos(out.pos())));
            SCYLLA_ASSERT(out.pos() - startpos == node->_branch_size + node->_node_size);
        } else {
            if (uint64_t(out.serialized_size(*node, sink_pos(out.pos())).value) > out.bytes_left_in_page()) {
                out.pad_to_page_boundary();
            }
            node->_branch_size = out.pos() - startpos;
            auto before = out.pos();
            node->_pos = sink_pos(out.write(*node, sink_pos(out.pos())));
            node->_node_size = node_size((out.pos() - before).value);
            expensive_assert(uint64_t(node->_node_size.value) <= out.page_size());
        }

        node->_first_transition_byte = std::byte(node->_transition[0]);
        stack.pop_back();
    }
    expensive_assert(out.pos() - starting_pos == self->_branch_size + self->_node_size);
    self->_branch_size = sink_offset(0);
    self->_node_size = node_size(0);
    self->_has_out_of_page_children = false;
    self->_has_out_of_page_descendants = false;
}

} // namespace sstables::trie
