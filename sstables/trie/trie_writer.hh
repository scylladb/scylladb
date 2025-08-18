/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

// Overview:
//
// This file implements a page-aware trie writer.
//
// The main class of this file, trie_writer, turns a sorted stream of keys into a stream of
// trie nodes and padding which tries to balance the following goals:
//
// 1. Each node lies entirely within one page (guaranteed).
// 2. Parents are located in the same page as their children (best-effort).
// 3. Padding is minimized (best-effort).
//
// It does mostly what you'd expect a trie builder to do (whenever a new key is added,
// branch out from the rightmost path and add a chain of new nodes, perhaps write some finished nodes to disk).
// The complicated part is the size calculations needed to group parents and children together.
//
// For the most part we follow Cassandra's way of doing that.
// See also:
// https://github.com/apache/cassandra/blob/f9e2f1b219c0b570eb59d529da42722178608572/src/java/org/apache/cassandra/io/tries/IncrementalTrieWriterPageAware.java#L32
// https://github.com/apache/cassandra/blob/f9e2f1b219c0b570eb59d529da42722178608572/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.md?plain=1#L347
// The main difference is that our writer operates in units of character chains,
// rather than on single characters.
// With a node per character, the performance of a writer based on single characters
// can get catastrophically bad for long keys.

#pragma once

#include <seastar/util/log.hh>
#include <map>
#include <set>
#include "utils/assert.hh"
#include "utils/small_vector.hh"
#include "common.hh"
#include "writer_node.impl.hh"

namespace sstables::trie {

// Turns a stream of keys into a stream of serializable trie nodes and appropriately
// sized padding, such that:
//
// 1. Each node lies entirely within one page (guaranteed).
// 2. Parents are located in the same page as their children (best-effort).
// 3. Padding is minimized (best-effort).
//
// The part of code responsible for serialization of nodes to bytes is
// separated from trie_writer for the sake of testability.
// It's passed to the writer via the Output parameter.
// It is a template parameter instead of a virtual object because we don't want to incur optimization barriers.
// The effect of this use of templates on the developer tooling (e.g. code completion, incremental compilation)
// is sad, though.
template <trie_writer_sink Output>
class trie_writer {
    template <typename T>
    using ptr = writer_node::ptr<T>;
    // As usual.
    constexpr static size_t default_allocator_segment_size = 128 * 1024;
    // Something much smaller than a page, but big enough to make the batching efficient.
    constexpr static size_t default_max_chain_length = 300;
    Output& _out;
    bump_allocator _allocator;
    size_t _current_depth = 0;
    size_t _max_chain_length;
    // Holds pointers to all nodes in the rightmost path in the tree.
    // Never empty. _stack[0] is the root of the trie.
    utils::small_vector<ptr<writer_node>, 8> _stack;
private:
    // Initializes/resets _root.
    void reset_root();
    // Completes and pops top nodes in _stack until _stack[depth] (exclusive).
    void complete_until_depth(size_t depth);
    // Completes the given node.
    void complete(ptr<writer_node> x);
    // If it fits into the current page
    // (or if it doesn't fit, but there is no hope that it will fit later either),
    // writes out the given subtree and returns true.
    // Otherwise returns false.
    bool try_write(ptr<writer_node> x);
    // Writes out the (proper) subtrees of the given node, and frees them,
    // leaving only the positions and first transition characters of the immediate children.
    void lay_out_children(ptr<writer_node> x);
    // The last part of `lay_out_children`, the one which frees the subtrees.
    // In a separate method just because it's allocator-specific.
    void compact_after_writing_children(ptr<writer_node> x);
public:
    trie_writer(
        Output&,
        size_t max_chain_length = default_max_chain_length,
        size_t allocator_segment_size = default_allocator_segment_size);
    ~trie_writer() = default;
    // Adds a new key to the trie.
    //
    // `depth` describes the level of the trie where we have to fork off a new branch
    // (a chain with the transition `key_tail` and payload `p` at the end).
    //
    // The fact that our caller calculates the `depth`, not us, might seem awkward,
    // but since the callers usually know the mismatch point for other reasons,
    // I figured this API spares us from a double key comparison.
    void add(size_t depth, const_bytes key_tail, const trie_payload& p);
    // Flushes all remaining nodes and returns the position of the root node.
    // The position is valid iff at least one key was added.
    sink_pos finish();
    Output& sink();
};

template <trie_writer_sink Output>
inline void trie_writer<Output>::reset_root() {
    std::byte empty[] = {std::byte(0)};
    _stack.push_back(writer_node::create(empty, _allocator));
}

template <trie_writer_sink Output>
inline trie_writer<Output>::trie_writer(Output& out, size_t max_chain_length, size_t allocator_segment_size)
    : _out(out)
    , _allocator(allocator_segment_size)
    , _max_chain_length(max_chain_length)
{
    reset_root();
}

// Called when the writer walks out of the node because it's done receiving children.
// This will initialize the members involved in size calculations.
// If the size of the subtree grows big enough, the node's children will be written
// out.
template <trie_writer_sink Output>
inline void trie_writer<Output>::complete(ptr<writer_node> x) {
    expensive_log("trie_writer::complete: x={}", fmt::ptr(x.get()));
    expensive_assert(!x->_branch_size.valid());
    expensive_assert(!x->_node_size.valid());
    expensive_assert(x->_has_out_of_page_children == false);
    expensive_assert(x->_has_out_of_page_descendants == false);
    expensive_assert(!x->_pos.valid());
    bool has_out_of_page_children = false;
    bool has_out_of_page_descendants = false;
    auto branch_size = sink_offset{0};
    for (const auto& c : x->get_children()) {
        branch_size = branch_size + c->_branch_size + sink_offset(c->_node_size);
        has_out_of_page_children |= c->_pos.valid();
        has_out_of_page_descendants |= c->_has_out_of_page_descendants || c->_has_out_of_page_children;
    }
    auto node_size = _out.serialized_size(*x, sink_pos((_out.pos() + branch_size)));
    // We try to keep parents in the same page as their children as much as possible.
    //
    // If the completed subtree fits into a page, we leave it in one piece.
    //
    // If it doesn't fit, we have to split it. We choose to do that by writing out *all* children right here.
    // `x` will be written out later by its ancestor.
    //
    // The details of when and how to perform the splitting are fairly arbitrary. But we aren't trying to be optimal.
    // We assume that our greedy strategy should be good enough.
    //
    // See https://github.com/apache/cassandra/blob/9dfcfaee6585a3443282f56d54e90446dc4ff012/src/java/org/apache/cassandra/io/tries/IncrementalTrieWriterPageAware.java#L32
    // for an alternative description of the process.
    if (branch_size + node_size <= sink_offset(_out.page_size())) {
        x->_branch_size = branch_size;
        x->_node_size = node_size;
        x->_has_out_of_page_children = has_out_of_page_children;
        x->_has_out_of_page_descendants = has_out_of_page_descendants;
    } else {
        lay_out_children(x);
        x->_branch_size = sink_offset(0);
        x->_node_size = _out.serialized_size(*x, sink_pos(_out.pos()));
        x->_has_out_of_page_children = true;
        x->_has_out_of_page_descendants = false;
    }
    expensive_assert(uint64_t(x->_node_size.value) <= _out.page_size());
}

template <trie_writer_sink Output>
inline bool trie_writer<Output>::try_write(ptr<writer_node> candidate) {
    // We picked the candidate based on size estimates which might not be exact.
    // In truth, the candidate might not fit into the current page.
    // If the estimates aren't known (via _has_out_of_page_*) to be exact,
    // we have to update them and check for fit.
    uint64_t true_size;
    if (candidate->_has_out_of_page_children || candidate->_has_out_of_page_descendants) {
        true_size = writer_node::recalc_sizes(candidate, _out, _out.pos()).value;
    } else {
        true_size = (candidate->_branch_size + candidate->_node_size).value;
    }
    bool guaranteed_fit = true;
    if (true_size > _out.bytes_left_in_page()) {
        if (true_size > _out.page_size()) {
            // We get here if, after updating the estimates, we see that the candidate branch
            // actually can't fit even in a full page.
            //
            // If we end up in this situation, we don't really have a sane choice other than just splitting it here.
            //
            // That's unfortunate, because it might mean an additional page hop for some range of keys.
            // But we assume that this inefficiency is rare enough to accept.
            //
            // Note: at this point Cassandra performs the split by recursively calling the equivalent
            // of lay_out_children on the candidate.
            // I'm not cool with that, because in the worst case it could lead to stack overflow.
            // I just write the candidate's children out in order, without applying the greedy packing heuristic.
            expensive_log("trie_writer::lay_out_children: size exceeded full page after recalc: {}", true_size);
            guaranteed_fit = false;
        } else {
            // The node doesn't actually fit in current page. The caller might want either to pad to next page,
            // or to pick a different node for writing.
            return false;
        }
    }
    // Write the candidate branch, in postorder, to the file.
    writer_node::write(candidate, _out, guaranteed_fit);
    return true;
}

template <trie_writer_sink Output>
inline void trie_writer<Output>::lay_out_children(ptr<writer_node> x) {
    expensive_log("trie_writer::lay_out_children x={}", fmt::ptr(x.get()));
    expensive_assert(!x->_pos.valid());

    // In this routine, we write out all child branches.
    // We are free to do it in an arbitrary order.
    //
    // In an attempt to minimize padding, we always first try to write out
    // the largest child branch which still fits into the current page.
    // If none of them fit, we pad to the next page boundary and try again.
    //
    // In theory, this results in an index that is from 1x to 2x smaller than
    // and index where all child branches are just written in transition order.
    // I don't know what's the number in practice. Maybe it's not worth the CPU
    // spent on sorting by size? (It probably is worth it, though).

    struct cmp {
        using is_transparent = bool;
        bool operator()(ptr<writer_node> a, ptr<writer_node> b) const {
            return std::make_pair(a->_branch_size + a->_node_size, a->_transition[0])
                 < std::make_pair(b->_branch_size + b->_node_size, b->_transition[0]);
        }
        bool operator()(int64_t a, ptr<writer_node> b) const {
            return a < (b->_branch_size + b->_node_size).value;
        }
    };

    // Note: `lay_out_children` is only called for select inner nodes,
    // those whose subtree is larger than a page.
    // So the CPU cost of using `std::set` should be amortized enough.
    auto unwritten_children = std::set<ptr<writer_node>, cmp>(cmp());

    for (const auto& c : x->get_children()) {
        if (!c->_pos.valid()) {
            unwritten_children.insert(c);
        }
    }

    while (unwritten_children.size()) {
        // Find the smallest child which doesn't fit.
        // (If all fit, then this will be the past-the-end iterator).
        // Its predecessor will be the biggest child which does fit.
        auto choice_it = unwritten_children.upper_bound(_out.bytes_left_in_page());
        if (choice_it == unwritten_children.begin()) {
            // None of the still-unwritten children fits into the current page,
            // so we pad to page boundary and try again.
            //
            // We don't have to call upper_bound again, though.
            // All children should fit into a full page,
            // so we can just the biggest child.
            _out.pad_to_page_boundary();
            expensive_assert(_out.bytes_left_in_page() == _out.page_size());
            // Pick the biggest child branch.
            choice_it = std::end(unwritten_children);
        }
        // The predecessor of upper_bound is the biggest child which still fits.
        choice_it = std::prev(choice_it);
        ptr<writer_node> candidate = *choice_it;
        unwritten_children.erase(choice_it);

        if (!try_write(candidate)) {
            // After updating the estimates, we see that the node doesn't actually fit into the current page.
            // Following Cassandra, we return this candidate to the set and try again to find a different
            // candidate that fits.
            //
            // I'm not sure what's the upper bound of the number of retries we might have to do because of this.
            // It's probably not worth caring about, but if we wanted,
            // we could have a hard limit on the number of retries,
            // after which we give up on trying to fit something into this page
            // and we pad to the next page instead.
            unwritten_children.insert(candidate);
        }
    }
    // Removes all unneeded information from the node's children.
    // This is the place where we free memory which isn't needed anymore.
    compact_after_writing_children(x);
}

template <trie_writer_sink Output>
inline void trie_writer<Output>::compact_after_writing_children(ptr<writer_node> x) {
    // Due to how the bump allocator works, we free the unneeded subtrees as follows:
    // 1. Copy the relevant metadata of children to a stack-allocated array.
    // 2. Discard everything that comes after x's transition. The allocation order
    //    guarantees that this will cover the written-out subtrees, but not anything
    //    yet-unwritten.
    // 3. Allocate a new array to hold the relevant child metadata,
    //    and copy the metadata saved on the stack into it.

    // Step 1.
    struct child_copy {
        std::byte _transition_byte;
        int16_t _node_size;
        int64_t _branch_size;
        int64_t _pos;
    };
    std::array<child_copy, 256> copy;
    size_t n_children = x->get_children().size();
    for (size_t i = 0; i < n_children; ++i) {
        copy[i]._transition_byte = x->get_children()[i]->_first_transition_byte;
        copy[i]._pos = x->get_children()[i]->_pos.value;
        copy[i]._branch_size = x->get_children()[i]->_branch_size.value;
        copy[i]._node_size = x->get_children()[i]->_node_size.value;
    }
    // Step 2.
    x->reset_children();
    #if TRIE_SANITIZE_BUMP_ALLOCATOR
        // Check that we aren't freeing things still in use.
        expensive_assert(x._global_pos < x->_transition._global_pos);
    #endif
    // Step 3.
    x->reserve_children(n_children, _allocator);
    // Even though only the 4 copied child fields are needed after this point,
    // we use an array of full `writer_node` objects to store them.
    // This is somewhat wasteful. With some extra flags and reinterpret_casts,
    // we could be more efficient.
    auto new_nodes = _allocator.alloc<writer_node[]>(n_children);
    for (size_t i = 0; i < n_children; ++i) {
        new (&new_nodes[i]) writer_node;
        new_nodes[i]._first_transition_byte = copy[i]._transition_byte;
        new_nodes[i]._pos = sink_pos(copy[i]._pos);
        new_nodes[i]._branch_size = sink_offset(copy[i]._branch_size);
        new_nodes[i]._node_size = node_size(copy[i]._node_size);
        x->push_child(new_nodes.element(i), _allocator);
    }
    #if TRIE_SANITIZE_BUMP_ALLOCATOR
        // Sanity check.
        expensive_assert(x->_children->_global_pos > x->_transition._global_pos);
    #endif
}

template <trie_writer_sink Output>
inline void trie_writer<Output>::complete_until_depth(size_t depth) {
    expensive_log("writer_node::complete_until_depth: start,_stack={}, depth={}, _current_depth={}", _stack.size(), depth, _current_depth);
    while (_current_depth > depth) {
        // Every node must be smaller than a page, and the transition chain
        // must be short enough to ensure that.
        //
        // But also smaller nodes are easier to pack (they result in less padding in the index),
        // so it's good for them to be smaller than some fraction of a page, e.g. 10%.
        //
        // So we limit the transition length for a single node to something around that size.
        size_t cut_off = std::min<size_t>(_current_depth - depth, _max_chain_length);

        auto back = _stack.back();
        auto transition = back->_transition;
        auto transition_size = back->_transition_length;
        #if TRIE_SANITIZE_BUMP_ALLOCATOR
            expensive_assert(transition._size == transition_size);
        #endif
        if (transition_size > cut_off) {
            // Either we are completing a transition chain which is longer than acceptable,
            // or we are branching off from a point in the middle of a chain.
            // So we have to split the chain.

            // First, we complete the current node, but with transition limited to the tail part.
            auto [t1, t2] = transition.split(transition_size - cut_off);
            back->_transition = t2;
            back->_transition_length = cut_off;
            complete(back);

            // Then, we insert a new node in between the completed one and its parent.
            // It points to the front of the transition string.
            //
            // To make compact_after_writing_children possible,
            // we must maintain the property that _transition comes later in the bump_allocator
            // than the node itself. So we do something tricky: we reuse the memory address
            // of the just-completed node, and move the just-completed node elsewhere.
            auto new_node = _allocator.alloc<writer_node>();
            new (new_node.get()) writer_node;
            std::swap(*new_node, *back);

            back->_transition = t1;
            back->_transition_length = transition_size - cut_off;
            back->reserve_children(2, _allocator);
            back->push_child(new_node, _allocator);

            _current_depth -= cut_off;
        } else {
            complete(back);
            _stack.pop_back();
            _current_depth -= transition_size;
        }
        expensive_assert(back->_transition_length > 0);
        expensive_assert(_stack.back()->_transition_length > 0);
    }
    expensive_log("writer_node::complete_until_depth: end, stack={}, depth={}, _current_depth={}", _stack.size(), depth, _current_depth);
}

template <trie_writer_sink Output>
inline void trie_writer<Output>::add(size_t depth, const_bytes key_tail, const trie_payload& p) {
    expensive_assert(_stack.size() >= 1);
    SCYLLA_ASSERT(_current_depth >= depth);
    // There is only one case where a zero-length tail is legal:
    // when inserting the empty key.
    SCYLLA_ASSERT(!key_tail.empty() || depth == 0);
    SCYLLA_ASSERT(p._payload_bits);

    complete_until_depth(depth);
    if (key_tail.size()) {
        _stack.push_back(_stack.back()->add_child(key_tail, _allocator));
        _current_depth = depth + key_tail.size();
    }
    _stack.back()->set_payload(p);
}

template <trie_writer_sink Output>
inline sink_pos trie_writer<Output>::finish() {
    expensive_assert(_stack.size() >= 1);
    if (!(_stack.front()->get_children().size() || _stack.front()->_payload._payload_bits)) {
        return sink_pos();
    }

    // Flush all held nodes, except the root.
    complete_until_depth(0);
    expensive_assert(_stack.size() == 1);
    complete(_stack.front());

    // Flush the root.
    if (!try_write(_stack[0])) {
        _out.pad_to_page_boundary();
        bool ok = try_write(_stack[0]);
        SCYLLA_ASSERT(ok);
    }
    auto root_pos = _stack[0]->_pos;

    // Reset the writer.
    _allocator.discard(_stack[0]);
    _stack.clear();
    _current_depth = 0;
    reset_root();

    return root_pos;
}

template <trie_writer_sink Output>
inline Output& trie_writer<Output>::sink() {
    return _out;
}

} // namespace sstables::trie
