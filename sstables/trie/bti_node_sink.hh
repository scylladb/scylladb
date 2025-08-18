/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "writer_node.hh"
#include "bti_node_type.hh"
#include "sstables/file_writer.hh"

namespace sstables::trie {

// Assuming the values of _node_size, _branch_size and _id in children are accurate,
// computes the distance between `x` and its farthest child.
//
// This is needed to pick the right BTI node type for `x`.
// (The node type must represent child offsets with enough bits to fit the biggest offset).
sink_offset max_offset_from_child(const writer_node& x, sink_pos pos);

// This object a stream of writer_node nodes (abstract nodes in RAM)
// into a stream of bytes on disk as defined by the BTI format at:
// https://github.com/apache/cassandra/blob/f16fb6765b8a3ff8f49accf61c908791520c0d6e/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.md
//
class bti_node_sink {
    sstables::file_writer& _w;
    size_t _page_size;
    constexpr static size_t max_page_size = 64 * 1024;
public:
    bti_node_sink(sstables::file_writer& w, size_t page_size);
private:
    // Truncates `x` to the given number of least significant bytes,
    // and writes out the result in big endian.
    // E.g. write_int(0x1122334455667788ull, 3) writes 0x66 0x77 0x88 to the file.
    void write_int(uint64_t x, size_t bytes);
    // Writes raw bytes to the output file.
    void write_bytes(const_bytes x);
    // Writes a sparse-type node (SPARSE_*) to the file.
    size_t write_sparse(const writer_node& x, node_type type, int bytes_per_pointer, sink_pos pos);
    // Computes the size of a sparse-type node (SPARSE_*).
    node_size size_sparse(const writer_node& x, int bits_per_pointer) const;
    // Writes a dense-type node (SPARSE_*) to the file.
    size_t write_dense(const writer_node& x, node_type type, int bytes_per_pointer, sink_pos pos);
    // Computes the size of a dense-type node (DENSE_*).
    node_size size_dense(const writer_node& x, int bits_per_pointer) const;
public:
    // Writes the final BTI node of the chain corresponding to the writer_node,
    // with the given BTI type.
    // Offsets to children are computed by taking the difference between `pos`
    // and the `_pos` of children.
    //
    // Note:
    // writer_node represents a trie node and the multi-character edge leading to it.
    // But BTI doesn't have a compact representation for a multi-character edge.
    // Each character in the trie gets its own BTI node,
    // and the character corresponding to this node's parent edge is stored in the parent node.
    //
    // For example, given a `writer_node` X, corresponding to edge "abc",
    // `write_body` would write the BTI node containing offsets
    // to X's children (and the first byte of their edges),
    // while `write_chain` would write the tiny BTI nodes
    // holding characters 'b' and 'c'.
    // (Character 'a' is held by the parent of X).
    //
    // Note:
    // For the purposes of serialization, `pos` is an abstract ID
    // used only to compute the delta between this node's ID and children's ID.
    // In real usage, this "abstract" node ID is of course the node's position
    // in the file, so that the reader can trivially use the "ID"
    // to jump directly to the node's position.
    // So the `pos` passed here is always equal to `current_pos()`.
    //
    // But in tests, the `pos` really is abstract, and the user
    // of bti_node_sink can fill the `_pos` of children and the `pos` passed
    // here via an argument with whatever it wants, something possibly
    // unrelated to `current_pos()`.
    void write_body(const writer_node& x, sink_pos pos, node_type type);
    // Writes all nodes, except the final node, of the chain corresponding to the writer_node.
    // (See write_body for what "chain" means).
    sink_pos write_chain(const writer_node& x, node_size body_offset);
    // Writes both the body and the chain.
    sink_pos write(const writer_node& x, sink_pos pos);
    // Computes the size of the given node after serialization, in bytes,
    // if it's serialized with the given BTI type.
    node_size serialized_size_body_type(const writer_node& x, node_type type) const;
    // Computes the size of the body (see write_body notes about "body" vs "chain")
    // of the given node, in bytes, assuming it's serialized with the optimal
    // BTI type for the given "position" of the body (see write_body notes about `pos`).
    node_size serialized_size_body(const writer_node& x, sink_pos pos) const;
    // Computes the size of the chain (see write_body notes about "body" vs "chain")
    // of the given node, in bytes, assuming it's serialized with the optimal
    // BTI types for the given offset between chain and body.
    // (Note: the chain is always written immediately after the body, so the offset between
    // body and chain is always equal to body size).
    node_size serialized_size_chain(const writer_node& x, node_size body_offset) const;
    // Computes the total size (body and chain, see write_body notes about "body" vs "chain")
    // of the given node, in bytes, assuming it's serialized with the optimal
    // BTI type for the given "position" of the body (see write_body notes about `pos`).
    node_size serialized_size(const writer_node& x, sink_pos pos) const;
    // Page size, same as the one passed via the constructor.
    uint64_t page_size() const;
    // Returns the number of bytes between current file position
    // and the start of the next page.
    // In range [0; page_size).
    uint64_t bytes_left_in_page() const;
    // Appends bytes_left_in_page() nul bytes to the file.
    void pad_to_page_boundary();
    // Returns the position reported by the underlying `file_writer`.
    sink_pos pos() const;
    sstables::file_writer& file_writer();
};
static_assert(trie_writer_sink<bti_node_sink>);

} // namespace sstables::trie
