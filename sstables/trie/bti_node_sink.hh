/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
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
// With page footers (`mu`), every page ends with a footer (see
// bti_page_footer.hh). The chunk table of each page is empty for now. The sink then keeps the current page in memory, and
// writes it to the file when the page ends. All bytes of the file, including the
// raw bytes that aren't trie nodes, must then be written through the sink.
// Without page footers, the sink writes straight to the file.
class bti_node_sink {
    sstables::file_writer& _w;
    size_t _page_size;
    // True if every page ends with a chunk table and a footer. The index writers
    // set it with bti_index_has_page_footers(), so it's true for `mu`.
    bool _page_footers;
    // With page footers: the bytes written to the current page so far.
    std::vector<char> _page;
    // With page footers: the number of bytes written to the current page so far.
    // It's the offset of the next byte in `_page`. The page starts at file
    // position `_w.offset()`, so pos() is `_w.offset() + _page_fill`.
    // It's 0 when the current page is empty, and always 0 without page footers.
    size_t _page_fill = 0;
    // Set by abandon().
    bool _abandoned = false;
    constexpr static size_t max_page_size = 64 * 1024;
public:
    bti_node_sink(sstables::file_writer& w, size_t page_size, bool page_footers = false);
    // With page footers, the current page is only in memory until it ends.
    // Destroying the sink with an unended page is an internal error, unless
    // abandon() was called: the page would be missing from the file.
    ~bti_node_sink();
private:
    // Truncates `x` to the given number of least significant bytes,
    // and writes out the result in big endian.
    // E.g. write_int(0x1122334455667788ull, 3) writes 0x66 0x77 0x88 to the file.
    void write_int(uint64_t x, size_t bytes);
    // Writes raw bytes to the output file.
    void write_bytes(const_bytes x);
    // Writes bytes into the usable area of the current page.
    // With page footers, they must fit before the chunk table.
    void put(const char* data, size_t n);
    // The offset of the chunk table in the current page.
    size_t table_start_in_page() const;
    // With page footers: writes the padding, the chunk table and the footer
    // of the current page, and starts the next page.
    void end_page();
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
    // The usable size of an empty page: the page size, minus the footer.
    uint64_t page_capacity() const;
    // The size of a chunk table entry. 0, because the chunk tables are empty for now.
    uint64_t table_entry_size() const;
    // Returns the number of bytes between current file position
    // and the start of the next page, or, with page footers,
    // the start of the current page's chunk table.
    // Without page footers: in range [1; page_size()].
    // With page footers: in range [0; page_capacity()].
    uint64_t bytes_left_in_page() const;
    // Ends the current page and starts the next one.
    // Without page footers: appends bytes_left_in_page() nul bytes to the file.
    // With page footers: appends the padding, the chunk table and the footer.
    void pad_to_page_boundary();
    // Returns the current position in the file.
    sink_pos pos() const;

    // Writes bytes that aren't a trie node (e.g. a row index header).
    // With page footers, the bytes can span pages: they skip the chunk table and
    // the footer of each page they cross.
    void write_raw(const char* data, size_t n);
    // Returns the position of the next byte that write_raw() will write.
    // Use it, not pos(), to record the position of raw bytes.
    //
    // With page footers, if the previous write ended exactly at the start of the
    // current page's chunk table, this ends the page first. Thus the position
    // points at the next byte, not at the table.
    sink_pos raw_pos();
    // With page footers: writes `tail` so that it ends exactly at the start of the
    // chunk table of a page, and ends that page. It's never split.
    // Precondition: tail.size() <= page_capacity().
    void write_page_tail(const char* data, size_t n);
    // Ends the last page, if it's not ended yet. Must be called after the last
    // write to the file. An empty file stays empty.
    void end_last_page();
    // Discards the current page, if it's not ended yet. Call it instead of
    // end_last_page() if the file won't be finished, e.g. because the write failed.
    void abandon() noexcept;
};
static_assert(trie_writer_sink<bti_node_sink>);

// Writes raw bytes through bti_node_sink::write_raw().
// Satisfies the sstables::Writer concept, for the serialization helpers.
struct bti_raw_writer {
    bti_node_sink& sink;
    void write(const char* data, size_t n) {
        sink.write_raw(data, n);
    }
};

} // namespace sstables::trie
