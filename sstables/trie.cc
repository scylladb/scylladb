/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

//
// This implementation of a trie-based index follows Cassandra's BTI implementation. 
//
// For an overview of the format, see:
// https://github.com/apache/cassandra/blob/9dfcfaee6585a3443282f56d54e90446dc4ff012/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.md
//
// For the writer logic, and it's arbitrary heuristics for splitting the trie into pages, see:
// https://github.com/apache/cassandra/blob/9dfcfaee6585a3443282f56d54e90446dc4ff012/src/java/org/apache/cassandra/io/tries/IncrementalTrieWriterPageAware.java#L32
//
// (The reader logic doesn't have much in the way of design -- the design of readers must follow the format).

#include "trie.hh"
#include <algorithm>
#include <cassert>
#include <immintrin.h>
#include "sstables/index_reader.hh"
#include "utils/small_vector.hh"
#include "trie_writer.hh"
#include "trie_serializer.hh"

namespace trie {

using namespace trie;

// Pimpl boilerplate
bti_trie_sink::bti_trie_sink() = default;
bti_trie_sink::~bti_trie_sink() = default;
bti_trie_sink& bti_trie_sink::operator=(bti_trie_sink&&) = default;
bti_trie_sink::bti_trie_sink(std::unique_ptr<bti_trie_sink_impl> x) : _impl(std::move(x)) {}

static_assert(trie_writer_sink<bti_trie_sink_impl>);

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
// 
// In practice, Output == trie_serializer.
// But for testability reasons, we don't hardcode the output type.
// (So that tests can test just the logic of this writer, without
// roping in file I/O and the serialization format)
// For performance reasons we don't want to make the output virtual,
// so we instead make it a template parameter.
template <trie_writer_sink Output>
class partition_index_writer_impl {
public:
    partition_index_writer_impl(Output&);
    void add(const_bytes key, int64_t data_file_pos, uint8_t hash_bits);
    sink_pos finish();
private:
    // The lower trie-writing layer, oblivious to the semantics of the partition index.
    trie_writer<Output> _wr;
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
    // The key added most recently.
    std::vector<std::byte> _last_key;
    // The payload of _last_key: the Data.db position and the hash bits. 
    int64_t _last_data_file_pos;
    uint8_t _last_hash_bits;
};

template <trie_writer_sink Output>
partition_index_writer_impl<Output>::partition_index_writer_impl(Output& out)
    : _wr(out) {
}

template <trie_writer_sink Output>
void partition_index_writer_impl<Output>::add(const_bytes key, int64_t data_file_pos, uint8_t hash_bits) {
    expensive_log("partition_index_writer_impl::add: this={} key={}, data_file_pos={}", fmt::ptr(this), fmt_hex(key), data_file_pos); 
    if (_added_keys > 0) {
        // First position where the new key differs from the last key.
        size_t mismatch = std::ranges::mismatch(key, _last_key).in2 - _last_key.begin();
        // From `_last_key_mismatch` (mismatch position between `_last_key` and its predecessor)
        // and `mismatch` (mismatch position between `_last_key` and its successor),
        // compute the minimal needed prefix of `_last_key`.
        // FIXME: the std::min(..., _last_key.size()) isn't really needed, since a key can't be a prefix of another key.
        size_t needed_prefix = std::min(std::max(_last_key_mismatch, mismatch) + 1, _last_key.size());
        // The new nodes/characters we have to add to the trie.
        auto tail = std::span(_last_key).subspan(_last_key_mismatch, needed_prefix - _last_key_mismatch);
        // Serialize the payload.
        std::array<std::byte, 9> payload_bytes;
        payload_bytes[0] = std::byte(_last_hash_bits);
        auto payload_bits = div_ceil(std::bit_width<uint64_t>(std::abs(_last_data_file_pos)) + 1, 8);
        uint64_t pos_be = seastar::cpu_to_be(_last_data_file_pos << 8*(8 - payload_bits));
        memcpy(&payload_bytes[1], &pos_be, 8);
        // Pass the new node chain and its payload to the lower layer.
        // Note: we pass (payload_bits | 0x8) because the additional 0x8 bit indicates that hash bits are present.
        // (Even though currently they are always present, and the reader assumes so).
        _wr.add(_last_key_mismatch, tail, trie_payload(payload_bits | 0x8, std::span(payload_bytes).subspan(0, payload_bits + 1)));
        // Update _last_* variables with the new key.
        _last_key_mismatch = mismatch;
    }
    _added_keys += 1;
    // Update _last_* variables with the new key.
    _last_key.assign(key.begin(), key.end());
    _last_data_file_pos = data_file_pos;
    _last_hash_bits = hash_bits;
}
template <trie_writer_sink Output>
sink_pos partition_index_writer_impl<Output>::finish() {
    if (_added_keys > 0) {
        // Mostly duplicates the code from add(), except there is only one mismatch position to care about,
        // not two which we have to take a max() of.
        //
        // FIXME: the std::min(..., _last_key.size()) isn't really needed, since a key can't be a prefix of another key.
        //
        // FIXME: maybe somehow get rid of the code duplication between here and add().
        size_t needed_prefix = std::min(_last_key_mismatch + 1, _last_key.size());
        // The new nodes/characters we have to add to the trie.
        auto tail = std::span(_last_key).subspan(_last_key_mismatch, needed_prefix - _last_key_mismatch);
        // Serialize the payload.
        std::array<std::byte, 9> payload_bytes;
        uint64_t pos_be = seastar::cpu_to_be(_last_data_file_pos);
        payload_bytes[0] = std::byte(_last_hash_bits);
        auto payload_bits = div_ceil(std::bit_width<uint64_t>(std::abs(_last_data_file_pos)) + 1, 8);
        std::memcpy(&payload_bytes[1], (const char*)(&pos_be) + 8 - payload_bits, payload_bits);
        // Pass the new node chain and its payload to the lower layer.
        // Note: we pass (payload_bits | 0x8) because the additional 0x8 bit indicates that hash bits are present.
        // (Even though currently they are always present, and the reader assumes so).
        _wr.add(_last_key_mismatch, tail, trie_payload(payload_bits | 0x8, std::span(payload_bytes).subspan(0, payload_bits + 1)));
    }
    return _wr.finish();
}

// Instantiation of partition_index_writer_impl with `Output` == `bti_trie_sink_impl`.
// This is the partition trie writer which is actually used in practice.
// Other instantiations of partition_index_writer_impl are only for testing.
class partition_trie_writer::impl : public partition_index_writer_impl<bti_trie_sink_impl> {
    using partition_index_writer_impl::partition_index_writer_impl;
};

// Pimpl boilerplate
partition_trie_writer::partition_trie_writer() = default;
partition_trie_writer::~partition_trie_writer() = default;
partition_trie_writer& partition_trie_writer::operator=(partition_trie_writer&&) = default;
partition_trie_writer::partition_trie_writer(bti_trie_sink& out)
    : _impl(std::make_unique<impl>(*out._impl)){
}
void partition_trie_writer::add(const_bytes key, int64_t data_file_pos, uint8_t hash_bits) {
    _impl->add(key, data_file_pos, hash_bits);
}
int64_t partition_trie_writer::finish() {
    return _impl->finish().value;
}

// A pointer (i.e. file position) to a node on disk, with some initial metadata parsed. 
struct reader_node {
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
    // Position of this node in the input file.
    size_t pos;
    // Number of children preemptively extracted from the representation.
    uint16_t n_children;
    // Payload bits preemptively extracted from the representation.
    uint8_t payload_bits;

    // Looks up the first child of this node greater with transition greater or equal to the given one.
    // If such child doesn't exist, the `idx` of the result will be negative.
    lookup_result lookup(std::byte transition, const_bytes page);
    // Looks up the child with the given index.
    // If there is no child with such an index (can happen in DENSE nodes, which have empty slots),
    // picks the closest child with idx greater (if `forward == true`) or smaller (if `forward == false`)
    // than the given. If there is no such child, the `idx` of the result will be negative.
    lookup_result get_child(int idx, bool forward, const_bytes page);
    // Returns a view of the payload of this node.
    // The `bytes` view can extend beyond 
    payload_result payload(const_bytes page) const;
    // Returns a view of the raw on-disk representation of the node.
    const_bytes raw(const_bytes page) const;
};

// For iterating over the trie, we need to keep a stack of those,
// forming a path from the root to the current node.
//
// child_idx is the index of the child currently visited by the containing trie_cursor.
// child_idx might be equal to -1, this means that the node itself, not any of its children,
// is currently visited. 
struct node_cursor {
    reader_node node;
    int child_idx;
};

template <typename T>
concept trie_reader_source = requires(T& o, uint64_t pos, reader_permit rp) {
    { o.read_page(pos) } -> std::same_as<future<std::pair<cached_file::ptr_type, bool>>>;
    { o.read_row_index_header(pos, rp) } -> std::same_as<future<row_index_header>>;
    { o.read_row_index_header(pos, rp) } -> std::same_as<future<row_index_header>>;
};

// Parses some basic type-oblivious metadata.
static reader_node pv_to_reader_node(size_t pos, const_bytes page) {
    //expensive_log("my_trie_reader_input::read: pos={} {}", pos, fmt_hex(sp.subspan(0, 32)));
    auto sp = page.subspan(pos % cached_file::page_size);
    auto type = uint8_t(sp[0]) >> 4;
    switch (type) {
    case PAYLOAD_ONLY:
        return reader_node{pos, 0, uint8_t(sp[0]) & 0xf};
    case SINGLE_NOPAYLOAD_4:
    case SINGLE_NOPAYLOAD_12:
        return reader_node{pos, 1, 0};
    case SINGLE_8:
    case SINGLE_16:
        return reader_node{pos, 1, uint8_t(sp[0]) & 0xf};
    case SPARSE_8:
    case SPARSE_12:
    case SPARSE_16:
    case SPARSE_24:
    case SPARSE_40: {
        auto n_children = uint8_t(sp[1]);
        return reader_node{pos, n_children, uint8_t(sp[0]) & 0xf};
    }
    case DENSE_12:
    case DENSE_16:
    case DENSE_24:
    case DENSE_32:
    case DENSE_40:
    case LONG_DENSE: {
        auto dense_span = uint8_t(sp[2]) + 1;
        return reader_node{pos, dense_span, uint8_t(sp[0]) & 0xf};
    }
    default: abort();
    }
}
static reader_node pv_to_reader_node(size_t pos, const cached_file::ptr_type& pv) {
    return pv_to_reader_node(pos, pv->get_view());
}


enum class set_result {
    eof,
    definitely_not_a_match,
    possible_match,
};

struct follow_result {
    int next_branch_depth;
    int prev_branch_depth;
    int prefix_depth;
    int end_depth;
};

// A trie cursor and iterator.
//
// Invariants upheld before calling every public method:
//
// 1. The cursor can be in one of the following mutually-exclusive states:
//   - Uninitialized: _path is empty.
//   - Pointing at EOF: _path contains exactly one entry (root), which has child_idx == n_children. 
//   - Pointing at a node:
//     _path is non-empty,
//     all non-last entries in _path have child_idx ∈ [0, n_children),
//     last entry has child_idx == -1.
//     The pointee can be payloaded (corresponding to some inserted key) or not.
//   - Pointing at a transition:
//     _path is non-empty,
//     all entries in _path have child_idx ∈ [0, n_children).
//     Semantically, this state means that the cursor is pointing at a fake "position" just before
//     the child node with index (child_idx + 1) of the last entry in _path.
//   Each method specifices the states it can be called with, and states guaranteed after they return.
// 2. When cursor is initialized, _page covers _path.back().
// 3. The sequence of positions in _pages and the sequence of positions in _paths are strictly declining. 
//
// Assumptions:
// 1. All leaves in the trie have a payload.
// 2. The only legal thing to do with a cursor after it throws an exception is to destroy it.
template <trie_reader_source Input>
class trie_cursor {
    // Reference wrapper to allow copying the cursor.
    std::reference_wrapper<Input> _in;
    // A stack holding the path from the root to the currently visited node.
    // When initialized, _path[0] is root.
    //
    // FIXME: To support stepping, the iterator has to keep
    // a stack of visited nodes (the path from root to the current node).
    // But single-partition reads don't need to support stepping, and
    // it this case maintaining the stack only adds overhead.
    // Consider adding a separate cursor type for single-partition reads,
    // or a special single-partititon variant of `set_before()` which won't bother maintaining the stack.
    utils::small_vector<node_cursor, 8> _path;
    // Holds a page buffer containing the current node.
    reader_node::page_ptr _page;
private:
    // If the page at the top of the stack covers the node at position `pos`,
    // pushes that node to _path and returns true.
    // Otherwise returns false.
    bool try_push(uint64_t pos);
    // Pushes the page covering `pos` to `_page`,
    // and pushes the node at `pos` to `_path`.
    future<> push_page_and_node(uint64_t pos);
    // Pushes the page covering `_path.back()` to `_page.`
    future<> push_page();
    // Shrinks `_path` to size `depth + 1` and updates `_page` to match the new `_page.back()`.
    future<> ascend(size_t depth);
    // Pops the top of `_path`.
    // If it was the only node covered by the top of `_pages`, pops the top of `_pages`.
    void pop();
    // Checks various invariants which every public method must uphold.
    // (See the comment before the class name).
    // For the purposes of debugging during development.
    void check_invariants() const;
    bool is_initialized() const;
    bool points_at_eof() const;
    bool points_at_node() const;
    bool points_at_payloaded_node() const;
public:
    trie_cursor(Input&);
    ~trie_cursor();
    trie_cursor& operator=(const trie_cursor&) = default;
    // Preconditions: none.
    // Postconditions: points at the root node.
    future<void> init(uint64_t root_pos);
    // Checks whether the cursor is initialized.
    // Preconditions: none.
    bool initialized() const;
    // Descends down from the root along the path matching `key`.
    // Stops after traversing all bytes in `key` or after there is
    // no child matching the next byte of `key`.
    // In the latter case, `child_idx` of the last node in `_path`
    // is set to the greatest transition strictly smaller than the next byte.
    // (Or to -1, if no such transition exists).
    //    
    // Performance-critical.
    //
    // Returns a struct with following members:
    // 
    // next_branch_depth: depth of leafmost node in _path which has a child right of _path. 
    // prev_branch_depth: depth of leafmost node in _path which has a child left of _path.
    // prefix_depth: depth of leafmost payloaded node in _path.
    // end_depth: depth of leaftmost node in _path. (Equal to _path.size() - 1).
    //
    // (In all cases, if such a node doesn't exist, the value is -1).
    //
    // For example, for a trie populated with keys: ad, b, be, beg, beh, bfi, bfj, c,
    // which looks like this:
    //
    // 0: ε
    // 1: a--b*----------c*
    // 2: d* e------f
    // 3:    g*-h*  i*-j*
    //
    // follow(be) and follow(beaaaa) will result in:
    // _path[0].child_idx == 1
    // _path[1].child_idx == 0
    // _path[2].child_idx == -1
    // next_branch_depth == 2
    // prev_branch_depth == 1
    // prefix_depth == 1
    // end_depth == 2
    //
    // follow(bfz) will result in:
    // _path[0].child_idx == 1
    // _path[1].child_idx == 1
    // _path[2].child_idx == 1
    // next_branch_depth == 0
    // prev_branch_depth == 2
    // prefix_depth == 1
    // end_depth == 2
    // 
    // follow(a) will result in:
    // _path[0].child_idx == 0
    // _path[1].child_idx == -1
    // next_branch_depth == 0
    // prev_branch_depth == -1
    // prefix_depth == -1
    // end_depth == 1
    //
    // Preconditions: initialized.
    // Postconditions: initialized.
    future<follow_result> follow(const_bytes key);
    // Starting from the next child of _path[next_branch_depth], descends to leftmost payload.
    // If depth < 0, goes to EOF.
    //
    // FIXME: the description above is too vague.
    //
    // Meant for usage with the next_branch_depth returned by an immediately preceding follow().
    //
    // Preconditions: initialized, next_branch_depth == -1 or _path[next_branch_depth] has a preceding child.
    // Postconditions: points at EOF or a payloaded node.
    future<> descend_next_branch(int next_branch_depth);
    // Starting from the preceding child of _path[prev_branch_depth], descends to rightmost payload.
    // If depth < 0, goes to first payloaded node.
    //
    // FIXME: the description above is too vague.
    //
    // Meant for usage with the next_branch_depth returned by an immediately preceding follow().
    //
    // Preconditions: initialized, prev_branch_depth == -1 or _path[prev_branch_depth] has a preceding child.
    // Postconditions: points at a payloaded node.
    future<> descend_prev_branch(int prev_branch_depth);
    // Goes up to the payloaded node at given depth.
    //
    // Meant for usage with the next_branch_depth returned by an immediately preceding follow().
    //
    // Preconditions: _prev[prefix_depth] is payloaded.
    // Postconditions: points at a payloaded node.
    future<> return_to_prefix(size_t prefix_depth);

    // Moves the cursor to the next key (or EOF).
    //
    // step() returns a set_result, but it can only return `eof` (when it steps beyond all keys),
    // or `definitely_not_a_match` otherwise.
    // Returning a "not EOF" result as "definitely_not_a_match" isn't clean,
    // but we can't easily return a more meaningful enum because that would
    // require adding futures to the continuation chains.
    //
    // Preconditions: points at a node or transition.
    // Postconditions: points at eof or a payloaded node.
    future<> step();
    // Moves the cursor to the previous key.
    // If there is no previous key, doesn't do anything. 
    //
    // Preconditions: points at eof or a payloaded (sic!) node.
    // Postconditions: points at eof or a payloaded node.
    future<> step_back();
    // Preconditions: points at a payloaded node.
    payload_result payload() const;
    // Checks whether the cursor in the EOF position.
    // 
    // Preconditions: points at eof or a node.
    bool eof() const;
    // Preconditions: none.
    // Postconditions: uninitialized.
    void reset();
};

// We want to be always inlined so that bits_per_pointer is substituted with a constant,
// and the read_offset can be simplified.
[[gnu::always_inline]]
static lookup_result find_child_sparse(int type, const_bytes raw, std::byte transition) {
    auto bpp = bits_per_pointer_arr[type];
    auto n_children = uint8_t(raw[1]);
    auto idx = std::lower_bound(&raw[2], &raw[2 + n_children], transition) - &raw[2];
    if (idx < n_children) {
        return {idx, raw[2 + idx], read_offset(raw.subspan(2+n_children), idx, bpp)};
    } else {
        return {idx, std::byte(0), 0};
    }
}

// We want to be always inlined so that bits_per_pointer is substituted with a constant,
// and the read_offset can be simplified.
[[gnu::always_inline]]
static lookup_result find_child_dense(int type, const_bytes raw, std::byte transition) {
    auto start = int(raw[1]);
    auto idx = std::max<int>(0, int(transition) - start);
    auto dense_span = uint64_t(raw[2]) + 1;
    auto bpp = bits_per_pointer_arr[type];
    while (idx < int(dense_span)) {
        if (auto off = read_offset(raw.subspan(3), idx, bpp)) {
            return {idx, std::byte(start + idx), off};
        } else {
            ++idx;
        }
    }
    return {dense_span, std::byte(0), 0};
}

// Looks up the first child of this node greater with transition greater or equal to the given one.
// If such child doesn't exist, the `idx` of the result will be negative.
static lookup_result find_child(const_bytes raw, std::byte transition) {
    auto type = uint8_t(raw[0]) >> 4;
    switch (type) {
    case PAYLOAD_ONLY:
        return {0, std::byte(0), 0};
    case SINGLE_NOPAYLOAD_4:
        if (transition <= raw[1]) {
            return {0, raw[1], uint64_t(raw[0]) & 0xf};
        }
        return {1, std::byte(0), 0};
    case SINGLE_8:
        if (transition <= raw[1]) {
            return {0, raw[1], uint64_t(raw[2])};
        }
        return {1, std::byte(0), 0};
    case SINGLE_NOPAYLOAD_12:
        if (transition <= raw[2]) {
            return {0, raw[2], (uint64_t(raw[0]) & 0xf) << 8 | uint64_t(raw[1])};
        }
        return {1, std::byte(0), 0};
    case SINGLE_16:
        if (transition <= raw[1]) {
            return {0, raw[1], uint64_t(raw[1]) << 8 | uint64_t(raw[2])};
        }
        return {1, std::byte(0), 0};
    case SPARSE_8:
        return find_child_sparse(type, raw, transition);
    case SPARSE_12:
        return find_child_sparse(type, raw, transition);
    case SPARSE_16:
        return find_child_sparse(type, raw, transition);
    case SPARSE_24:
        return find_child_sparse(type, raw, transition);
    case SPARSE_40:
        return find_child_sparse(type, raw, transition);
    case DENSE_12:
        return find_child_dense(type, raw, transition);
    case DENSE_16:
        return find_child_dense(type, raw, transition);
    case DENSE_24:
        return find_child_dense(type, raw, transition);
    case DENSE_32:
        return find_child_dense(type, raw, transition);
    case DENSE_40:
        return find_child_dense(type, raw, transition);
    case LONG_DENSE:
        return find_child_dense(type, raw, transition);
    default: abort();
    }
}

const_bytes reader_node::raw(const_bytes page) const {
    return page.subspan(pos % cached_file::page_size);
}

payload_result reader_node::payload(const_bytes page) const {
    return get_payload(raw(page));
}

lookup_result reader_node::lookup(std::byte transition, const_bytes page) {
    return ::trie::find_child(raw(page), transition);
}

lookup_result reader_node::get_child(int idx, bool forward, const_bytes page) {
    return ::trie::get_child(raw(page), idx, forward);
}

template <trie_reader_source Input>
trie_cursor<Input>::trie_cursor(Input& in)
    : _in(in)
{
    check_invariants();
}

template <trie_reader_source Input>
trie_cursor<Input>::~trie_cursor()
{
    check_invariants();
}

template <trie_reader_source Input>
bool trie_cursor<Input>::points_at_eof() const {
    return is_initialized() && size_t(_path.begin()->child_idx) == _path.begin()->node.n_children;
}

template <trie_reader_source Input>
bool trie_cursor<Input>::points_at_node() const {
    return is_initialized() && !points_at_eof() && _path.back().child_idx == -1;
}

template <trie_reader_source Input>
bool trie_cursor<Input>::points_at_payloaded_node() const {
    return points_at_node() && _path.back().node.payload_bits;
}

template <trie_reader_source Input>
bool trie_cursor<Input>::is_initialized() const {
    return !_path.empty();
}

// Documented near the declaration.
template <trie_reader_source Input>
void trie_cursor<Input>::check_invariants() const {
    if constexpr (!developer_build) {
        return;
    }

    for (size_t i = 0; i + 1 < _path.size(); ++i) {
        expensive_assert(_path[i].child_idx >= 0 && _path[i].child_idx < _path[i].node.n_children);
    }

    bool is_initialized = !_path.empty();
    if (is_initialized) {
        expensive_assert(_path.back().node.pos / cached_file::page_size == _page->pos() / cached_file::page_size);
        bool is_eof = _path.front().child_idx == _path.front().node.n_children;
        if (is_eof) {
            expensive_assert(_path.size() == 1);
        } else {
            expensive_assert(_path.back().child_idx >= -1 && _path.back().child_idx < _path.back().node.n_children);
        }
    }
}

// Documented near the declaration. 
template <trie_reader_source Input>
[[gnu::noinline]]
future<> trie_cursor<Input>::push_page() {
    if (_path.back().node.pos / cached_file::page_size == _page->pos() / cached_file::page_size) {
        return make_ready_future<>();
    }
    return _in.get().read_page(_path.back().node.pos).then([this] (auto v) {
        _page = std::move(v.first);
        expensive_assert(_path.back().node.pos / cached_file::page_size == _page->pos() / cached_file::page_size);
    });
}

// Documented near the declaration. 
template <trie_reader_source Input>
[[gnu::noinline]]
future<> trie_cursor<Input>::push_page_and_node(uint64_t root_pos) {
    return _in.get().read_page(root_pos).then([this, root_pos] (auto v) {
        _page = std::move(v.first);
        _path.push_back({pv_to_reader_node(root_pos, _page), -1});
        expensive_assert(_path.back().node.pos / cached_file::page_size == _page->pos() / cached_file::page_size);
    });
}

// Documented near the declaration.
template <trie_reader_source Input>
future<void> trie_cursor<Input>::init(uint64_t root_pos) {
    check_invariants();
    reset();
    return push_page_and_node(root_pos);
}

// Documented near the declaration.
template <trie_reader_source Input>
bool trie_cursor<Input>::try_push(uint64_t pos) {
    expensive_assert(_path.back().node.pos / cached_file::page_size == _page->pos() / cached_file::page_size);
    if (_path.back().node.pos / cached_file::page_size == pos / cached_file::page_size) {
        _path.push_back({pv_to_reader_node(pos, _page), -1});
        return true;
    }
    return false;
}

// Documented near the declaration.
template <trie_reader_source Input>
void trie_cursor<Input>::pop() {
    _path.pop_back();
}


// Documented near the declaration.
template <trie_reader_source Input>
future<follow_result> trie_cursor<Input>::follow(const_bytes key) {
    check_invariants();
    expensive_assert(initialized());
    auto post_check = defer([this] {
        check_invariants();
    });
    expensive_log("follow, root_pos={}, key={}", _path[0].node.pos, fmt_hex(key));
    expensive_assert(_path.back().child_idx == -1 || eof());
    // Reset the cursor back to a freshly-initialized, unset state, with only the root in _path.
    co_await ascend(0);
    _path.back().child_idx = -1;

    int next_branch_depth = -1;
    int prev_branch_depth = -1;
    int prefix_depth = -1;
    size_t i = 0;
    while (i < key.size()) {
        // Fast path for long chains without any payloads or branches.
        //
        // const uint8_t* __restrict__ p = reinterpret_cast<const uint8_t* __restrict__>(_page->get_view().data() + _path.back().node.pos % cached_file::page_size);
        // if (*p == (SINGLE_NOPAYLOAD_4 << 4 | 2) & *(p + 1) == uint8_t(key[i])) {
        //     const uint8_t* start = p; 
        //     const uint8_t* beg = p - _path.back().node.pos % cached_file::page_size;
        //     const size_t keysize = key.size();
        //     while (p - 32 >= beg && i+16 <= keysize - 1) {
        //         typedef unsigned char  vector32b  __attribute__((__vector_size__(32)));
        //         typedef unsigned char  vector16b  __attribute__((__vector_size__(16)));
        //         vector32b a = {};
        //         memcpy(&a, p - 32, 32);
        //         auto z = uint8_t(SINGLE_NOPAYLOAD_4 << 4 | 2);
        //         vector16b b = {};
        //         memcpy(&b, &key[i], 16);
        //         vector16b c = {z, z, z, z, z, z, z, z, z, z, z, z, z, z, z, z};
        //         vector32b d = __builtin_shufflevector(c, b, 0, 16, 1, 17, 2, 18, 3, 19, 4, 20, 5, 21, 6, 22, 7, 23, 8, 24, 9, 25, 10, 26, 11, 27, 12, 28, 13, 29, 14, 30, 15, 31);
        //         if (!__builtin_reduce_and(a == d)) {
        //             break;
        //         }
        //         p -= 32;
        //         i += 16;
        //     }
        //     _path.back() = node_cursor{reader_node{_path.back().node.pos - (start - p), 1, (*p)&0xf}, -1};
        // }
        
        lookup_result it = _path.back().node.lookup(key[i], _page->get_view());

        assert(it.idx <= _path.back().node.n_children);
        expensive_log("follow, lookup query: (pos={} key={:x} n_children={}), lookup result: (offset={}, transition={:x} idx={})",
            _path.back().node.pos, uint8_t(key[i]),_path.back().node.n_children, it.offset, it.byte, it.idx);

        if (it.idx > 0) {
            prev_branch_depth = _path.size() - 1;
        }

        if (it.byte != key[i] || it.idx == int(_path.back().node.n_children)) {
            _path.back().child_idx = it.idx - 1;
            break;
        }

        _path.back().child_idx = it.idx;
        if (_path.back().node.payload_bits) {
            prefix_depth = _path.size() - 1;
        }
        if (_path.back().child_idx + 1 < _path.back().node.n_children) {
            next_branch_depth = _path.size() - 1;
        }
        if (auto target_pos = _path.back().node.pos - it.offset; !try_push(target_pos)) {
            co_await push_page_and_node(target_pos);
        }
        i += 1;
    }
    if (_path.back().node.payload_bits) {
        prefix_depth = _path.size() - 1;
    }
    if (_path.back().child_idx + 1 < _path.back().node.n_children) {
        next_branch_depth = _path.size() - 1;
    }
    expensive_log("follow, result: (prev={}, next={}, prefix={}, end={}, payload={}, child={})", prev_branch_depth, next_branch_depth, prefix_depth, i, _path.back().node.payload_bits, _path.back().child_idx);
    co_return follow_result{
        .next_branch_depth = next_branch_depth,
        .prev_branch_depth = prev_branch_depth,
        .prefix_depth = prefix_depth,
        .end_depth = i};
}

// Documented near the declaration.
template <trie_reader_source Input>
future<> trie_cursor<Input>::ascend(size_t depth) {
    expensive_assert(depth < _path.size());
    _path.resize(depth + 1);
    return push_page();
}

// Documented near the declaration.
template <trie_reader_source Input>
future<> trie_cursor<Input>::descend_next_branch(int depth) {
    expensive_log("descend_next_branch, root_pos={}, depth={}", _path[0].node.pos, depth);
    if (depth < 0) {
        co_await ascend(0);
        _path.back().child_idx = _path.back().node.n_children;
        co_return;
    }
    assert(depth < int(_path.size()));
    co_await ascend(depth);

    _path.back().child_idx += 1;
    assert(_path.back().child_idx >= 0);
    assert(_path.back().child_idx < _path.back().node.n_children);

    // Enter the successor branch.
    lookup_result child = _path.back().node.get_child(_path.back().child_idx, true, _page->get_view());
    expensive_log("push child {}", _path.back().child_idx);
    if (auto target_pos = _path.back().node.pos - child.offset; !try_push(target_pos)) {
        co_await push_page_and_node(target_pos);
    }
    // Descend to the first payloaded node along the leftmost path.
    while (!_path.back().node.payload_bits) {
        _path.back().child_idx += 1;
        expensive_assert(_path.back().child_idx < int(_path.back().node.n_children));
        child = _path.back().node.get_child(_path.back().child_idx, true, _page->get_view());
        expensive_log("push child {}", _path.back().child_idx);
        if (auto target_pos = _path.back().node.pos - child.offset; !try_push(target_pos)) {
            co_await push_page_and_node(target_pos);
        }
    }
    co_return;
}

// Documented near the declaration.
template <trie_reader_source Input>
future<> trie_cursor<Input>::descend_prev_branch(int depth) {
    expensive_log("descend_prev_branch, root_pos={}, depth={}", _path[0].node.pos, depth);

    // Special case: if depth < 0, we go to the first payloaded node.
    if (depth < 0) {
        co_await ascend(0);
        _path.back().child_idx = -1;
        if (!_path.back().node.payload_bits) {
            if (_path.back().node.n_children) {
                co_await descend_next_branch(0);
            } else {
                _path.back().child_idx = 0;
            }
        }
        co_return;
    }

    bool last = depth == int(_path.size() - 1);
    co_await ascend(depth);
    if (!last) {
        // child_idx of the last node isn't pointing at a child, but between `child_idx` and `child_idx + 1`.
        // Therefore, the previous branch is the one rooted at `child_idx`.
        //
        // For nodes in _path other than the last node, child_idx is pointing at the currently-visited child.
        // Therefore, the previous branch is the one rooted at `child_idx - 1`.
        _path.back().child_idx -= 1;
    }
    assert(_path.back().child_idx >= 0);
    assert(_path.back().child_idx < _path.back().node.n_children);

    // Enter the predecessor branch.
    auto target_pos = _path.back().node.pos -_path.back().node.get_child(_path.back().child_idx, false, _page->get_view()).offset;
    if (!try_push(target_pos)) {
        co_await push_page_and_node(target_pos);
    }
    // Descend to the rightmost leaf.
    while (_path.back().node.n_children) {
        _path.back().child_idx = _path.back().node.n_children - 1;
        target_pos = _path.back().node.pos -_path.back().node.get_child(_path.back().child_idx, false, _page->get_view()).offset;
        co_await push_page_and_node(target_pos);
    }
    co_return;
}

// Documented near the declaration.
template <trie_reader_source Input>
future<> trie_cursor<Input>::return_to_prefix(size_t depth) {
    expensive_log("return_to_prefix, root_pos={}, depth={}", _path[0].node.pos, depth);
    co_await ascend(depth);
    _path.back().child_idx = -1;
    expensive_assert(_path.back().node.payload_bits);
}

// Documented near the declaration.
template <trie_reader_source Input>
future<> trie_cursor<Input>::step() {
    check_invariants();
    expensive_assert(initialized());
    auto post_check = defer([this] {
        check_invariants();
        expensive_assert(points_at_eof() || points_at_payloaded_node());
    });
    expensive_assert(initialized() && !eof());

    // Ascend to the leafmost ancestor which isn't childless and isn't followed in _path by its rightmost child,
    // (or, if there is no such ancestor, ascend to the root)
    // and increment its child_idx by 1.
    _path.back().child_idx += 1;
    while (size_t(_path.back().child_idx) == _path.back().node.n_children) {
        if (_path.size() == 1) {
            // If we ascended to the root, we stop and return EOF, even though `child_idx == n_children`.
            // The root is the only node for which `child_idx == n_children` is a legal postcondition.
            // That's is how EOF is represented.
            co_return;
        }
        pop();
        _path.back().child_idx += 1;
        assert(_path.back().child_idx <= _path.back().node.n_children);
    }
    co_await push_page();

    // Descend (starting at the child with index child_idx) along the leftmost path to the first payloaded node.
    // It is assumed that every leaf node is payloaded, so we are guaranteed to succeed.
    auto getc = _path.back().node.get_child(_path.back().child_idx, true, _page->get_view());
    if (auto target_pos = _path.back().node.pos - getc.offset; !try_push(target_pos)) {
        co_await push_page_and_node(target_pos);
    }
    while (!_path.back().node.payload_bits) {
        _path.back().child_idx += 1;
        expensive_assert(_path.back().child_idx < int(_path.back().node.n_children));
        getc = _path.back().node.get_child(_path.back().child_idx, true, _page->get_view());
        if (auto target_pos = _path.back().node.pos - getc.offset; !try_push(target_pos)) {
            co_await push_page_and_node(target_pos);
        }
    }
    co_return;
}

template <trie_reader_source Input>
payload_result trie_cursor<Input>::payload() const {
    check_invariants();
    expensive_assert(points_at_payloaded_node());
    return _path.back().node.payload(_page->get_view());
}

template <trie_reader_source Input>
bool trie_cursor<Input>::eof() const {
    check_invariants();
    expensive_assert(initialized());
    return points_at_eof();
}

template <trie_reader_source Input>
bool trie_cursor<Input>::initialized() const {
    check_invariants();
    return is_initialized();
}

template <trie_reader_source Input>
void trie_cursor<Input>::reset() {
    check_invariants();
    _path.clear();
    _page.reset();
    check_invariants();
}

template <trie_writer_sink Output>
class row_index_writer_impl {
public:
    row_index_writer_impl(Output&);
    ~row_index_writer_impl();
    void add(const_bytes first_ck, const_bytes last_ck, uint64_t data_file_pos, sstables::deletion_time);
    sink_pos finish();
    using buf = std::vector<std::byte>;

    struct row_index_payload {
        uint64_t data_file_pos;
        sstables::deletion_time dt;
    };

private:
    trie_writer<Output> _wr;
    size_t _added_blocks = 0;
    buf _last_separator;
    buf _last_key;
};

template <trie_writer_sink Output>
row_index_writer_impl<Output>::row_index_writer_impl(Output& out)
    : _wr(out)
{}
template <trie_writer_sink Output>
row_index_writer_impl<Output>::~row_index_writer_impl() {
}

[[gnu::target("avx2")]]
size_t mismatch_idx(const std::byte* __restrict__ a, const std::byte* __restrict__ b, size_t n) {
    size_t i;
    for (i = 0; i + 32 <= n; i += 32) {
        __m256i av, bv;
        memcpy(&av, &a[i], 32);
        memcpy(&bv, &b[i], 32);
        __m256i pcmp = _mm256_cmpeq_epi32(av, bv);
        unsigned bitmask = _mm256_movemask_epi8(pcmp);
        if (bitmask != 0xffffffffU) {
            break;
        }
    }
    for (; i < n; ++i) {
        if (a[i] != b[i]) {
            break;
        }
    }
    return i;
}

template <trie_writer_sink Output>
void row_index_writer_impl<Output>::add(
    const_bytes first_ck,
    const_bytes last_ck,
    uint64_t data_file_pos,
    sstables::deletion_time dt
) {
    expensive_log("row_index_writer_impl::add() this={} first_ck={} last_ck={} data_file_pos={} dt={} _last_key={} _last_sep={}",
        fmt::ptr(this),
        fmt_hex(first_ck),
        fmt_hex(last_ck),
        data_file_pos,
        dt,
        fmt_hex(_last_key),
        fmt_hex(_last_separator)
    );
    size_t n = std::min(first_ck.size(), _last_key.size());
    size_t separator_mismatch = mismatch_idx(first_ck.data(), _last_key.data(), n);

    // size_t separator_mismatch = std::ranges::mismatch(first_ck, _last_key).in2 - _last_key.begin();
    expensive_assert(separator_mismatch < first_ck.size());
    expensive_assert(separator_mismatch <= _last_key.size());
    if (_added_blocks > 0) {
        // For the first block, _last_key is empty.
        // We leave it that way. The key we insert into the trie to represent the first block is empty.
        //
        // For later blocks, we need to insert some separator S which is greater than the last key (A) of the previous
        // block and not smaller than the first key (B) of the current block.
        //
        // The choice of this separator affects the efficiency of lookups for range queries starting at any X within the keyless range (A, B).
        // Such queries lie entirely after the previous block, so the optimal answer from the index is the current block.
        // But whether the index returns the previous or the current block, can depend only on whether X is smaller or not smaller than the separator.
        //
        // For example, assume that A=0 and B=9.
        // Imagine a query for the range (5, +∞). If S=1, then index will return the current block, which is optimal.
        // If S=9, then index will return the previous block, and the reader will waste time scanning through it.
        // 
        // Therefore it is good to construct S to be as close as possible to A (not to B) as possible.
        // In this regard, the optimal separator is A concatenated with a zero byte.
        //
        // But at the same time, we don't want to use a separator as long as a full key if much shorter possible separators exist.
        //
        // Therefore, as an arbitrary compromise, we use the optimal-distance separator in the set
        // of optimal-length separators. Which means we just nudge the byte at the point of mismatch by 1.
        _last_key.resize(separator_mismatch + 1);
        // The byte at the point of mismatch must be greater in the next key than in the previous key.
        // So the byte in the previous key can't possibly be 0xff.
        expensive_assert(_last_key[separator_mismatch] != std::byte(0xff));
        // Sanity check. Encoding of keys is supposed to be prefix-free.
        expensive_assert(separator_mismatch < _last_key.size());
        // The condition of this `if` is always true iff key encoding is prefix-free.
        // (And in the BTI format, it is. The previous line asserts that).
        // But with this `if` in place, the writer doesn't depend on a prefix-free encoding,
        // so I left it in the code for posterity. 
        if (separator_mismatch < _last_key.size()) {
            _last_key[separator_mismatch] = std::byte(uint8_t(_last_key[separator_mismatch]) + 1);
        }
    }

    // size_t needed_prefix = std::min(std::max(_last_sep_mismatch, mismatch) + 1, _last_separator.size());
    size_t mismatch = std::ranges::mismatch(_last_key, _last_separator).in2 - _last_separator.begin();
    auto tail = std::span(_last_key).subspan(mismatch, _last_key.size() - mismatch);

    std::array<std::byte, 20> payload_bytes;
    auto payload_bits = div_ceil(std::bit_width<uint64_t>(data_file_pos), 8);
    std::byte* p = payload_bytes.data();
    uint64_t offset_be = seastar::cpu_to_be(data_file_pos);
    std::memcpy(p, (const char*)(&offset_be) + 8 - payload_bits, payload_bits);
    p += payload_bits;
    uint8_t has_tombstone_flag = 0;
    if (!dt.live()) {
        has_tombstone_flag = 0x8;
        p = write_unaligned(p, seastar::cpu_to_be(dt.marked_for_delete_at));
        p = write_unaligned(p, seastar::cpu_to_be(dt.local_deletion_time));
    }

    expensive_log("row_index_trie_writer::add(): _wr.add({}, {}, {}, {}, {:016x})", mismatch, fmt_hex(tail), fmt_hex(payload_bytes), payload_bits, data_file_pos);
    _wr.add(mismatch, tail, trie_payload(has_tombstone_flag | payload_bits, {payload_bytes.data(), p}));

    _added_blocks += 1;
    std::swap(_last_separator, _last_key);
    _last_key.assign(last_ck.begin(), last_ck.end());
}

template <trie_writer_sink Output>
sink_pos row_index_writer_impl<Output>::finish() {
    expensive_log("row_index_writer_impl::finish() this={}", fmt::ptr(this));
    auto result = _wr.finish();
    _added_blocks = 0;
    _last_key.clear();
    _last_separator.clear();
    return result;
}

class row_trie_writer::impl : public row_index_writer_impl<bti_trie_sink_impl> {
    using row_index_writer_impl::row_index_writer_impl;
};

// Pimpl boilerplate
row_trie_writer::row_trie_writer() = default;
row_trie_writer::~row_trie_writer() = default;
row_trie_writer& row_trie_writer::operator=(row_trie_writer&&) = default;
row_trie_writer::row_trie_writer(bti_trie_sink& out)
    : _impl(std::make_unique<impl>(*out._impl)) {
}
void row_trie_writer::add(const_bytes first_ck, const_bytes last_ck, uint64_t data_file_pos, sstables::deletion_time dt) {
    return _impl->add(first_ck, last_ck, data_file_pos, dt);
}
int64_t row_trie_writer::finish() {
    return _impl->finish().value;
}

bti_trie_sink make_bti_trie_sink(sstables::file_writer& w, size_t page_size) {
    return bti_trie_sink(std::make_unique<bti_trie_sink_impl>(w, page_size));
}

partition_trie_writer make_partition_trie_writer(bti_trie_sink& out) {
    return partition_trie_writer(out);
}

row_trie_writer make_row_trie_writer(bti_trie_sink& out) {
    return row_trie_writer(out);
}

} // namespace trie
