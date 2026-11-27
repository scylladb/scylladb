/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Value encodings of Parquet data pages and dictionary pages
// (Encodings.md of the parquet-format repository).
//
// The writer side buffers the values of a pending page in typed form, keeps
// a size estimate of every candidate encoding as values arrive, and encodes
// the page with the encoding that the column writer picks. The reader side
// decodes a whole page from bytes in memory into typed values.
//
// The C++ value type of each physical type:
//
//   | Physical type        | Writer input                | Decoder output   |
//   |----------------------|-----------------------------|------------------|
//   | BOOLEAN              | bool                        | bool             |
//   | INT32                | int32_t                     | int32_t          |
//   | INT64                | int64_t                     | int64_t          |
//   | FLOAT                | float                       | float            |
//   | DOUBLE               | double                      | double           |
//   | BYTE_ARRAY           | any FragmentedView          | byte_array_view  |
//   | FIXED_LEN_BYTE_ARRAY | any FragmentedView          | byte_array_view  |
//
// The encodings that this codec writes and reads:
//
//   | Physical type        | Data page encodings                                         |
//   |----------------------|-------------------------------------------------------------|
//   | BOOLEAN              | PLAIN, RLE                                                  |
//   | INT32, INT64         | PLAIN, DELTA_BINARY_PACKED, RLE_DICTIONARY                  |
//   | FLOAT, DOUBLE        | PLAIN, RLE_DICTIONARY                                       |
//   | BYTE_ARRAY           | PLAIN, DELTA_LENGTH_BYTE_ARRAY, RLE_DICTIONARY              |
//   | FIXED_LEN_BYTE_ARRAY | PLAIN, RLE_DICTIONARY                                       |
//
// A dictionary page is PLAIN. The reader also accepts the deprecated
// PLAIN_DICTIONARY, which has the same format as RLE_DICTIONARY in a data
// page and as PLAIN in a dictionary page. Dictionaries are not offered for
// BOOLEAN, where RLE does the same job.

#include <array>
#include <cstdint>
#include <iterator>
#include <memory>
#include <optional>
#include <span>
#include <type_traits>

#include <seastar/core/future.hh>

#include "bytes_ostream.hh"
#include "sstables/parquet/bit_util.hh"
#include "sstables/parquet/metadata.hh"
#include "sstables/parquet/rle.hh"
#include "utils/bit_cast.hh"
#include "utils/chunked_vector.hh"
#include "utils/fragment_range.hh"
#include "utils/managed_bytes.hh"

namespace sstables::parquet {

/// Whether the reader can decode data pages of type `t` with encoding `e`.
bool is_decodable(physical_type t, encoding e) noexcept;

/// The encodings that the writer may choose for data pages of type `t`,
/// apart from RLE_DICTIONARY. PLAIN is always first.
std::span<const encoding> writer_encodings(physical_type t) noexcept;

/// Whether the writer may use a dictionary for type `t`.
bool supports_dictionary(physical_type t) noexcept;

/// The C++ type of a fixed-width physical type.
template <typename T>
concept fixed_width_value = std::same_as<T, bool> || std::same_as<T, int32_t> || std::same_as<T, int64_t>
        || std::same_as<T, float> || std::same_as<T, double>;

template <typename T>
concept delta_value = std::same_as<T, int32_t> || std::same_as<T, int64_t>;

/// A decoded value: a fixed-width value or a view of a byte array.
template <typename T>
concept decoded_value = fixed_width_value<T> || std::same_as<T, byte_array_view>;

/// The physical type of a fixed-width C++ type.
template <fixed_width_value T>
constexpr physical_type physical_type_of() noexcept {
    if constexpr (std::same_as<T, bool>) {
        return physical_type::boolean;
    } else if constexpr (std::same_as<T, int32_t>) {
        return physical_type::int32;
    } else if constexpr (std::same_as<T, int64_t>) {
        return physical_type::int64;
    } else if constexpr (std::same_as<T, float>) {
        return physical_type::float_;
    } else {
        return physical_type::double_;
    }
}

// DELTA_BINARY_PACKED

/// The block layout that the writer uses, as in other common writers.
inline constexpr unsigned delta_values_per_block = 128;
inline constexpr unsigned delta_miniblocks_per_block = 4;
inline constexpr unsigned delta_values_per_miniblock = delta_values_per_block / delta_miniblocks_per_block;

/// Computes the exact size of the DELTA_BINARY_PACKED encoding of a
/// sequence, in constant time per value.
///
/// The bit width of a miniblock depends only on the largest delta in the
/// miniblock and the smallest delta in its block. So the estimator keeps
/// these extremes instead of the deltas.
template <delta_value T>
class delta_size_estimator {
    using U = std::make_unsigned_t<T>;
    uint64_t _count = 0;
    T _first = 0;
    T _prev = 0;
    // The deltas of the current block.
    unsigned _in_block = 0;
    T _block_min = 0;
    std::array<T, delta_miniblocks_per_block> _miniblock_max;
    // The size of the blocks that are complete.
    uint64_t _blocks_size = 0;

    uint64_t block_size() const noexcept;
public:
    void append(T v) noexcept {
        if (_count++ == 0) {
            _first = _prev = v;
            return;
        }
        // Deltas wrap around in the width of T, as in the encoder.
        const T delta = T(U(v) - U(_prev));
        _prev = v;
        const unsigned mb = _in_block / delta_values_per_miniblock;
        if (_in_block == 0 || delta < _block_min) {
            _block_min = delta;
        }
        if (_in_block % delta_values_per_miniblock == 0 || delta > _miniblock_max[mb]) {
            _miniblock_max[mb] = delta;
        }
        if (++_in_block == delta_values_per_block) {
            _blocks_size += block_size();
            _in_block = 0;
        }
    }
    uint64_t count() const noexcept { return _count; }
    /// The exact size of the encoding of the values appended so far.
    uint64_t size() const noexcept;
    void clear() noexcept { *this = {}; }
};

/// Writes the DELTA_BINARY_PACKED encoding of exactly `count` values.
///
/// The header holds the value count, so the caller states it up front.
/// The encoder holds one block of deltas, about 1 KiB.
template <delta_value T>
class delta_binary_packed_encoder {
    using U = std::make_unsigned_t<T>;
    bytes_ostream& _out;
    uint64_t _count;
    uint64_t _written = 0;
    T _prev = 0;
    unsigned _in_block = 0;
    std::array<T, delta_values_per_block> _deltas;

    void write_header(T first);
    void write_block();
public:
    delta_binary_packed_encoder(bytes_ostream& out, uint64_t count) noexcept : _out(out), _count(count) {}
    void put(T v);
    /// Writes the last block. Must be called after the last put().
    void finish();
};

// Pending values of a page, on the writer side.

/// The values of the pending page of a fixed-width column.
///
/// append() stores the value and updates the size estimate of every
/// candidate encoding in constant time. The column writer compares
/// encoded_size() of the candidates when it cuts the page, and calls
/// encode() with the smallest. Nulls are not stored here: they are the
/// definition levels' business.
template <fixed_width_value T>
class fixed_width_values {
    utils::chunked_vector<T> _values;
    // Used for int32 and int64 only.
    delta_size_estimator<std::conditional_t<delta_value<T>, T, int32_t>> _delta;
    // Used for bool only.
    rle_size_estimator _rle;
public:
    static constexpr physical_type type = physical_type_of<T>();

    void append(T v) {
        _values.push_back(v);
        if constexpr (delta_value<T>) {
            _delta.append(v);
        }
        if constexpr (std::same_as<T, bool>) {
            _rle.append(v);
        }
    }
    size_t size() const noexcept { return _values.size(); }
    bool empty() const noexcept { return _values.empty(); }
    const utils::chunked_vector<T>& values() const noexcept { return _values; }

    /// The size of the PLAIN encoding, which is also the raw size of the values.
    uint64_t plain_size() const noexcept;
    /// The size of the values in encoding `e`, one of writer_encodings(type).
    /// Exact for PLAIN and DELTA_BINARY_PACKED, an estimate for RLE.
    uint64_t encoded_size(encoding e) const;
    /// Appends the values in encoding `e`, one of writer_encodings(type).
    void encode(encoding e, bytes_ostream& out) const;
    void clear() noexcept;
};

/// The values of the pending page of a BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY column.
///
/// The value bytes are copied into one bytes_ostream, in contiguous pieces
/// of at most 128 KiB, and the lengths are kept separately. This is the
/// layout of DELTA_LENGTH_BYTE_ARRAY, so that encoding costs no more than a
/// copy.
class byte_array_values {
    std::optional<uint32_t> _fixed_length;
    bytes_ostream _data;
    utils::chunked_vector<uint32_t> _lengths;
    delta_size_estimator<int32_t> _length_estimator;

    void check_length(size_t length) const;
public:
    /// `fixed_length` is set for FIXED_LEN_BYTE_ARRAY.
    explicit byte_array_values(std::optional<uint32_t> fixed_length = std::nullopt);

    physical_type type() const noexcept {
        return _fixed_length ? physical_type::fixed_len_byte_array : physical_type::byte_array;
    }

    template <FragmentedView View>
    void append(View v) {
        check_length(v.size_bytes());
        _lengths.push_back(v.size_bytes());
        _length_estimator.append(v.size_bytes());
        _data.write(v);
    }
    void append(bytes_view v) { append(single_fragmented_view(v)); }

    size_t size() const noexcept { return _lengths.size(); }
    bool empty() const noexcept { return _lengths.empty(); }
    /// The total size of the values, without lengths.
    uint64_t data_size() const noexcept { return _data.size(); }

    uint64_t plain_size() const noexcept;
    uint64_t encoded_size(encoding e) const;
    /// In a seastar thread, yields between the contiguous pieces of the values.
    void encode(encoding e, bytes_ostream& out) const;
    void clear() noexcept;
};

// Dictionaries, on the writer side.

/// Open addressing from the hash of a dictionary entry to its index.
/// The entries themselves are stored by the dictionary builder, which
/// compares them on a hash match.
class dictionary_hash_index {
    // The entry index plus 1, or 0 for an empty slot.
    utils::chunked_vector<uint32_t> _slots;
    utils::chunked_vector<uint64_t> _hashes;
    size_t _mask = 0;

    void grow();
public:
    /// Calls `equal(index)` for each entry with hash `h` until it returns true.
    /// Returns the index of that entry.
    template <typename Equal>
    std::optional<uint32_t> find(uint64_t h, Equal&& equal) const {
        if (_slots.empty()) {
            return std::nullopt;
        }
        for (size_t pos = h & _mask;; pos = (pos + 1) & _mask) {
            const uint32_t slot = _slots[pos];
            if (slot == 0) {
                return std::nullopt;
            }
            if (_hashes[slot - 1] == h && equal(slot - 1)) {
                return slot - 1;
            }
        }
    }
    /// Records that the next entry, with index size(), has hash `h`.
    void add(uint64_t h);
    size_t size() const noexcept { return _hashes.size(); }
    size_t memory_usage() const noexcept { return _slots.size() * sizeof(uint32_t) + _hashes.size() * sizeof(uint64_t); }
    void clear() noexcept;
};

/// Builds the dictionary of a column chunk of a fixed-width type.
///
/// The dictionary page has a size limit. When a new value would exceed
/// it, insert() returns nothing and leaves the dictionary unchanged. The
/// column writer then encodes the pending page and the rest of the chunk
/// without the dictionary. This is Parquet's standard fallback, because
/// each page header names its own encoding. The column writer keeps the
/// raw values of the pending page (fixed_width_values) for this case.
template <fixed_width_value T>
requires (!std::same_as<T, bool>)
class dictionary_builder {
    uint64_t _max_page_size;
    utils::chunked_vector<T> _entries;
    dictionary_hash_index _index;
public:
    explicit dictionary_builder(uint64_t max_page_size) noexcept : _max_page_size(max_page_size) {}
    /// Returns the index of `v`, adding it if it is new.
    std::optional<uint32_t> insert(T v);
    uint32_t size() const noexcept { return _entries.size(); }
    /// The size of the PLAIN dictionary page.
    uint64_t page_size() const noexcept { return _entries.size() * sizeof(T); }
    /// The memory of the entries and of the hash index.
    uint64_t memory_usage() const noexcept { return page_size() + _index.memory_usage(); }
    /// Appends the dictionary page body, in PLAIN encoding.
    void write_page(bytes_ostream& out) const;
    void clear() noexcept;
};

/// Builds the dictionary of a column chunk of BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY.
/// See dictionary_builder.
class byte_array_dictionary_builder {
    uint64_t _max_page_size;
    std::optional<uint32_t> _fixed_length;
    utils::chunked_vector<managed_bytes> _entries;
    dictionary_hash_index _index;
    uint64_t _page_size = 0;

    template <FragmentedView View>
    static uint64_t hash(View v);
public:
    byte_array_dictionary_builder(uint64_t max_page_size, std::optional<uint32_t> fixed_length = std::nullopt) noexcept
        : _max_page_size(max_page_size), _fixed_length(fixed_length) {}
    template <FragmentedView View>
    std::optional<uint32_t> insert(View v);
    std::optional<uint32_t> insert(bytes_view v) { return insert(single_fragmented_view(v)); }
    uint32_t size() const noexcept { return _entries.size(); }
    uint64_t page_size() const noexcept { return _page_size; }
    /// An estimate of the memory of the entries and of the hash index.
    uint64_t memory_usage() const noexcept { return _page_size + _entries.size() * sizeof(managed_bytes) + _index.memory_usage(); }
    void write_page(bytes_ostream& out) const;
    void clear() noexcept;
};

/// The bit width of the indices into a dictionary of `size` entries.
/// It is at least 1, as other writers do.
inline unsigned dictionary_index_bit_width(uint32_t size) noexcept {
    return std::max(1u, bit_width_of(size ? size - 1 : 0));
}

/// The dictionary indices of the values of the pending page.
class dictionary_indices {
    utils::chunked_vector<uint32_t> _indices;
    rle_size_estimator _estimator;
public:
    void append(uint32_t index) {
        _indices.push_back(index);
        _estimator.append(index);
    }
    size_t size() const noexcept { return _indices.size(); }
    /// An estimate of the RLE_DICTIONARY encoding for a dictionary of `dictionary_size` entries.
    uint64_t encoded_size(uint32_t dictionary_size) const noexcept;
    /// Appends the RLE_DICTIONARY encoding: the bit width byte and the indices.
    void encode(uint32_t dictionary_size, bytes_ostream& out) const;
    void clear() noexcept;
};

// Reader side.

/// The size of the compact form of a dictionary (see dictionary) whose page
/// body has `body_size` bytes and `num_values` entries of type `t`.
size_t dictionary_compact_size(physical_type t, uint64_t body_size, uint64_t num_values) noexcept;

/// A decoded dictionary page, in a compact form without pointers, so that a
/// copy of the form is a dictionary too:
///
/// - The number of entries and the size of the page body, as 32-bit integers.
/// - The page body, uncompressed, in PLAIN encoding.
/// - For BYTE_ARRAY only, zeros up to a multiple of 4 bytes, and then the
///   position in the body of the bytes of each entry, after its length,
///   as 32-bit integers.
///
/// The integers are in the byte order of the machine, because the form is
/// never written to a file. The form is in one fragmented_temporary_buffer
/// whose fragments, apart from the last, have the default fragment size,
/// which is a multiple of 8. So no fixed-width entry and no position crosses
/// a fragment, and an entry is found without a search.
///
/// Byte array entries are views of the buffer, which the dictionary owns.
/// Moving the dictionary keeps the views valid, because the contiguous
/// pieces of the buffer do not move.
template <decoded_value T>
class dictionary {
    static constexpr size_t header_size = 2 * sizeof(uint32_t);
    static constexpr size_t fragment_size = fragmented_temporary_buffer::default_fragment_size;
    static_assert(fragment_size % 8 == 0);

    fragmented_temporary_buffer _data;
    uint32_t _size = 0;
    uint32_t _body_size = 0;
    // The length of a FIXED_LEN_BYTE_ARRAY entry, or 0.
    uint32_t _type_length = 0;
    // Where the positions of BYTE_ARRAY entries start in _data.
    size_t _positions = 0;
    // The start of _data if it is in one contiguous piece, as it is for a
    // dictionary within the writer's page size limit. Otherwise null. An
    // entry is then read without finding its contiguous piece first.
    const char* _contiguous = nullptr;

    void set_contiguous() noexcept {
        _contiguous = _data.begin() != _data.end() && std::next(_data.begin()) == _data.end() ? _data.begin()->get() : nullptr;
    }

    const char* at(size_t pos) const noexcept {
        return (_data.begin() + pos / fragment_size)->get() + pos % fragment_size;
    }
    byte_array_view view_at(size_t pos, size_t size) const noexcept {
        if (!size) {
            return byte_array_view(bytes_view());
        }
        return byte_array_view(_data.begin() + pos / fragment_size, pos % fragment_size, size);
    }
    uint32_t position(size_t i) const noexcept {
        return read_unaligned<uint32_t>(at(_positions + i * sizeof(uint32_t)));
    }
public:
    dictionary() = default;
    dictionary(dictionary&&) noexcept = default;
    dictionary& operator=(dictionary&&) noexcept = default;

    /// Decodes the body of a PLAIN dictionary page of `num_values` entries.
    /// `type_length` is the length of a FIXED_LEN_BYTE_ARRAY value.
    /// The whole body must hold exactly the entries. The entry count is
    /// checked against the body size before anything is allocated. The
    /// dictionary holds a copy of the body. Yields between batches of
    /// entries.
    static future<dictionary> decode(byte_array_view body, uint32_t num_values, physical_type type, int32_t type_length = 0);
    static future<dictionary> decode(fragmented_temporary_buffer body, uint32_t num_values, physical_type type, int32_t type_length = 0);

    /// Takes the compact form of a dictionary of type `type`, as compact()
    /// returns it. A copy must be allocated with allocate_to_fit(), so that
    /// its fragments have the default size.
    static dictionary from_compact(fragmented_temporary_buffer data, physical_type type, int32_t type_length = 0);

    /// The compact form.
    const fragmented_temporary_buffer& compact() const noexcept { return _data; }

    size_t size() const noexcept { return _size; }
    T operator[](size_t i) const noexcept {
        if (_contiguous) [[likely]] {
            const char* body = _contiguous + header_size;
            if constexpr (std::same_as<T, byte_array_view>) {
                auto entry = [] (const char* p, size_t size) {
                    return byte_array_view(bytes_view(reinterpret_cast<const bytes_view::value_type*>(p), size));
                };
                if (_type_length) {
                    return entry(body + i * _type_length, _type_length);
                }
                const char* positions = _contiguous + _positions;
                const uint32_t start = read_unaligned<uint32_t>(positions + i * sizeof(uint32_t));
                const uint32_t end = i + 1 < _size ? read_unaligned<uint32_t>(positions + (i + 1) * sizeof(uint32_t)) - sizeof(uint32_t) : _body_size;
                return entry(body + start, end - start);
            } else {
                return read_unaligned<T>(body + i * sizeof(T));
            }
        }
        if constexpr (std::same_as<T, byte_array_view>) {
            if (_type_length) {
                return view_at(header_size + i * _type_length, _type_length);
            }
            const uint32_t start = position(i);
            const uint32_t end = i + 1 < _size ? position(i + 1) - sizeof(uint32_t) : _body_size;
            return view_at(header_size + start, end - start);
        } else {
            return read_unaligned<T>(at(header_size + i * sizeof(T)));
        }
    }
    size_t memory_usage() const noexcept { return sizeof(*this) + _data.size_bytes(); }
};

/// Decodes the values of one data page. The caller allocates the output.
template <decoded_value T>
class value_decoder {
protected:
    uint64_t _left;
    void consume(size_t n);
public:
    explicit value_decoder(uint64_t num_values) noexcept : _left(num_values) {}
    virtual ~value_decoder() = default;
    /// Decodes the next out.size() values. Asking for more values than the
    /// page has left is a bug in the caller.
    virtual void decode(std::span<T> out) = 0;
    uint64_t values_left() const noexcept { return _left; }
};

/// Creates a decoder for the values section of a data page.
///
/// `data` must outlive the decoder, and byte array values are views of it.
/// `num_values` is the number of non-null values in the page.
/// `dict` is the chunk's dictionary, or null if the chunk has none.
/// Raises the malformed sstable error if the encoding does not apply to the
/// type, if the page needs a dictionary that is missing, or if the encoding's
/// header is malformed. `decode()` raises it if the data is malformed.
template <decoded_value T>
std::unique_ptr<value_decoder<T>> make_value_decoder(physical_type type, encoding e, byte_array_view data,
        uint64_t num_values, const dictionary<T>* dict = nullptr, int32_t type_length = 0);

} // namespace sstables::parquet
