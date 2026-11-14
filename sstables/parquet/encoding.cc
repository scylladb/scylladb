/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/encoding.hh"

#include <algorithm>
#include <bit>
#include <limits>

#include <seastar/core/align.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/format.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/thread.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "utils/log.hh"
#include "utils/xx_hasher.hh"

namespace sstables::parquet {

static logging::logger encoding_logger("parquet_encoding");

// A preemption point for the encoders of the writer, which runs in a
// seastar thread. Elsewhere, for example in tests, it does nothing.
static void maybe_yield() {
    if (seastar::thread::running_in_thread()) {
        seastar::thread::maybe_yield();
    }
}

// The encodings of each physical type

static constexpr encoding boolean_encodings[] = {encoding::plain, encoding::rle};
static constexpr encoding integer_encodings[] = {encoding::plain, encoding::delta_binary_packed};
static constexpr encoding plain_only[] = {encoding::plain};
static constexpr encoding byte_array_encodings[] = {encoding::plain, encoding::delta_length_byte_array};

std::span<const encoding> writer_encodings(physical_type t) noexcept {
    switch (t) {
    case physical_type::boolean: return boolean_encodings;
    case physical_type::int32:
    case physical_type::int64: return integer_encodings;
    case physical_type::byte_array: return byte_array_encodings;
    default: return plain_only;
    }
}

bool supports_dictionary(physical_type t) noexcept {
    switch (t) {
    case physical_type::int32:
    case physical_type::int64:
    case physical_type::float_:
    case physical_type::double_:
    case physical_type::byte_array:
    case physical_type::fixed_len_byte_array:
        return true;
    default:
        return false;
    }
}

bool is_decodable(physical_type t, encoding e) noexcept {
    if (e == encoding::rle_dictionary || e == encoding::plain_dictionary) {
        return supports_dictionary(t);
    }
    if (t == physical_type::int96) {
        return false;
    }
    return std::ranges::contains(writer_encodings(t), e);
}

// DELTA_BINARY_PACKED

static constexpr uint64_t delta_header_fixed_size = 3; // ULEB128 of 128 is two bytes, of 4 one byte.

template <delta_value T>
uint64_t delta_size_estimator<T>::block_size() const noexcept {
    uint64_t size = uleb128_size(zigzag_encode(_block_min)) + delta_miniblocks_per_block;
    const unsigned used = (_in_block + delta_values_per_miniblock - 1) / delta_values_per_miniblock;
    for (unsigned mb = 0; mb < used; ++mb) {
        const unsigned width = bit_width_of(U(U(_miniblock_max[mb]) - U(_block_min)));
        size += uint64_t(width) * delta_values_per_miniblock / 8;
    }
    return size;
}

template <delta_value T>
uint64_t delta_size_estimator<T>::size() const noexcept {
    const uint64_t header = delta_header_fixed_size + uleb128_size(_count) + uleb128_size(zigzag_encode(_first));
    return header + _blocks_size + (_in_block ? block_size() : 0);
}

template <delta_value T>
void delta_binary_packed_encoder<T>::write_header(T first) {
    write_uleb128(_out, delta_values_per_block);
    write_uleb128(_out, delta_miniblocks_per_block);
    write_uleb128(_out, _count);
    write_uleb128(_out, zigzag_encode(first));
}

template <delta_value T>
void delta_binary_packed_encoder<T>::put(T v) {
    if (_written == _count) {
        on_internal_error(encoding_logger, "delta_binary_packed_encoder: more values than declared");
    }
    if (_written++ == 0) {
        write_header(v);
    } else {
        _deltas[_in_block++] = T(U(v) - U(_prev));
        if (_in_block == delta_values_per_block) {
            write_block();
        }
    }
    _prev = v;
}

template <delta_value T>
void delta_binary_packed_encoder<T>::write_block() {
    const unsigned n = _in_block;
    const T min = *std::min_element(_deltas.begin(), _deltas.begin() + n);
    write_uleb128(_out, zigzag_encode(min));
    // Subtracting the minimum in the unsigned domain gives a value that
    // fits in the width of T, as the specification requires.
    std::array<uint64_t, delta_values_per_block> packed_values{};
    for (unsigned i = 0; i < n; ++i) {
        packed_values[i] = U(U(_deltas[i]) - U(min));
    }
    const unsigned used = (n + delta_values_per_miniblock - 1) / delta_values_per_miniblock;
    std::array<uint8_t, delta_miniblocks_per_block> widths{};
    for (unsigned mb = 0; mb < used; ++mb) {
        auto first = packed_values.begin() + mb * delta_values_per_miniblock;
        widths[mb] = bit_width_of(*std::max_element(first, first + delta_values_per_miniblock));
    }
    _out.write(reinterpret_cast<const char*>(widths.data()), widths.size());
    // The padding of the last miniblock is zero, from the initialization above.
    for (unsigned mb = 0; mb < used; ++mb) {
        for (unsigned g = 0; g < delta_values_per_miniblock / 8; ++g) {
            uint8_t buf[64];
            pack8(packed_values.data() + mb * delta_values_per_miniblock + g * 8, widths[mb], buf);
            _out.write(reinterpret_cast<const char*>(buf), widths[mb]);
        }
    }
    _in_block = 0;
}

template <delta_value T>
void delta_binary_packed_encoder<T>::finish() {
    if (_written != _count) {
        on_internal_error(encoding_logger, seastar::format("delta_binary_packed_encoder: {} values written, {} declared", _written, _count));
    }
    if (_count == 0) {
        write_header(0);
    }
    if (_in_block) {
        write_block();
    }
}

template class delta_size_estimator<int32_t>;
template class delta_size_estimator<int64_t>;
template class delta_binary_packed_encoder<int32_t>;
template class delta_binary_packed_encoder<int64_t>;

// fixed_width_values

[[noreturn]] static void bad_encoding(physical_type t, encoding e) {
    on_internal_error(encoding_logger, seastar::format("encoding {} is not a writer encoding of physical type {}", int(e), int(t)));
}

// Copies values to `out` through a small stack buffer, to avoid a call per value.
template <typename T>
static void write_plain_values(const utils::chunked_vector<T>& values, bytes_ostream& out) {
    constexpr size_t batch = 512;
    T buf[batch];
    size_t n = 0;
    for (const T& v : values) {
        buf[n++] = v;
        if (n == batch) {
            out.write(reinterpret_cast<const char*>(buf), n * sizeof(T));
            n = 0;
        }
    }
    out.write(reinterpret_cast<const char*>(buf), n * sizeof(T));
}

static void write_plain_booleans(const utils::chunked_vector<bool>& values, bytes_ostream& out) {
    uint8_t byte = 0;
    unsigned bit = 0;
    for (bool v : values) {
        byte |= uint8_t(v) << bit;
        if (++bit == 8) {
            out.write(reinterpret_cast<const char*>(&byte), 1);
            byte = 0;
            bit = 0;
        }
    }
    if (bit) {
        out.write(reinterpret_cast<const char*>(&byte), 1);
    }
}

template <fixed_width_value T>
uint64_t fixed_width_values<T>::plain_size() const noexcept {
    if constexpr (std::same_as<T, bool>) {
        return (_values.size() + 7) / 8;
    } else {
        return _values.size() * sizeof(T);
    }
}

template <fixed_width_value T>
uint64_t fixed_width_values<T>::encoded_size(encoding e) const {
    if (e == encoding::plain) {
        return plain_size();
    }
    if constexpr (delta_value<T>) {
        if (e == encoding::delta_binary_packed) {
            return _delta.size();
        }
    }
    if constexpr (std::same_as<T, bool>) {
        if (e == encoding::rle) {
            return sizeof(uint32_t) + _rle.estimate(1);
        }
    }
    bad_encoding(type, e);
}

template <fixed_width_value T>
void fixed_width_values<T>::encode(encoding e, bytes_ostream& out) const {
    if (e == encoding::plain) {
        if constexpr (std::same_as<T, bool>) {
            write_plain_booleans(_values, out);
        } else {
            write_plain_values(_values, out);
        }
        return;
    }
    if constexpr (delta_value<T>) {
        if (e == encoding::delta_binary_packed) {
            delta_binary_packed_encoder<T> enc(out, _values.size());
            for (T v : _values) {
                enc.put(v);
            }
            enc.finish();
            return;
        }
    }
    if constexpr (std::same_as<T, bool>) {
        if (e == encoding::rle) {
            // In a data page, RLE booleans are preceded by their length.
            rle_bp_encoder enc(1);
            for (bool v : _values) {
                enc.append(v);
            }
            auto body = enc.finish();
            write_le<uint32_t>(out, body.size());
            out.append(body);
            return;
        }
    }
    bad_encoding(type, e);
}

template <fixed_width_value T>
void fixed_width_values<T>::clear() noexcept {
    _values.clear();
    _delta.clear();
    _rle.clear();
}

template class fixed_width_values<bool>;
template class fixed_width_values<int32_t>;
template class fixed_width_values<int64_t>;
template class fixed_width_values<float>;
template class fixed_width_values<double>;

// byte_array_values

byte_array_values::byte_array_values(std::optional<uint32_t> fixed_length)
    : _fixed_length(fixed_length)
{
    if (fixed_length && *fixed_length == 0) {
        on_internal_error(encoding_logger, "byte_array_values: a fixed length of zero");
    }
}

void byte_array_values::check_length(size_t length) const {
    if (_fixed_length ? length != *_fixed_length : length > size_t(std::numeric_limits<int32_t>::max())) {
        on_internal_error(encoding_logger, seastar::format("byte_array_values: a value of {} bytes, fixed length {}", length, _fixed_length));
    }
}

uint64_t byte_array_values::plain_size() const noexcept {
    return _data.size() + (_fixed_length ? 0 : sizeof(uint32_t) * _lengths.size());
}

uint64_t byte_array_values::encoded_size(encoding e) const {
    if (e == encoding::plain) {
        return plain_size();
    }
    if (e == encoding::delta_length_byte_array && !_fixed_length) {
        return _length_estimator.size() + _data.size();
    }
    bad_encoding(type(), e);
}

// Copies the value bytes to `out`, with a preemption point between
// contiguous pieces.
static void append_data(const bytes_ostream& data, bytes_ostream& out) {
    for (bytes_view frag : data.fragments()) {
        out.write(frag);
        maybe_yield();
    }
}

void byte_array_values::encode(encoding e, bytes_ostream& out) const {
    if (e == encoding::plain) {
        if (_fixed_length) {
            append_data(_data, out);
            return;
        }
        // Interleave the lengths with the value bytes.
        auto frags = _data.fragments().begin();
        bytes_view frag;
        for (uint32_t len : _lengths) {
            write_le<uint32_t>(out, len);
            while (len) {
                if (frag.empty()) {
                    // A preemption point between contiguous pieces of the values.
                    maybe_yield();
                    frag = *frags++;
                }
                auto now = std::min<size_t>(len, frag.size());
                out.write(frag.substr(0, now));
                frag.remove_prefix(now);
                len -= now;
            }
        }
        return;
    }
    if (e == encoding::delta_length_byte_array && !_fixed_length) {
        delta_binary_packed_encoder<int32_t> enc(out, _lengths.size());
        for (uint32_t len : _lengths) {
            enc.put(len);
        }
        enc.finish();
        append_data(_data, out);
        return;
    }
    bad_encoding(type(), e);
}

void byte_array_values::clear() noexcept {
    _data.clear();
    _lengths.clear();
    _length_estimator.clear();
}

// Dictionaries on the writer side

// The dictionary index of a data page is a signed 32-bit number in other readers.
static constexpr uint32_t max_dictionary_entries = std::numeric_limits<int32_t>::max();

void dictionary_hash_index::add(uint64_t h) {
    _hashes.push_back(h);
    if (_hashes.size() * 2 > _slots.size()) {
        grow();
        return;
    }
    size_t pos = h & _mask;
    while (_slots[pos]) {
        pos = (pos + 1) & _mask;
    }
    _slots[pos] = _hashes.size();
}

void dictionary_hash_index::grow() {
    const size_t capacity = std::max<size_t>(16, _slots.size() * 2);
    _slots.clear();
    _slots.resize(capacity);
    _mask = capacity - 1;
    for (size_t i = 0; i < _hashes.size(); ++i) {
        size_t pos = _hashes[i] & _mask;
        while (_slots[pos]) {
            pos = (pos + 1) & _mask;
        }
        _slots[pos] = i + 1;
    }
}

void dictionary_hash_index::clear() noexcept {
    _slots.clear();
    _hashes.clear();
    _mask = 0;
}

static uint64_t hash_bytes(const void* p, size_t n) noexcept {
    xx_hasher h;
    h.update(static_cast<const char*>(p), n);
    return h.finalize_uint64();
}

template <fixed_width_value T>
requires (!std::same_as<T, bool>)
std::optional<uint32_t> dictionary_builder<T>::insert(T v) {
    // Floating point values are compared by their bits, so that every NaN
    // payload and the sign of zero are preserved.
    const uint64_t h = hash_bytes(&v, sizeof(T));
    auto found = _index.find(h, [&] (uint32_t i) {
        return std::memcmp(&_entries[i], &v, sizeof(T)) == 0;
    });
    if (found) {
        return found;
    }
    if (page_size() + sizeof(T) > _max_page_size || _entries.size() == max_dictionary_entries) {
        return std::nullopt;
    }
    _entries.push_back(v);
    _index.add(h);
    return _entries.size() - 1;
}

template <fixed_width_value T>
requires (!std::same_as<T, bool>)
void dictionary_builder<T>::write_page(bytes_ostream& out) const {
    write_plain_values(_entries, out);
}

template <fixed_width_value T>
requires (!std::same_as<T, bool>)
void dictionary_builder<T>::clear() noexcept {
    _entries.clear();
    _index.clear();
}

template class dictionary_builder<int32_t>;
template class dictionary_builder<int64_t>;
template class dictionary_builder<float>;
template class dictionary_builder<double>;

template <FragmentedView View>
uint64_t byte_array_dictionary_builder::hash(View v) {
    xx_hasher h;
    for (bytes_view frag : fragment_range(v)) {
        h.update(reinterpret_cast<const char*>(frag.data()), frag.size());
    }
    return h.finalize_uint64();
}

template <FragmentedView View>
std::optional<uint32_t> byte_array_dictionary_builder::insert(View v) {
    if (_fixed_length && v.size_bytes() != *_fixed_length) {
        on_internal_error(encoding_logger, seastar::format("byte_array_dictionary_builder: a value of {} bytes, fixed length {}", v.size_bytes(), *_fixed_length));
    }
    const uint64_t entry_size = v.size_bytes() + (_fixed_length ? 0 : sizeof(uint32_t));
    // Every entry is counted in _page_size, so a value with a larger entry
    // is not in the dictionary. If it does not fit either, it is rejected
    // before it is hashed, which saves a pass over a large value.
    if (entry_size > _page_size && _page_size + entry_size > _max_page_size) {
        return std::nullopt;
    }
    const uint64_t h = hash(v);
    auto found = _index.find(h, [&] (uint32_t i) {
        return equal_unsigned(managed_bytes_view(_entries[i]), v);
    });
    if (found) {
        return found;
    }
    if (_page_size + entry_size > _max_page_size || _entries.size() == max_dictionary_entries) {
        return std::nullopt;
    }
    _entries.emplace_back(v);
    _index.add(h);
    _page_size += entry_size;
    return _entries.size() - 1;
}

template std::optional<uint32_t> byte_array_dictionary_builder::insert(single_fragmented_view);
template std::optional<uint32_t> byte_array_dictionary_builder::insert(managed_bytes_view);
template std::optional<uint32_t> byte_array_dictionary_builder::insert(byte_array_view);

void byte_array_dictionary_builder::write_page(bytes_ostream& out) const {
    for (const managed_bytes& e : _entries) {
        if (!_fixed_length) {
            write_le<uint32_t>(out, e.size());
        }
        out.write(managed_bytes_view(e));
    }
}

void byte_array_dictionary_builder::clear() noexcept {
    _entries.clear();
    _index.clear();
    _page_size = 0;
}

uint64_t dictionary_indices::encoded_size(uint32_t dictionary_size) const noexcept {
    return 1 + _estimator.estimate(dictionary_index_bit_width(dictionary_size));
}

void dictionary_indices::encode(uint32_t dictionary_size, bytes_ostream& out) const {
    const uint8_t width = dictionary_index_bit_width(dictionary_size);
    out.write(reinterpret_cast<const char*>(&width), 1);
    rle_bp_encoder enc(width);
    for (uint32_t i : _indices) {
        enc.append(i);
    }
    out.append(enc.finish());
}

void dictionary_indices::clear() noexcept {
    _indices.clear();
    _estimator.clear();
}

// Decoders

template <decoded_value T>
void value_decoder<T>::consume(size_t n) {
    if (n > _left) {
        on_internal_error(encoding_logger, seastar::format("value_decoder: {} values requested, {} left", n, _left));
    }
    _left -= n;
}

template <decoded_value T>
static constexpr bool matches(physical_type t) noexcept {
    if constexpr (std::same_as<T, byte_array_view>) {
        return t == physical_type::byte_array || t == physical_type::fixed_len_byte_array;
    } else {
        return t == physical_type_of<T>();
    }
}

template <decoded_value T>
static void check_type(physical_type t) {
    if (!matches<T>(t)) {
        on_internal_error(encoding_logger, seastar::format("a decoder of the wrong C++ type for physical type {}", int(t)));
    }
}

static void check_type_length(physical_type t, int32_t type_length) {
    if (t == physical_type::fixed_len_byte_array && type_length <= 0) {
        on_malformed_data(seastar::format("invalid length {} of a fixed-length byte array", type_length));
    }
}

namespace {

template <decoded_value T>
class plain_decoder final : public value_decoder<T> {
    byte_reader _in;
    uint32_t _type_length;
    // The current byte of a boolean page, and the position of the next bit in it.
    uint8_t _byte = 0;
    unsigned _bit = 8;
public:
    plain_decoder(byte_array_view data, uint64_t num_values, uint32_t type_length)
        : value_decoder<T>(num_values), _in(data), _type_length(type_length) {}

    void decode(std::span<T> out) override {
        this->consume(out.size());
        if constexpr (std::same_as<T, bool>) {
            for (bool& v : out) {
                if (_bit == 8) {
                    _byte = _in.read_byte();
                    _bit = 0;
                }
                v = (_byte >> _bit++) & 1;
            }
        } else if constexpr (std::same_as<T, byte_array_view>) {
            for (auto& v : out) {
                const uint32_t len = _type_length ? _type_length : _in.read_le<uint32_t>();
                v = _in.read_view(len);
            }
        } else {
            _in.read(out.data(), out.size_bytes());
        }
    }
};

// Booleans in RLE: a 4-byte length, then the hybrid encoding with bit width 1.
class rle_boolean_decoder final : public value_decoder<bool> {
    rle_bp_decoder _rle;
public:
    rle_boolean_decoder(byte_array_view data, uint64_t num_values)
        : value_decoder<bool>(num_values)
    {
        byte_reader in(data);
        const uint32_t len = in.read_le<uint32_t>();
        _rle = rle_bp_decoder(in.read_view(len), 1);
    }

    void decode(std::span<bool> out) override {
        consume(out.size());
        _rle.decode(out);
    }
};

template <decoded_value T>
class dictionary_decoder final : public value_decoder<T> {
    const dictionary<T>& _dict;
    rle_bp_decoder _indices;
public:
    dictionary_decoder(byte_array_view data, uint64_t num_values, const dictionary<T>& dict)
        : value_decoder<T>(num_values), _dict(dict)
    {
        // A page without values may omit the bit width.
        if (num_values == 0 && data.empty()) {
            return;
        }
        byte_reader in(data);
        const uint8_t width = in.read_byte();
        if (width > 32) {
            in.fail(seastar::format("dictionary index bit width {} is above 32", width));
        }
        _indices = rle_bp_decoder(in.view(), width);
    }

    void decode(std::span<T> out) override {
        this->consume(out.size());
        std::array<uint32_t, 256> batch;
        while (!out.empty()) {
            const size_t n = std::min(out.size(), batch.size());
            _indices.decode(std::span(batch.data(), n));
            for (size_t i = 0; i < n; ++i) {
                if (batch[i] >= _dict.size()) [[unlikely]] {
                    on_malformed_data(seastar::format("dictionary index {} is out of range, the dictionary has {} entries", batch[i], _dict.size()));
                }
                out[i] = _dict[batch[i]];
            }
            out = out.subspan(n);
        }
    }
};

// Reads a DELTA_BINARY_PACKED sequence one value at a time.
template <delta_value T>
class delta_reader {
    using U = std::make_unsigned_t<T>;
    // Larger blocks are valid, but no writer is known to use them. The
    // limit bounds the memory for the bit widths of one block.
    static constexpr uint64_t max_values_per_block = 1 << 20;

    byte_reader _in;
    uint64_t _left;
    uint32_t _values_per_block;
    uint32_t _miniblocks;
    uint32_t _groups_per_miniblock;
    utils::small_vector<uint8_t, delta_miniblocks_per_block> _widths;
    U _prev;
    U _min_delta = 0;
    bool _first = true;
    // The next miniblock of the current block. _miniblocks if none is left.
    uint32_t _miniblock;
    uint32_t _groups_left = 0;
    unsigned _width = 0;
    std::array<uint64_t, 8> _group;
    unsigned _group_pos = 8;

    void next_miniblock() {
        if (_miniblock == _miniblocks) {
            _min_delta = U(_in.read_zigzag());
            _in.read(_widths.data(), _miniblocks);
            _miniblock = 0;
        }
        _width = _widths[_miniblock++];
        // Only the widths of miniblocks that hold values are checked,
        // because the specification allows any value in the others.
        if (_width > sizeof(T) * 8) {
            _in.fail(seastar::format("miniblock bit width {} is above {}", _width, sizeof(T) * 8));
        }
        _groups_left = _groups_per_miniblock;
    }
public:
    delta_reader(byte_reader in, uint64_t expected_count) : _in(in) {
        _values_per_block = _in.read_uleb128_32();
        _miniblocks = _in.read_uleb128_32();
        const uint64_t count = _in.read_uleb128();
        _prev = U(_in.read_zigzag());
        if (_values_per_block == 0 || _values_per_block % 128 || _values_per_block > max_values_per_block) {
            _in.fail(seastar::format("invalid block size {}", _values_per_block));
        }
        if (_miniblocks == 0 || _values_per_block % _miniblocks || (_values_per_block / _miniblocks) % 32) {
            _in.fail(seastar::format("invalid miniblock count {} for block size {}", _miniblocks, _values_per_block));
        }
        if (count != expected_count) {
            _in.fail(seastar::format("value count {} differs from the expected {}", count, expected_count));
        }
        _left = count;
        _groups_per_miniblock = _values_per_block / _miniblocks / 8;
        _widths.resize(_miniblocks);
        _miniblock = _miniblocks;
    }

    /// Reads the next out.size() values.
    void read(std::span<T> out) {
        const size_t n = out.size();
        if (n > _left) {
            on_internal_error(encoding_logger, "delta_reader: read past the last value");
        }
        size_t i = 0;
        if (_first && n) {
            _first = false;
            out[i++] = T(_prev);
        }
        U prev = _prev;
        while (i < n) {
            if (_group_pos == 8) {
                if (_groups_left == 0) {
                    next_miniblock();
                }
                if (const uint8_t* in = _in.try_read_padded(_width, unpack_padding)) [[likely]] {
                    unpack8_padded(in, _width, _group.data());
                } else {
                    uint8_t packed[64];
                    _in.read(packed, _width);
                    unpack8(packed, _width, _group.data());
                }
                _group_pos = 0;
                --_groups_left;
            }
            const size_t k = std::min<size_t>(8 - _group_pos, n - i);
            const U min_delta = _min_delta;
            for (size_t j = 0; j < k; ++j) {
                prev = U(prev + min_delta + U(_group[_group_pos + j]));
                out[i + j] = T(prev);
            }
            _group_pos += k;
            i += k;
        }
        _prev = prev;
        _left -= n;
        if (_left == 0) {
            // The last miniblock with values is padded to its full size.
            _in.skip(uint64_t(_groups_left) * _width);
            _groups_left = 0;
        }
    }

    T next() {
        T v;
        read(std::span<T>(&v, 1));
        return v;
    }

    uint64_t left() const noexcept { return _left; }
    /// The input after the sequence. Valid once all values were read.
    const byte_reader& input() const noexcept { return _in; }
};

template <delta_value T>
class delta_binary_packed_decoder final : public value_decoder<T> {
    delta_reader<T> _reader;
public:
    delta_binary_packed_decoder(byte_array_view data, uint64_t num_values)
        : value_decoder<T>(num_values), _reader(byte_reader(data), num_values) {}

    void decode(std::span<T> out) override {
        this->consume(out.size());
        _reader.read(out);
    }
};

class delta_length_byte_array_decoder final : public value_decoder<byte_array_view> {
    delta_reader<int32_t> _lengths;
    byte_reader _data;

    // Finds where the lengths end and the value bytes start, by reading
    // the lengths once. This avoids storing them.
    static byte_reader data_start(byte_array_view data, uint64_t num_values) {
        delta_reader<int32_t> lengths(byte_reader(data), num_values);
        std::array<int32_t, 256> batch;
        while (lengths.left()) {
            lengths.read(std::span(batch.data(), std::min<uint64_t>(batch.size(), lengths.left())));
        }
        return lengths.input();
    }
public:
    delta_length_byte_array_decoder(byte_array_view data, uint64_t num_values)
        : value_decoder<byte_array_view>(num_values)
        , _lengths(byte_reader(data), num_values)
        , _data(data_start(data, num_values).view())
    {}

    void decode(std::span<byte_array_view> out) override {
        consume(out.size());
        std::array<int32_t, 256> batch;
        while (!out.empty()) {
            const size_t n = std::min(out.size(), batch.size());
            _lengths.read(std::span(batch.data(), n));
            for (size_t i = 0; i < n; ++i) {
                const int32_t len = batch[i];
                if (len < 0) {
                    on_malformed_data(seastar::format("negative byte array length {}", len));
                }
                out[i] = _data.read_view(len);
            }
            out = out.subspan(n);
        }
    }
};

} // anonymous namespace

template <decoded_value T>
std::unique_ptr<value_decoder<T>> make_value_decoder(physical_type type, encoding e, byte_array_view data,
        uint64_t num_values, const dictionary<T>* dict, int32_t type_length) {
    check_type<T>(type);
    check_type_length(type, type_length);
    if (!is_decodable(type, e)) {
        on_malformed_data(seastar::format("encoding {} is not supported for physical type {}", int(e), int(type)));
    }
    const uint32_t fixed_length = type == physical_type::fixed_len_byte_array ? type_length : 0;
    switch (e) {
    case encoding::plain:
        return std::make_unique<plain_decoder<T>>(data, num_values, fixed_length);
    case encoding::rle_dictionary:
    case encoding::plain_dictionary:
        if (!dict) {
            on_malformed_data("a dictionary-encoded page in a column chunk without a dictionary");
        }
        return std::make_unique<dictionary_decoder<T>>(data, num_values, *dict);
    default:
        break;
    }
    if constexpr (std::same_as<T, bool>) {
        if (e == encoding::rle) {
            return std::make_unique<rle_boolean_decoder>(data, num_values);
        }
    }
    if constexpr (delta_value<T>) {
        if (e == encoding::delta_binary_packed) {
            return std::make_unique<delta_binary_packed_decoder<T>>(data, num_values);
        }
    }
    if constexpr (std::same_as<T, byte_array_view>) {
        if (e == encoding::delta_length_byte_array) {
            return std::make_unique<delta_length_byte_array_decoder>(data, num_values);
        }
    }
    on_internal_error(encoding_logger, seastar::format("make_value_decoder: unhandled encoding {}", int(e)));
}

size_t dictionary_compact_size(physical_type t, uint64_t body_size, uint64_t num_values) noexcept {
    size_t size = 2 * sizeof(uint32_t) + body_size;
    if (t == physical_type::byte_array) {
        size = align_up(size, sizeof(uint32_t)) + num_values * sizeof(uint32_t);
    }
    return size;
}

template <decoded_value T>
future<dictionary<T>> dictionary<T>::decode(fragmented_temporary_buffer body, uint32_t num_values, physical_type type, int32_t type_length) {
    co_return co_await decode(byte_array_view(body), num_values, type, type_length);
}

template <decoded_value T>
future<dictionary<T>> dictionary<T>::decode(byte_array_view body, uint32_t num_values, physical_type type, int32_t type_length) {
    check_type<T>(type);
    check_type_length(type, type_length);
    if constexpr (std::same_as<T, bool>) {
        on_malformed_data("a BOOLEAN column has a dictionary");
    }
    const uint64_t size = body.size_bytes();
    // The smallest possible encoding of each entry bounds the entry count
    // before the entries are allocated.
    uint64_t min_size;
    if constexpr (std::same_as<T, byte_array_view>) {
        min_size = uint64_t(num_values) * (type == physical_type::fixed_len_byte_array ? uint32_t(type_length) : sizeof(uint32_t));
    } else {
        min_size = uint64_t(num_values) * sizeof(T);
    }
    if (min_size > size) {
        on_malformed_data(seastar::format("dictionary page of {} bytes cannot hold {} entries", size, num_values));
    }
    // The positions in the compact form are 32-bit. A page size is a
    // 32-bit integer in its header too.
    if (size > std::numeric_limits<int32_t>::max()) {
        on_malformed_data(seastar::format("dictionary page of {} bytes is too large", size));
    }
    // Check that the entries fill the body. Byte arrays are checked below,
    // after the entries are read.
    if (!std::same_as<T, byte_array_view> || type == physical_type::fixed_len_byte_array) {
        if (min_size != size) {
            on_malformed_data(seastar::format("dictionary page has {} bytes after its {} entries", size - min_size, num_values));
        }
    }
    dictionary<T> d;
    d._data = fragmented_temporary_buffer::allocate_to_fit(dictionary_compact_size(type, size, num_values));
    d._size = num_values;
    d._body_size = size;
    if (type == physical_type::fixed_len_byte_array) {
        d._type_length = type_length;
    }
    auto out = d._data.get_ostream();
    const uint32_t header[] = {num_values, uint32_t(size)};
    out.write(reinterpret_cast<const char*>(header), sizeof(header));
    for (bytes_view frag : body) {
        out.write(reinterpret_cast<const char*>(frag.data()), frag.size());
    }
    if (type == physical_type::byte_array) {
        static constexpr char zeros[sizeof(uint32_t)] = {};
        d._positions = align_up(header_size + size, sizeof(uint32_t));
        out.write(zeros, d._positions - header_size - size);
        // The position of each entry's bytes, which follow its length.
        plain_decoder<byte_array_view> decoder(body, num_values, 0);
        uint64_t used = 0;
        std::array<byte_array_view, 256> batch;
        for (uint32_t left = num_values; left;) {
            const uint32_t n = std::min<uint32_t>(left, batch.size());
            decoder.decode(std::span(batch.data(), n));
            for (uint32_t i = 0; i < n; ++i) {
                const uint32_t position = used + sizeof(uint32_t);
                out.write(reinterpret_cast<const char*>(&position), sizeof(position));
                used = position + batch[i].size_bytes();
            }
            left -= n;
            co_await coroutine::maybe_yield();
        }
        if (used != size) {
            on_malformed_data(seastar::format("dictionary page has {} bytes after its {} entries", size - used, num_values));
        }
    }
    d.set_contiguous();
    co_return d;
}

template <decoded_value T>
dictionary<T> dictionary<T>::from_compact(fragmented_temporary_buffer data, physical_type type, int32_t type_length) {
    check_type<T>(type);
    dictionary<T> d;
    uint32_t header[2] = {};
    if (data.size_bytes() >= sizeof(header)) {
        auto in = byte_array_view(data);
        read_fragmented(in, sizeof(header), reinterpret_cast<bytes::value_type*>(header));
    }
    d._size = header[0];
    d._body_size = header[1];
    if (type == physical_type::fixed_len_byte_array) {
        d._type_length = type_length;
    } else if (type == physical_type::byte_array) {
        d._positions = align_up(header_size + d._body_size, sizeof(uint32_t));
    }
    bool fragments_ok = true;
    for (auto it = data.begin(); it != data.end(); ++it) {
        fragments_ok &= it->size() == fragment_size || std::next(it) == data.end();
    }
    if (data.size_bytes() < sizeof(header) || data.size_bytes() != dictionary_compact_size(type, d._body_size, d._size) || !fragments_ok) {
        on_internal_error(encoding_logger, seastar::format("a compact dictionary of {} bytes in {} fragments is inconsistent with its header: {} entries, {} bytes",
                data.size_bytes(), std::distance(data.begin(), data.end()), d._size, d._body_size));
    }
    d._data = std::move(data);
    d.set_contiguous();
    return d;
}

template class value_decoder<bool>;
template class dictionary<bool>;
template std::unique_ptr<value_decoder<bool>> make_value_decoder<bool>(physical_type, encoding, byte_array_view, uint64_t,
        const dictionary<bool>*, int32_t);

template class value_decoder<int32_t>;
template class dictionary<int32_t>;
template std::unique_ptr<value_decoder<int32_t>> make_value_decoder<int32_t>(physical_type, encoding, byte_array_view, uint64_t,
        const dictionary<int32_t>*, int32_t);

template class value_decoder<int64_t>;
template class dictionary<int64_t>;
template std::unique_ptr<value_decoder<int64_t>> make_value_decoder<int64_t>(physical_type, encoding, byte_array_view, uint64_t,
        const dictionary<int64_t>*, int32_t);

template class value_decoder<float>;
template class dictionary<float>;
template std::unique_ptr<value_decoder<float>> make_value_decoder<float>(physical_type, encoding, byte_array_view, uint64_t,
        const dictionary<float>*, int32_t);

template class value_decoder<double>;
template class dictionary<double>;
template std::unique_ptr<value_decoder<double>> make_value_decoder<double>(physical_type, encoding, byte_array_view, uint64_t,
        const dictionary<double>*, int32_t);

template class value_decoder<byte_array_view>;
template class dictionary<byte_array_view>;
template std::unique_ptr<value_decoder<byte_array_view>> make_value_decoder<byte_array_view>(physical_type, encoding, byte_array_view, uint64_t,
        const dictionary<byte_array_view>*, int32_t);

} // namespace sstables::parquet
