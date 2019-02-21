/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "core/iostream.hh"
#include "core/fstream.hh"
#include "types.hh"
#include "checksum_utils.hh"
#include "progress_monitor.hh"
#include "vint-serialization.hh"
#include <seastar/core/byteorder.hh>
#include "version.hh"
#include "data/value_view.hh"
#include "service/storage_service.hh"

namespace sstables {

class file_writer {
    output_stream<char> _out;
    writer_offset_tracker _offset;
public:
    file_writer(file f, file_output_stream_options options)
        : _out(make_file_output_stream(std::move(f), std::move(options))) {}

    file_writer(output_stream<char>&& out)
        : _out(std::move(out)) {}

    virtual ~file_writer() = default;
    file_writer(file_writer&&) = default;
    // Must be called in a seastar thread.
    void write(const char* buf, size_t n) {
        _offset.offset += n;
        _out.write(buf, n).get();
    }
    // Must be called in a seastar thread.
    void write(bytes_view s) {
        _offset.offset += s.size();
        _out.write(reinterpret_cast<const char*>(s.begin()), s.size()).get();
    }
    // Must be called in a seastar thread.
    void flush() {
        _out.flush().get();
    }
    // Must be called in a seastar thread.
    void close() {
        _out.close().get();
    }
    uint64_t offset() const {
        return _offset.offset;
    }

    const writer_offset_tracker& offset_tracker() const {
        return _offset;
    }
};


class sizing_data_sink : public data_sink_impl {
    uint64_t& _size;
public:
    explicit sizing_data_sink(uint64_t& dest) : _size(dest) {
        _size = 0;
    }
    virtual temporary_buffer<char> allocate_buffer(size_t size) {
        return temporary_buffer<char>(size);
    }
    virtual future<> put(net::packet data) override {
        _size += data.len();
        return make_ready_future<>();
    }
    virtual future<> put(std::vector<temporary_buffer<char>> data) override {
        _size += boost::accumulate(data | boost::adaptors::transformed(std::mem_fn(&temporary_buffer<char>::size)), 0);
        return make_ready_future<>();
    }
    virtual future<> put(temporary_buffer<char> buf) override {
        _size += buf.size();
        return make_ready_future<>();
    }
    virtual future<> flush() override {
        return make_ready_future<>();
    }
    virtual future<> close() override {
        return make_ready_future<>();
    }
};

inline
output_stream<char>
make_sizing_output_stream(uint64_t& dest) {
    return output_stream<char>(data_sink(std::make_unique<sizing_data_sink>(std::ref(dest))), 4096);
}

// Must be called from a thread
template <typename T>
uint64_t
serialized_size(sstable_version_types v, const T& object) {
    uint64_t size = 0;
    auto writer = file_writer(make_sizing_output_stream(size));
    write(v, writer, object);
    writer.flush();
    writer.close();
    return size;
}

template <typename ChecksumType>
GCC6_CONCEPT(
    requires ChecksumUtils<ChecksumType>
)
class checksummed_file_data_sink_impl : public data_sink_impl {
    data_sink _out;
    struct checksum& _c;
    uint32_t& _full_checksum;
public:
    checksummed_file_data_sink_impl(file f, struct checksum& c, uint32_t& full_file_checksum, file_output_stream_options options)
            : _out(make_file_data_sink(std::move(f), std::move(options)))
            , _c(c)
            , _full_checksum(full_file_checksum)
            {}

    virtual temporary_buffer<char> allocate_buffer(size_t size) override {
        return _out.allocate_buffer(size); // preserve alignment requirements
    }
    future<> put(net::packet data) { abort(); }
    virtual future<> put(temporary_buffer<char> buf) override {
        // bufs will usually be a multiple of chunk size, but this won't be the case for
        // the last buffer being flushed.

        for (size_t offset = 0; offset < buf.size(); offset += _c.chunk_size) {
            size_t size = std::min(size_t(_c.chunk_size), buf.size() - offset);
            uint32_t per_chunk_checksum = ChecksumType::init_checksum();

            per_chunk_checksum = ChecksumType::checksum(per_chunk_checksum, buf.begin() + offset, size);
            _full_checksum = checksum_combine_or_feed<ChecksumType>(_full_checksum, per_chunk_checksum, buf.begin() + offset, size);
            _c.checksums.push_back(per_chunk_checksum);
        }
        return _out.put(std::move(buf));
    }

    virtual future<> close() {
        // Nothing to do, because close at the file_stream level will call flush on us.
        return _out.close();
    }
};

template <typename ChecksumType>
GCC6_CONCEPT(
    requires ChecksumUtils<ChecksumType>
)
class checksummed_file_data_sink : public data_sink {
public:
    checksummed_file_data_sink(file f, struct checksum& cinfo, uint32_t& full_file_checksum, file_output_stream_options options)
        : data_sink(std::make_unique<checksummed_file_data_sink_impl<ChecksumType>>(std::move(f), cinfo, full_file_checksum, std::move(options))) {}
};

template <typename ChecksumType>
GCC6_CONCEPT(
    requires ChecksumUtils<ChecksumType>
)
inline
output_stream<char> make_checksummed_file_output_stream(file f, struct checksum& cinfo, uint32_t& full_file_checksum, file_output_stream_options options) {
    auto buffer_size = options.buffer_size;
    return output_stream<char>(checksummed_file_data_sink<ChecksumType>(std::move(f), cinfo, full_file_checksum, std::move(options)), buffer_size, true);
}

template <typename ChecksumType>
GCC6_CONCEPT(
    requires ChecksumUtils<ChecksumType>
)
class checksummed_file_writer : public file_writer {
    checksum _c;
    uint32_t _full_checksum;
public:
    checksummed_file_writer(file f, file_output_stream_options options)
            : file_writer(make_checksummed_file_output_stream<ChecksumType>(std::move(f), _c, _full_checksum, options))
            , _c({uint32_t(std::min(size_t(DEFAULT_CHUNK_SIZE), size_t(options.buffer_size)))})
            , _full_checksum(ChecksumType::init_checksum()) {}

    // Since we are exposing a reference to _full_checksum, we delete the move
    // constructor.  If it is moved, the reference will refer to the old
    // location.
    checksummed_file_writer(checksummed_file_writer&&) = delete;
    checksummed_file_writer(const checksummed_file_writer&) = default;

    checksum& finalize_checksum() {
        return _c;
    }
    uint32_t full_checksum() {
        return _full_checksum;
    }
};

using adler32_checksummed_file_writer = checksummed_file_writer<adler32_utils>;
using crc32_checksummed_file_writer = checksummed_file_writer<crc32_utils>;

template <typename T, typename W>
GCC6_CONCEPT(requires Writer<W>())
inline void write_vint_impl(W& out, T value) {
    using vint_type = std::conditional_t<std::is_unsigned_v<T>, unsigned_vint, signed_vint>;
    std::array<bytes::value_type, max_vint_length> encoding_buffer;
    const auto size = vint_type::serialize(value, encoding_buffer.begin());
    out.write(reinterpret_cast<const char*>(encoding_buffer.data()), size);
}

template <typename W>
GCC6_CONCEPT(requires Writer<W>())
void write_unsigned_vint(W& out, uint64_t value) {
    return write_vint_impl(out, value);
}

template <typename W>
GCC6_CONCEPT(requires Writer<W>())
void write_signed_vint(W& out, int64_t value) {
    return write_vint_impl(out, value);
}

template <typename T, typename W>
GCC6_CONCEPT(requires Writer<W>())
typename std::enable_if_t<!std::is_integral_v<T>>
write_vint(W& out, T t) = delete;

template <typename T, typename W>
GCC6_CONCEPT(requires Writer<W>())
inline void write_vint(W& out, T value) {
    static_assert(std::is_integral_v<T>, "Non-integral values can't be written using write_vint");
    return std::is_unsigned_v<T> ? write_unsigned_vint(out, value) : write_signed_vint(out, value);
}


template <typename T, typename W>
GCC6_CONCEPT(requires Writer<W>())
inline typename std::enable_if_t<std::is_integral<T>::value, void>
write(sstable_version_types v, W& out, T i) {
    auto *nr = reinterpret_cast<const net::packed<T> *>(&i);
    i = net::hton(*nr);
    auto p = reinterpret_cast<const char*>(&i);
    out.write(p, sizeof(T));
}

template <typename T, typename W>
GCC6_CONCEPT(requires Writer<W>())
inline typename std::enable_if_t<std::is_enum<T>::value, void>
write(sstable_version_types v, W& out, T i) {
    write(v, out, static_cast<typename std::underlying_type<T>::type>(i));
}


template <typename W>
GCC6_CONCEPT(requires Writer<W>())
inline void write(sstable_version_types v, W& out, bool i) {
    write(v, out, static_cast<uint8_t>(i));
}

inline void write(sstable_version_types v, file_writer& out, double d) {
    auto *nr = reinterpret_cast<const net::packed<unsigned long> *>(&d);
    auto tmp = net::hton(*nr);
    auto p = reinterpret_cast<const char*>(&tmp);
    out.write(p, sizeof(unsigned long));
}


template <typename W>
GCC6_CONCEPT(requires Writer<W>())
inline void write(sstable_version_types v, W& out, const bytes& s) {
    out.write(s);
}

template <typename W>
GCC6_CONCEPT(requires Writer<W>())
inline void write(sstable_version_types v, W& out, bytes_view s) {
    out.write(reinterpret_cast<const char*>(s.data()), s.size());
}

inline void write(sstable_version_types v, file_writer& out, bytes_ostream s) {
    for (bytes_view fragment : s) {
        write(v, out, fragment);
    }
}


template<typename W, typename First, typename Second, typename... Rest>
GCC6_CONCEPT(requires Writer<W>())
inline void write(sstable_version_types v, W& out, const First& first, const Second& second, Rest&&... rest) {
    write(v, out, first);
    write(v, out, second, std::forward<Rest>(rest)...);
}

template <class T, typename W>
GCC6_CONCEPT(requires Writer<W>())
inline void write(sstable_version_types v, W& out, const vint<T>& t) {
    write_vint(out, t.value);
}

template <class T, typename W>
GCC6_CONCEPT(requires Writer<W>())
typename std::enable_if_t<!std::is_integral<T>::value && !std::is_enum<T>::value, void>
write(sstable_version_types v, W& out, const T& t) {
    // describe_type() is not const correct, so cheat here:
    const_cast<T&>(t).describe_type(v, [v, &out] (auto&&... what) -> void {
        write(v, out, std::forward<decltype(what)>(what)...);
    });
}

template <typename T, typename U>
void check_truncate_and_assign(T& to, const U from) {
    static_assert(std::is_integral<T>::value && std::is_integral<U>::value, "T and U must be integral");
    to = from;
    if (to != from) {
        throw std::overflow_error("assigning U to T caused an overflow");
    }
}

template <typename Size>
void write(sstable_version_types v, file_writer& out, const disk_string<Size>& s) {
    Size len = 0;
    check_truncate_and_assign(len, s.value.size());
    write(v, out, len);
    write(v, out, s.value);
}

inline void write(sstable_version_types v, file_writer& out, const disk_string_vint_size& s) {
    uint64_t len = 0;
    check_truncate_and_assign(len, s.value.size());
    write_vint(out, len);
    write(v, out, s.value);
}

template <typename Size>
inline void write(sstable_version_types v, file_writer& out, const disk_string_view<Size>& s) {
    Size len;
    check_truncate_and_assign(len, s.value.size());
    write(v, out, len, s.value);
}

template<typename SizeType>
inline void write(sstable_version_types ver, file_writer& out, const disk_data_value_view<SizeType>& v) {
    SizeType length;
    check_truncate_and_assign(length, v.value.size_bytes());
    write(ver, out, length);
    using boost::range::for_each;
    for_each(v.value, [&] (bytes_view fragment) { write(ver, out, fragment); });
}

template <typename Members>
inline typename std::enable_if_t<!std::is_integral<Members>::value, void>
write(sstable_version_types v, file_writer& out, const utils::chunked_vector<Members>& arr) {
    for (auto& a : arr) {
        write(v, out, a);
    }
}

template <typename Members>
inline typename std::enable_if_t<std::is_integral<Members>::value, void>
write(sstable_version_types v, file_writer& out, const utils::chunked_vector<Members>& arr) {
    std::vector<Members> tmp;
    size_t per_loop = 100000 / sizeof(Members);
    tmp.resize(per_loop);
    size_t idx = 0;
    while (idx != arr.size()) {
        auto now = std::min(arr.size() - idx, per_loop);
        // copy arr into tmp converting each entry into big-endian representation.
        auto nr = arr.begin() + idx;
        for (size_t i = 0; i < now; i++) {
            tmp[i] = net::hton(nr[i]);
        }
        auto p = reinterpret_cast<const char*>(tmp.data());
        auto bytes = now * sizeof(Members);
        out.write(p, bytes);
        idx += now;
    }
}

template <typename Size, typename Members>
inline void write(sstable_version_types v, file_writer& out, const disk_array<Size, Members>& arr) {
    Size len = 0;
    check_truncate_and_assign(len, arr.elements.size());
    write(v, out, len);
    write(v, out, arr.elements);
}

template <typename Members>
inline void write(sstable_version_types v, file_writer& out, const disk_array_vint_size<Members>& arr) {
    uint64_t len = 0;
    check_truncate_and_assign(len, arr.elements.size());
    write_vint(out, len);
    write(v, out, arr.elements);
}

template <typename Size, typename Members>
inline void write(sstable_version_types v, file_writer& out, const disk_array_ref<Size, Members>& arr) {
    Size len = 0;
    check_truncate_and_assign(len, arr.elements.size());
    write(v, out, len);
    write(v, out, arr.elements);
}

template <typename Key, typename Value>
inline void write(sstable_version_types v, file_writer& out, const std::unordered_map<Key, Value>& map) {
    for (auto& val: map) {
        write(v, out, val.first, val.second);
    };
}

template <typename First, typename Second>
inline void write(sstable_version_types v, file_writer& out, const std::pair<First, Second>& val) {
    write(v, out, val.first, val.second);
}

template <typename Size, typename Key, typename Value>
inline void write(sstable_version_types v, file_writer& out, const disk_hash<Size, Key, Value>& h) {
    Size len = 0;
    check_truncate_and_assign(len, h.map.size());
    write(v, out, len);
    write(v, out, h.map);
}

class bytes_writer_for_column_name {
    bytes _buf;
    bytes::iterator _pos;
public:
    void prepare(size_t size) {
        _buf = bytes(bytes::initialized_later(), size);
        _pos = _buf.begin();
    }

    template<typename... Args>
    void write(Args&&... args) {
        auto write_one = [this] (bytes_view data) {
            _pos = std::copy(data.begin(), data.end(), _pos);
        };
        auto ignore = { (write_one(bytes_view(args)), 0)... };
        (void)ignore;
    }

    bytes&& release() && {
        return std::move(_buf);
    }
};

class file_writer_for_column_name {
    sstable_version_types _v;
    file_writer& _fw;
public:
    file_writer_for_column_name(sstable_version_types v, file_writer& fw) : _v(v), _fw(fw) { }

    void prepare(uint16_t size) {
        sstables::write(_v, _fw, size);
    }

    template<typename... Args>
    void write(Args&&... args) {
        sstables::write(_v, _fw, std::forward<Args>(args)...);
    }
};

template<typename Writer>
void write_compound_non_dense_column_name(sstable_version_types v, Writer& out, const composite& clustering_key, const std::vector<bytes_view>& column_names, composite::eoc marker = composite::eoc::none) {
    // was defined in the schema, for example.
    auto c = composite::from_exploded(column_names, true, marker);
    auto ck_bview = bytes_view(clustering_key);

    // The marker is not a component, so if the last component is empty (IOW,
    // only serializes to the marker), then we just replace the key's last byte
    // with the marker. If the component however it is not empty, then the
    // marker should be in the end of it, and we just join them together as we
    // do for any normal component
    if (c.size() == 1) {
        ck_bview.remove_suffix(1);
    }
    size_t sz = ck_bview.size() + c.size();
    if (sz > std::numeric_limits<uint16_t>::max()) {
        throw std::runtime_error(sprint("Column name too large (%d > %d)", sz, std::numeric_limits<uint16_t>::max()));
    }
    out.prepare(uint16_t(sz));
    out.write(ck_bview, c);
}

inline void write_compound_non_dense_column_name(sstable_version_types v, file_writer& out, const composite& clustering_key, const std::vector<bytes_view>& column_names, composite::eoc marker = composite::eoc::none) {
    auto w = file_writer_for_column_name(v, out);
    write_compound_non_dense_column_name(v, w, clustering_key, column_names, marker);
}

template<typename Writer>
void write_column_name(sstable_version_types v, Writer& out, bytes_view column_names) {
    size_t sz = column_names.size();
    if (sz > std::numeric_limits<uint16_t>::max()) {
        throw std::runtime_error(sprint("Column name too large (%d > %d)", sz, std::numeric_limits<uint16_t>::max()));
    }
    out.prepare(uint16_t(sz));
    out.write(column_names);
}

inline void write_column_name(sstable_version_types v, file_writer& out, bytes_view column_names) {
    auto w = file_writer_for_column_name(v, out);
    write_column_name(v, w, column_names);
}

template<typename Writer>
void write_column_name(sstable_version_types v, Writer& out, const schema& s, const composite& clustering_element, const std::vector<bytes_view>& column_names, composite::eoc marker = composite::eoc::none) {
    if (s.is_dense()) {
        write_column_name(v, out, bytes_view(clustering_element));
    } else if (s.is_compound()) {
        write_compound_non_dense_column_name(v, out, clustering_element, column_names, marker);
    } else {
        write_column_name(v, out, column_names[0]);
    }
}

template <typename W>
GCC6_CONCEPT(requires Writer<W>())
void write_cell_value(W& out, const abstract_type& type, bytes_view value) {
    if (!value.empty()) {
        if (type.value_length_if_fixed()) {
            write(sstable_version_types::mc, out, value);
        } else {
            write_vint(out, value.size());
            write(sstable_version_types::mc, out, value);
        }
    }
}

template <typename W>
GCC6_CONCEPT(requires Writer<W>())
void write_cell_value(W& out, const abstract_type& type, atomic_cell_value_view value) {
    if (!value.empty()) {
        if (!type.value_length_if_fixed()) {
            write_vint(out, value.size_bytes());
        }
        using boost::range::for_each;
        for_each(value, [&] (bytes_view fragment) { write(sstable_version_types::mc, out, fragment); });
    }
}

template <typename WriteLengthFunc, typename W>
GCC6_CONCEPT(requires Writer<W>())
void write_counter_value(counter_cell_view ccv, W& out, sstable_version_types v, WriteLengthFunc&& write_len_func) {
    auto shard_count = ccv.shard_count();
    static constexpr auto header_entry_size = sizeof(int16_t);
    static constexpr auto counter_shard_size = 32u; // counter_id: 16 + clock: 8 + value: 8
    auto total_size = sizeof(int16_t) + shard_count * (header_entry_size + counter_shard_size);

    write_len_func(out, uint32_t(total_size));
    write(v, out, int16_t(shard_count));
    for (auto i = 0u; i < shard_count; i++) {
        write<int16_t>(v, out, std::numeric_limits<int16_t>::min() + i);
    }
    auto write_shard = [&] (auto&& s) {
        auto uuid = s.id().to_uuid();
        write(v, out, int64_t(uuid.get_most_significant_bits()),
            int64_t(uuid.get_least_significant_bits()),
            int64_t(s.logical_clock()), int64_t(s.value()));
    };
    if (service::get_local_storage_service().cluster_supports_correct_counter_order()) {
        for (auto&& s : ccv.shards()) {
            write_shard(s);
        }
    } else {
        for (auto&& s : ccv.shards_compatible_with_1_7_4()) {
            write_shard(s);
        }
    }
}

void maybe_add_summary_entry(summary&, const dht::token&, bytes_view key, uint64_t data_offset,
    uint64_t index_offset, index_sampling_state&);

// Get the currently loaded configuration, or the default configuration in
// case none has been loaded (this happens, for example, in unit tests).
const db::config& get_config();

// Returns the cost for writing a byte to summary such that the ratio of summary
// to data will be 1 to cost by the time sstable is sealed.
size_t summary_byte_cost();

void prepare_summary(summary& s, uint64_t expected_partition_count, uint32_t min_index_interval);

void seal_summary(summary& s,
    std::experimental::optional<key>&& first_key,
    std::experimental::optional<key>&& last_key,
    const index_sampling_state& state);

void seal_statistics(sstable_version_types, statistics&, metadata_collector&,
    const sstring partitioner, double bloom_filter_fp_chance, schema_ptr,
    const dht::decorated_key& first_key, const dht::decorated_key& last_key, encoding_stats enc_stats = {});

void write(sstable_version_types, file_writer&, const utils::estimated_histogram&);
void write(sstable_version_types, file_writer&, const utils::streaming_histogram&);
void write(sstable_version_types, file_writer&, const commitlog_interval&);
void write(sstable_version_types, file_writer&, const compression&);

}
