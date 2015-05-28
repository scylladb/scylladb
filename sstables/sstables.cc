/*
 * Copyright 2015 Cloudius Systems
 */

#include "log.hh"
#include <vector>
#include <typeinfo>
#include <limits>
#include "core/future.hh"
#include "core/future-util.hh"
#include "core/sstring.hh"
#include "core/fstream.hh"
#include "core/shared_ptr.hh"
#include "core/do_with.hh"
#include <iterator>

#include "types.hh"
#include "sstables.hh"
#include "compress.hh"
#include <boost/algorithm/string.hpp>

namespace sstables {

class random_access_reader {
    input_stream<char> _in;
protected:
    virtual input_stream<char> open_at(uint64_t pos) = 0;
public:
    future<temporary_buffer<char>> read_exactly(size_t n) {
        return _in.read_exactly(n);
    }
    void seek(uint64_t pos) {
        _in = open_at(pos);
    }
    bool eof() { return _in.eof(); }
    virtual ~random_access_reader() { }
};

class file_random_access_reader : public random_access_reader {
    lw_shared_ptr<file> _file;
    size_t _buffer_size;
public:
    virtual input_stream<char> open_at(uint64_t pos) override {
        return make_file_input_stream(_file, pos, _buffer_size);
    }
    explicit file_random_access_reader(file&& f, size_t buffer_size = 8192)
        : file_random_access_reader(make_lw_shared<file>(std::move(f)), buffer_size) {}

    explicit file_random_access_reader(lw_shared_ptr<file> f, size_t buffer_size = 8192)
        : _file(f), _buffer_size(buffer_size)
    {
        seek(0);
    }
};

class file_writer {
    output_stream<char> _out;
    size_t _offset = 0;
public:
    file_writer(lw_shared_ptr<file> f, size_t buffer_size = 8192)
        : _out(make_file_output_stream(std::move(f), buffer_size)) {}

    virtual future<> write(const char* buf, size_t n) {
        _offset += n;
        return _out.write(buf, n);
    }
    virtual future<> write(const bytes& s) {
        _offset += s.size();
        return _out.write(s);
    }
    future<> flush() {
        return _out.flush();
    }
    future<> close() {
        return _out.close();
    }
    size_t offset() {
        return _offset;
    }

    friend class checksummed_file_writer;
};

class checksummed_file_writer : public file_writer {
    checksum _c;
    uint32_t _per_chunk_checksum;
    uint32_t _full_checksum;
    bool _checksum_file;
private:
    void close_checksum() {
        if (!_checksum_file) {
            return;
        }
        if ((_offset % _c.chunk_size) != 0) {
            _c.checksums.push_back(_per_chunk_checksum);
        }
    }

    // NOTE: adler32 is the algorithm used to compute checksum.
    void do_compute_checksum(const char* buf, size_t n) {
        uint32_t buf_checksum = checksum_adler32(buf, n);

        _offset += n;
        if (_checksum_file) {
            _per_chunk_checksum = checksum_adler32_combine(_per_chunk_checksum, buf_checksum, n);
        }
        _full_checksum = checksum_adler32_combine(_full_checksum, buf_checksum, n);
    }

    // compute a checksum per chunk of size _c.chunk_size
    void compute_checksum(const char* buf, size_t n) {
        if (!_checksum_file) {
            do_compute_checksum(buf, n);
            return;
        }

        size_t remaining = n;
        while (remaining) {
            // available means available space in the current chunk.
            size_t available = _c.chunk_size - (_offset % _c.chunk_size);

            if (remaining < available) {
                do_compute_checksum(buf, remaining);
                remaining = 0;
            } else {
                do_compute_checksum(buf, available);
                _c.checksums.push_back(_per_chunk_checksum);
                _per_chunk_checksum = init_checksum_adler32();
                buf += available;
                remaining -= available;
            }
        }
    }
public:
    checksummed_file_writer(lw_shared_ptr<file> f, size_t buffer_size = 8192, bool checksum_file = false)
            : file_writer(std::move(f), buffer_size) {
        _checksum_file = checksum_file;
        _c.chunk_size = DEFAULT_CHUNK_SIZE;
        _per_chunk_checksum = init_checksum_adler32();
        _full_checksum = init_checksum_adler32();
    }

    virtual future<> write(const char* buf, size_t n) {
        compute_checksum(buf, n);
        return _out.write(buf, n);
    }
    virtual future<> write(const bytes& s) {
        compute_checksum(reinterpret_cast<const char*>(s.c_str()), s.size());
        return _out.write(s);
    }
    checksum& finalize_checksum() {
        close_checksum();
        return _c;
    }
    uint32_t full_checksum() {
        return _full_checksum;
    }
};

// FIXME: We don't use this API, and it can be removed. The compressed reader
// is only needed for the data file, and for that we have the nicer
// data_stream_at() API below.
class compressed_file_random_access_reader : public random_access_reader {
    lw_shared_ptr<file> _file;
    sstables::compression* _cm;
public:
    explicit compressed_file_random_access_reader(
                lw_shared_ptr<file> f, sstables::compression* cm)
        : _file(std::move(f))
        , _cm(cm)
    {
        seek(0);
    }
    compressed_file_random_access_reader(compressed_file_random_access_reader&&) = default;
    virtual input_stream<char> open_at(uint64_t pos) override {
        return make_compressed_file_input_stream(_file, _cm, pos);
    }

};

thread_local logging::logger sstlog("sstable");

std::unordered_map<sstable::version_types, sstring, enum_hash<sstable::version_types>> sstable::_version_string = {
    { sstable::version_types::la , "la" }
};

std::unordered_map<sstable::format_types, sstring, enum_hash<sstable::format_types>> sstable::_format_string = {
    { sstable::format_types::big , "big" }
};

std::unordered_map<sstable::component_type, sstring, enum_hash<sstable::component_type>> sstable::_component_map = {
    { component_type::Index, "Index.db"},
    { component_type::CompressionInfo, "CompressionInfo.db" },
    { component_type::Data, "Data.db" },
    { component_type::TOC, "TOC.txt" },
    { component_type::Summary, "Summary.db" },
    { component_type::Digest, "Digest.sha1" },
    { component_type::CRC, "CRC.db" },
    { component_type::Filter, "Filter.db" },
    { component_type::Statistics, "Statistics.db" },
};

// This assumes that the mappings are small enough, and called unfrequent
// enough.  If that changes, it would be adviseable to create a full static
// reverse mapping, even if it is done at runtime.
template <typename Map>
static typename Map::key_type reverse_map(const typename Map::mapped_type& value, Map& map) {
    for (auto& pair: map) {
        if (pair.second == value) {
            return pair.first;
        }
    }
    throw std::out_of_range("unable to reverse map");
}

struct bufsize_mismatch_exception : malformed_sstable_exception {
    bufsize_mismatch_exception(size_t size, size_t expected) :
        malformed_sstable_exception(sprint("Buffer improperly sized to hold requested data. Got: %ld. Expected: %ld", size, expected))
    {}
};

// This should be used every time we use read_exactly directly.
//
// read_exactly is a lot more convenient of an interface to use, because we'll
// be parsing known quantities.
//
// However, anything other than the size we have asked for, is certainly a bug,
// and we need to do something about it.
static void check_buf_size(temporary_buffer<char>& buf, size_t expected) {
    if (buf.size() < expected) {
        throw bufsize_mismatch_exception(buf.size(), expected);
    }
}

template <typename T, typename U>
static void check_truncate_and_assign(T& to, const U from) {
    static_assert(std::is_integral<T>::value && std::is_integral<U>::value, "T and U must be integral");
    to = from;
    if (to != from) {
        throw std::overflow_error("assigning U to T caused an overflow");
    }
}

// Base parser, parses an integer type
template <typename T>
typename std::enable_if_t<std::is_integral<T>::value, void>
read_integer(temporary_buffer<char>& buf, T& i) {
    auto *nr = reinterpret_cast<const net::packed<T> *>(buf.get());
    i = net::ntoh(*nr);
}

template <typename T>
typename std::enable_if_t<std::is_integral<T>::value, future<>>
parse(random_access_reader& in, T& i) {
    return in.read_exactly(sizeof(T)).then([&i] (auto buf) {
        check_buf_size(buf, sizeof(T));

        read_integer(buf, i);
        return make_ready_future<>();
    });
}

template <typename T>
typename std::enable_if_t<std::is_integral<T>::value, future<>>
write(file_writer& out, T i) {
    auto *nr = reinterpret_cast<const net::packed<T> *>(&i);
    i = net::hton(*nr);
    auto p = reinterpret_cast<const char*>(&i);
    return out.write(p, sizeof(T)).then([&out] (...) -> future<> {
        // TODO: handle result
        return make_ready_future<>();
    });
}

template <typename T>
typename std::enable_if_t<std::is_enum<T>::value, future<>>
parse(random_access_reader& in, T& i) {
    return parse(in, reinterpret_cast<typename std::underlying_type<T>::type&>(i));
}

template <typename T>
typename std::enable_if_t<std::is_enum<T>::value, future<>>
write(file_writer& out, T i) {
    return write(out, static_cast<typename std::underlying_type<T>::type>(i));
}

future<> parse(random_access_reader& in, bool& i) {
    return parse(in, reinterpret_cast<uint8_t&>(i));
}

future<> write(file_writer& out, bool i) {
    return write(out, static_cast<uint8_t>(i));
}

template <typename To, typename From>
static inline To convert(From f) {
    static_assert(sizeof(To) == sizeof(From), "Sizes must match");
    union {
        To to;
        From from;
    } conv;

    conv.from = f;
    return conv.to;
}

future<> parse(random_access_reader& in, double& d) {
    return in.read_exactly(sizeof(double)).then([&d] (auto buf) {
        check_buf_size(buf, sizeof(double));

        auto *nr = reinterpret_cast<const net::packed<unsigned long> *>(buf.get());
        d = convert<double>(net::ntoh(*nr));
        return make_ready_future<>();
    });
}

future<> write(file_writer& out, double d) {
    auto *nr = reinterpret_cast<const net::packed<unsigned long> *>(&d);
    auto tmp = net::hton(*nr);
    auto p = reinterpret_cast<const char*>(&tmp);
    return out.write(p, sizeof(unsigned long)).then([&out] (...) -> future<> {
        // TODO: handle result
        return make_ready_future<>();
    });
}

template <typename T>
future<> parse(random_access_reader& in, T& len, bytes& s) {
    return in.read_exactly(len).then([&s, len] (auto buf) {
        check_buf_size(buf, len);
        // Likely a different type of char. Most bufs are unsigned, whereas the bytes type is signed.
        s = bytes(reinterpret_cast<const bytes::value_type *>(buf.get()), len);
    });
}

future<> write(file_writer& out, bytes& s) {
    return out.write(s).then([&out, &s] (...) -> future<> {
        // TODO: handle result
        return make_ready_future<>();
    });
}

future<> write(file_writer& out, bytes_view s) {
    return out.write(reinterpret_cast<const char*>(s.data()), s.size());
}

// All composite parsers must come after this
template<typename First, typename... Rest>
future<> parse(random_access_reader& in, First& first, Rest&&... rest) {
    return parse(in, first).then([&in, &rest...] {
        return parse(in, std::forward<Rest>(rest)...);
    });
}

template<typename First, typename... Rest>
future<> write(file_writer& out, First& first, Rest&&... rest) {
    return write(out, first).then([&out, &rest...] {
        return write(out, rest...);
    });
}

// Intended to be used for a type that describes itself through describe_type().
template <class T>
typename std::enable_if_t<!std::is_integral<T>::value && !std::is_enum<T>::value, future<>>
parse(random_access_reader& in, T& t) {
    return t.describe_type([&in] (auto&&... what) -> future<> {
        return parse(in, what...);
    });
}

template <class T>
typename std::enable_if_t<!std::is_integral<T>::value && !std::is_enum<T>::value, future<>>
write(file_writer& out, T& t) {
    return t.describe_type([&out] (auto&&... what) -> future<> {
        return write(out, what...);
    });
}

// For all types that take a size, we provide a template that takes the type
// alone, and another, separate one, that takes a size parameter as well, of
// type Size. This is because although most of the time the size and the data
// are contiguous, it is not always the case. So we want to have the
// flexibility of parsing them separately.
template <typename Size>
future<> parse(random_access_reader& in, disk_string<Size>& s) {
    auto len = std::make_unique<Size>();
    auto f = parse(in, *len);
    return f.then([&in, &s, len = std::move(len)] {
        return parse(in, *len, s.value);
    });
}

template <typename Size>
future<> write(file_writer& out, disk_string<Size>& s) {
    Size len = 0;
    check_truncate_and_assign(len, s.value.size());
    return write(out, len).then([&out, &s] {
        return write(out, s.value);
    });
}

template <typename Size>
future<> write(file_writer& out, disk_string_view<Size>& s) {
    Size len;
    check_truncate_and_assign(len, s.value.size());
    return write(out, len, s.value);
}

// We cannot simply read the whole array at once, because we don't know its
// full size. We know the number of elements, but if we are talking about
// disk_strings, for instance, we have no idea how much of the stream each
// element will take.
//
// Sometimes we do know the size, like the case of integers. There, all we have
// to do is to convert each member because they are all stored big endian.
// We'll offer a specialization for that case below.
template <typename Size, typename Members>
typename std::enable_if_t<!std::is_integral<Members>::value, future<>>
parse(random_access_reader& in, Size& len, std::vector<Members>& arr) {

    auto count = make_lw_shared<size_t>(0);
    auto eoarr = [count, len] { return *count == len; };

    return do_until(eoarr, [count, &in, &arr] {
        return parse(in, arr[(*count)++]);
    });
}

template <typename Size, typename Members>
typename std::enable_if_t<std::is_integral<Members>::value, future<>>
parse(random_access_reader& in, Size& len, std::vector<Members>& arr) {
    return in.read_exactly(len * sizeof(Members)).then([&arr, len] (auto buf) {
        check_buf_size(buf, len * sizeof(Members));

        auto *nr = reinterpret_cast<const net::packed<Members> *>(buf.get());
        for (size_t i = 0; i < len; ++i) {
            arr[i] = net::ntoh(nr[i]);
        }
        return make_ready_future<>();
    });
}

// We resize the array here, before we pass it to the integer / non-integer
// specializations
template <typename Size, typename Members>
future<> parse(random_access_reader& in, disk_array<Size, Members>& arr) {
    auto len = std::make_unique<Size>();
    auto f = parse(in, *len);
    return f.then([&in, &arr, len = std::move(len)] {
        arr.elements.resize(*len);
        return parse(in, *len, arr.elements);
    });
}

template <typename Members>
typename std::enable_if_t<!std::is_integral<Members>::value, future<>>
write(file_writer& out, std::vector<Members>& arr) {

    auto count = make_lw_shared<size_t>(0);
    auto eoarr = [count, &arr] { return *count == arr.size(); };

    return do_until(eoarr, [count, &out, &arr] {
        return write(out, arr[(*count)++]);
    });
}

template <typename Members>
typename std::enable_if_t<std::is_integral<Members>::value, future<>>
write(file_writer& out, std::vector<Members>& arr) {
    return do_with(std::vector<Members>(), [&out, &arr] (auto& tmp) {
        tmp.resize(arr.size());
        // copy arr into tmp converting each entry into big-endian representation.
        auto *nr = reinterpret_cast<const net::packed<Members> *>(arr.data());
        for (size_t i = 0; i < arr.size(); i++) {
            tmp[i] = net::hton(nr[i]);
        }
        auto p = reinterpret_cast<const char*>(tmp.data());
        auto bytes = tmp.size() * sizeof(Members);
        return out.write(p, bytes).then([&out] (...) -> future<> {
            return make_ready_future<>();
        });
    });
}

template <typename Size, typename Members>
future<> write(file_writer& out, disk_array<Size, Members>& arr) {
    Size len = 0;
    check_truncate_and_assign(len, arr.elements.size());
    return write(out, len).then([&out, &arr] {
        return write(out, arr.elements);
    });
}

template <typename Size, typename Key, typename Value>
future<> parse(random_access_reader& in, Size& len, std::unordered_map<Key, Value>& map) {
    auto count = make_lw_shared<Size>();
    auto eos = [len, count] { return len == *count; };
    return do_until(eos, [len, count, &in, &map] {
        struct kv {
            Key key;
            Value value;
        };
        ++*count;

        auto el = std::make_unique<kv>();
        auto f = parse(in, el->key, el->value);
        return f.then([el = std::move(el), &map] {
            map.emplace(el->key, el->value);
        });
    });
}

template <typename Size, typename Key, typename Value>
future<> parse(random_access_reader& in, disk_hash<Size, Key, Value>& h) {
    auto w = std::make_unique<Size>();
    auto f = parse(in, *w);
    return f.then([&in, &h, w = std::move(w)] {
        return parse(in, *w, h.map);
    });
}

template <typename Key, typename Value>
future<> write(file_writer& out, std::unordered_map<Key, Value>& map) {
    return do_for_each(map.begin(), map.end(), [&out, &map] (auto& val) {
        Key key = val.first;
        Value value = val.second;
        return write(out, key, value);
    });
}

template <typename Size, typename Key, typename Value>
future<> write(file_writer& out, disk_hash<Size, Key, Value>& h) {
    Size len = 0;
    check_truncate_and_assign(len, h.map.size());
    return write(out, len).then([&out, &h] {
        return write(out, h.map);
    });
}

future<> parse(random_access_reader& in, summary& s) {
    using pos_type = typename decltype(summary::positions)::value_type;

    return parse(in, s.header.min_index_interval,
                     s.header.size,
                     s.header.memory_size,
                     s.header.sampling_level,
                     s.header.size_at_full_sampling).then([&in, &s] {
        return in.read_exactly(s.header.size * sizeof(pos_type)).then([&in, &s] (auto buf) {
            auto len = s.header.size * sizeof(pos_type);
            check_buf_size(buf, len);

            s.entries.resize(s.header.size);

            auto *nr = reinterpret_cast<const pos_type *>(buf.get());
            s.positions = std::vector<pos_type>(nr, nr + s.header.size);

            // Since the keys in the index are not sized, we need to calculate
            // the start position of the index i+1 to determine the boundaries
            // of index i. The "memory_size" field in the header determines the
            // total memory used by the map, so if we push it to the vector, we
            // can guarantee that no conditionals are used, and we can always
            // query the position of the "next" index.
            s.positions.push_back(s.header.memory_size);
        }).then([&in, &s] {
            in.seek(sizeof(summary::header) + s.header.memory_size);
            return parse(in, s.first_key, s.last_key);
        }).then([&in, &s] {

            in.seek(s.positions[0] + sizeof(summary::header));

            assert(s.positions.size() == (s.entries.size() + 1));

            auto idx = make_lw_shared<size_t>(0);
            return do_for_each(s.entries.begin(), s.entries.end(), [idx, &in, &s] (auto& entry) {
                auto pos = s.positions[(*idx)++];
                auto next = s.positions[*idx];

                auto entrysize = next - pos;

                return in.read_exactly(entrysize).then([&entry, entrysize] (auto buf) {
                    check_buf_size(buf, entrysize);

                    auto keysize = entrysize - 8;
                    entry.key = bytes(reinterpret_cast<const int8_t*>(buf.get()), keysize);
                    buf.trim_front(keysize);
                    // FIXME: This is a le read. We should make this explicit
                    entry.position = *(reinterpret_cast<const net::packed<uint64_t> *>(buf.get()));

                    return make_ready_future<>();
                });
            }).then([&s] {
                // Delete last element which isn't part of the on-disk format.
                s.positions.pop_back();
            });
        });
    });
}

future<> write(file_writer& out, summary_entry& entry)
{
    // FIXME: summary entry is supposedly written in memory order, but that
    // would prevent portability of summary file between machines of different
    // endianness. We can treat it as little endian to preserve portability.
    return write(out, entry.key).then([&out, &entry] {
        auto p = reinterpret_cast<const char*>(&entry.position);
        return out.write(p, sizeof(uint64_t));
    });
}

future<> write(file_writer& out, summary& s) {
    using pos_type = typename decltype(summary::positions)::value_type;

    // NOTE: positions and entries must be stored in NATIVE BYTE ORDER, not BIG-ENDIAN.
    return write(out, s.header.min_index_interval,
                      s.header.size,
                      s.header.memory_size,
                      s.header.sampling_level,
                      s.header.size_at_full_sampling).then([&out, &s] {
        auto p = reinterpret_cast<const char*>(s.positions.data());
        return out.write(p, sizeof(pos_type) * s.positions.size());
    }).then([&out, &s] {
        return write(out, s.entries);
    }).then([&out, &s] {
        return write(out, s.first_key, s.last_key);
    });
}

future<summary_entry&> sstable::read_summary_entry(size_t i) {
    // The last one is the boundary marker
    if (i >= (_summary.entries.size())) {
        throw std::out_of_range(sprint("Invalid Summary index: %ld", i));
    }

    return make_ready_future<summary_entry&>(_summary.entries[i]);
}

future<> parse(random_access_reader& in, index_entry& ie) {
    return parse(in, ie.key, ie.position, ie.promoted_index);
}

future<> parse(random_access_reader& in, deletion_time& d) {
    return parse(in, d.local_deletion_time, d.marked_for_delete_at);
}

template <typename Child>
future<> parse(random_access_reader& in, std::unique_ptr<metadata>& p) {
    p.reset(new Child);
    return parse(in, *static_cast<Child *>(p.get()));
}

template <typename Child>
future<> write(file_writer& out, std::unique_ptr<metadata>& p) {
    return write(out, *static_cast<Child *>(p.get()));
}

future<> parse(random_access_reader& in, statistics& s) {
    return parse(in, s.hash).then([&in, &s] {
        return do_for_each(s.hash.map.begin(), s.hash.map.end(), [&in, &s] (auto val) mutable {
            in.seek(val.second);

            switch (val.first) {
                case metadata_type::Validation:
                    return parse<validation_metadata>(in, s.contents[val.first]);
                case metadata_type::Compaction:
                    return parse<compaction_metadata>(in, s.contents[val.first]);
                case metadata_type::Stats:
                    return parse<stats_metadata>(in, s.contents[val.first]);
                default:
                    sstlog.warn("Invalid metadata type at Statistics file: {} ", int(val.first));
                    return make_ready_future<>();
                }
        });
    });
}

future<> write(file_writer& out, statistics& s) {
    return write(out, s.hash).then([&out, &s] {
        struct kv {
            metadata_type key;
            uint32_t value;
        };
        // sort map by file offset value and store the result into a vector.
        // this is indeed needed because output stream cannot afford random writes.
        auto v = make_shared<std::vector<kv>>();
        v->reserve(s.hash.map.size());
        for (auto val : s.hash.map) {
            kv tmp = { val.first, val.second };
            v->push_back(tmp);
        }
        std::sort(v->begin(), v->end(), [] (kv i, kv j) { return i.value < j.value; });
        return do_for_each(v->begin(), v->end(), [&out, &s, v] (auto val) mutable {
            switch (val.key) {
                case metadata_type::Validation:
                    return write<validation_metadata>(out, s.contents[val.key]);
                case metadata_type::Compaction:
                    return write<compaction_metadata>(out, s.contents[val.key]);
                case metadata_type::Stats:
                    return write<stats_metadata>(out, s.contents[val.key]);
                default:
                    sstlog.warn("Invalid metadata type at Statistics file: {} ", int(val.key));
                    return make_ready_future<>();
                }
        });
    });
}

// This is small enough, and well-defined. Easier to just read it all
// at once
future<> sstable::read_toc() {
    auto file_path = filename(sstable::component_type::TOC);

    sstlog.debug("Reading TOC file {} ", file_path);

    return engine().open_file_dma(file_path, open_flags::ro).then([this] (file f) {
        auto bufptr = allocate_aligned_buffer<char>(4096, 4096);
        auto buf = bufptr.get();

        auto fut = f.dma_read(0, buf, 4096);
        return std::move(fut).then([this, f = std::move(f), bufptr = std::move(bufptr)] (size_t size) {
            // This file is supposed to be very small. Theoretically we should check its size,
            // but if we so much as read a whole page from it, there is definitely something fishy
            // going on - and this simplifies the code.
            if (size >= 4096) {
                throw malformed_sstable_exception("SSTable too big: " + to_sstring(size) + " bytes.");
            }

            std::experimental::string_view buf(bufptr.get(), size);
            std::vector<sstring> comps;

            boost::split(comps , buf, boost::is_any_of("\n"));

            for (auto& c: comps) {
                // accept trailing newlines
                if (c == "") {
                    continue;
                }
                try {
                   _components.insert(reverse_map(c, _component_map));
                } catch (std::out_of_range& oor) {
                    throw malformed_sstable_exception("Unrecognized TOC component: " + c);
                }
            }
            if (!_components.size()) {
                throw malformed_sstable_exception("Empty TOC");
            }
            return make_ready_future<>();
        });
    }).then_wrapped([file_path] (future<> f) {
        try {
            f.get();
        } catch (std::system_error& e) {
            if (e.code() == std::error_code(ENOENT, std::system_category())) {
                throw malformed_sstable_exception(file_path + ": file not found");
            }
        }
    });

}

future<> sstable::write_toc() {
    auto file_path = filename(sstable::component_type::TOC);

    sstlog.debug("Writing TOC file {} ", file_path);

    return engine().open_file_dma(file_path, open_flags::wo | open_flags::create | open_flags::truncate).then([this] (file f) {
        auto out = file_writer(make_lw_shared<file>(std::move(f)), 4096);
        auto w = make_shared<file_writer>(std::move(out));

        return do_for_each(_components, [this, w] (auto key) {
            // new line character is appended to the end of each component name.
            auto value = _component_map[key] + "\n";
            bytes b = bytes(reinterpret_cast<const bytes::value_type *>(value.c_str()), value.size());
            return write(*w, b);
        }).then([w] {
            return w->flush().then([w] {
                return w->close().then([w] {});
            });
        });
    });
}

future<> write_crc(const sstring file_path, checksum& c) {
    sstlog.debug("Writing CRC file {} ", file_path);

    auto oflags = open_flags::wo | open_flags::create | open_flags::exclusive;
    return engine().open_file_dma(file_path, oflags).then([&c] (file f) {
        auto out = file_writer(make_lw_shared<file>(std::move(f)), 4096);
        auto w = make_shared<file_writer>(std::move(out));

        return write(*w, c).then([w] {
            return w->close().then([w] {});
        });
    });
}

// Digest file stores the full checksum of data file converted into a string.
future<> write_digest(const sstring file_path, uint32_t full_checksum) {
    sstlog.debug("Writing Digest file {} ", file_path);

    auto oflags = open_flags::wo | open_flags::create | open_flags::exclusive;
    return engine().open_file_dma(file_path, oflags).then([full_checksum] (file f) {
        auto out = file_writer(make_lw_shared<file>(std::move(f)), 4096);
        auto w = make_shared<file_writer>(std::move(out));

        return do_with(to_sstring<bytes>(full_checksum), [w] (bytes& digest) {
            return write(*w, digest).then([w] {
                return w->close().then([w] {});
            });
        });
    });
}

future<index_list> sstable::read_indexes(uint64_t position, uint64_t quantity) {
    struct reader {
        uint64_t count = 0;
        std::vector<index_entry> indexes;
        file_random_access_reader stream;
        reader(lw_shared_ptr<file> f, uint64_t quantity) : stream(f) { indexes.reserve(quantity); }
    };

    auto r = make_lw_shared<reader>(_index_file, quantity);

    r->stream.seek(position);

    auto end = [r, quantity] { return r->count >= quantity; };

    return do_until(end, [this, r] {
        r->indexes.emplace_back();
        auto fut = parse(r->stream, r->indexes.back());
        return std::move(fut).then_wrapped([this, r] (future<> f) mutable {
            try {
               f.get();
               r->count++;
            } catch (bufsize_mismatch_exception &e) {
                // We have optimistically emplaced back one element of the
                // vector. If we have failed to parse, we should remove it
                // so size() gives us the right picture.
                r->indexes.pop_back();

                // FIXME: If the file ends at an index boundary, there is
                // no problem. Essentially, we can't know how many indexes
                // are in a sampling group, so there isn't really any way
                // to know, other than reading.
                //
                // If, however, we end in the middle of an index, this is a
                // corrupted file. This code is not perfect because we only
                // know that an exception happened, and it happened due to
                // eof. We don't really know if eof happened at the index
                // boundary.  To know that, we would have to keep track of
                // the real position of the stream (including what's
                // already in the buffer) before we start to read the
                // index, and after. We won't go through such complexity at
                // the moment.
                if (r->stream.eof()) {
                    r->count = std::numeric_limits<std::remove_reference<decltype(r->count)>::type>::max();
                } else {
                    throw e;
                }
            }
            return make_ready_future<>();
        });
    }).then([r] {
        return make_ready_future<index_list>(std::move(r->indexes));
    });
}

template <sstable::component_type Type, typename T>
future<> sstable::read_simple(T& component) {

    auto file_path = filename(Type);
    sstlog.debug(("Reading " + _component_map[Type] + " file {} ").c_str(), file_path);
    return engine().open_file_dma(file_path, open_flags::ro).then([this, &component] (file f) {

        auto r = std::make_unique<file_random_access_reader>(std::move(f), 4096);
        auto fut = parse(*r, component);
        return fut.then([r = std::move(r)] {});
    }).then_wrapped([this, file_path] (future<> f) {
        try {
            f.get();
        } catch (std::system_error& e) {
            if (e.code() == std::error_code(ENOENT, std::system_category())) {
                throw malformed_sstable_exception(file_path + ": file not found");
            }
        }
    });
}

template <sstable::component_type Type, typename T>
future<> sstable::write_simple(T& component) {
    auto file_path = filename(Type);
    sstlog.debug(("Writing " + _component_map[Type] + " file {} ").c_str(), file_path);
    return engine().open_file_dma(file_path, open_flags::wo | open_flags::create | open_flags::truncate).then([this, &component] (file f) {

        auto out = file_writer(make_lw_shared<file>(std::move(f)), 4096);
        auto w = make_shared<file_writer>(std::move(out));
        auto fut = write(*w, component);
        return fut.then([w] {
            return w->flush().then([w] {
                return w->close().then([w] {}); // the underlying file is synced here.
            });
        });
    }).then_wrapped([this, file_path] (future<> f) {
        try {
            f.get();
        } catch (std::system_error& e) {
            // TODO: handle exception.
        }
    });
}
template future<> sstable::read_simple<sstable::component_type::Filter>(sstables::filter& f);
template future<> sstable::write_simple<sstable::component_type::Filter>(sstables::filter& f);

future<> sstable::read_compression() {
     // FIXME: If there is no compression, we should expect a CRC file to be present.
    if (!has_component(sstable::component_type::CompressionInfo)) {
        return make_ready_future<>();
    }

    return read_simple<component_type::CompressionInfo>(_compression);
}

future<> sstable::write_compression() {
    if (!has_component(sstable::component_type::CompressionInfo)) {
        return make_ready_future<>();
    }

    return write_simple<component_type::CompressionInfo>(_compression);
}

future<> sstable::read_statistics() {
    return read_simple<component_type::Statistics>(_statistics);
}

future<> sstable::write_statistics() {
    return write_simple<component_type::Statistics>(_statistics);
}

future<> sstable::open_data() {
    return when_all(engine().open_file_dma(filename(component_type::Index), open_flags::ro),
                    engine().open_file_dma(filename(component_type::Data), open_flags::ro)).then([this] (auto files) {
        _index_file = make_lw_shared<file>(std::move(std::get<file>(std::get<0>(files).get())));
        _data_file  = make_lw_shared<file>(std::move(std::get<file>(std::get<1>(files).get())));
        return _data_file->size().then([this] (auto size) {
          _data_file_size = size;
        });
    });
}

future<> sstable::create_data() {
    auto oflags = open_flags::wo | open_flags::create | open_flags::exclusive;
    return when_all(engine().open_file_dma(filename(component_type::Index), oflags),
                    engine().open_file_dma(filename(component_type::Data), oflags)).then([this] (auto files) {
        _index_file = make_lw_shared<file>(std::move(std::get<file>(std::get<0>(files).get())));
        _data_file  = make_lw_shared<file>(std::move(std::get<file>(std::get<1>(files).get())));
    });
}

future<> sstable::load() {
    return read_toc().then([this] {
        return read_statistics();
    }).then([this] {
        return read_compression();
    }).then([this] {
        return read_filter();
    }).then([this] {;
        return read_summary();
    }).then([this] {
        return open_data();
    }).then([this] {
        // After we have _compression and _data_file_size, we can update
        // _compression with additional information it needs:
        if (has_component(sstable::component_type::CompressionInfo)) {
            _compression.update(_data_file_size);
        }
    });
}

future<> sstable::store() {
    // TODO: write other components as well.
    return write_toc().then([this] {
        return write_statistics();
    }).then([this] {
        return write_compression();
    }).then([this] {
        return write_filter();
    }).then([this] {
        return write_summary();
    });
}

// @clustering_key: it's expected that clustering key is already in its composite form.
// NOTE: empty clustering key means that there is no clustering key.
static future<> write_column_name(file_writer& out, const composite& clustering_key, const std::vector<bytes_view>& column_names) {
    // FIXME: This code assumes name is always composite, but it wouldn't if "WITH COMPACT STORAGE"
    // was defined in the schema, for example.
    return do_with(composite::from_exploded(column_names), [&out, &clustering_key] (composite& c) {
        uint16_t sz = clustering_key.size() + c.size();
        return write(out, sz, clustering_key, c);
    });
}

static future<> write_static_column_name(file_writer& out, const schema& schema, const std::vector<bytes_view>& column_names) {
    return do_with(composite::from_exploded(column_names), [&out, &schema] (composite& c) {
        return do_with(composite::static_prefix(schema), [&out, &c] (composite& sp) {
            uint16_t sz = sp.size() + c.size();
            return write(out, sz, sp, c);
        });
    });
}

// Intended to write all cell components that follow column name.
static future<> write_cell(file_writer& out, atomic_cell_view cell) {
    // FIXME: range tombstone and counter cells aren't supported yet.

    uint64_t timestamp = cell.timestamp();

    if (cell.is_live_and_has_ttl()) {
        // expiring cell

        column_mask mask = column_mask::expiration;
        uint32_t ttl = cell.ttl().count();
        uint32_t expiration = cell.expiry().time_since_epoch().count();
        disk_string_view<uint32_t> cell_value { cell.value() };

        return do_with(std::move(cell_value), [&out, mask, ttl, expiration, timestamp] (auto& cell_value) {
            return write(out, mask, ttl, expiration, timestamp, cell_value);
        });
    } else if (cell.is_dead()) {
        // tombstone cell

        column_mask mask = column_mask::deletion;
        uint32_t deletion_time_size = sizeof(uint32_t);
        uint32_t deletion_time = cell.deletion_time().time_since_epoch().count();

        return write(out, mask, timestamp, deletion_time_size, deletion_time);
    } else {
        // regular cell

        column_mask mask = column_mask::none;
        disk_string_view<uint32_t> cell_value { cell.value() };

        return do_with(std::move(cell_value), [&out, mask, timestamp] (auto& cell_value) {
            return write(out, mask, timestamp, cell_value);
        });
    }
}

static future<> write_row_marker(file_writer& out, const rows_entry& clustered_row, const composite& clustering_key) {
    // Missing created_at (api::missing_timestamp) means no row marker.
    if (clustered_row.row().created_at() == api::missing_timestamp) {
        return make_ready_future<>();
    }

    // Write row mark cell to the beginning of clustered row.
    return write_column_name(out, clustering_key, { bytes_view() }).then([&out, &clustered_row] {
        column_mask mask = column_mask::none;
        uint64_t timestamp = clustered_row.row().created_at();
        uint32_t value_length = 0;

        return write(out, mask, timestamp, value_length);
    });
}

// write_datafile_clustered_row() is about writing a clustered_row to data file according to SSTables format.
// clustered_row contains a set of cells sharing the same clustering key.
static future<> write_clustered_row(file_writer& out, schema_ptr schema, const rows_entry& clustered_row) {
    auto clustering_key = composite::from_clustering_key(*schema, clustered_row.key());

    return do_with(std::move(clustering_key), [&out, schema, &clustered_row] (auto& clustering_key) {
        return write_row_marker(out, clustered_row, clustering_key).then(
                [&out, &clustered_row, schema, &clustering_key] {
            // FIXME: Before writing cells, range tombstone must be written if the row has any (deletable_row::t).
            assert(!clustered_row.row().deleted_at());

            // Write all cells of a partition's row.
            return do_for_each(clustered_row.row().cells(), [&out, schema, &clustering_key] (auto& value) {
                auto column_id = value.first;
                auto&& column_definition = schema->regular_column_at(column_id);
                // non atomic cell isn't supported yet. atomic cell maps to a single trift cell.
                // non atomic cell maps to multiple trift cell, e.g. collection.
                if (!column_definition.is_atomic()) {
                    fail(unimplemented::cause::NONATOMIC);
                }
                assert(column_definition.is_regular());
                atomic_cell_view cell = value.second.as_atomic_cell();
                const bytes& column_name = column_definition.name();

                return write_column_name(out, clustering_key, { bytes_view(column_name) }).then([&out, cell] {
                    return write_cell(out, cell);
                });
            });
        });
    });
}

static future<> write_static_row(file_writer& out, schema_ptr schema, const row& static_row) {
    return do_for_each(static_row, [&out, schema] (auto& value) {
        auto column_id = value.first;
        auto&& column_definition = schema->static_column_at(column_id);
        if (!column_definition.is_atomic()) {
            fail(unimplemented::cause::NONATOMIC);
        }
        assert(column_definition.is_static());
        atomic_cell_view cell = value.second.as_atomic_cell();
        return write_static_column_name(out, *schema, { bytes_view(column_definition.name()) }).then([&out, cell] {
            return write_cell(out, cell);
        });
    });
}

///
/// Write an index entry into an index file.
/// @param key partition key.
/// @param pos position of partition key into data file.
///
static future<> write_index_entry(file_writer& out, disk_string_view<uint16_t>& key, uint64_t pos) {
    // FIXME: support promoted indexes.
    uint32_t promoted_index_size = 0;

    return write(out, key, pos, promoted_index_size);
}

static constexpr int BASE_SAMPLING_LEVEL = 128;

static void prepare_summary(summary& s, const memtable& mt) {
    auto&& all_partitions = mt.all_partitions();
    assert(all_partitions.size() >= 1);

    s.header.min_index_interval = BASE_SAMPLING_LEVEL;
    s.header.sampling_level = BASE_SAMPLING_LEVEL;

    uint64_t max_expected_entries = all_partitions.size() / BASE_SAMPLING_LEVEL + 1;
    // FIXME: handle case where max_expected_entries is greater than max value stored by uint32_t.
    assert(max_expected_entries <= std::numeric_limits<uint32_t>::max());
    s.header.size = max_expected_entries;
    assert(s.header.size >= 1);

    // memory_size only accounts size of vector positions at this point.
    s.header.memory_size = s.header.size * sizeof(uint32_t);
    s.header.size_at_full_sampling = s.header.size;

    s.positions.reserve(s.header.size);
    s.entries.reserve(s.header.size);
    s.keys_written = 0;

    auto begin = all_partitions.begin();
    auto last = --all_partitions.end();

    auto first_key = key::from_partition_key(*mt.schema(), begin->first._key);
    s.first_key.value = std::move(first_key.get_bytes());

    auto last_key = key::from_partition_key(*mt.schema(), last->first._key);
    s.last_key.value = std::move(last_key.get_bytes());
}

static void maybe_add_summary_entry(summary& s, bytes_view key, uint64_t offset) {
    if ((s.keys_written % s.header.min_index_interval) == 0) {
        s.positions.push_back(s.header.memory_size);
        s.entries.push_back({ bytes(key.data(), key.size()), offset });
        s.header.memory_size += key.size() + sizeof(uint64_t);
    }
    s.keys_written++;
}

future<> sstable::write_components(const memtable& mt) {
    return create_data().then([&mt, this] {
        bool checksum_file = true;
        // FIXME: CRC component must only be present when compression isn't enabled.
        if (checksum_file) {
            _components.insert(component_type::CRC);
        }

        // TODO: Add compression support by having a specialized output stream.
        auto w = make_shared<checksummed_file_writer>(_data_file, 4096, checksum_file);
        auto index = make_shared<file_writer>(_index_file, 4096);

        prepare_summary(_summary, mt);
        auto filter_fp_chance = mt.schema()->bloom_filter_fp_chance();
        if (filter_fp_chance != 1.0) {
            _components.insert(component_type::Filter);
        }
        _filter = utils::i_filter::get_filter(mt.all_partitions().size(), filter_fp_chance);

        // Iterate through CQL partitions, then CQL rows, then CQL columns.
        // Each mt.all_partitions() entry is a set of clustered rows sharing the same partition key.
        return do_for_each(mt.all_partitions(),
                [w, index, &mt, this] (const std::pair<const dht::decorated_key, mutation_partition>& partition_entry) {

            return do_with(key::from_partition_key(*mt.schema(), partition_entry.first._key),
                    [w, index, &partition_entry, this] (auto& partition_key) {

                // Maybe add summary entry into in-memory representation of summary file.
                maybe_add_summary_entry(_summary, bytes_view(partition_key), index->offset());
                _filter->add(bytes_view(partition_key));

                return do_with(disk_string_view<uint16_t>(), [w, index, &partition_key] (auto& p_key) {
                    p_key.value = bytes_view(partition_key);

                    // Write index file entry from partition key into index file.
                    return write_index_entry(*index, p_key, w->offset()).then([w, &p_key] {
                        // Write partition key into data file.
                        return write(*w, p_key);
                    });
                }).then([w, &partition_entry] {
                    auto tombstone = partition_entry.second.partition_tombstone();
                    deletion_time d;

                    if (tombstone) {
                        d.local_deletion_time = tombstone.deletion_time.time_since_epoch().count();
                        d.marked_for_delete_at = tombstone.timestamp;
                    } else {
                        // Default values for live, undeleted rows.
                        d.local_deletion_time = std::numeric_limits<int32_t>::max();
                        d.marked_for_delete_at = std::numeric_limits<int64_t>::min();
                    }

                    return do_with(std::move(d), [w] (auto& d) {
                        return write(*w, d);
                    });
                });
            }).then([w, &mt, &partition_entry] {
                auto& partition = partition_entry.second;

                auto& static_row = partition.static_row();
                return write_static_row(*w, mt.schema(), static_row).then([w, &mt, &partition] {

                    // Write all CQL rows from a given mutation partition.
                    return do_for_each(partition.clustered_rows(), [w, &mt] (const rows_entry& clustered_row) {
                        return write_clustered_row(*w, mt.schema(), clustered_row);
                    }).then([w] {
                        // end_of_row is appended to the end of each partition.
                        int16_t end_of_row = 0;
                        return write(*w, end_of_row);
                    });
                });
            });
        }).then([this, w] {
            return write_digest(filename(sstable::component_type::Digest), w->full_checksum());
        }).then([this, w, checksum_file] {
            if (checksum_file) {
                return write_crc(filename(sstable::component_type::CRC), w->finalize_checksum());
            }
            return make_ready_future<>();
        }).then([w] {
            return w->close().then([w] {});
        }).then([index] {
            return index->close().then([index] {});
        }).then([this] {
            return write_summary();
        }).then([this] {
            return write_filter();
        }).then([this] {
            _components.insert(component_type::TOC);
            _components.insert(component_type::Digest);
            _components.insert(component_type::Index);
            _components.insert(component_type::Summary);
            _components.insert(component_type::Data);

            return write_toc();
        });
    });
}

size_t sstable::data_size() {
    if (has_component(sstable::component_type::CompressionInfo)) {
        return _compression.data_len;
    }
    return _data_file_size;
}

const bool sstable::has_component(component_type f) {
    return _components.count(f);
}

const sstring sstable::filename(component_type f) {

    auto& version = _version_string.at(_version);
    auto& format = _format_string.at(_format);
    auto& component = _component_map.at(f);
    auto generation =  to_sstring(_generation);

    return _dir + "/" + version + "-" + generation + "-" + format + "-" + component;
}

const sstring sstable::filename(sstring dir, version_types version, unsigned long generation,
                                format_types format, component_type component) {
    auto& v = _version_string.at(version);
    auto& f = _format_string.at(format);
    auto& c= _component_map.at(component);
    auto g =  to_sstring(generation);

    return dir + "/" + v + "-" + g + "-" + f + "-" + c;
}

sstable::version_types sstable::version_from_sstring(sstring &s) {
    return reverse_map(s, _version_string);
}

sstable::format_types sstable::format_from_sstring(sstring &s) {
    return reverse_map(s, _format_string);
}

input_stream<char> sstable::data_stream_at(uint64_t pos) {
    if (_compression) {
        return make_compressed_file_input_stream(
                _data_file, &_compression, pos);
    } else {
        return make_file_input_stream(_data_file, pos);
    }
}

// FIXME: to read a specific byte range, we shouldn't use the input stream
// interface - it may cause too much read when we intend to read a small
// range, and too small reads, and repeated waits, when reading a large range
// which we should have started at once.
future<temporary_buffer<char>> sstable::data_read(uint64_t pos, size_t len) {
    return do_with(data_stream_at(pos), [len] (auto& stream) {
        return stream.read_exactly(len);
    });
}

}
