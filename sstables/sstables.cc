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
write(output_stream<char>& out, T i) {
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
write(output_stream<char>& out, T i) {
    return write(out, static_cast<typename std::underlying_type<T>::type>(i));
}

future<> parse(random_access_reader& in, bool& i) {
    return parse(in, reinterpret_cast<uint8_t&>(i));
}

future<> write(output_stream<char>& out, bool i) {
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

future<> write(output_stream<char>& out, double d) {
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

future<> write(output_stream<char>& out, bytes& s) {
    return out.write(s).then([&out, &s] (...) -> future<> {
        // TODO: handle result
        return make_ready_future<>();
    });
}

future<> write(output_stream<char>& out, bytes_view s) {
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
future<> write(output_stream<char>& out, First& first, Rest&&... rest) {
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
write(output_stream<char>& out, T& t) {
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
future<> write(output_stream<char>& out, disk_string<Size>& s) {
    Size len = 0;
    check_truncate_and_assign(len, s.value.size());
    return write(out, len).then([&out, &s] {
        return write(out, s.value);
    });
}

template <typename Size>
future<> write(output_stream<char>& out, disk_string_view<Size>& s) {
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
write(output_stream<char>& out, std::vector<Members>& arr) {

    auto count = make_lw_shared<size_t>(0);
    auto eoarr = [count, &arr] { return *count == arr.size(); };

    return do_until(eoarr, [count, &out, &arr] {
        return write(out, arr[(*count)++]);
    });
}

template <typename Members>
typename std::enable_if_t<std::is_integral<Members>::value, future<>>
write(output_stream<char>& out, std::vector<Members>& arr) {
    std::vector<Members> tmp;
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
}

template <typename Size, typename Members>
future<> write(output_stream<char>& out, disk_array<Size, Members>& arr) {
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
future<> write(output_stream<char>& out, std::unordered_map<Key, Value>& map) {
    return do_for_each(map.begin(), map.end(), [&out, &map] (auto val) {
        Key key = val.first;
        Value value = val.second;
        return write(out, key, value);
    });
}

template <typename Size, typename Key, typename Value>
future<> write(output_stream<char>& out, disk_hash<Size, Key, Value>& h) {
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
                    read_integer(buf, entry.position);

                    return make_ready_future<>();
                });
            }).then([&s] {
                // Delete last element which isn't part of the on-disk format.
                s.positions.pop_back();
            });
        });
    });
}

future<> write(output_stream<char>& out, summary& s) {
    using pos_type = typename decltype(summary::positions)::value_type;

    return write(out, s.header.min_index_interval,
                      s.header.size,
                      s.header.memory_size,
                      s.header.sampling_level,
                      s.header.size_at_full_sampling).then([&out, &s] {
        // NOTE: s.positions must be stored in NATIVE BYTE ORDER, not BIG-ENDIAN.
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
future<> write(output_stream<char>& out, std::unique_ptr<metadata>& p) {
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

future<> write(output_stream<char>& out, statistics& s) {
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
        auto out = make_file_output_stream(make_lw_shared<file>(std::move(f)), 4096);
        auto w = make_shared<output_stream<char>>(std::move(out));

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

template <typename T, sstable::component_type Type, T sstable::* Comptr>
future<> sstable::read_simple() {

    auto file_path = filename(Type);
    sstlog.debug(("Reading " + _component_map[Type] + " file {} ").c_str(), file_path);
    return engine().open_file_dma(file_path, open_flags::ro).then([this] (file f) {

        auto r = std::make_unique<file_random_access_reader>(std::move(f), 4096);
        auto fut = parse(*r, *this.*Comptr);
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

template <typename T, sstable::component_type Type, T sstable::* Comptr>
future<> sstable::write_simple() {

    auto file_path = filename(Type);
    sstlog.debug(("Writing " + _component_map[Type] + " file {} ").c_str(), file_path);
    return engine().open_file_dma(file_path, open_flags::wo | open_flags::create | open_flags::truncate).then([this] (file f) {

        auto out = make_file_output_stream(make_lw_shared<file>(std::move(f)), 4096);
        auto w = make_shared<output_stream<char>>(std::move(out));
        auto fut = write(*w, *this.*Comptr);
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

future<> sstable::read_compression() {
     // FIXME: If there is no compression, we should expect a CRC file to be present.
    if (!has_component(sstable::component_type::CompressionInfo)) {
        return make_ready_future<>();
    }

    return read_simple<compression, component_type::CompressionInfo, &sstable::_compression>();
}

future<> sstable::write_compression() {
    if (!has_component(sstable::component_type::CompressionInfo)) {
        return make_ready_future<>();
    }

    return write_simple<compression, component_type::CompressionInfo, &sstable::_compression>();
}

future<> sstable::read_statistics() {
    return read_simple<statistics, component_type::Statistics, &sstable::_statistics>();
}

future<> sstable::write_statistics() {
    return write_simple<statistics, component_type::Statistics, &sstable::_statistics>();
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
    auto oflags = open_flags::wo | open_flags::create | open_flags::truncate;
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

static future<> write_composite_component(output_stream<char>& out, disk_string<uint16_t>&& column_name,
    int8_t end_of_component = 0) {
    // NOTE: end of component can also be -1 and 1 to represent ranges.
    return do_with(std::move(column_name), [&out, end_of_component] (auto& column_name) {
        return write(out, column_name, end_of_component);
    });
}

// @clustering_key: it's expected that clustering key is already in its composite form.
// NOTE: empty clustering key means that there is no clustering key.
static future<> write_column_name(output_stream<char>& out, bytes& clustering_key, const bytes& column_name) {
    // FIXME: This code assumes name is always composite, but it wouldn't if "WITH COMPACT STORAGE"
    // was defined in the schema, for example.

    // uint16_t and int8_t are about the 16-bit length and end of component, respectively.
    uint16_t size = clustering_key.size() + column_name.size() + sizeof(uint16_t) + sizeof(int8_t);

    return write(out, size).then([&out, &clustering_key] {
        if (!clustering_key.size()) {
            return make_ready_future<>();
        }

        return write(out, clustering_key);
    }).then([&out, &column_name] {
        // if size of column_name is zero, then column_name is a row marker.
        disk_string<uint16_t> c_name;
        c_name.value = column_name;

        return write_composite_component(out, std::move(c_name));
    });
}

// magic value for identifying a static column.
static constexpr uint16_t STATIC_MARKER = 0xffff;

static future<> write_static_column_name(output_stream<char>& out, const bytes& column_name) {
    // first component of static column name is composed of the 16-bit magic value ff ff,
    // followed by a null composite, i.e. three null bytes.
    uint16_t first_component_size = sizeof(uint16_t) + sizeof(uint16_t) + sizeof(int8_t);
    uint16_t second_component_size = column_name.size() + sizeof(uint16_t) + sizeof(int8_t);
    uint16_t total_size = first_component_size + second_component_size;

    return write(out, total_size).then([&out] {

        return write(out, STATIC_MARKER).then([&out] {
            return write_composite_component(out, {});
        });
    }).then([&out, &column_name] {
        disk_string<uint16_t> c_name;
        c_name.value = column_name;

        return write_composite_component(out, std::move(c_name));
    });
}

// Intended to write all cell components that follow column name.
static future<> write_cell(output_stream<char>& out, atomic_cell_view cell) {
    // FIXME: cell with expiration time isn't supported.
    // cell with expiration time has a different mask and additional data in representation.
    // FIXME: deleted cell isn't supported either.
    column_mask mask = column_mask::none;
    uint64_t timestamp = cell.timestamp();
    disk_string_view<uint32_t> cell_value;
    cell_value.value = cell.value();

    return do_with(std::move(cell_value), [&out, mask, timestamp] (auto& cell_value) {
        return write(out, mask, timestamp, cell_value);
    });
}

static future<> write_row_marker(output_stream<char>& out, const rows_entry& clustered_row, bytes& clustering_key) {
    // Missing created_at (api::missing_timestamp) means no row marker.
    if (clustered_row.row().created_at == api::missing_timestamp) {
        return make_ready_future<>();
    }

    // Write row mark cell to the beginning of clustered row.
    return write_column_name(out, clustering_key, {}).then([&out, &clustered_row] {
        column_mask mask = column_mask::none;
        uint64_t timestamp = clustered_row.row().created_at;
        uint32_t value_length = 0;

        return write(out, mask, timestamp, value_length);
    });
}

// write_datafile_clustered_row() is about writing a clustered_row to data file according to SSTables format.
// clustered_row contains a set of cells sharing the same clustering key.
static future<> write_clustered_row(output_stream<char>& out, schema_ptr schema, const rows_entry& clustered_row) {
    bytes clustering_key = composite_from_clustering_key(*schema, clustered_row.key());

    return do_with(std::move(clustering_key), [&out, schema, &clustered_row] (auto& clustering_key) {
        return write_row_marker(out, clustered_row, clustering_key).then(
                [&out, &clustered_row, schema, &clustering_key] {
            // FIXME: Before writing cells, range tombstone must be written if the row has any (deletable_row::t).
            assert(!clustered_row.row().t);

            // Write all cells of a partition's row.
            return do_for_each(clustered_row.row().cells, [&out, schema, &clustering_key] (auto& value) {
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

                return write_column_name(out, clustering_key, column_name).then([&out, cell] {
                    return write_cell(out, cell);
                });
            });
        });
    });
}

static future<> write_static_row(output_stream<char>& out, schema_ptr schema, const row& static_row) {
    return do_for_each(static_row, [&out, schema] (auto& value) {
        auto column_id = value.first;
        auto&& column_definition = schema->static_column_at(column_id);
        if (!column_definition.is_atomic()) {
            fail(unimplemented::cause::NONATOMIC);
        }
        assert(column_definition.is_static());
        atomic_cell_view cell = value.second.as_atomic_cell();
        const bytes& column_name = column_definition.name();

        return write_static_column_name(out, column_name).then([&out, cell] {
            return write_cell(out, cell);
        });
    });
}

future<> write_datafile(column_family& cf, sstring datafile) {
    auto oflags = open_flags::wo | open_flags::create | open_flags::truncate;
    return engine().open_file_dma(datafile, oflags).then([&cf] (file f) {
        // TODO: Add compression support by having a specialized output stream.
        auto out = make_file_output_stream(make_lw_shared<file>(std::move(f)), 4096);
        auto w = make_shared<output_stream<char>>(std::move(out));

        // Iterate through CQL partitions, then CQL rows, then CQL columns.
        // Each cf.partitions entry is a set of clustered rows sharing the same partition key.
        return do_for_each(cf.partitions,
                [w, &cf] (std::pair<const dht::decorated_key, mutation_partition>& partition_entry) {
            // TODO: Write index and summary files on-the-fly.

            key partition_key = key::from_partition_key(*cf._schema, partition_entry.first._key);

            return do_with(std::move(partition_key), [w, &partition_entry] (auto& partition_key) {
                disk_string_view<uint16_t> p_key;
                p_key.value = bytes_view(partition_key);

                return do_with(std::move(p_key), [w] (auto& p_key) {
                    return write(*w, p_key);
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

                    return write(*w, d);
                });
            }).then([w, &cf, &partition_entry] {
                auto& partition = partition_entry.second;

                auto& static_row = partition.static_row();
                return write_static_row(*w, cf._schema, static_row).then([w, &cf, &partition] {

                    // Write all CQL rows from a given mutation partition.
                    return do_for_each(partition.clustered_rows(), [w, &cf] (const rows_entry& clustered_row) {
                        return write_clustered_row(*w, cf._schema, clustered_row);
                    }).then([w] {
                        // end_of_row is appended to the end of each partition.
                        int16_t end_of_row = 0;
                        return write(*w, end_of_row);
                    });
                });
            });
        }).then([w] {
            return w->close().then([w] {});
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
