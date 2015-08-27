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
#include "core/thread.hh"
#include <iterator>

#include "types.hh"
#include "sstables.hh"
#include "compress.hh"
#include "unimplemented.hh"
#include <boost/algorithm/string.hpp>
#include <regex>
#include <core/align.hh>

namespace sstables {

logging::logger sstlog("sstable");

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
    file _file;
    size_t _buffer_size;
public:
    virtual input_stream<char> open_at(uint64_t pos) override {
        return make_file_input_stream(_file, pos, _buffer_size);
    }
    explicit file_random_access_reader(file f, size_t buffer_size = 8192)
        : _file(std::move(f)), _buffer_size(buffer_size)
    {
        seek(0);
    }
    ~file_random_access_reader() {
        _file.close().handle_exception([save = _file] (auto ep) {
            sstlog.warn("sstable close failed: {}", ep);
        });
    }
};

class shared_file_random_access_reader : public random_access_reader {
    file _file;
    size_t _buffer_size;
public:
    virtual input_stream<char> open_at(uint64_t pos) override {
        return make_file_input_stream(_file, pos, _buffer_size);
    }
    explicit shared_file_random_access_reader(file f, size_t buffer_size = 8192)
        : _file(std::move(f)), _buffer_size(buffer_size)
    {
        seek(0);
    }
};

std::unordered_map<sstable::version_types, sstring, enum_hash<sstable::version_types>> sstable::_version_string = {
    { sstable::version_types::ka , "ka" },
    { sstable::version_types::la , "la" }
};

std::unordered_map<sstable::format_types, sstring, enum_hash<sstable::format_types>> sstable::_format_string = {
    { sstable::format_types::big , "big" }
};

// FIXME: this should be version-dependent
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
inline typename std::enable_if_t<std::is_integral<T>::value, void>
write(file_writer& out, T i) {
    auto *nr = reinterpret_cast<const net::packed<T> *>(&i);
    i = net::hton(*nr);
    auto p = reinterpret_cast<const char*>(&i);
    out.write(p, sizeof(T)).get();
}

template <typename T>
typename std::enable_if_t<std::is_enum<T>::value, future<>>
parse(random_access_reader& in, T& i) {
    return parse(in, reinterpret_cast<typename std::underlying_type<T>::type&>(i));
}

template <typename T>
inline typename std::enable_if_t<std::is_enum<T>::value, void>
write(file_writer& out, T i) {
    write(out, static_cast<typename std::underlying_type<T>::type>(i));
}

future<> parse(random_access_reader& in, bool& i) {
    return parse(in, reinterpret_cast<uint8_t&>(i));
}

inline void write(file_writer& out, bool i) {
    write(out, static_cast<uint8_t>(i));
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

inline void write(file_writer& out, double d) {
    auto *nr = reinterpret_cast<const net::packed<unsigned long> *>(&d);
    auto tmp = net::hton(*nr);
    auto p = reinterpret_cast<const char*>(&tmp);
    out.write(p, sizeof(unsigned long)).get();
}

template <typename T>
future<> parse(random_access_reader& in, T& len, bytes& s) {
    return in.read_exactly(len).then([&s, len] (auto buf) {
        check_buf_size(buf, len);
        // Likely a different type of char. Most bufs are unsigned, whereas the bytes type is signed.
        s = bytes(reinterpret_cast<const bytes::value_type *>(buf.get()), len);
    });
}

inline void write(file_writer& out, bytes& s) {
    out.write(s).get();
}

inline void write(file_writer& out, bytes_view s) {
    out.write(reinterpret_cast<const char*>(s.data()), s.size()).get();
}

// All composite parsers must come after this
template<typename First, typename... Rest>
future<> parse(random_access_reader& in, First& first, Rest&&... rest) {
    return parse(in, first).then([&in, &rest...] {
        return parse(in, std::forward<Rest>(rest)...);
    });
}

template<typename First, typename... Rest>
inline void write(file_writer& out, First& first, Rest&&... rest) {
    write(out, first);
    write(out, std::forward<Rest>(rest)...);
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
inline typename std::enable_if_t<!std::is_integral<T>::value && !std::is_enum<T>::value, void>
write(file_writer& out, T& t) {
    t.describe_type([&out] (auto&&... what) -> void {
        write(out, std::forward<decltype(what)>(what)...);
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
inline void write(file_writer& out, disk_string<Size>& s) {
    Size len = 0;
    check_truncate_and_assign(len, s.value.size());
    write(out, len);
    write(out, s.value);
}

template <typename Size>
inline void write(file_writer& out, disk_string_view<Size>& s) {
    Size len;
    check_truncate_and_assign(len, s.value.size());
    write(out, len, s.value);
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
parse(random_access_reader& in, Size& len, std::deque<Members>& arr) {

    auto count = make_lw_shared<size_t>(0);
    auto eoarr = [count, len] { return *count == len; };

    return do_until(eoarr, [count, &in, &arr] {
        return parse(in, arr[(*count)++]);
    });
}

template <typename Size, typename Members>
typename std::enable_if_t<std::is_integral<Members>::value, future<>>
parse(random_access_reader& in, Size& len, std::deque<Members>& arr) {
    auto done = make_lw_shared<size_t>(0);
    return repeat([&in, &len, &arr, done]  {
        auto now = std::min(len - *done, 100000 / sizeof(Members));
        return in.read_exactly(now * sizeof(Members)).then([&arr, len, now, done] (auto buf) {
            check_buf_size(buf, now * sizeof(Members));

            auto *nr = reinterpret_cast<const net::packed<Members> *>(buf.get());
            for (size_t i = 0; i < now; ++i) {
                arr[*done + i] = net::ntoh(nr[i]);
            }
            *done += now;
            return make_ready_future<stop_iteration>(*done == len ? stop_iteration::yes : stop_iteration::no);
        });
    });
}

// We resize the array here, before we pass it to the integer / non-integer
// specializations
template <typename Size, typename Members>
future<> parse(random_access_reader& in, disk_array<Size, Members>& arr) {
    auto len = make_lw_shared<Size>();
    auto f = parse(in, *len);
    return f.then([&in, &arr, len] {
        arr.elements.resize(*len);
        return parse(in, *len, arr.elements);
    }).finally([len] {});
}

template <typename Members>
inline typename std::enable_if_t<!std::is_integral<Members>::value, void>
write(file_writer& out, std::deque<Members>& arr) {
    for (auto& a : arr) {
        write(out, a);
    }
}

template <typename Members>
inline typename std::enable_if_t<std::is_integral<Members>::value, void>
write(file_writer& out, std::deque<Members>& arr) {
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
        out.write(p, bytes).get();
        idx += now;
    }
}

template <typename Size, typename Members>
inline void write(file_writer& out, disk_array<Size, Members>& arr) {
    Size len = 0;
    check_truncate_and_assign(len, arr.elements.size());
    write(out, len);
    write(out, arr.elements);
}

template <typename Size, typename Key, typename Value>
future<> parse(random_access_reader& in, Size& len, std::unordered_map<Key, Value>& map) {
    return do_with(Size(), [&in, len, &map] (Size& count) {
        auto eos = [len, &count] { return len == count++; };
        return do_until(eos, [len, &in, &map] {
            struct kv {
                Key key;
                Value value;
            };

            return do_with(kv(), [&in, &map] (auto& el) {
                return parse(in, el.key, el.value).then([&el, &map] {
                    map.emplace(el.key, el.value);
                });
            });
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
inline void write(file_writer& out, std::unordered_map<Key, Value>& map) {
    for (auto& val: map) {
        write(out, val.first, val.second);
    };
}

template <typename Size, typename Key, typename Value>
inline void write(file_writer& out, disk_hash<Size, Key, Value>& h) {
    Size len = 0;
    check_truncate_and_assign(len, h.map.size());
    write(out, len);
    write(out, h.map);
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
            s.positions = std::deque<pos_type>(nr, nr + s.header.size);

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

inline void write(file_writer& out, summary_entry& entry) {
    // FIXME: summary entry is supposedly written in memory order, but that
    // would prevent portability of summary file between machines of different
    // endianness. We can treat it as little endian to preserve portability.
    write(out, entry.key);
    auto p = reinterpret_cast<const char*>(&entry.position);
    out.write(p, sizeof(uint64_t)).get();
}

inline void write(file_writer& out, summary& s) {
    // NOTE: positions and entries must be stored in NATIVE BYTE ORDER, not BIG-ENDIAN.
    write(out, s.header.min_index_interval,
                  s.header.size,
                  s.header.memory_size,
                  s.header.sampling_level,
                  s.header.size_at_full_sampling);
    for (auto&& e : s.positions) {
        out.write(reinterpret_cast<const char*>(&e), sizeof(e)).get();
    }
    write(out, s.entries);
    write(out, s.first_key, s.last_key);
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
inline void write(file_writer& out, std::unique_ptr<metadata>& p) {
    write(out, *static_cast<Child *>(p.get()));
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

inline void write(file_writer& out, statistics& s) {
    write(out, s.hash);
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
    for (auto& val: *v) {
        switch (val.key) {
            case metadata_type::Validation:
                write<validation_metadata>(out, s.contents[val.key]);
                break;
            case metadata_type::Compaction:
                write<compaction_metadata>(out, s.contents[val.key]);
                break;
            case metadata_type::Stats:
                write<stats_metadata>(out, s.contents[val.key]);
                break;
            default:
                sstlog.warn("Invalid metadata type at Statistics file: {} ", int(val.key));
                return; // FIXME: should throw
            }
    }
}

future<> parse(random_access_reader& in, estimated_histogram& eh) {
    auto len = std::make_unique<uint32_t>();

    auto f = parse(in, *len);
    return f.then([&in, &eh, len = std::move(len)] {
        uint32_t length = *len;

        assert(length > 0);
        eh.bucket_offsets.resize(length - 1);
        eh.buckets.resize(length);

        auto type_size = sizeof(uint64_t) * 2;
        return in.read_exactly(length * type_size).then([&eh, length, type_size] (auto buf) {
            check_buf_size(buf, length * type_size);

            auto *nr = reinterpret_cast<const net::packed<uint64_t> *>(buf.get());
            size_t j = 0;
            for (size_t i = 0; i < length; ++i) {
                eh.bucket_offsets[i == 0 ? 0 : i - 1] = net::ntoh(nr[j++]);
                eh.buckets[i] = net::ntoh(nr[j++]);
            }
            return make_ready_future<>();
        });
    });
}

inline void write(file_writer& out, estimated_histogram& eh) {
    uint32_t len = 0;
    check_truncate_and_assign(len, eh.buckets.size());

    write(out, len);
    struct element {
        uint64_t offsets;
        uint64_t buckets;
    };
    std::vector<element> elements;
    elements.resize(eh.buckets.size());

    auto *offsets_nr = reinterpret_cast<const net::packed<uint64_t> *>(eh.bucket_offsets.data());
    auto *buckets_nr = reinterpret_cast<const net::packed<uint64_t> *>(eh.buckets.data());
    for (size_t i = 0; i < eh.buckets.size(); i++) {
        elements[i].offsets = net::hton(offsets_nr[i == 0 ? 0 : i - 1]);
        elements[i].buckets = net::hton(buckets_nr[i]);
    }

    auto p = reinterpret_cast<const char*>(elements.data());
    auto bytes = elements.size() * sizeof(element);
    out.write(p, bytes).get();
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
        return std::move(fut).then([this, f = std::move(f), bufptr = std::move(bufptr)] (size_t size) mutable {
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
            return f.close().finally([f] {});
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

void sstable::write_toc() {
    // Create TOC file with the string 'tmp-' prepended to it, meaning TOC
    // is a temporary file.
    auto file_path = temporary_filename(sstable::component_type::TOC);

    sstlog.debug("Writing TOC file {} ", file_path);

    file f = engine().open_file_dma(file_path, open_flags::wo | open_flags::create | open_flags::truncate).get0();
    auto out = file_writer(std::move(f), 4096);
    auto w = file_writer(std::move(out));

    for (auto&& key : _components) {
            // new line character is appended to the end of each component name.
        auto value = _component_map[key] + "\n";
        bytes b = bytes(reinterpret_cast<const bytes::value_type *>(value.c_str()), value.size());
        write(w, b);
    }
    w.flush().get();
    w.close().get();

    file dir_f = engine().open_directory(_dir).get0();
    // Guarantee that every component of this sstable reached the disk.
    dir_f.flush().get();
    // Rename TOC because it's no longer temporary.
    engine().rename_file(file_path, filename(sstable::component_type::TOC)).get();
    // Guarantee that the changes above reached the disk.
    dir_f.flush().get();
    dir_f.close().get();
    // If this point was reached, sstable should be safe in disk.
}

void write_crc(const sstring file_path, checksum& c) {
    sstlog.debug("Writing CRC file {} ", file_path);

    auto oflags = open_flags::wo | open_flags::create | open_flags::exclusive;
    file f = engine().open_file_dma(file_path, oflags).get0();
    auto out = file_writer(std::move(f), 4096);
    auto w = file_writer(std::move(out));
    write(w, c);
    w.close().get();
}

// Digest file stores the full checksum of data file converted into a string.
void write_digest(const sstring file_path, uint32_t full_checksum) {
    sstlog.debug("Writing Digest file {} ", file_path);

    auto oflags = open_flags::wo | open_flags::create | open_flags::exclusive;
    auto f = engine().open_file_dma(file_path, oflags).get0();
    auto out = file_writer(std::move(f), 4096);
    auto w = file_writer(std::move(out));

    auto digest = to_sstring<bytes>(full_checksum);
    write(w, digest);
    w.close().get();
}

future<index_list> sstable::read_indexes(uint64_t summary_idx) {
    if (summary_idx >= _summary.header.size) {
        return make_ready_future<index_list>(index_list());
    }

    uint64_t position = _summary.entries[summary_idx].position;
    uint64_t quantity = _summary.header.sampling_level;

    uint64_t estimated_size;
    if (++summary_idx >= _summary.header.size) {
        estimated_size = index_size() - position;
    } else {
        estimated_size = _summary.entries[summary_idx].position - position;
    }

    estimated_size = std::min(uint64_t(sstable_buffer_size), align_up(estimated_size, uint64_t(8 << 10)));

    struct reader {
        uint64_t count = 0;
        std::vector<index_entry> indexes;
        shared_file_random_access_reader stream;
        reader(file f, uint64_t quantity, uint64_t estimated_size) : stream(f, estimated_size) { indexes.reserve(quantity); }
    };

    auto r = make_lw_shared<reader>(_index_file, quantity, estimated_size);

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
void sstable::write_simple(T& component) {
    auto file_path = filename(Type);
    sstlog.debug(("Writing " + _component_map[Type] + " file {} ").c_str(), file_path);
    file f = engine().open_file_dma(file_path, open_flags::wo | open_flags::create | open_flags::truncate).get0();
    auto out = file_writer(std::move(f), 4096);
    auto w = file_writer(std::move(out));
    write(w, component);
    w.flush().get();
    w.close().get();
}

template future<> sstable::read_simple<sstable::component_type::Filter>(sstables::filter& f);
template void sstable::write_simple<sstable::component_type::Filter>(sstables::filter& f);

future<> sstable::read_compression() {
     // FIXME: If there is no compression, we should expect a CRC file to be present.
    if (!has_component(sstable::component_type::CompressionInfo)) {
        return make_ready_future<>();
    }

    return read_simple<component_type::CompressionInfo>(_compression);
}

void sstable::write_compression() {
    if (!has_component(sstable::component_type::CompressionInfo)) {
        return;
    }

    write_simple<component_type::CompressionInfo>(_compression);
}

future<> sstable::read_statistics() {
    return read_simple<component_type::Statistics>(_statistics);
}

void sstable::write_statistics() {
    write_simple<component_type::Statistics>(_statistics);
}

future<> sstable::open_data() {
    return when_all(engine().open_file_dma(filename(component_type::Index), open_flags::ro),
                    engine().open_file_dma(filename(component_type::Data), open_flags::ro)).then([this] (auto files) {
        _index_file = std::get<file>(std::get<0>(files).get());
        _data_file  = std::get<file>(std::get<1>(files).get());
        return _data_file.size().then([this] (auto size) {
          _data_file_size = size;
        }).then([this] {
            return _index_file.size().then([this] (auto size) {
              _index_file_size = size;
            });
        });

    });
}

future<> sstable::create_data() {
    auto oflags = open_flags::wo | open_flags::create | open_flags::exclusive;
    return when_all(engine().open_file_dma(filename(component_type::Index), oflags),
                    engine().open_file_dma(filename(component_type::Data), oflags)).then([this] (auto files) {
        // FIXME: If both files could not be created, the first get below will
        // throw an exception, and second get() will not be attempted, and
        // we'll get a warning about the second future being destructed
        // without its exception being examined.
        _index_file = std::get<file>(std::get<0>(files).get());
        _data_file  = std::get<file>(std::get<1>(files).get());
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

// @clustering_key: it's expected that clustering key is already in its composite form.
// NOTE: empty clustering key means that there is no clustering key.
void sstable::write_column_name(file_writer& out, const composite& clustering_key, const std::vector<bytes_view>& column_names, composite_marker m) {
    // FIXME: min_components and max_components also keep track of clustering
    // prefix, so we must merge clustering_key and column_names somehow and
    // pass the result to the functions below.
    column_name_helper::min_components(_c_stats.min_column_names, column_names);
    column_name_helper::max_components(_c_stats.max_column_names, column_names);

    // was defined in the schema, for example.
    auto c= composite::from_exploded(column_names, m);
    auto ck_bview = bytes_view(clustering_key);

    // The marker is not a component, so if the last component is empty (IOW,
    // only serializes to the marker), then we just replace the key's last byte
    // with the marker. If the component however it is not empty, then the
    // marker should be in the end of it, and we just join them together as we
    // do for any normal component
    if (c.size() == 1) {
        ck_bview.remove_suffix(1);
    }
    uint16_t sz = ck_bview.size() + c.size();
    write(out, sz, ck_bview, c);
}

void sstable::write_column_name(file_writer& out, bytes_view column_names) {
    column_name_helper::min_components(_c_stats.min_column_names, { column_names });
    column_name_helper::max_components(_c_stats.max_column_names, { column_names });

    uint16_t sz = column_names.size();
    write(out, sz, column_names);
}


static inline void update_cell_stats(column_stats& c_stats, uint64_t timestamp) {
    c_stats.update_min_timestamp(timestamp);
    c_stats.update_max_timestamp(timestamp);
    c_stats.column_count++;
}

// Intended to write all cell components that follow column name.
void sstable::write_cell(file_writer& out, atomic_cell_view cell) {
    // FIXME: range tombstone and counter cells aren't supported yet.

    uint64_t timestamp = cell.timestamp();

    update_cell_stats(_c_stats, timestamp);

    if (cell.is_dead(_now)) {
        // tombstone cell

        column_mask mask = column_mask::deletion;
        uint32_t deletion_time_size = sizeof(uint32_t);
        uint32_t deletion_time = cell.deletion_time().time_since_epoch().count();

        _c_stats.tombstone_histogram.update(deletion_time);

        write(out, mask, timestamp, deletion_time_size, deletion_time);
    } else if (cell.is_live_and_has_ttl()) {
        // expiring cell

        column_mask mask = column_mask::expiration;
        uint32_t ttl = cell.ttl().count();
        uint32_t expiration = cell.expiry().time_since_epoch().count();
        disk_string_view<uint32_t> cell_value { cell.value() };

        write(out, mask, ttl, expiration, timestamp, cell_value);
    } else {
        // regular cell

        column_mask mask = column_mask::none;
        disk_string_view<uint32_t> cell_value { cell.value() };

        write(out, mask, timestamp, cell_value);
    }
}

void sstable::write_row_marker(file_writer& out, const rows_entry& clustered_row, const composite& clustering_key) {
    const auto& marker = clustered_row.row().marker();
    if (marker.is_missing()) {
        return;
    }

    // Write row mark cell to the beginning of clustered row.
    write_column_name(out, clustering_key, { bytes_view() });
    uint64_t timestamp = marker.timestamp();
    uint32_t value_length = 0;

    update_cell_stats(_c_stats, timestamp);

    if (marker.is_dead(_now)) {
        column_mask mask = column_mask::deletion;
        uint32_t deletion_time_size = sizeof(uint32_t);
        uint32_t deletion_time = marker.deletion_time().time_since_epoch().count();

        _c_stats.tombstone_histogram.update(deletion_time);

        write(out, mask, timestamp, deletion_time_size, deletion_time);
    } else if (marker.is_expiring()) {
        column_mask mask = column_mask::expiration;
        uint32_t ttl = marker.ttl().count();
        uint32_t expiration = marker.expiry().time_since_epoch().count();
        write(out, mask, ttl, expiration, timestamp, value_length);
    } else {
        column_mask mask = column_mask::none;
        write(out, mask, timestamp, value_length);
    }
}

void sstable::write_range_tombstone(file_writer& out, const composite& clustering_prefix, std::vector<bytes_view> suffix, const tombstone t) {
    if (!t) {
        return;
    }

    write_column_name(out, clustering_prefix, suffix, composite_marker::start_range);
    column_mask mask = column_mask::range_tombstone;
    write(out, mask);
    write_column_name(out, clustering_prefix, suffix, composite_marker::end_range);
    uint64_t timestamp = t.timestamp;
    uint32_t deletion_time = t.deletion_time.time_since_epoch().count();

    update_cell_stats(_c_stats, timestamp);
    _c_stats.tombstone_histogram.update(deletion_time);

    write(out, deletion_time, timestamp);
}

void sstable::write_collection(file_writer& out, const composite& clustering_key, const column_definition& cdef, collection_mutation::view collection) {

    auto t = static_pointer_cast<const collection_type_impl>(cdef.type);
    auto mview = t->deserialize_mutation_form(collection);
    const bytes& column_name = cdef.name();
    write_range_tombstone(out, clustering_key, { bytes_view(column_name) }, mview.tomb);
    for (auto& cp: mview.cells) {
        write_column_name(out, clustering_key, { column_name, cp.first });
        write_cell(out, cp.second);
    }
}

// write_datafile_clustered_row() is about writing a clustered_row to data file according to SSTables format.
// clustered_row contains a set of cells sharing the same clustering key.
void sstable::write_clustered_row(file_writer& out, const schema& schema, const rows_entry& clustered_row) {
    auto clustering_key = composite::from_clustering_element(schema, clustered_row.key());

    if (schema.is_compound() && !schema.is_dense()) {
        write_row_marker(out, clustered_row, clustering_key);
    }
    // Before writing cells, range tombstone must be written if the row has any (deletable_row::t).
    if (clustered_row.row().deleted_at()) {
        write_range_tombstone(out, clustering_key, {}, clustered_row.row().deleted_at());
    }

    // Write all cells of a partition's row.
    for (auto& value: clustered_row.row().cells()) {
        auto column_id = value.id();
        auto&& column_definition = schema.regular_column_at(column_id);
        // non atomic cell isn't supported yet. atomic cell maps to a single trift cell.
        // non atomic cell maps to multiple trift cell, e.g. collection.
        if (!column_definition.is_atomic()) {
            write_collection(out, clustering_key, column_definition, value.cell().as_collection_mutation());
            return;
        }
        assert(column_definition.is_regular());
        atomic_cell_view cell = value.cell().as_atomic_cell();
        const bytes& column_name = column_definition.name();

        if (schema.is_compound()) {
            if (schema.is_dense()) {
                write_column_name(out, bytes_view(clustering_key));
            } else {
                write_column_name(out, clustering_key, { bytes_view(column_name) });
            }
        } else {
            if (schema.is_dense()) {
                write_column_name(out, bytes_view(clustered_row.key()));
            } else {
                write_column_name(out, bytes_view(column_name));
            }
        }
        write_cell(out, cell);
    }
}

void sstable::write_static_row(file_writer& out, const schema& schema, const row& static_row) {
    for (auto& value: static_row) {
        auto column_id = value.id();
        auto&& column_definition = schema.static_column_at(column_id);
        if (!column_definition.is_atomic()) {
            auto sp = composite::static_prefix(schema);
            write_collection(out, sp, column_definition, value.cell().as_collection_mutation());
            return;
        }
        assert(column_definition.is_static());
        atomic_cell_view cell = value.cell().as_atomic_cell();
        auto sp = composite::static_prefix(schema);
        write_column_name(out, sp, { bytes_view(column_definition.name()) });
        write_cell(out, cell);
    }
}

static void write_index_entry(file_writer& out, disk_string_view<uint16_t>& key, uint64_t pos) {
    // FIXME: support promoted indexes.
    uint32_t promoted_index_size = 0;

    write(out, key, pos, promoted_index_size);
}

static constexpr int BASE_SAMPLING_LEVEL = 128;

static void prepare_summary(summary& s, uint64_t expected_partition_count) {
    assert(expected_partition_count >= 1);

    s.header.min_index_interval = BASE_SAMPLING_LEVEL;
    s.header.sampling_level = BASE_SAMPLING_LEVEL;
    uint64_t max_expected_entries =
            (expected_partition_count / BASE_SAMPLING_LEVEL) +
            !!(expected_partition_count % BASE_SAMPLING_LEVEL);
    // FIXME: handle case where max_expected_entries is greater than max value stored by uint32_t.
    if (max_expected_entries > std::numeric_limits<uint32_t>::max()) {
        throw malformed_sstable_exception("Current sampling level (" + to_sstring(BASE_SAMPLING_LEVEL) + ") not enough to generate summary.");
    }

    s.keys_written = 0;
    s.header.memory_size = 0;
}

static void seal_summary(summary& s,
        std::experimental::optional<key>&& first_key,
        std::experimental::optional<key>&& last_key,
        const schema& schema) {
    s.header.size = s.entries.size();
    s.header.size_at_full_sampling = s.header.size;

    s.header.memory_size = s.header.size * sizeof(uint32_t);
    for (auto& e: s.entries) {
        s.positions.push_back(s.header.memory_size);
        s.header.memory_size += e.key.size() + sizeof(e.position);
    }
    assert(first_key); // assume non-empty sstable
    s.first_key.value = first_key->get_bytes();

    if (last_key) {
        s.last_key.value = last_key->get_bytes();
    } else {
        // An empty last_mutation indicates we had just one partition
        s.last_key.value = s.first_key.value;
    }
}

static void prepare_compression(compression& c, const schema& schema) {
    const auto& cp = schema.get_compressor_params();
    c.set_compressor(cp.get_compressor());
    c.chunk_len = cp.chunk_length();
    c.data_len = 0;
    // FIXME: crc_check_chance can be configured by the user.
    // probability to verify the checksum of a compressed chunk we read.
    // defaults to 1.0.
    c.options.elements.push_back({"crc_check_chance", "1.0"});
    c.init_full_checksum();
}

static void maybe_add_summary_entry(summary& s, bytes_view key, uint64_t offset) {
    // Maybe add summary entry into in-memory representation of summary file.
    if ((s.keys_written++ % s.header.min_index_interval) == 0) {
        s.entries.push_back({ bytes(key.data(), key.size()), offset });
    }
}

// In the beginning of the statistics file, there is a disk_hash used to
// map each metadata type to its correspondent position in the file.
static void seal_statistics(statistics& s, metadata_collector& collector,
        const sstring partitioner, double bloom_filter_fp_chance) {
    static constexpr int METADATA_TYPE_COUNT = 3;

    size_t old_offset, offset = 0;
    // account disk_hash size.
    offset += sizeof(uint32_t);
    // account disk_hash members.
    offset += (METADATA_TYPE_COUNT * (sizeof(metadata_type) + sizeof(uint32_t)));

    validation_metadata validation;
    compaction_metadata compaction;
    stats_metadata stats;

    old_offset = offset;
    validation.partitioner.value = to_bytes(partitioner);
    validation.filter_chance = bloom_filter_fp_chance;
    offset += validation.serialized_size();
    s.contents[metadata_type::Validation] = std::make_unique<validation_metadata>(std::move(validation));
    s.hash.map[metadata_type::Validation] = old_offset;

    old_offset = offset;
    collector.construct_compaction(compaction);
    offset += compaction.serialized_size();
    s.contents[metadata_type::Compaction] = std::make_unique<compaction_metadata>(std::move(compaction));
    s.hash.map[metadata_type::Compaction] = old_offset;

    collector.construct_stats(stats);
    // NOTE: method serialized_size of stats_metadata must be implemented for
    // a new type of compaction to get supported.
    s.contents[metadata_type::Stats] = std::make_unique<stats_metadata>(std::move(stats));
    s.hash.map[metadata_type::Stats] = offset;
}

///
///  @param out holds an output stream to data file.
///
void sstable::do_write_components(::mutation_reader mr,
        uint64_t estimated_partitions, schema_ptr schema, file_writer& out) {
    auto index = make_shared<file_writer>(_index_file, sstable_buffer_size);

    auto filter_fp_chance = schema->bloom_filter_fp_chance();
    if (filter_fp_chance != 1.0) {
        _components.insert(component_type::Filter);
    }
    _filter = utils::i_filter::get_filter(estimated_partitions, filter_fp_chance);

    prepare_summary(_summary, estimated_partitions);

    // FIXME: it's likely that we need to set both sstable_level and repaired_at stats at this point.

    // Remember first and last keys, which we need for the summary file.
    std::experimental::optional<key> first_key, last_key;

    // Iterate through CQL partitions, then CQL rows, then CQL columns.
    // Each mt.all_partitions() entry is a set of clustered rows sharing the same partition key.
    while (mutation_opt mut = mr().get0()) {
        // Set current index of data to later compute row size.
        _c_stats.start_offset = out.offset();

        auto partition_key = key::from_partition_key(*schema, mut->key());

        maybe_add_summary_entry(_summary, bytes_view(partition_key), index->offset());
        _filter->add(bytes_view(partition_key));
        _collector.add_key(bytes_view(partition_key));

        auto p_key = disk_string_view<uint16_t>();
        p_key.value = bytes_view(partition_key);

        // Write index file entry from partition key into index file.
        write_index_entry(*index, p_key, out.offset());

        // Write partition key into data file.
        write(out, p_key);

        auto tombstone = mut->partition().partition_tombstone();
        deletion_time d;

        if (tombstone) {
            d.local_deletion_time = tombstone.deletion_time.time_since_epoch().count();
            d.marked_for_delete_at = tombstone.timestamp;

            _c_stats.tombstone_histogram.update(d.local_deletion_time);
            _c_stats.update_max_local_deletion_time(d.local_deletion_time);
            _c_stats.update_min_timestamp(d.marked_for_delete_at);
            _c_stats.update_max_timestamp(d.marked_for_delete_at);
        } else {
            // Default values for live, undeleted rows.
            d.local_deletion_time = std::numeric_limits<int32_t>::max();
            d.marked_for_delete_at = std::numeric_limits<int64_t>::min();
        }
        write(out, d);

        auto& partition = mut->partition();
        auto& static_row = partition.static_row();

        write_static_row(out, *schema, static_row);
        for (const auto& rt: partition.row_tombstones()) {
            auto prefix = composite::from_clustering_element(*schema, rt.prefix());
            write_range_tombstone(out, prefix, {}, rt.t());
        }

        // Write all CQL rows from a given mutation partition.
        for (auto& clustered_row: partition.clustered_rows()) {
            write_clustered_row(out, *schema, clustered_row);
        }
        int16_t end_of_row = 0;
        write(out, end_of_row);

        // compute size of the current row.
        _c_stats.row_size = out.offset() - _c_stats.start_offset;
        // update is about merging column_stats with the data being stored by collector.
        _collector.update(std::move(_c_stats));
        _c_stats.reset();

        if (!first_key) {
            first_key = std::move(partition_key);
        } else {
            last_key = std::move(partition_key);
        }

    }
    seal_summary(_summary, std::move(first_key), std::move(last_key), *schema);

    index->close().get();
    _index_file = file(); // index->close() closed _index_file

    _components.insert(component_type::TOC);
    _components.insert(component_type::Statistics);
    _components.insert(component_type::Digest);
    _components.insert(component_type::Index);
    _components.insert(component_type::Summary);
    _components.insert(component_type::Data);

    if (has_component(sstable::component_type::CompressionInfo)) {
        _collector.add_compression_ratio(_compression.compressed_file_length(), _compression.uncompressed_file_length());
    }

    // NOTE: Cassandra gets partition name by calling getClass().getCanonicalName() on
    // partition class.
    seal_statistics(_statistics, _collector, dht::global_partitioner().name(), filter_fp_chance);
}

void sstable::prepare_write_components(::mutation_reader mr, uint64_t estimated_partitions, schema_ptr schema) {
    // CRC component must only be present when compression isn't enabled.
    bool checksum_file = schema->get_compressor_params().get_compressor() == compressor::none;

    if (checksum_file) {
        auto w = make_shared<checksummed_file_writer>(_data_file, sstable_buffer_size, checksum_file);
        _components.insert(component_type::CRC);
        this->do_write_components(std::move(mr), estimated_partitions, std::move(schema), *w);
        w->close().get();
        _data_file = file(); // w->close() closed _data_file

        write_digest(filename(sstable::component_type::Digest), w->full_checksum());
        write_crc(filename(sstable::component_type::CRC), w->finalize_checksum());
    } else {
        prepare_compression(_compression, *schema);
        auto w = make_shared<file_writer>(make_compressed_file_output_stream(_data_file, &_compression));
        _components.insert(component_type::CompressionInfo);
        this->do_write_components(std::move(mr), estimated_partitions, std::move(schema), *w);
        w->close().get();
        _data_file = file(); // w->close() closed _data_file

        write_digest(filename(sstable::component_type::Digest), _compression.full_checksum());
    }
}

future<> sstable::write_components(const memtable& mt) {
    return write_components(mt.make_reader(),
            mt.partition_count(), mt.schema());
}

future<> sstable::write_components(::mutation_reader mr,
        uint64_t estimated_partitions, schema_ptr schema) {
    return seastar::async([this, mr = std::move(mr), estimated_partitions, schema = std::move(schema)] () mutable {
        touch_directory(_dir).get();
        create_data().get();
        prepare_write_components(std::move(mr), estimated_partitions, std::move(schema));
        write_summary();
        write_filter();
        write_statistics();
        // NOTE: write_compression means maybe_write_compression.
        write_compression();
        write_toc();
    });
}

uint64_t sstable::data_size() {
    if (has_component(sstable::component_type::CompressionInfo)) {
        return _compression.data_len;
    }
    return _data_file_size;
}

future<uint64_t> sstable::bytes_on_disk() {
    if (_bytes_on_disk) {
        return make_ready_future<uint64_t>(_bytes_on_disk);
    }
    return do_for_each(_components, [this] (component_type c) {
        return engine().file_size(filename(c)).then([this] (uint64_t bytes) {
            _bytes_on_disk += bytes;
        });
    }).then([this] {
        return make_ready_future<uint64_t>(_bytes_on_disk);
    });
}

const bool sstable::has_component(component_type f) {
    return _components.count(f);
}

const sstring sstable::filename(component_type f) {
    return filename(_dir, _ks, _cf, _version, _generation, _format, f);
}

const sstring sstable::temporary_filename(component_type f) {
    return filename(_dir, _ks, _cf, _version, _generation, _format, f, true);
}

const sstring sstable::filename(sstring dir, sstring ks, sstring cf, version_types version, unsigned long generation,
                                format_types format, component_type component, bool temporary) {

    static std::unordered_map<version_types, std::function<sstring (entry_descriptor d)>, enum_hash<version_types>> strmap = {
        { sstable::version_types::ka, [] (entry_descriptor d) {
            return d.ks + "-" + d.cf + "-" + _version_string.at(d.version) + "-" + to_sstring(d.generation) + "-" + _component_map.at(d.component); }
        },
        { sstable::version_types::la, [] (entry_descriptor d) {
            return _version_string.at(d.version) + "-" + to_sstring(d.generation) + "-" + _format_string.at(d.format) + "-" + _component_map.at(d.component); }
        }
    };

    if (temporary) {
        return dir + "/tmp-" + strmap[version](entry_descriptor(ks, cf, version, generation, format, component));
    } else {
        return dir + "/" + strmap[version](entry_descriptor(ks, cf, version, generation, format, component));
    }
}

entry_descriptor entry_descriptor::make_descriptor(sstring fname) {
    static std::regex la("la-(\\d+)-(\\w+)-(.*)");
    static std::regex ka("(\\w+)-(\\w+)-ka-(\\d+)-(.*)");

    std::smatch match;

    sstable::version_types version;

    sstring generation;
    sstring format;
    sstring component;
    sstring ks;
    sstring cf;

    std::string s(fname);
    if (std::regex_match(s, match, la)) {
        sstring ks = "";
        sstring cf = "";
        version = sstable::version_types::la;
        generation = match[1].str();
        format = sstring(match[2].str());
        component = sstring(match[3].str());
    } else if (std::regex_match(s, match, ka)) {
        ks = match[1].str();
        cf = match[2].str();
        version = sstable::version_types::ka;
        format = sstring("big");
        generation = match[3].str();
        component = sstring(match[4].str());
    } else {
        throw malformed_sstable_exception("invalid version");
    }
    return entry_descriptor(ks, cf, version, boost::lexical_cast<unsigned long>(generation), sstable::format_from_sstring(format), sstable::component_from_sstring(component));
}

sstable::version_types sstable::version_from_sstring(sstring &s) {
    return reverse_map(s, _version_string);
}

sstable::format_types sstable::format_from_sstring(sstring &s) {
    return reverse_map(s, _format_string);
}

sstable::component_type sstable::component_from_sstring(sstring &s) {
    return reverse_map(s, _component_map);
}

input_stream<char> sstable::data_stream_at(uint64_t pos, uint64_t buf_size) {
    if (_compression) {
        return make_compressed_file_input_stream(
                _data_file, &_compression, pos);
    } else {
        return make_file_input_stream(_data_file, pos, buf_size);
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

partition_key
sstable::get_first_partition_key(const schema& s) const {
    return key::from_bytes(_summary.first_key.value).to_partition_key(s);
}

partition_key
sstable::get_last_partition_key(const schema& s) const {
    return key::from_bytes(_summary.last_key.value).to_partition_key(s);
}

sstable::~sstable() {
    if (_index_file) {
        _index_file.close().handle_exception([save = _index_file] (auto ep) {
            sstlog.warn("sstable close index_file failed: {}", ep);
        });
    }
    if (_data_file) {
        _data_file.close().handle_exception([save = _data_file] (auto ep) {
            sstlog.warn("sstable close data_file failed: {}", ep);
        });
    }

    if (_marked_for_deletion) {
        // We need to delete the on-disk files for this table. Since this is a
        // destructor, we can't wait for this to finish, or return any errors,
        // but just need to do our best. If a deletion fails for some reason we
        // log and ignore this failure, because on startup we'll again try to
        // clean up unused sstables, and because we'll never reuse the same
        // generation number anyway.
        try {
            for (auto component : _components) {
                remove_file(filename(component)).handle_exception(
                        [] (std::exception_ptr eptr) {
                            sstlog.warn("Exception when deleting sstable file: {}", eptr);
                        });
            }
        } catch (...) {
            sstlog.warn("Exception when deleting sstable file: {}", std::current_exception());
        }

    }
}

}
